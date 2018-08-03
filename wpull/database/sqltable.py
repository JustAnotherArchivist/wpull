'''SQLAlchemy table implementations.'''
import abc
import contextlib
import logging
import urllib.parse
import os
import socket

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy.sql.expression import insert, update, select, and_, delete, \
    bindparam, cast
from sqlalchemy.dialects.postgresql import insert as insert_pgsql
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql.functions import func
import sqlalchemy.event

from wpull.database.base import BaseURLTable, NotFound
from wpull.database.sqlmodel import Process, URL, Visit, DBBase
from wpull.item import Status


_logger = logging.getLogger(__name__)


class BaseSQLURLTable(BaseURLTable):
    def __init__(self, process_name = None):
        if process_name is None:
            process_name = '{}-{}'.format(socket.gethostname(), os.getpid())
        self._process_name = process_name
        self._process = None # Process object

    @abc.abstractproperty
    def _session_maker(self):
        pass

    @contextlib.contextmanager
    def _session(self):
        """Provide a transactional scope around a series of operations."""
        # Taken from the session docs.
        session = self._session_maker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def count(self):
        with self._session() as session:
            return session.query(URL).count()

    def get_one(self, url):
        with self._session() as session:
            result = session.query(URL).filter(cast(func.md5(URL.url), UUID) == cast(func.md5(url), UUID)).first()

            if not result:
                raise NotFound()
            else:
                return result.to_plain()

    def get_all(self):
        with self._session() as session:
            for item in session.query(URL):
                yield item.to_plain()

    def add_many(self, new_urls, **kwargs):
        assert not isinstance(new_urls, (str, bytes)), \
            'Expected a list-like. Got {}'.format(new_urls)
        referrer = kwargs.pop('referrer', None)
        top_url = kwargs.pop('top_url', None)

        new_urls = tuple(new_urls)

        if not new_urls:
            return ()

        assert isinstance(new_urls[0], dict), type(new_urls[0])
        assert all('url' in item and 'referrer' not in item and 'top_url' not in item for item in new_urls)

        with self._session() as session:
            # Get IDs of referrer and top_url; we know that these must already exist in the DB,
            # and they can't get modified currently (referrer will be in_progress by the current item,
            # and top_url will be done already), so this is safe.
            if referrer:
                referrer_id = session.query(URL).filter(cast(func.md5(URL.url), UUID) == cast(func.md5(referrer), UUID)).first().id
            else:
                referrer_id = None
            if top_url:
                top_url_id = session.query(URL).filter(cast(func.md5(URL.url), UUID) == cast(func.md5(top_url), UUID)).first().id
            else:
                top_url_id = None

            bind_values = dict(status = Status.todo, referrer_id = referrer_id, top_url_id = top_url_id)
            bind_values.update(**kwargs)

            if session.bind.dialect.name == 'postgresql':
                query = insert_pgsql(URL).on_conflict_do_nothing().values(bind_values)
            else:
                query = insert(URL).prefix_with('OR IGNORE').values(bind_values)

            if session.bind.dialect.name == 'postgresql':
                # In PostgreSQL with concurrent transactions, we need to insert the URLs in the a consistent order.
                # If we don't, we will end in a deadlock eventually, where transaction 1 writes URL 1, then
                # transaction 2 writes URL 2 and attempts to write URL 1, then transaction 1 attempts to write URL 2.
                # In that scenario, both transactions block each other.
                # The easiest way to avoid this is to ensure that URLs are always inserted in a specific order.
                # A deadlock as described above then becomes impossible.
                # Cf. https://dba.stackexchange.com/a/195220
                new_urls = sorted(new_urls, key = lambda x: x['url'])
                session.execute(query, new_urls)
                # TODO: Return inserted rows
                return []
            else:
                last_primary_key = session.query(func.max(URL.id)).scalar() or 0

                session.execute(query, new_urls)

                query = select([URL.url]).where(URL.id > last_primary_key)
                added_urls = [row[0] for row in session.execute(query)]

        return added_urls

    def check_out(self, filter_status, level=None):
        with self._session() as session:
            if self._process is None:
                self._process = Process(name = self._process_name)
                session.add(self._process)

            if level is None:
                q = session.query(URL).filter_by(
                    status=filter_status)
                if session.bind.dialect.name == 'postgresql':
                    q = q.with_for_update(skip_locked = True)
                url_record = q.first()
            else:
                q = session.query(URL)\
                    .filter(
                        URL.status == filter_status,
                        URL.level < level,
                )
                if session.bind.dialect.name == 'postgresql':
                    q = q.with_for_update(skip_locked = True)
                url_record = q.first()

            if not url_record:
                raise NotFound()

            url_record.status = Status.in_progress
            url_record.processes.append(self._process)

            return url_record.to_plain()

    def check_in(self, url, new_status, increment_try_count=True, **kwargs):
        with self._session() as session:
            values = {
                URL.status: new_status
            }

            for key, value in kwargs.items():
                values[getattr(URL, key)] = value

            if increment_try_count:
                values[URL.try_count] = URL.try_count + 1

            query = update(URL).values(values).where(cast(func.md5(URL.url), UUID) == cast(func.md5(url), UUID))

            session.execute(query)

    def update_one(self, url, **kwargs):
        with self._session() as session:
            values = {}

            for key, value in kwargs.items():
                values[getattr(URL, key)] = value

            query = update(URL).values(values).where(cast(func.md5(URL.url), UUID) == cast(func.md5(url), UUID))

            session.execute(query)

    def release(self):
        with self._session() as session:
            query = update(URL).values({URL.status: Status.todo})\
                .where(URL.status==Status.in_progress)
            session.execute(query)

    def remove_many(self, urls):
        assert not isinstance(urls, (str, bytes)), \
            'Expected list-like. Got {}.'.format(urls)

        with self._session() as session:
            for url in urls:
                query = delete(URL).where(cast(func.md5(URL.url), UUID) == cast(func.md5(url), UUID))
                session.execute(query)

    def add_visits(self, visits):
        with self._session() as session:
            for url, warc_id, payload_digest in visits:
                session.execute(
                    insert_pgsql(Visit).on_conflict_do_nothing() if session.bind.dialect.name == 'postgresql' else insert(Visit).prefix_with('OR IGNORE'),
                    dict(
                        url=url,
                        warc_id=warc_id,
                        payload_digest=payload_digest
                    )
                )

    def get_revisit_id(self, url, payload_digest):
        query = select([Visit.warc_id]).where(
            and_(
                Visit.url == url,
                Visit.payload_digest == payload_digest
            )
        )

        with self._session() as session:
            row = session.execute(query).first()

            if row:
                return row.warc_id


class SQLiteURLTable(BaseSQLURLTable):
    '''URL table with SQLite storage.

    Args:
        path: A SQLite filename
    '''
    def __init__(self, path=':memory:', **kwargs):
        super().__init__(**kwargs)
        # We use a SingletonThreadPool always because we are using WAL
        # and want SQLite to handle the checkpoints. Otherwise NullPool
        # will open and close the connection rapidly, defeating the purpose
        # of WAL.
        escaped_path = path.replace('?', '_')
        self._engine = create_engine(
            'sqlite:///{0}'.format(escaped_path), poolclass=SingletonThreadPool)
        sqlalchemy.event.listen(
            self._engine, 'connect', self._apply_pragmas_callback)
        DBBase.metadata.create_all(self._engine)
        self._session_maker_instance = sessionmaker(bind=self._engine)

    @classmethod
    def _apply_pragmas_callback(cls, connection, record):
        '''Set SQLite pragmas.

        Write-ahead logging, synchronous=NORMAL is used.
        '''
        _logger.debug('Setting pragmas.')
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('PRAGMA synchronous=NORMAL')

    @property
    def _session_maker(self):
        return self._session_maker_instance

    def close(self):
        self._engine.dispose()


class GenericSQLURLTable(BaseSQLURLTable):
    '''URL table using SQLAlchemy without any customizations.

    Args:
        url: A SQLAlchemy database URL.
    '''
    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self._engine = create_engine(url)
        DBBase.metadata.create_all(self._engine)
        self._session_maker_instance = sessionmaker(bind=self._engine)

    @property
    def _session_maker(self):
        return self._session_maker_instance

    def close(self):
        self._engine.dispose()


URLTable = SQLiteURLTable
'''The default URL table implementation.'''


__all__ = (
    'BaseSQLURLTable', 'SQLiteURLTable', 'GenericSQLURLTable', 'URLTable'
)
