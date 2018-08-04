'''SQLAlchemy table implementations.'''
import abc
import collections
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
from sqlalchemy.orm import joinedload
import sqlalchemy.event

from wpull.database.base import BaseURLTable, NotFound
from wpull.database.sqlmodel import Process, URL, Visit, DBBase, urls_processes_table
from wpull.item import Status


_logger = logging.getLogger(__name__)


class BaseSQLURLTable(BaseURLTable):
    def __init__(self, process_name = None):
        if process_name is None:
            process_name = '{}-{}'.format(socket.gethostname(), os.getpid())
        self._process_name = process_name
        self._process_id = None # process ID

        self._url_block = collections.deque() # deque of URLRecord objects reserved by this process and not checked out yet
        self._url_block_status = None
        self._url_block_url_index = {} # dict of URL.url -> URL.id
        self._url_block_changes = {} # dict of URL.id -> (dict of column -> new value)
        self._url_block_remaining_counter = 0

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
            # TODO: Optimise: referrer has to be from the current URL block, and the top URLs can be cached locally (lru cache or something)
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
                # TODO: This executes one query per URL, I think.
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
        # TODO: Add a similar check for the level; however, wpull actually doesn't use the level argument of this method anywhere currently.
        assert level is None, 'level != None currently not supported'
        if self._url_block_status is not None and filter_status != self._url_block_status:
            raise NotFound()

        if len(self._url_block) == 0 and self._url_block_remaining_counter > 0:
            raise NotFound()
        if len(self._url_block) == 0:
            assert len(self._url_block_url_index) == 0
            assert len(self._url_block_changes) == 0

            # Reserve up to 1000 URLs from the DB
            with self._session() as session:
                # Get our process object
                if self._process_id is None:
                    process = Process(name = self._process_name)
                    session.add(process)
                    session.flush()
                    self._process_id = process.id

                q = session.query(URL).filter_by(status = filter_status).limit(1000)
                if session.bind.dialect.name == 'postgresql':
                    q = q.with_for_update(skip_locked = True)
                urls = q.all()

                if not urls:
                    # No more URLs (with this status) in the DB
                    raise NotFound()

                # Mark the URLs as in_progress
                session.query(URL).filter(URL.id.in_([url.id for url in urls])).update({URL.status: Status.in_progress}, synchronize_session = False)
                # TODO: This runs an individual query for each URL  - https://stackoverflow.com/a/8034650 https://github.com/pandas-dev/pandas/issues/8953
                #   http://docs.sqlalchemy.org/en/latest/core/tutorial.html#executing-multiple-statements
                #session.execute(insert(urls_processes_table), [{'url_id': url.id, 'process_id': self._process_id} for url in urls])
                session.execute(insert(urls_processes_table).values([{'url_id': url.id, 'process_id': self._process_id} for url in urls]))

                # Due to synchronize_session in the UPDATE above, the status variable is not updated in the url object, so we need to override it explicitly.
                url_records = [url.to_plain(status_override = Status.in_progress) for url in urls]
                url_index = {url.url: url.id for url in urls}

            # Set block variables
            self._url_block.extend(url_records)
            self._url_block_status = filter_status
            self._url_block_url_index = url_index
            self._url_block_remaining_counter = len(url_records)

        # Pop a URL from the block and return it
        return self._url_block.popleft()

    def check_in(self, url, new_status, increment_try_count=True, **kwargs):
        url_id = self._url_block_url_index[url]

        # Build changes dictionary and store it
        changes = {getattr(URL, key): value for key, value in dict(status = new_status, **kwargs).items()}
        if increment_try_count:
            changes[URL.try_count] = URL.try_count + 1
        if url_id in self._url_block_changes:
            self._url_block_changes[url_id].update(changes)
        else:
            self._url_block_changes[url_id] = changes

        self._url_block_remaining_counter -= 1

        if self._url_block_remaining_counter == 0:
            assert len(self._url_block) == 0

            # All URLs in the block are done; check them back in
            # TODO: Optimise - https://stackoverflow.com/a/18799497 https://gist.github.com/doobeh/b16e800cdd51d6413c09
            #   http://docs.sqlalchemy.org/en/latest/core/tutorial.html#inserts-updates-and-deletes
            #   https://stackoverflow.com/a/45152661
            with self._session() as session:
                for id in self._url_block_changes:
                    query = update(URL).values(self._url_block_changes[id]).where(URL.id == id)
                    session.execute(query)

            # Empty block variables
            self._url_block_url_index = {}
            self._url_block_changes = {}
            self._url_block_status = None

    def update_one(self, url, **kwargs):
        # Only called when url is currently in_progress, so we can just add to the changes dict instead of writing to the DB immediately
        url_id = self._url_block_url_index[url]
        if url_id not in self._url_block_changes:
            self._url_block_changes[url_id] = {}
        self._url_block_changes[url_id].update({getattr(URL, key): value for key, value in kwargs.items()})

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
