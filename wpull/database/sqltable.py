'''SQLAlchemy table implementations.'''
import abc
import collections
import contextlib
import logging
import operator
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

import psycopg2
import psycopg2.extras

from wpull.database.base import BaseURLTable, NotFound
from wpull.database.sqlmodel import Process, URL, Visit, DBBase, urls_processes_table
from wpull.item import Status, URLRecord, LinkType


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

    def has_in_progress_items(self):
        with self._session() as session:
            return session.query(URL).filter(status == Status.in_progress).count() > 0

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


class PostgreSQLURLTable(BaseURLTable):
    def __init__(self, url, process_name = None):
        self._connection = psycopg2.connect(url, connection_factory = psycopg2.extras.LoggingConnection)
        self._connection.initialize(_logger)

        # Create tables if necessary
        with self._cursor() as cursor:
            cursor.execute('SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s);', ('public', 'urls'))
            if not cursor.fetchone()[0]:
                cursor.execute('CREATE TYPE status AS ENUM %s', ((Status.done, Status.error, Status.in_progress, Status.skipped, Status.todo),))
                cursor.execute('CREATE TYPE link_type AS ENUM %s', ((LinkType.html, LinkType.css, LinkType.javascript, LinkType.media,
                    LinkType.sitemap, LinkType.file, LinkType.directory),))
                cursor.execute('''CREATE TABLE urls (
                      id SERIAL NOT NULL,
                      url VARCHAR NOT NULL,
                      status status NOT NULL DEFAULT %s,
                      try_count INTEGER NOT NULL DEFAULT 0,
                      level INTEGER NOT NULL DEFAULT 0,
                      top_url_id INTEGER,
                      status_code INTEGER,
                      referrer_id INTEGER,
                      inline INTEGER,
                      link_type link_type,
                      post_data VARCHAR,
                      filename VARCHAR,
                      PRIMARY KEY (id),
                      FOREIGN KEY(top_url_id) REFERENCES urls (id),
                      FOREIGN KEY(referrer_id) REFERENCES urls (id)
                    )''', (Status.todo,))
                cursor.execute('CREATE INDEX ix_urls_status ON urls (status)')
                cursor.execute('CREATE UNIQUE INDEX ix_urls_url ON urls ((md5(url)::uuid))')
                cursor.execute('CREATE TABLE processes (id SERIAL NOT NULL, name VARCHAR, PRIMARY KEY (id))')
                cursor.execute('''CREATE TABLE urls_processes (
                      id SERIAL NOT NULL,
                      url_id INTEGER,
                      process_id INTEGER,
                      PRIMARY KEY (id),
                      FOREIGN KEY(url_id) REFERENCES urls (id),
                      FOREIGN KEY(process_id) REFERENCES processes (id)
                    )''')
                cursor.execute('''CREATE TABLE visits (
                      url VARCHAR NOT NULL,
                      warc_id VARCHAR NOT NULL,
                      payload_digest VARCHAR NOT NULL,
                      PRIMARY KEY (url)
                    )''')
                # TODO: The visits table needs some indices.
                cursor.execute('CREATE TYPE ignore_op AS ENUM %s', (('add', 'remove'),))
                cursor.execute('''CREATE TABLE ignore_patterns (
                      id SERIAL NOT NULL,
                      generation INTEGER,
                      op ignore_op,
                      pattern VARCHAR,
                      PRIMARY KEY (id)
                    )''')
                cursor.execute('CREATE INDEX ix_ignore_patterns_generation ON ignore_patterns (generation)')

        # Insert process
        if process_name is None:
            process_name = '{}-{}'.format(socket.gethostname(), os.getpid())
        self._process_name = process_name
        with self._cursor() as cursor:
            cursor.execute('INSERT INTO processes (name) VALUES (%s) RETURNING id', (self._process_name,))
            self._process_id = cursor.fetchone()[0]

        # Define block variables
        self._url_block = collections.deque() # deque of URLRecord objects reserved by this process and not checked out yet
        self._url_block_records = {} # dict of urls.id -> URLRecord
        self._url_block_status = None
        self._url_block_url_index = {} # dict of urls.url -> (urls.id, urls.top_url_id)
        self._url_block_changes = {} # dict of urls.id -> (dict of column -> new value)
        self._url_block_checked_in = set() # set of urls.id
        self._url_block_children = {} # dict of urls.id -> tuple (new_urls: list of dicts, common_values: dict)
        self._url_block_children_column_set = set() # set of column names (str)
        self._url_block_remaining_counter = 0

        # Ignore patterns
        self._mutable_reject_regexes_filter = None
        self._ignore_pattern_generation = -1

    @contextlib.contextmanager
    def _cursor(self, cursor = None):
        # Transactional scope; automatically handles COMMIT and ROLLBACK
        if cursor is not None:
            yield cursor
        else:
            with self._connection:
                with self._connection.cursor() as cursor:
                    yield cursor

    def count(self):
        with self._cursor() as cursor:
            cursor.execute('SELECT COUNT(id) FROM urls')
            return cursor.fetchone()[0]

    def _result_to_url_record(self, result, status_override = None):
        return URLRecord(
            # id
            result[1], # url
            status_override if status_override is not None else result[2], # status
            result[3], # try_count
            result[4], # level
            # top_url_id
            result[6], # top_url
            result[7], # status_code
            result[8], # referrer_url
            result[9], # inline
            result[10], # link_type
            result[11], # post_data
            result[12], # filename
        )

    @contextlib.contextmanager
    def _select_urls(self, statement = '', params = None, cursor = None):
        # statement: string, e.g. 'WHERE urls.id > 1000 AND urls.status = %s ORDER BY urls.id LIMIT 1'
        # params: None | tuple of parameters
        # retval: 'one' (return first result), 'iter' (yield each result), or 'all' (return list of tuples)
        # returns: list of results

        with self._cursor(cursor) as cursor:
            cursor.execute('''SELECT urls.id, urls.url, urls.status, urls.try_count, urls.level,
                urls.top_url_id, urls_top.url AS top_url, urls.status_code, urls_referrer.url AS referrer_url,
                urls.inline, urls.link_type, urls.post_data, urls.filename
                FROM urls
                LEFT JOIN urls AS urls_top ON urls.top_url_id = urls_top.id
                LEFT JOIN urls AS urls_referrer ON urls.referrer_id = urls_referrer.id
                ''' + statement,
                params)
            yield cursor

    def get_one(self, url):
        with self._select_urls('WHERE (md5(urls.url)::uuid) = (md5(%s)::uuid) LIMIT 1', (url,)) as cursor:
            result = cursor.fetchone()

        if not result:
            raise NotFound()
        else:
            return self._result_to_url_record(result)

    def get_all(self):
        with self._select_urls() as cursor:
            for result in cursor:
                yield self._result_to_url_record(result)

    def has_in_progress_items(self):
        with self._cursor() as cursor:
            cursor.execute('SELECT COUNT(id) FROM urls WHERE status = %s', (Status.in_progress,))
            return cursor.fetchone()[0] > 0

    def add_many(self, new_urls, **kwargs):
        # Restriction: referrer and top_url can only be specified together, and top_url must be the top-level URL of the referrer

        #TODO Collect, insert all together at the end of the block?
        assert not isinstance(new_urls, (str, bytes)), \
            'Expected a list-like. Got {}'.format(new_urls)
        referrer = kwargs.pop('referrer', None)
        top_url = kwargs.pop('top_url', None)
        assert (referrer is not None) == (top_url is not None) # Either both or neither

        new_urls = tuple(new_urls)

        if not new_urls:
            return ()

        assert isinstance(new_urls[0], dict), type(new_urls[0])
        assert all('url' in item and 'referrer' not in item and 'top_url' not in item for item in new_urls)

        # Ensure that all new_urls entries share the same columns (together with the kwargs), otherwise a bulk insert isn't possible
        if len(self._url_block_children):
            column_set = self._url_block_children_column_set
            # This requires that all calls to add_many within a block either have referrer/top_url or don't...
            assert bool(referrer) == ('referrer_id' in column_set)
            assert (set(kwargs.keys()) | set(new_urls[0].keys())) | set(('status',)) | (set(('referrer_id', 'top_url_id')) if referrer else set()) == column_set
        else:
            column_set = set(kwargs.keys()) | set(new_urls[0].keys())
            column_set.add('status')
            self._url_block_children_column_set = column_set
            # ... and that the columns are valid
            assert all(key in ('url', 'status', 'try_count', 'level', 'status_code', 'inline', 'link_type', 'post_data', 'filename') for key in column_set)
            # (implicitly also checks that referrer_id and top_url_id aren't specified)
        assert all(set(item.keys()).issubset(column_set) for item in new_urls)
        if referrer:
            column_set.add('referrer_id')
            column_set.add('top_url_id')

        if referrer:
            referrer_id, top_url_id = self._url_block_url_index[referrer]
            assert top_url_id is None or self._url_block_records[referrer_id].top_url == top_url
            if top_url_id is None: # referrer itself is a top-level URL
                top_url_id = referrer_id
        else:
            referrer_id = None # Needed below for _url_block_children

        # Prepare the insert values
        common_values = {'status': Status.todo}
        if referrer:
            common_values['referrer_id'] = referrer_id
            common_values['top_url_id'] = top_url_id
        common_values.update(**kwargs)

        self._url_block_children[referrer_id] = (new_urls, common_values)
        if referrer_id is None:
            # Initial URLs, need to be flushed immediately. There shouldn't be anything in the URL block at that time
            assert len(self._url_block) == 0
            assert len(self._url_block_changes) == 0
            assert len(self._url_block_checked_in) == 0
            self._url_block_checked_in.add(None)
            self._insert_children()
            self._url_block_checked_in = set()
            self._url_block_children = {}
            self._url_block_children_column_set = set()

        # It is impossible to return the inserted URLs because we won't insert them until the end of the block...
        return []

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
            with self._cursor() as cursor:
                with self._select_urls('WHERE urls.status = %s LIMIT 1000 FOR UPDATE OF urls SKIP LOCKED', (filter_status,), cursor = cursor) as cursor:
                    results = cursor.fetchall()

                if not results:
                    # No more URLs (with this status) in the DB
                    raise NotFound()

                # Mark the URLs as in_progress
                cursor.execute('UPDATE urls SET status = %s WHERE id IN %s', (Status.in_progress, tuple(result[0] for result in results)))
                psycopg2.extras.execute_values(cursor, 'INSERT INTO urls_processes (url_id, process_id) VALUES %s', [(result[0], self._process_id) for result in results])

            # Set block variables
            # The status in urls is still the old one, so that needs to be overridden
            self._url_block_records = {result[0]: self._result_to_url_record(result, status_override = Status.in_progress) for result in results}
            self._url_block.extend(self._result_to_url_record(result, status_override = Status.in_progress) for result in results)
            self._url_block_status = filter_status
            self._url_block_url_index = {result[1]: (result[0], result[5]) for result in results}
            self._url_block_remaining_counter = len(results)

            # Check for ignore pattern updates
            if self._mutable_reject_regexes_filter:
                with self._cursor() as cursor:
                    cursor.execute('SELECT generation, op, pattern FROM ignore_patterns WHERE generation > %s', (self._ignore_pattern_generation,))
                    updates = cursor.fetchall()
                if updates:
                    _logger.info('Updating ignore patterns from generation {} to {}'.format(self._ignore_pattern_generation, updates[-1][0]))
                    for generation, op, pattern in updates:
                        if op == 'add':
                            self._mutable_reject_regexes_filter.add(pattern)
                        else:
                            self._mutable_reject_regexes_filter.remove(pattern)
                    self._ignore_pattern_generation = updates[-1][0]

        # Pop a URL from the block and return it
        return self._url_block.popleft()

    def _check_in_block(self):
        # Flush changes for all checked in URLs to the DB
        # This does *not* remove the flushed changes from the URL block variables!

        # Build value table
        # Each row might need different updates, so we need to combine it with the data retrieved on checkout

        # Collect column names
        columns = set() # TODO: Is there a more efficient way to gather all columns?
        for url_id in self._url_block_checked_in:
            for k in self._url_block_changes[url_id].keys():
                columns.add(k)

        # Fill in missing values and id field
        for url_id in self._url_block_checked_in:
            for key in columns:
                if key not in self._url_block_changes[url_id]:
                    self._url_block_changes[url_id][key] = getattr(self._url_block_records[url_id], key)
            self._url_block_changes[url_id]['id'] = url_id

        # Build values list
        columns.add('id')
        sorted_cols = sorted(columns)
        values = [tuple(self._url_block_changes[id][key] for key in sorted_cols) for id in self._url_block_checked_in]

        column_types = {
            'status': 'status',
            'link_type': 'link_type',
            'status_code': 'int',
            'inline': 'int',
        }

        # UPDATE
        with self._cursor() as cursor:
            psycopg2.extras.execute_values(cursor,
                'UPDATE urls SET ' + ', '.join('{} = v.{}{}'.format(column, column, '::{}'.format(column_types[column]) if column in column_types else '')
                                               for column in sorted_cols if column != 'id') + ' '
                'FROM (VALUES %s) AS v (' + ', '.join(column for column in sorted_cols) + ') '
                'WHERE urls.id = v.id',
                values)

        self._insert_children()

    def _insert_children(self):
        if len(self._url_block_checked_in) == 0 or len(self._url_block_children) == 0:
            return

        column_set = self._url_block_children_column_set
        sorted_cols = sorted(column_set)

        values = []
        for url_id in self._url_block_checked_in:
            if url_id in self._url_block_children:
                new_urls, global_values = self._url_block_children[url_id]
                values.extend(tuple(item[key] if key in item else global_values[key] for key in sorted_cols) for item in new_urls)

        # Sort the urls and pre-filter duplicates
        # In PostgreSQL with concurrent transactions, we need to insert the URLs in the a consistent order.
        # If we don't, we will end in a deadlock eventually, where transaction 1 writes URL 1, then
        # transaction 2 writes URL 2 and attempts to write URL 1, then transaction 1 attempts to write URL 2.
        # In that scenario, both transactions block each other.
        # The easiest way to avoid this is to ensure that URLs are always inserted in a specific order.
        # A deadlock as described above then becomes impossible.
        # Cf. https://dba.stackexchange.com/a/195220
        #
        # The pre-filtering of duplicates is merely an optimisation step to reduce traffic to the DB, which is beneficial in high-latency cases.
        # The entry with the lowest referrer_id is kept; if there is no referrer_id field, an arbitrary entry is kept.
        url_index = sorted_cols.index('url')
        if 'referrer_id' in sorted_cols:
            referrer_id_index = sorted_cols.index('referrer_id')
            f = operator.itemgetter(url_index, referrer_id_index)
        else:
            f = operator.itemgetter(url_index)
        values.sort(key = f)

        previous_url = None
        unfiltered_values = values
        values = []
        for row in unfiltered_values:
            if row[url_index] == previous_url:
                continue
            values.append(row)
            previous_url = row[url_index]
        del unfiltered_values

        with self._cursor() as cursor:
            # Insert the URLs
            # Note that it would be cleaner to use psycopg2.sql (https://stackoverflow.com/a/27291545), but we already made sure that all column names are okay, so this is not a security issue.
            query = 'INSERT INTO urls ({}) VALUES %s ON CONFLICT DO NOTHING'.format(', '.join(sorted_cols))
            psycopg2.extras.execute_values(cursor, query, values)

    def check_in(self, url, new_status, increment_try_count=True, **kwargs):
        url_id, _ = self._url_block_url_index[url]

        # TODO: level and inline should probably not be in here, also status not
        assert all(key in ('status', 'level', 'status_code', 'inline', 'link_type', 'post_data', 'filename') for key in kwargs.keys())

        # Build changes dictionary and store it
        changes = {'status': new_status}
        changes.update(**kwargs)
        if increment_try_count:
            if 'try_count' in changes:
                # This probably isn't used, but let's be safe...
                changes['try_count'] += 1
            else:
                changes['try_count'] = self._url_block_records[url_id].try_count + 1
        if url_id in self._url_block_changes:
            self._url_block_changes[url_id].update(changes)
        else:
            self._url_block_changes[url_id] = changes
        self._url_block_checked_in.add(url_id)

        self._url_block_remaining_counter -= 1

        if self._url_block_remaining_counter == 0:
            assert len(self._url_block) == 0
            # All URLs in the block are done; check them back in
            self._check_in_block()

            # Empty block variables
            self._url_block_records = {}
            self._url_block_url_index = {}
            self._url_block_changes = {}
            self._url_block_checked_in = set()
            self._url_block_children = {}
            self._url_block_children_column_set = set()
            self._url_block_status = None

    def update_one(self, url, **kwargs):
        # Only called when url is currently in_progress, so we can just add to the changes dict instead of writing to the DB immediately
        url_id = self._url_block_url_index[url][0]
        if url_id not in self._url_block_changes:
            self._url_block_changes[url_id] = {}
        self._url_block_changes[url_id].update(**kwargs)

    def release(self):
        with self._cursor() as cursor:
            cursor.execute('UPDATE urls SET status = %s WHERE status = %s', (Status.todo, Status.in_progress))

    def remove_many(self, urls):
        assert not isinstance(urls, (str, bytes)), \
            'Expected list-like. Got {}.'.format(urls)

        with self._cursor() as cursor:
            cursor.execute('DELETE FROM urls WHERE (md5(url)::uuid) IN (SELECT (md5(urls)::uuid) FROM UNNEST(%s) AS urls)', urls)

    def add_visits(self, visits):
        with self._cursor() as cursor:
            psycopg2.extras.execute_values(cursor, 'INSERT INTO visits (url, warc_id, payload_digest) VALUES %s ON CONFLICT DO NOTHING', visits)

    def get_revisit_id(self, url, payload_digest):
        with self._cursor() as cursor:
            cursor.execute('SELECT warc_id FROM visits WHERE (md5(url)::uuid) = (md5(%s)::uuid) AND payload_digest = %s LIMIT 1', (url, payload_digest))
            result = cursor.fetchone()
            if result:
                return result[0]

    def set_mutable_reject_regexes_filter(self, filter):
        self._mutable_reject_regexes_filter = filter

    def close(self):
        # Unmark remaining items
        # This implicitly checks in the block at the end of the loop if everything goes right
        for record in self._url_block:
            self.check_in(record.url, self._url_block_status, increment_try_count = False)
        # But if for some reason it didn't get checked in, force it...
        if self._url_block_remaining_counter > 0:
            self._check_in_block()
        self._connection.close()


URLTable = SQLiteURLTable
'''The default URL table implementation.'''


__all__ = (
    'BaseSQLURLTable', 'SQLiteURLTable', 'GenericSQLURLTable', 'PostgreSQLURLTable', 'URLTable'
)
