'''Database SQLAlchemy model.'''

from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Table, Column, ForeignKey, Index
from sqlalchemy.sql.sqltypes import Integer, Enum, Boolean, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import cast
import sqlalchemy.ext.declarative

from wpull.item import Status, URLRecord, LinkType


DBBase = sqlalchemy.ext.declarative.declarative_base()


urls_processes_table = Table('urls_processes', DBBase.metadata,
    Column('id', Integer, primary_key = True, autoincrement = True),
    Column('url_id', Integer, ForeignKey('urls.id')),
    Column('process_id', Integer, ForeignKey('processes.id'))
)


class URL(DBBase):
    '''URL table containing each URL to be downloaded.'''
    __tablename__ = 'urls'

    id = Column(Integer, primary_key=True, autoincrement=True)

    url = Column(String, nullable=False)

    status = Column(
        Enum(
            Status.done, Status.error, Status.in_progress,
            Status.skipped, Status.todo, name = 'status'
        ),
        index=True,
        default=Status.todo,
        nullable=False,
        doc='Status of the completion of the item.'
    )
    try_count = Column(
        Integer, nullable=False, default=0,
        doc='Number of attempts made in order to process the item.'
    )
    level = Column(
        Integer, nullable=False, default=0,
        doc='Recursive depth of the item. 0 is root, 1 is child of root, etc.'
    )

    top_url_id = Column(
        Integer, ForeignKey('urls.id'),
        doc='Root URL.'
    )
    top_url_record = relationship(
        'URL', uselist=False, foreign_keys=[top_url_id])
    top_url = association_proxy('top_url_record', 'url')

    status_code = Column(Integer, doc='HTTP status code or FTP rely code.')

    referrer_id = Column(
        Integer, ForeignKey('urls.id'),
        doc='Parent item of this item.'
    )
    referrer_record = relationship(
        'URL', uselist=False, foreign_keys=[referrer_id])
    referrer = association_proxy('referrer_record', 'url')
    inline = Column(
        Integer,
        doc='Depth of the page requisite object. '
            '0 is the object, 1 is the object\'s dependency, etc.'
    )
    link_type = Column(
        Enum(
            LinkType.html, LinkType.css, LinkType.javascript, LinkType.media,
            LinkType.sitemap, LinkType.file, LinkType.directory, name = 'link_type'
        ),
        doc='Expected content type of extracted link.'
    )
    post_data = Column(String, doc='Additional percent-encoded data for POST.')
    filename = Column(String, doc='Local filename of the item.')

    processes = relationship('Process', secondary = urls_processes_table)

    def to_plain(self):
        return URLRecord(
            self.url,
            self.status,
            self.try_count,
            self.level,
            self.top_url,
            self.status_code,
            self.referrer,
            self.inline,
            self.link_type,
            self.post_data,
            self.filename,
        )

Index('idx_url_md5', cast(func.md5(URL.url), UUID), unique = True)


class Process(DBBase):
    __tablename__ = 'processes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)


class Visit(DBBase):
    '''Standalone table for ``--cdx-dedup`` feature.'''
    __tablename__ = 'visits'

    url = Column(String, primary_key=True, nullable=False)
    warc_id = Column(String, nullable=False)
    payload_digest = Column(String, nullable=False)


__all__ = ('DBBase', 'URL', 'Visit')
