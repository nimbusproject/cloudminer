import datetime
import uuid
import simplejson as json
import sqlalchemy
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relation, backref
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table
from sqlalchemy import Integer
from sqlalchemy import String, MetaData, Sequence
from sqlalchemy import Column
import cloudyvents.cyvents as cyvents

class _CYventExtra(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

class _CYvent(object):
    """Convenience class for a parsed event.
    """

    def __init__(self, runname, iaasid, source, name, key, timestamp, extra):

        self.runname = runname
        self.iaasid = iaasid
        self.source = source
        self.name = name
        self.key = key
        self.timestamp = timestamp
        self.extra = extra

metadata = MetaData()
event_table = Table('events', metadata,
    Column('id', Integer, Sequence('event_id_seq'), primary_key=True),
    Column('source', String(50)),
    Column('name', String(50)),
    Column('key', String(50)),
    Column('timestamp', sqlalchemy.types.Time),
    Column('runname', String(50)),
    Column('iaasid', String(50)),
    )

xtra_table = Table('extras', metadata,
    Column('id', Integer, Sequence('extra_id_seq'), primary_key=True),
    Column('key', String(50)),
    Column('value', String(50)),
    Column('event_id', Integer, ForeignKey('events.id'))
    )

mapper(_CYventExtra, xtra_table)
mapper(_CYvent, event_table, properties={
    'extra': relation(_CYventExtra)})


class CloudMiner(object):

    def __init__(self, dburl, module=None):
        if module == None:
            self.engine = sqlalchemy.create_engine(dburl)
        else:
            self.engine = sqlalchemy.create_engine(dburl, module=module)
        metadata.create_all(self.engine) 
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def add_cloudyvent(self, runname, iaasid, cyv):
        xtras_list = []
        if cyv.extra != None:
            for k in cyv.extra.keys():
                e = _CYventExtra(k, cyv.extra[k])
                xtras_list.append(e)

        _cyv = _CYvent(runname, iaasid, cyv.source, cyv.name, cyv.key, cyv.timestamp, xtras_list)
        self.session.add(_cyv)

    def get_events_by_runname(self, runname):
        return self.session.query(_CYvent).filter(_CYvent.runname == runname).all()

    def commit(self):
        self.session.commit()

