import unittest
import datetime
import logging
import os
import shutil
import uuid
import tempfile
import time
import cloudyvents.cyvents as cyvents
from cloudminer import CloudMiner
from cloudyvents.cyvents import CYvent
import tempfile

# Set this to False to look at generated log files afterwards.  There will be
# many directories like /tmp/cytestlog*
DESTROY_LOGDIR = True

logger = logging.getLogger(__name__)

class CloudMinerRealFileTestCase(unittest.TestCase):
    
    def setUp(self):
        (osf, self.filename) = tempfile.mkstemp()
        os.close(osf)
        print self.filename
        self.cdb = CloudMiner('sqlite:///' + self.filename)
        self.runname = "runitk"
        self.iaasid = "iceicebaby"
        self.nodeid = "toocold"
        self.parent = None
        self.service_type = "iaasid1"
        self.hostname = "localhost"
        self.runlogdir = ""
        self.vmlogdir = ""


    def tearDown(self):
        os.remove(self.filename)
        pass
   
    def test_file_db(self):
        extras = {}
        key = "key1"
        source = "src1"
        name = "name1"
        extras['hi'] = 'there'
        cye = CYvent(source, name, key, datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

    def test_multiply_cms_simple(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

        cye = CYvent('src1', 'name1', 'key2', datetime.datetime.now(), extras)
        cdb2 = CloudMiner('sqlite:///' + self.filename)
        cdb2.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        cdb2.commit()

        rc = self.cdb.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 2, "len is %d and should be %d" % (len(rc), 2))
        self.assertEqual(rc[0].source, "src1")
        self.assertEqual(rc[0].unique_event_key, 'key')
        self.assertEqual(rc[0].name, 'name1')

    def test_multiple_cms_commit(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)

        cdb2 = CloudMiner('sqlite:///' + self.filename)

        rc = cdb2.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 0)

        self.cdb.commit()
        cdb2.commit()
        rc = cdb2.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 1)
        self.assertEqual(rc[0].source, "src1")
        self.assertEqual(rc[0].unique_event_key, 'key')
        self.assertEqual(rc[0].name, 'name1')
