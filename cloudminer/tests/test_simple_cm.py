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

class CloudMinerTestCase(unittest.TestCase):
    
    def setUp(self):
        self.cdb = CloudMiner('sqlite:///:memory:')
        self.runname = "run1"
        self.iaasid = "iaasid1"

    def tearDown(self):
        pass
    
    def test_simple_insert_with_extra_none(self):
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), None)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, cye)
        self.cdb.commit()

    def test_simple_insert_with_extra(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, cye)
        self.cdb.commit()

    def test_simple_insert_with_extra_empty(self):
        extras = {}
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, cye)
        self.cdb.commit()

    def test_simple_query(self):
        extras = {}
        key = "key1"
        source = "src1"
        name = "name1"
        extras['hi'] = 'there'
        cye = CYvent(source, name, key, datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, cye)
        self.cdb.commit()

        rc = self.cdb.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 1)
        self.assertEqual(rc[0].source, source)
        self.assertEqual(rc[0].key, key)
        self.assertEqual(rc[0].name, name)

    def test_man_run_query(self):
        run2 = str(uuid.uuid1())

        extras = {}
        key = "key1"
        source = "src1"
        name = "name1"
        key2 = "key2"
        source2 = "src2"
        name2 = "name2"
        extras['hi'] = 'there'
        cye = CYvent(source, name, key, datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, cye)
        cye = CYvent(source2, name2, key2, datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(run2, self.iaasid, cye)
        self.cdb.commit()

        rc = self.cdb.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 1, "len is %d should be 0" % (len(rc)))
        self.assertEqual(rc[0].source, source)
        self.assertEqual(rc[0].key, key)
        self.assertEqual(rc[0].name, name)

        rc = self.cdb.get_events_by_runname(run2)
        self.assertEqual(len(rc), 1)
        self.assertEqual(rc[0].source, source2)
        self.assertEqual(rc[0].key, key2)
        self.assertEqual(rc[0].name, name2)

    def test_multiply_cms_simple(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, cye)
        self.cdb.commit()

        cdb2 = CloudMiner('sqlite:///:memory:')
        cdb2.add_cloudyvent(self.runname, self.iaasid, cye)
        cdb2.commit()

    def test_multiply_cms_simple(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, cye)
        self.cdb.commit()

        cdb2 = CloudMiner('sqlite:///:memory:')
        cdb2.add_cloudyvent(self.runname, self.iaasid, cye)
        cdb2.commit()

        rc = self.cdb.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 1)
        self.assertEqual(rc[0].source, "src1")
        self.assertEqual(rc[0].key, 'key')
        self.assertEqual(rc[0].name, 'name1')

