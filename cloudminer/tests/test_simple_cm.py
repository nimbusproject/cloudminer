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
        self.nodeid = "nodeid1"
        self.service_type = "iaasid1"
        self.parent = None
        self.hostname = "localhost"
        self.runlogdir = ""
        self.vmlogdir = ""

    def tearDown(self):
        pass
    
    def test_simple_insert_with_extra_none(self):
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), None)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

    def test_simple_insert_with_extra(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

    def test_simple_insert_with_extra_empty(self):
        extras = {}
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

    def test_simple_query(self):
        extras = {}
        key = "key1"
        source = "src1"
        name = "name1"
        extras['hi'] = 'there'
        cye = CYvent(source, name, key, datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

        rc = self.cdb.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 1)
        self.assertEqual(rc[0].source, source)
        self.assertEqual(rc[0].unique_event_key, key)
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
        iaas2 = "iaas2"
        nodeid2 = "nodeid2"
        extras['hi'] = 'there'
        cye = CYvent(source, name, key, datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        cye = CYvent(source2, name2, key2, datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(run2, iaas2, nodeid2, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

        rc = self.cdb.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 1, "len is %d should be 0" % (len(rc)))
        self.assertEqual(rc[0].source, source)
        self.assertEqual(rc[0].unique_event_key, key)
        self.assertEqual(rc[0].name, name)

        rc = self.cdb.get_events_by_runname(run2)
        self.assertEqual(len(rc), 1)
        self.assertEqual(rc[0].source, source2)
        self.assertEqual(rc[0].unique_event_key, key2)
        self.assertEqual(rc[0].name, name2)

    def test_multiply_cms_simple(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

        cdb2 = CloudMiner('sqlite:///:memory:')
        cdb2.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        cdb2.commit()

    def test_multiply_cms_simple(self):
        extras = {}
        extras['hi'] = 'there'
        cye = CYvent('src1', 'name1', 'key', datetime.datetime.now(), extras)
        self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

        cdb2 = CloudMiner('sqlite:///:memory:')
        cdb2.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        cdb2.commit()

        rc = self.cdb.get_events_by_runname(self.runname)
        self.assertEqual(len(rc), 1)
        self.assertEqual(rc[0].source, "src1")
        self.assertEqual(rc[0].unique_event_key, 'key')
        self.assertEqual(rc[0].name, 'name1')

    def test_get_by_type(self):
        extras = {}

        e_count = 10
        for i in range(0, e_count):
            cye = CYvent('src1', 'name%d' % (i), 'key%d' % (i), datetime.datetime.now(), extras)
            self.cdb.add_cloudyvent(self.runname, self.iaasid, self.nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)

        self.cdb.commit()

        cyvm_a = self.cdb.get_vms_by_type(self.runname, self.service_type)
        self.assertEqual(len(cyvm_a), 1)
        vm = cyvm_a[0]
        self.assertEqual(len(vm.events), e_count, "%d should equal %d" % (len(vm.events), e_count))
        for e in vm.events:
            self.assertEqual(e.name[:4], "name")


