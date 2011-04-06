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

class CloudIaaSTestCase(unittest.TestCase):
    
    def setUp(self):
        self.cdb = CloudMiner('sqlite:///:memory:')
        self.runname = "run1"
        self.iaasid = "iceicebaby"
        self.nodeid = "toocold"
        self.service_type = "iaasid1"
        self.parent = None
        self.hostname = "localhost"
        self.runlogdir = ""
        self.vmlogdir = ""


    def tearDown(self):
        pass
    
    def test_iaas_query(self):
        extras = {}
        extras['hi'] = 'there'
        runname = str(uuid.uuid1())
        iaasid = str(uuid.uuid1())
        nodeid = str(uuid.uuid1())
        source = "src"
        event_count = 10

        # create and add events
        for i in range(0, event_count):
            name = str(uuid.uuid1())
            cye = CYvent(source, name, 'key%d' % (i), datetime.datetime.now(), extras)
            self.cdb.add_cloudyvent(runname, iaasid, nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
        self.cdb.commit()

        # now get an IaaS object
        cyvm = self.cdb.get_by_iaasid(iaasid)
        self.assertEqual(len(cyvm.events), event_count)
        
    def test_many_iaas_1_runname(self):
        runname = str(uuid.uuid1())
        source = "src"
        key = "key"
        name = "name"
        vm_count = 10

        for i in range(0, vm_count):
            iaasid = str(uuid.uuid1())
            nodeid = str(uuid.uuid1())
            cye = CYvent(source, name, 'key%d' % (i), datetime.datetime.now(), None)
            self.cdb.add_cloudyvent(runname, iaasid, nodeid, self.hostname, self.service_type, self.parent, self.runlogdir, self.vmlogdir, cye)
            self.cdb.commit()

        cyvm_a = self.cdb.get_events_by_runname(runname)
        self.assertEqual(len(cyvm_a), vm_count, "%d should equal %d" % (len(cyvm_a), vm_count))

