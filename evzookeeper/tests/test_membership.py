# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2011-2012 AT&T Labs, Inc. Yun Mao <yunmao@gmail.com>
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
import sys
import unittest

import eventlet

import evzookeeper
from evzookeeper import membership
from evzookeeper import utils

class NodeManager(object):

    def __init__(self, name, session):
        self.name = name
        self.membership = membership.Membership(session,
                                                "/basedir", name)


class MembershipTestCase(unittest.TestCase):


    def setUp(self):
        f = open(os.devnull,"w")
        self.session = evzookeeper.ZKSession("localhost:2181", recv_timeout=4000,
                       zklog_fd=f)

    def tearDown(self):
        self.session.close()

    def test_join(self):
        """Two nodes joined, one membership monitor
        """
        spc = utils.StatePipeCondition()
        def callback(members):
            spc.set_and_notify(members)
        membership.MembershipMonitor(self.session,"/basedir",
                                                cb_func=callback)
        _n1 = membership.Membership(self.session, "/basedir", "node1")
        _n2 = membership.Membership(self.session, "/basedir", "node2")
        eventlet.sleep(0.5)
        members = spc.wait_and_get(1)
        self.assertEqual(sorted(members), ["node1", "node2"])


if __name__ == "__main__":
    unittest.main()
