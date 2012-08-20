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

import logging
import os
import sys
import unittest

import eventlet

import evzookeeper
from evzookeeper import membership
from evzookeeper import utils

logging.basicConfig(level=logging.DEBUG)


class MembershipTestCase(unittest.TestCase):

    devnull = open(os.devnull,"w")
    #devnull = sys.stderr

    def setUp(self):
        self.session = evzookeeper.ZKSession("localhost:2181", recv_timeout=4000,
                       zklog_fd=self.devnull, timeout=1)
        eventlet.sleep(5)

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
        eventlet.sleep(0.5)
        members = spc.wait_and_get(1)
        self.assertEqual(members, set(["node1"]))
        _n2 = membership.Membership(self.session, "/basedir", "node2")
        eventlet.sleep(0.5)
        members = spc.wait_and_get(1)
        self.assertEqual(members, set(["node1", "node2"]))

    def test_join_leave(self):
        """Two nodes joined, one left later
        """
        spc = utils.StatePipeCondition()
        def callback(members):
            spc.set_and_notify(members)
        membership.MembershipMonitor(self.session,"/basedir",
                                                cb_func=callback)
        _n1 = membership.Membership(self.session, "/basedir", "node1")
        _n2 = membership.Membership(self.session, "/basedir", "node2")
        eventlet.sleep(0.5)
        _n2.leave()
        eventlet.sleep(0.5)
        members = spc.wait_and_get(1)
        self.assertEqual(members, set(["node1"]))

    def test_expire(self):
        """test expire session"""
        spc = utils.StatePipeCondition()
        def callback(members):
            spc.set_and_notify(members)
        membership.MembershipMonitor(self.session,"/basedir",
                                                cb_func=callback)
        _n1 = membership.Membership(self.session, "/basedir", "node1")
        _n2 = membership.Membership(self.session, "/basedir", "node2")
        eventlet.sleep(0.5)
        session2 = evzookeeper.ZKSession("localhost:2181", timeout=1, recv_timeout=4000,
                                         ident=self.session.client_id(),
                                         zklog_fd=self.devnull)
        session2.close()
        eventlet.sleep(3)
        members = spc.wait_and_get(1)
        self.assertEqual(members, set(["node1", "node2"]))

if __name__ == "__main__":
    unittest.main()
