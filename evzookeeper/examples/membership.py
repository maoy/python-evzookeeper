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

import eventlet
import logging
import sys

import evzookeeper
from evzookeeper import membership


logging.basicConfig(level=logging.DEBUG)


class NodeManager(object):

    def __init__(self, name, session):
        self.name = name
        self.membership = membership.Membership(session,
                                                "/basedir", name)


class NodeMonitor(object):

    def __init__(self, session):
        self.mon = membership.MembershipMonitor(session,
                                                "/basedir",
                                                cb_func=self.monitor)

    def monitor(self, members):
        print "in monitoring callback thread", members
        for member in members:
            print "detail of %s: %s" % (member,
                                        self.mon.get_member_details(member))


def demo():
    session = evzookeeper.ZKSession("localhost:2181", recv_timeout=4000,
                                    zklog_fd=sys.stderr)
    _n = NodeMonitor(session)
    if len(sys.argv) > 1:
        _nm = NodeManager(sys.argv[1], session)
        eventlet.sleep(1000)
    else:
        _nm1 = NodeManager("node1", session)
        _nm2 = NodeManager("node2", session)
        eventlet.sleep(5)
        _nm3 = NodeManager("node3", session)
        eventlet.sleep(60)
        _nm4 = NodeManager("node4", session)
        eventlet.sleep(1000)

if __name__ == "__main__":
    demo()
