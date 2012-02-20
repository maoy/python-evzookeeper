# Copyright (c) 2011-2012 Yun Mao <yunmao at gmail dot com>.
# All Rights Reserved.
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

import sys

import eventlet
import recipes

from evzookeeper import ZKSession, utils

class NodeManager(object):
    def __init__(self, name):
        self._session = ZKSession("localhost:2181", 10, zklog_fd=sys.stderr)
        self.membership = recipes.Membership(self._session, "/basedir")
        self.pc = utils.PipeCondition()
        self.membership.join(name)
        print 'in __init__', self.membership.get_all(self.pc)
        eventlet.spawn(self.monitor)

    def monitor(self):
        while 1:
            try:
                self.pc.wait()
            except Exception, e:
                print e
            print "changed"
            self.pc = utils.PipeCondition()
            try:
                print 'in monitor', self.membership.get_all(self.pc)
            except Exception, e:
                print 'exception is', e, type(e)
        
def demo():
    nm1 = NodeManager("node1")
    nm2 = NodeManager("node2")
    #print nm1.membership.get_all()
    eventlet.sleep(1000)
    
if __name__=="__main__":
    demo()
