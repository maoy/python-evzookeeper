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

from evzookeeper import ZKSession
from evzookeeper import ZOO_OPEN_ACL_UNSAFE
import zookeeper


def demo():
    session = ZKSession("localhost:2181", timeout=10)
    print 'connected'
    session.create("/test-tmp", "abc",
                   [ZOO_OPEN_ACL_UNSAFE], zookeeper.EPHEMERAL)
    print 'test-tmp created'
    print "(acl,stat)=", session.get_acl("/test-tmp")
    try:
        session.create("/test", "abc", [ZOO_OPEN_ACL_UNSAFE], 0)
    except zookeeper.NodeExistsException:
        pass
    print session.exists("/test")
    session.delete("/test")
    session.create("/test", "abc", [ZOO_OPEN_ACL_UNSAFE], 0)
    print session.get("/test")
    print 'done.'
    session.set("/test", "def")
    print session.get("/test")

if __name__ == '__main__':
    demo()
