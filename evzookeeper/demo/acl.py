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


'''
'''
import zookeeper
from evzookeeper import ZKSession, ZOO_OPEN_ACL_UNSAFE


def demo_acl():
    session = ZKSession("localhost:2181", timeout=10)
    print 'connected'
    acl = [{"perms":zookeeper.PERM_ALL, "scheme":"auth", "id":""}]
    
    session.add_auth("digest", "user:pass")
    session.create("/test-acl", "abc", acl, zookeeper.EPHEMERAL)
    print 'test-acl created'
    print "(acl,stat)=", session.get_acl("/test-acl")


if __name__ == '__main__':
    demo_acl()
