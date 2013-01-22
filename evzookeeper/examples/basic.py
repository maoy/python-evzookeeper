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

from evzookeeper import ZKSession
from evzookeeper import ZOO_OPEN_ACL_UNSAFE
import zookeeper

logging.basicConfig(level=logging.DEBUG)


def demo():
    ## create session
    session = ZKSession("localhost:2181", timeout=10)
    print 'session connected'

    ## first dir
    session.create("/test-tmp", "abc", acl=[ZOO_OPEN_ACL_UNSAFE],
                   flags=zookeeper.EPHEMERAL)
    try:
        print 'test-tmp created'
        print "(acl,stat)=", session.get_acl("/test-tmp")
    finally:
        session.delete('/test-tmp')

    ## second dir
    session.create("/test", "abc", acl=[ZOO_OPEN_ACL_UNSAFE])
    try:
        print session.exists("/test")
        print session.get("/test")
        session.set("/test", "def")
        ## create some leaves
        session.create("/test/n-1", "n-1", acl=[ZOO_OPEN_ACL_UNSAFE])
        session.create("/test/n-2", "n-2", acl=[ZOO_OPEN_ACL_UNSAFE])
        session.ensure_path("/test/n-3/n-3-1", "n-3",
                            acl=[ZOO_OPEN_ACL_UNSAFE])
        print session.get_children('/test')
    finally:
        session.delete('/test', recursive=True)

    print 'done.'

if __name__ == '__main__':
    demo()
