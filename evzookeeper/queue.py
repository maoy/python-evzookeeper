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

import functools
import logging

import zookeeper

from evzookeeper import utils

LOG = logging.getLogger(__name__)


class ZKQueue(object):
    '''A simple queue model, support concurrent enqueue/dequeue
    '''
    def __init__(self, session, basepath, acl=None):
        self._session = session
        self.basepath = basepath
        self.acl = acl
        try:
            self._session.create(basepath, "ZKQueue", acl)
        except zookeeper.NodeExistsException:
            pass

    def enqueue(self, val):
        '''Concurrently enqueue'''
        return self._session.create(self.basepath + "/item-", val,
                                    self.acl, zookeeper.SEQUENCE)

    def _get_and_delete(self, path):
        '''Try to get and delete a node from zookeeper
        if not-exist return None
        '''
        try:
            (data, stat) = self._session.get(path, None)
            self._session.delete(path, stat["version"])
            return data
        except zookeeper.NoNodeException:
            return None

    def dequeue(self, timeout=None):
        '''Concurrently dequeue.

        blocking for 'timeout' seconds;
        if timeout is None, block indefinitely (by default)
        if timeout is 0, equivalent to non-blocking
        '''
        def watcher(pc, _handle, _event, _state, _path):
            pc.notify()

        while True:
            pc = utils.PipeCondition()
            children = sorted(self._session.
                              get_children(self.basepath,
                                           functools.partial(watcher, pc)))
            for child in children:
                data = self._get_and_delete(self.basepath + "/" + child)
                if data is not None:
                    return data
            pc.wait(timeout)
