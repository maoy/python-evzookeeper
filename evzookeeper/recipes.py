# Copyright (c) 2011 Yun Mao <yunmao at gmail dot com>.
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

import functools

import zookeeper

from evzookeeper import utils

class ZKQueue(object):
    '''
    queue model, support concurrent enqueue/dequeue
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
        '''
        concurrent enqueue
        '''
        return self._session.create(self.basepath + "/item-", val, 
                                    self.acl, zookeeper.SEQUENCE)

    def _get_and_delete(self, path):
        '''
        try to get and delete a node from zookeeper
        if not-exist return None
        '''
        try:
            (data, stat) = self._session.get(path, None)
            self._session.delete(path, stat["version"])
            return data
        except zookeeper.NoNodeException:
            return None

    def dequeue(self, timeout=None):
        '''
        concurrent dequeue
        blocking for 'timeout' seconds; 
        if timeout is None, block indefinitely (by default)
        if timeout is 0, equivalent to non-blocking
        '''
        def watcher(pc, handle, event, state, path):
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


class Membership(object):
    '''
    Use EPHEMERAL zknodes to maintain a failure-aware node membership list
    '''
    def __init__(self, session, basepath, acl=None):
        self._session = session
        self.basepath = basepath
        self.acl = acl
        try:
            # make sure basepath exists
            self._session.create(basepath, "ZKMembers", acl)
        except zookeeper.NodeExistsException:
            pass
    
    def join(self, name, value=''):
        """Use @param name to join the membership"""
        return self._session.create("%s/%s" % (self.basepath, name), value, 
                                    self.acl, zookeeper.EPHEMERAL)
        
    def get_all(self, watch_condition=None):
        """
        @param watch_condition: a PipeCondition object if set. If the
        membership changes, the condition will be set to trigger a callback
        
        @return: a list of node names 
        """
        def watcher(pc, handle, event, state, path):
            pc.notify()
        callback = watch_condition or functools.partial(watcher, watch_condition)
        children = self._session.\
            get_children(self.basepath, callback)
        return children

    def get(self, nodename):
        """Get the value for a specific node"""
        return self._session.get("%s/%s" % (self.basepath, nodename))
