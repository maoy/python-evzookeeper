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

import functools
import logging
import random

import eventlet
import zookeeper

from . import utils, ZOO_OPEN_ACL_UNSAFE

LOG = logging.getLogger("evzookeeper.recipes")

class Membership(object):
    '''Use ephemeral zknodes to maintain a failure-aware node membership list.
    
    ZooKeeper data structure:
    /basepath = "ZKMembers"
    /basepath/member1 = session_token1
    /basepath/member2 = session_token2
    ...
    Each member has a ephemeral zknode with value as a randomly generated number
    as unique session token.
    '''
    REFRESH_INTERVAL = 10

    def __init__(self, session, basepath, name, acl=None, cb_func=None):
        """Join the membership
        
        @param session: a ZKSession object
        @param basepath: the parent dir for membership zknodes.
        @param name: name of this member
        @param acl: access control list, by default [ZOO_OPEN_ACL_UNSAFE] is used
        @param cb_func: when the membership changes, cb_func is called 
        with the new membership list in another green thread 
        """
        self._session = session
        self.basepath = basepath
        self.acl = acl if acl else [ZOO_OPEN_ACL_UNSAFE]
        self._name = name
        self._session_token = str(random.random())
        self._cb_func = cb_func or (lambda x: None)
        self.joined = False
        self._members = []
        conn_spc = utils.StatePipeCondition()
        self._session.add_connection_callback(conn_spc)
        self.conn_spc = conn_spc
        self.monitor_pc = utils.StatePipeCondition()
        if self._session.is_connected():
            conn_spc.set_and_notify((None, zookeeper.SESSION_EVENT,
                                    zookeeper.CONNECTED_STATE, ''))
        eventlet.spawn(self._watch_connection)
        eventlet.spawn(self._watch_membership)

    def _watch_connection(self):
        """Runs in a green thread to periodically check connection state,
        and makes sure that the zknode is in place.
        """
        while 1:
            timeout = False
            state = None
            try:
                _, _, state, _ = self.conn_spc.wait_and_get(
                    timeout=self.REFRESH_INTERVAL)
            except eventlet.Timeout:
                timeout = True
            if timeout:
                if self._session.is_connected():
                    self._refresh()
            else:
                if state == zookeeper.CONNECTED_STATE:
                    self._on_connected()
                else:
                    self._on_disconnected(state)
    
    def _safe_callback(self):
        try:
            return self._cb_func(self._members)
        except Exception:
            LOG.exception("ignoring unexpected callback function exception")

    def _watch_membership(self):
        """Runs in a green thread to get all members."""
        while 1:
            event, state = self.monitor_pc.wait_and_get()
            if event == zookeeper.SESSION_EVENT and \
                    state != zookeeper.CONNECTED_STATE:
                # disconnected
                self.joined = False
                self._members = []
            else:
                self._members = self._get_members()
            self._safe_callback()

    def _get_members(self):
        try:
            def watcher(spc, handle, event, state, path):
                spc.set_and_notify((event, state))
            callback = functools.partial(watcher, self.monitor_pc)
            return self._session.get_children(self.basepath, callback)
        except Exception:
            LOG.exception("in Membership.get_all")
            return []

    def _on_connected(self):
        LOG.debug("recipes.Membership connected on %s", self._name)
        self._refresh()

    def _refresh(self, quiet=True):
        # if another node has the same name, we'll get an exception
        try:
            if self._join():
                self.monitor_pc.set_and_notify((zookeeper.SESSION_EVENT, 
                                                zookeeper.CONNECTED_STATE))
        except Exception:
            # error during creating the node.
            if not quiet:
                raise

    def _join(self):
        """Make sure the ephemeral node is in ZK, assuming the session
        is connected. 
        Called periodically when the session is in connected state or 
        when initially connected
        
        @return: True if the node didn't exist and was created;
        False if already joined; 
        or raise RuntimeError if another session is occupying
        the node currently.
        """
        # make sure base path exists
        try:
            self._session.create(self.basepath, "ZKMembers", self.acl)
        except zookeeper.NodeExistsException:
            pass
        
        path = "%s/%s" % (self.basepath, self._name)
        try:
            self._session.create(path, self._session_token, self.acl, 
                                 zookeeper.EPHEMERAL)
            LOG.debug("created zknode %s", path)
            if self.joined:
                LOG.warn("node %s successfully created even after joined. data loss?", 
                         path)
            self.joined = True        
            return True
        except zookeeper.NodeExistsException:
            (data, _) = self._session.get(path)
            if data != self._session_token:
                LOG.critical("Duplicated names %s with different session id",
                             self._name)
                raise RuntimeError("Duplicated membership name %s" % self._name)
            # otherwise, node is already there correctly
        return False

    def _on_disconnected(self, state):
        LOG.error("Membership disconnected on %s with state %s",
                  self._name, state)
        if state==zookeeper.EXPIRED_SESSION_STATE:
            LOG.debug("Membership session expired. Try reconnect")
            self._session.connect()

    def _leave(self):
        if self._name:
            self._session.delete("%s/%s" % (self.basepath, self._name))
            return True
        return False
        
    def get_all(self):
        """@return: a list of node names"""
        return self._members
