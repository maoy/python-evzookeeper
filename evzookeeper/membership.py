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
import random

import eventlet
import zookeeper

from evzookeeper import utils
from evzookeeper import ZKSessionWatcher
from evzookeeper import ZOO_OPEN_ACL_UNSAFE


LOG = logging.getLogger(__name__)


class _BasicMembership(ZKSessionWatcher):
    """Base 'abstract' class for MembershipMonitor and Membership classes"""

    def __init__(self, session, basepath, acl=None):
        """Constructor
        
        @param session: a ZKSession object
        @param basepath: the parent dir for membership zknodes.
        @param acl: access control list, by default [ZOO_OPEN_ACL_UNSAFE] is
        used
        """
        super(_BasicMembership, self).__init__()
        self._session = session
        self._basepath = basepath
        self.acl = acl if acl else [ZOO_OPEN_ACL_UNSAFE]
        self._session.add_connection_watcher(self)

    def on_connected(self):
        LOG.debug("_BasicMembership _on_connected on %s", self._basepath)
        self.refresh()

    def on_disconnected(self, state):
        LOG.error("_BasicMembership _on_disconnected on with state %s",
                  state)
        if state == zookeeper.EXPIRED_SESSION_STATE:
            LOG.debug("_BasicMembership session expired. Try reconnect")
            self._session.connect()

    def refresh(self, quiet=True):
        LOG.debug("_BasicMembership refresh() on %s", self._basepath)


class MembershipMonitor(_BasicMembership):
    """Monitor membership services"""

    def __init__(self, session, basepath, acl=None, cb_func=None):
        """
        @param session: a ZKSession object
        @param basepath: the parent dir for membership zknodes.
        @param acl: access control list, by default [ZOO_OPEN_ACL_UNSAFE] is
                    used
        @param cb_func: when the membership changes, cb_func is called
        with the new membership list in another green thread.
        If the ZooKeeper quorum is (temporarily) unavailable, None
        is returned in the call back. It does NOT mean that the members
        have all left.
        """
        super(MembershipMonitor, self).__init__(session, basepath, acl)
        self._cb_func = cb_func or (lambda x: None)
        self._members = None
        self._monitor_pc = utils.StatePipeCondition()
        eventlet.spawn(self._watch_membership)

    def _safe_callback(self):
        try:
            return self._cb_func(self._members)
        except Exception:
            LOG.exception("ignoring unexpected callback function exception")

    def refresh(self, quiet=True):
        LOG.debug("MembershipMonitor refresh on %s", self._basepath)
        # if another node has the same name, we'll get an exception
        try:
            # if self._join():
            self._monitor_pc.set_and_notify((zookeeper.SESSION_EVENT,
                                                zookeeper.CONNECTED_STATE))
        except Exception:
            # error during creating the node.
            if not quiet:
                raise

    def _watch_membership(self):
        """Runs in a green thread to get all members."""
        while 1:
            event, state = self._monitor_pc.wait_and_get()
            LOG.debug("MembershipMonitor _watch_membership on %(path)s "
                      "event= %(event)s state = %(state)s",
                      {'path': self._basepath,
                       'event': event, 'state': state})
            if event == zookeeper.SESSION_EVENT and \
                    state != zookeeper.CONNECTED_STATE:
                # disconnected
                self._members = None
            else:
                self._members = self._get_members()
            self._safe_callback()

    def get_all(self):
        """@return: a list of node names from the local cache"""
        return self._members

    def _get_members(self):
        try:
            def watcher(spc, handle, event, state, path):
                spc.set_and_notify((event, state))
            callback = functools.partial(watcher, self._monitor_pc)
            return self._session.get_children(self._basepath, callback)
        except Exception:
            LOG.exception("in MembershipMonitor._get_members")
            return None


class Membership(_BasicMembership):
    '''Use ephemeral zknodes to maintain a failure-aware node membership list.

    ZooKeeper data structure:
    /basepath = "ZKMembers"
    /basepath/member1 = session_token1
    /basepath/member2 = session_token2
    ...
    Each member has a ephemeral zknode with value as a randomly generated
    number as unique session token.

    The member is joined by default, and can leave later. After it is left,
    it is not allowed to re-join.
    '''

    def __init__(self, session, basepath, name, acl=None):
        """Initialize and join the membership

        @param session: a ZKSession object
        @param basepath: the parent dir for membership zknodes.
        @param name: name of this member
        @param acl: access control list, by default [ZOO_OPEN_ACL_UNSAFE] is
            used
        """
        self._name = name
        self._session_token = str(random.random())
        super(Membership, self).__init__(session, basepath, acl)
        self._joined = False
        if self._session.is_connected():
            self.refresh()

    def refresh(self, quiet=True):
        LOG.debug("Membership refresh on %s",
                  self._basepath + '/' + self._name)
        # if another node has the same name, we'll get an exception
        try:
            self._join()
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
        path = self._basepath + '/' + self._name
        LOG.debug("Membership _join on %s", path)
        if self._joined:
            return False
        # make sure base path exists
        try:
            self._session.create(self._basepath, "ZKMembers", self.acl)
        except zookeeper.NodeExistsException:
            pass

        try:
            self._session.create(path, self._session_token, self.acl,
                                 zookeeper.EPHEMERAL)
            LOG.debug("created zknode %s", path)
            if self._joined:
                LOG.warn("node %s successfully created even after joined.\
data loss?", path)
            self._joined = True
            return True
        except zookeeper.NodeExistsException:
            (data, _) = self._session.get(path)
            if data != self._session_token:
                LOG.critical("Duplicated names %s with different session id",
                             self._name)
                raise RuntimeError("Duplicated membership name %s" % path)
            # otherwise, node is already there correctly
        return False

    def leave(self):
        LOG.debug("Membership leave on %s/%s", self._basepath, self._name)
        if self._name:
            self._session.remove_connection_watcher(self)
            self._session.delete("%s/%s" % (self._basepath, self._name))
            self._name = None
            return True
        return False
