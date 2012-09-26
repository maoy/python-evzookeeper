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

import zookeeper

import evzookeeper
from evzookeeper import utils

LOG = logging.getLogger(__name__)


class MembershipMonitor(evzookeeper.ZKServiceBase):
    """Monitor membership changes as an outside observer."""

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
        self._cb_func = cb_func or (lambda x: None)
        self._members = None
        super(MembershipMonitor, self).__init__(session, basepath, acl=acl)

    def _safe_callback(self):
        try:
            return self._cb_func(self._members)
        except Exception:
            LOG.exception("Ignore exception from the callback function")

    def _session_callback(self):
        """Runs in a green thread to get all members."""
        self._members = self._get_members()
        self._safe_callback()
        while 1:
            try:
                _handle, event, state, _path = self._session_spc.wait_and_get()
            except utils.PipeConditionClosedError:
                LOG.info("pipe condition is closed. exit the green thread.")
                break
            LOG.debug("MembershipMonitor _watch_membership on %(path)s "
                      "event=%(event)s state=%(state)s",
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
    
    def get_member_details(self, name):
        """Return the zknode info as (value, stat) .
        Raise appropriate exceptions in the case of failures."""
        value, stat = self._session.get("%s/%s" % (self._basepath, name))
        return value, stat

    def _get_members(self, quiet=True):
        try:
            def watcher(spc, handle, event, state, path):
                spc.set_and_notify((handle, event, state, path))
            callback = functools.partial(watcher, self._session_spc)
            return set(self._session.get_children(self._basepath, callback))
        except Exception:
            LOG.exception("in MembershipMonitor._get_members")
            if quiet:
                return None
            raise


class Membership(evzookeeper.ZKServiceBase):
    '''Use ephemeral zknodes to maintain a failure-aware node membership list.

    ZooKeeper data structure:
    /basepath = "ZKMembers"
    /basepath/member1 = session_token1
    /basepath/member2 = session_token2
    ...
    Each member has a ephemeral zknode with value as a randomly generated
    number as unique session token.

    The member is joined during init, and can leave later. After it is left,
    it is not allowed to re-join.
    '''

    def __init__(self, session, basepath, name, acl=None, async_mode=False):
        """Initialize and join the membership

        @param session: a ZKSession object
        @param basepath: the parent dir for membership zknodes.
        @param name: name of this member
        @param acl: access control list, by default [ZOO_OPEN_ACL_UNSAFE] is
            used
        @param async_mode: if True then the zknode will be created asynchronously.
        """
        self._name = name
        self._session_token = str(random.random())
        self._joined = False
        self._async_mode = async_mode
        super(Membership, self).__init__(session, basepath, acl=acl)
        if not async_mode:
            self.refresh(quiet=False)

    def _session_callback(self):
        """Runs in a green thread to react to session state change."""
        if self._async_mode:
            self.refresh(quiet=True)
        while 1:
            try:
                handle, event, state, path = self._session_spc.wait_and_get()
            except utils.PipeConditionClosedError:
                LOG.info("pipe condition is closed. exit the green thread.")
                break
            LOG.debug("Session state changed: handle=%(handle)s, path=%(path)s, "
                      "event=%(event)s, state=%(state)s",
                      locals())
            if state == zookeeper.EXPIRED_SESSION_STATE:
                LOG.debug("session %s expired.", handle)
                self._joined = False
            if state == zookeeper.CONNECTED_STATE:
                LOG.debug("session %s connected.", handle)
                self.refresh(quiet=True)
    
    def refresh(self, quiet=True):
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
        Called when initially connected

        @return: True if the node didn't exist and was created;
        False if already joined;
        None: the node left the membership before zknode is created;
        or raise RuntimeError if another session is occupying
        the node currently.
        """
        if self._name is None:
            return None
        name = self._name
        path = self._basepath + '/' + name
        LOG.debug("Membership._join on %s", path)
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
                LOG.warn("node %s successfully created even after joined."
                         "data loss?", path)
            self._joined = True
            if self._name is None:
                # leave() was called when creating node. so the node is
                # not really deleted. try again.
                self._name = name
                self.leave()
                return None
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
        if self._name is None:
            return False

        path = "%s/%s" % (self._basepath, self._name)
        LOG.debug("Membership leave on %s", path)
        self._name = None
        self._joined = False
        try:
            self._session.remove_connection_callback(self._session_spc)
        except KeyError:
            pass
        self._session_spc.close()
        try:
            self._session.delete(path)
            LOG.debug("Membership.leave: deleted %s", (path,))
        except zookeeper.NoNodeException:
            LOG.warn("No such node when deleting membership node. Ignore.")
        except zookeeper.ZooKeeperException:
            LOG.exception("error when deleting membership node. ignore.")
        return True

    def __del__(self):
        self.leave()
        super(Membership, self).__del__()
