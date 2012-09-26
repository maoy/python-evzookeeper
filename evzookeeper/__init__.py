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
import os
import sys
import thread

import eventlet
import zookeeper

from evzookeeper import utils


ZOO_OPEN_ACL_UNSAFE = {"perms": zookeeper.PERM_ALL, "scheme": "world",
                       "id": "anyone"}


LOG = logging.getLogger(__name__)


def _generic_completion(spc, *args):
    """The default async completion function
    @param spc: a StatePipeCondition object to notify main thread
    *args: depends on the completion function

    Usually, the arguments are handle, event, state, path
    event is one of CHILD_EVENT, DELETED_EVENT, CHANGED_EVENT,
    CREATED_EVENT, SESSION_EVENT, NOTWATCHING_EVENT

    state is one of INVALIDSTATE, AUTH_FAILED_STATE,
    CONNECTING_STATE, CONNECTED_STATE,
    ASSOCIATING_STATE, EXPIRED_SESSION_STATE
    """
    spc.set_and_notify(args)



class ZKSession(object):

    __slots__ = ("_zhandle", "_host", "_recv_timeout", "_refresh_interval",
                 "_ident", "_zklog_fd", "_conn_cbs", "_conn_spc",
                 "_conn_watchers")

    def __init__(self, host, timeout=None, recv_timeout=10000,
                 ident=(-1, ""), zklog_fd=None,
                 init_cbs=None):
        """
        @param host: comma separated host:port pairs, each corresponding to
        a zk server. e.g. '127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002'

        (subsequent parameters are optional)

        @param timeout: None by default means asynchronous session
        establishment, or the time to wait until the session is established.

        @param recv_timeout: ZK clients detects server failures in 2/3 of
        recv_timeout, and then it retries the same IP at every recv_timeout
        period if only one of ensemble is given. If more than two ensemble IP
        are given, ZK clients will try next IP immediately.

        @param ident: (clientid, passwd)
        clientid the id of a previously established session that this
        client will be reconnecting to. Clients can access the session id of
        an established, valid, connection by calling zoo_client_id. If
        the specified clientid has expired, or if the clientid is invalid for
        any reason, the returned zhandle_t will be invalid -- the zhandle_t
        state will indicate the reason for failure (typically
        EXPIRED_SESSION_STATE).

        @param zklog_fd: the file descriptor to redirect zookeeper logs.
        By default, it redirects to /dev/null

        @param init_cbs: initial callback objects of type StatePipeCondition
        """
        LOG.debug("Create a new ZKSession: timeout=%(timeout)s "
                  "recv_timeout=%(recv_timeout)s,"
                  "ident=%(ident)s,"                  
                  "zklog_fd=%(zklog_fd)s, init_cbs=%(init_cbs)s",
                  locals())
        self._host = host
        self._recv_timeout = recv_timeout
        self._ident = ident
        if zklog_fd is None:
            zklog_fd = open("/dev/null")
        self._zklog_fd = zklog_fd
        self._zhandle = None
        self._conn_cbs = set([])
        if init_cbs:
            self._conn_cbs.update(init_cbs)
        zookeeper.set_log_stream(self._zklog_fd)

        self._conn_watchers = set([])
        conn_spc = utils.StatePipeCondition()
        self.add_connection_callback(conn_spc)
        self._conn_spc = conn_spc
        eventlet.spawn(self._session_thread)
        self.connect(timeout=timeout)

    def _init_watcher(self, handle, event_type, state, path):
        #called when init is successful or connection state is changed
        # we make a copy of _conn_cbs because it might be changed in
        #the main thread during iteration
        for cb in self._conn_cbs.copy():
            try:
                cb.set_and_notify((handle, event_type, state, path))
            except Exception:
                # Ignoring exception in notifying a connection callback
                pass

    def _session_thread(self):
        """When the session state is changed, this function is triggered
        in a green thread.
        """
        while 1:
            handle, event, state, path = self._conn_spc.wait_and_get()
            LOG.debug("Session state changed: handle=%(handle)s, path=%(path)s, "
                      "event=%(event)s, state=%(state)s",
                      locals())
            if state == zookeeper.EXPIRED_SESSION_STATE:
                LOG.debug("session %s expired. Try reconnect.", handle)
                self.connect()

        
    def add_auth(self, scheme, cert):
        """
        specify application credentials.

        The application calls this function to specify its credentials for
        purposes of authentication. The server will use the security provider
        specified by the scheme parameter to authenticate the client
        connection. If the authentication request has failed:
        - the server connection is dropped
        - the watcher is called with the AUTH_FAILED_STATE value as the state
        parameter.

        PARAMETERS:
        @param scheme: the id of authentication scheme. Natively supported:
        'digest' password-based authentication
        @param cert: application credentials. The actual value depends on the
        scheme.


        RETURNS None.

        If error occurs, one of the following corresponding exceptions will be
        thrown.
        OK on success or one of the following errcodes on failure:
        AUTHFAILED authentication failed
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE or \
AUTH_FAILED_STATE
        MARSHALLINGERROR - failed to marshall a request; possibly, out of \
memory
        SYSTEMERROR - a system error occurred
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.add_auth(self._zhandle, scheme, cert,
                               functools.partial(_generic_completion,
                                                 pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        #unpack result as void_completion
        handle, rc = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return
        self._raise_exception(rc)

    def connect(self, timeout=None):
        """Establish the ZooKeeper session. The method will make sure to close
        the existing connection if available.
        @param timeout: None by default means asynchronous session
         establishment. Otherwise, it's the time to wait until the
         session is established.
        """
        self.close(quiet=True)

        timeout_spc = None
        if timeout is not None:
            timeout_spc = utils.StatePipeCondition()
            self.add_connection_callback(timeout_spc)
        self._zhandle = zookeeper.init(self._host, self._init_watcher,
                                       self._recv_timeout, self._ident)
        if timeout is not None:
            try:
                timeout_spc.wait_and_get(timeout=timeout)
            finally:
                self._conn_cbs.remove(timeout_spc)

    def add_connection_callback(self, callback):
        """
        @param callback: StatePipeCondition object
        """
        self._conn_cbs.add(callback)

    def remove_connection_callback(self, callback):
        """
        @param callback: StatePipeCondition object
        """
        self._conn_cbs.remove(callback)

    def is_connected(self):
        return (self._zhandle is not None and
                self.state() == zookeeper.CONNECTED_STATE)

    def client_id(self):
        return zookeeper.client_id(self._zhandle)

    def close(self, quiet=True):
        """
        close the handle. (Is it a blocking call or not?)
        """
        if self._zhandle is not None:
            try:
                zookeeper.close(self._zhandle)
            except Exception:
                LOG.exception("In close() found unexpected exception with "
                              "handle=%s", self._zhandle)
                if not quiet:
                    raise
            finally:
                self._zhandle = None

    def create(self, path, value, acl=None, flags=0):
        """
        create a node synchronously.

        This method will create a node in ZooKeeper. A node can only be created
        if it does not already exists. The Create Flags affect the creation of
        nodes.
        If the EPHEMERAL flag is set, the node will automatically get removed
        if the client session goes away. If the SEQUENCE flag is set, a unique
        monotonically increasing sequence number is appended to the path name.

        PARAMETERS:
        path: The name of the node. Expressed as a file name with slashes
              separating ancestors of the node.

        value: The data to be stored in the node.

        acl: The initial ACL of the node. If None, the ACL of the parent will
             be used.

        flags: this parameter can be set to 0 for normal create or an OR
               of the Create Flags

        The real path that is created (this might be different than the
        path to create because of the SEQUENCE flag.
        the maximum length of real path you would want.

        RETURNS:
        The actual znode path that was created (may be different from path due
        to use of SEQUENTIAL flag).

        EXCEPTIONS:
        NONODE the parent node does not exist.
        NODEEXISTS the node already exists
        NOAUTH the client does not have permission.
        NOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE or
        AUTH_FAILED_STATE
        MARSHALLINGERROR - failed to marshall a request; possibly, out of
         memory
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.acreate(self._zhandle, path, value, acl, flags,
                               functools.partial(_generic_completion,
                                                 pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as string_completion
        handle, rc, real_path = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return real_path
        self._raise_exception(rc)

    def delete(self, path, version=-1):
        """
        delete a node in zookeeper synchronously.

        PARAMETERS:
        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.

        (Subsequent parameters are optional)
        version: the expected version of the node. The function will fail if
        the actual version of the node does not match the expected version.
         If -1 (the default) is used the version check will not take place.

        RETURNS:
        OK operation completed successfully

        One of the following exceptions is returned when an error occurs.
        NONODE the node does not exist.
        NOAUTH the client does not have permission.
        BADVERSION expected version does not match actual version.
        NOTEMPTY children are present; node cannot be deleted.
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE or
        AUTH_FAILED_STATE
        MARSHALLINGERROR - failed to marshal a request; possibly, out of
         memory
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.adelete(self._zhandle, path, version,
                               functools.partial(_generic_completion,
                                                 pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as void_completion
        handle, rc = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return rc
        self._raise_exception(rc)

    def exists(self, path, watch=None):
        """checks the existence of a node in zookeeper.

        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.

        (Subsequent parameters are optional)

        watch: if not None, a watch will be set at the server to notify the
        client if the node changes. The watch will be set even if the node
        does not exist. This allows clients to watch for nodes to appear.

        Return: stat if the node exists
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.aexists(self._zhandle, path, watch,
                               functools.partial(_generic_completion,
                                                 pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as stat_completion
        handle, rc, stat = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return stat
        self._raise_exception(rc)

    def get(self, path, watcher=None):
        """
        gets the data associated with a node synchronously.

        PARAMETERS:
        path: the name of the node. Expressed as a file name with slashes
            separating ancestors of the node.

        (subsequent parameters are optional)
        watcher: if not None, a watch will be set at the server to notify
        the client if the node changes.

        RETURNS:
        the (data, stat) tuple associated with the node
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.aget(self._zhandle, path, watcher,
                               functools.partial(_generic_completion,
                                                 pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as data_completion
        handle, rc, data, stat = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return (data, stat)
        self._raise_exception(rc)

    def get_acl(self, path):
        """
        Gets the acl associated with a node.

        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.

        Return:
        (acl, stat)
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.aget_acl(self._zhandle, path,
                               functools.partial(_generic_completion,
                                                 pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as acl_completion
        handle, rc, acl, stat = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return (acl, stat)
        self._raise_exception(rc)

    def get_children(self, path, watcher=None):
        """
        lists the children of a node synchronously.

        PARAMETERS:
        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.

        (subsequent parameters are optional)
        watcher: if non-null, a watch will be set at the server to notify
        the client if the node changes.

        RETURNS:
        A list of znode names
        EXCEPTIONS:
        NONODE the node does not exist.
        NOAUTH the client does not have permission.
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE
         or AUTH_FAILED_STATE
        MARSHALLINGERROR - failed to marshall a request; possibly, out
         of memory
        ConnectionLossException
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.aget_children(self._zhandle, path, watcher,
                                     functools.partial(_generic_completion,
                                                       pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as strings_completion
        handle, rc, children = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return children
        self._raise_exception(rc)

    def set2(self, path, data, version=-1):
        """
        sets the data associated with a node.

        PARAMETERS:
        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.
        data: the buffer holding data to be written to the node.

        (subsequent parameters are optional)
        version: the expected version of the node. The function will fail if
        the actual version of the node does not match the expected version.
         If -1 is used the version check will not take place.

        RETURNS:
        the new stat of the node.

        EXCEPTIONS:
        NONODE the node does not exist.
        NOAUTH the client does not have permission.
        BADVERSION expected version does not match actual version.
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE or
        AUTH_FAILED_STATE
        MARSHALLINGERROR - failed to marshall a request; possibly, out of
        memory
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.aset(self._zhandle, path, data, version,
                                     functools.partial(_generic_completion,
                                                       pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as stat_completion
        handle, rc, stat = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return stat
        self._raise_exception(rc)

    def set(self, path, data, version=-1):
        """
        sets the data associated with a node. See set2 function if
        you require access to the stat information associated with the znode.

        PARAMETERS:
        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.
        data: the buffer holding data to be written to the node.

        (subsequent parameters are optional)
        version: the expected version of the node. The function will fail if
        the actual version of the node does not match the expected version.
         If -1 is used the version check will not take place.

        RETURNS:
        OK status

        EXCEPTIONS:
        NONODE the node does not exist.
        NOAUTH the client does not have permission.
        BADVERSION expected version does not match actual version.
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE or
         AUTH_FAILED_STATE
        MARSHALLINGERROR - failed to marshall a request; possibly, out of
         memory
        """
        self.set2(path, data, version)
        return zookeeper.OK

    def set_acl(self, path, version, acl):
        """sets the acl associated with a node synchronously.

        PARAMETERS:
        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.
        version: the expected version of the path.
        acl: the acl to be set on the path.

        RETURNS:
        OK operation completed successfully
        EXCEPTIONS:
        NONODE the node does not exist.
        NOAUTH the client does not have permission.
        INVALIDACL invalid ACL specified
        BADVERSION expected version does not match actual version.
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE
         or AUTH_FAILED_STATE
        MARSHALLINGERROR - failed to marshall a request; possibly, out
         of memory
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.aset_acl(self._zhandle, path, version, acl,
                                functools.partial(_generic_completion,
                                                  pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as void_completion
        handle, rc = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return rc
        self._raise_exception(rc)

    def state(self):
        return zookeeper.state(self._zhandle)

    def sync(self, path):
        """
        Flush leader channel.
        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.

        Returns OK on success.
        """
        pc = utils.StatePipeCondition()
        ok = zookeeper.async(self._zhandle, path,
                             functools.partial(_generic_completion,
                                               pc))
        assert ok == zookeeper.OK
        results = pc.wait_and_get()
        pc.close()
        #unpack result as void_completion
        handle, rc = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return rc
        self._raise_exception(rc)

    def __del__(self):
        self.close()

    def _raise_exception(self, rc):
        raise self._rc2exception[rc](zookeeper.zerror(rc))

    # translate return code to exception
    _rc2exception = {zookeeper.APIERROR: zookeeper.ApiErrorException,
                    zookeeper.AUTHFAILED: zookeeper.AuthFailedException,
                    zookeeper.BADARGUMENTS: zookeeper.BadArgumentsException,
                    zookeeper.BADVERSION: zookeeper.BadVersionException,
                    zookeeper.CLOSING: zookeeper.ClosingException,
                    zookeeper.CONNECTIONLOSS:
                        zookeeper.ConnectionLossException,
                    zookeeper.DATAINCONSISTENCY:
                        zookeeper.DataInconsistencyException,
                    zookeeper.INVALIDACL: zookeeper.InvalidACLException,
                    zookeeper.INVALIDCALLBACK:
                        zookeeper.InvalidCallbackException,
                    zookeeper.INVALIDSTATE: zookeeper.InvalidStateException,
                    zookeeper.MARSHALLINGERROR:
                        zookeeper.MarshallingErrorException,
                    zookeeper.NONODE: zookeeper.NoNodeException,
                    zookeeper.NOAUTH: zookeeper.NoAuthException,
                    zookeeper.NODEEXISTS: zookeeper.NodeExistsException,
                    zookeeper.NOCHILDRENFOREPHEMERALS:
                        zookeeper.NoChildrenForEphemeralsException,
                    zookeeper.NOTEMPTY: zookeeper.NotEmptyException,
                    zookeeper.NOTHING: zookeeper.NothingException,
                    zookeeper.OPERATIONTIMEOUT:
                        zookeeper.OperationTimeoutException,
                    zookeeper.RUNTIMEINCONSISTENCY:
                        zookeeper.RuntimeInconsistencyException,
                    zookeeper.SESSIONEXPIRED:
                        zookeeper.SessionExpiredException,
                    zookeeper.SYSTEMERROR: zookeeper.SystemErrorException,
                    zookeeper.UNIMPLEMENTED: zookeeper.UnimplementedException,
                    }


class ZKServiceBase(object):
    """A typical ZK service that requires a default basepath, acl, and a
    monitor to the session state"""

    def __init__(self, session, basepath, acl=None):
        """Constructor
        
        @param session: a ZKSession object
        @param basepath: the default parent dir for zknodes.
        @param acl: access control list, by default [ZOO_OPEN_ACL_UNSAFE] is
        used
        """
        self._session = session
        self._basepath = basepath
        self.acl = acl if acl else [ZOO_OPEN_ACL_UNSAFE]
        self._session_spc = utils.StatePipeCondition()
        self._session.add_connection_callback(self._session_spc)
        eventlet.spawn(self._session_callback)

    def _session_callback(self):
        while 1:
            _handle, event, state, _path = self._session_spc.wait_and_get()
            LOG.debug("Default session callback handler on basepath=%(path)s, "
                      "event=%(event)s, state=%(state)s",
                      {'path': self._basepath,
                       'event': event, 'state': state})

    def __del__(self):
        #LOG.debug("In destructor of ZKServiceBase")
        if self._session_spc and self._session:
            try:
                self._session.remove_connection_callback(self._session_spc)
            except KeyError:
                pass
            self._session_spc.close()
