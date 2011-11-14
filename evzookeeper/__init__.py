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

import eventlet
import zookeeper
import os
import functools

from evzookeeper import utils

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

def generic_completion(pc, results, *args):
    """
    used as the async completion function
    pc: a PipeCondition object to notify main thread
    results: an empty list to send result back
    *args: depends on the completion function
    """
    assert not results
    results.extend(args)
    pc.notify()
    
class ZKSession(object):
    
    __slots__ = ("_zhandle", )
    
    def __init__(self, host, timeout=10, recv_timeout=10000, 
                 ident=(-1,""), zklog_fd=None):
        """
        This method creates a new handle and a zookeeper session that corresponds
        to that handle. Session establishment is asynchronous, meaning that the
        session should not be considered established until (and unless) an
        event of state CONNECTED_STATE is received.
        PARAMETERS:
        host: comma separated host:port pairs, each corresponding to a zk
        server. e.g. '127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002'
        
        (subsequent parameters are optional)
        fn: the global watcher callback function. When notifications are
        triggered this function will be invoked.
        recv_timeout: 
        ident = (clientid, passwd)
        clientid the id of a previously established session that this
        client will be reconnecting to. Clients can access the session id of an 
         established, valid,
        connection by calling zoo_client_id. If
        the specified clientid has expired, or if the clientid is invalid for 
        any reason, the returned zhandle_t will be invalid -- the zhandle_t 
        state will indicate the reason for failure (typically
        EXPIRED_SESSION_STATE).
        zklog_fd is the file descriptor to redirect zookeeper logs.
        By default, it redirects to /dev/null
        """
        self._zhandle = None
        pc = utils.PipeCondition()
        def init_watcher(handle, event_type, stat, path):
            #called when init is successful
            pc.notify()
        if zklog_fd is None:
            zklog_fd = open("/dev/null")
        zookeeper.set_log_stream(zklog_fd)
        self._zhandle = zookeeper.init(host, init_watcher, recv_timeout, ident)
        pc.wait(timeout)
        
    def close(self):
        """
        close the handle. potentially a blocking call?
        """
        if self._zhandle is not None:
            zookeeper.close(self._zhandle)
            # if closed successfully
            self._zhandle = None
    
    def create(self, path, value, acl=None, flags=0):
        """
        create a node synchronously.
    
        This method will create a node in ZooKeeper. A node can only be created if
        it does not already exists. The Create Flags affect the creation of nodes.
        If the EPHEMERAL flag is set, the node will automatically get removed if the
        client session goes away. If the SEQUENCE flag is set, a unique
        monotonically increasing sequence number is appended to the path name.
    
        PARAMETERS:
        path: The name of the node. Expressed as a file name with slashes 
        separating ancestors of the node.

        value: The data to be stored in the node.

        acl: The initial ACL of the node. If None, the ACL of the parent will be
            used.
    
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
        results = []
        pc = utils.PipeCondition()
        ok = zookeeper.acreate(self._zhandle, path, value, acl, flags,
                               functools.partial(generic_completion, 
                                                 pc, results))
        assert ok == zookeeper.OK
        pc.wait()
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
        version: the expected version of the node. The function will fail if the
        actual version of the node does not match the expected version.
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
        results = []
        pc = utils.PipeCondition()
        ok = zookeeper.adelete(self._zhandle, path, version, 
                               functools.partial(generic_completion, 
                                                 pc, results))
        assert ok == zookeeper.OK
        pc.wait()
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
        client if the node changes. The watch will be set even if the node does not 
        exist. This allows clients to watch for nodes to appear.
        
        Return: stat if the node exists    
        """
        results = []
        pc = utils.PipeCondition()        
        ok = zookeeper.aexists(self._zhandle, path, watch,
                               functools.partial(generic_completion, 
                                                 pc, results))                               
        assert ok == zookeeper.OK
        pc.wait()
        #unpack result as stat_completion
        handle, rc, stat = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return stat
        self._raise_exception(rc)
        
    def get(self, path, watcher=None, bufferlen=1024*1024):
        """
        gets the data associated with a node synchronously.
        
        PARAMETERS:
        path: the name of the node. Expressed as a file name with slashes 
            separating ancestors of the node.
    
        (subsequent parameters are optional)
        watcher: if not None, a watch will be set at the server to notify 
        the client if the node changes.
        bufferlen: This value defaults to 1024*1024 - 1Mb. This method 
         returns 
        the minimum of bufferlen and the true length of the znode's 
         data. 
    
        RETURNS:
        the (data, stat) tuple associated with the node
        """
        results = []
        pc = utils.PipeCondition()        
        ok = zookeeper.aget(self._zhandle, path, watcher,
                               functools.partial(generic_completion, 
                                                 pc, results))                               
        assert ok == zookeeper.OK
        pc.wait()
        #unpack result as data_completion
        handle, rc, data, stat = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return (data, stat)
        self._raise_exception(rc)

    def get_acl(self, path):
        zookeeper.aget_acl()
        
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
        """
        results = []
        pc = utils.PipeCondition()        
        ok = zookeeper.aget_children(self._zhandle, path, watcher,
                                     functools.partial(generic_completion, 
                                                       pc, results))                               
        assert ok == zookeeper.OK
        pc.wait()
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
        results = []
        pc = utils.PipeCondition()        
        ok = zookeeper.aset(self._zhandle, path, data, version,
                                     functools.partial(generic_completion, 
                                                       pc, results))                               
        assert ok == zookeeper.OK
        pc.wait()
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
        results = []
        pc = utils.PipeCondition()        
        ok = zookeeper.aset_acl(self._zhandle, path, version, acl,
                                functools.partial(generic_completion, 
                                                  pc, results))                               
        assert ok == zookeeper.OK
        pc.wait()
        #unpack result as void_completion
        handle, rc = results
        assert handle == self._zhandle
        if rc == zookeeper.OK:
            return rc
        self._raise_exception(rc)
            
    def sync(self, path):
        """
        Flush leader channel.
        path: the name of the node. Expressed as a file name with slashes
        separating ancestors of the node.
         
        Returns OK on success.
        """
        results = []
        pc = utils.PipeCondition()        
        ok = zookeeper.async(self._zhandle, path,
                             functools.partial(generic_completion, 
                                               pc, results))                               
        assert ok == zookeeper.OK
        pc.wait()
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
                    zookeeper.CONNECTIONLOSS: zookeeper.ConnectionLossException,
                    zookeeper.DATAINCONSISTENCY: zookeeper.DataInconsistencyException,
                    zookeeper.INVALIDACL: zookeeper.InvalidACLException,
                    zookeeper.INVALIDCALLBACK: zookeeper.InvalidCallbackException,
                    zookeeper.INVALIDSTATE: zookeeper.InvalidStateException,
                    zookeeper.MARSHALLINGERROR: zookeeper.MarshallingErrorException,
                    zookeeper.NONODE: zookeeper.NoNodeException,
                    zookeeper.NOAUTH: zookeeper.NoAuthException,
                    zookeeper.NODEEXISTS:zookeeper.NodeExistsException,
                    zookeeper.NOCHILDRENFOREPHEMERALS:
                        zookeeper.NoChildrenForEphemeralsException,
                    zookeeper.NOTEMPTY: zookeeper.NotEmptyException,
                    zookeeper.NOTHING: zookeeper.NothingException,
                    zookeeper.OPERATIONTIMEOUT: zookeeper.OperationTimeoutException,
                    zookeeper.RUNTIMEINCONSISTENCY: zookeeper.RuntimeInconsistencyException,
                    zookeeper.SESSIONEXPIRED: zookeeper.SessionExpiredException,
                    zookeeper.SYSTEMERROR: zookeeper.SystemErrorException,
                    zookeeper.UNIMPLEMENTED: zookeeper.UnimplementedException,
                    }
    
        
def test():
    session = ZKSession("localhost:2181", 10)
    print 'connected'
    session.create("/test-tmp", "abc", [ZOO_OPEN_ACL_UNSAFE], zookeeper.EPHEMERAL)
    print 'test-tmp created'
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
    test_queue(session)


def test_queue(session):    
    import recipes
    q = recipes.ZKQueue(session, "/myqueue", [ZOO_OPEN_ACL_UNSAFE])
    q.enqueue("Zoo")
    q.enqueue("Keeper")
    
    def dequeue_thread():
        while True:
            value = q.dequeue()
            print "from dequeue", value
            if value == "EOF":
                return
    
    def enqueue_thread():
        for i in range(10):
            q.enqueue("value%i" % (i,))
            eventlet.sleep(1)
        q.enqueue("EOF")
    
    dt = eventlet.spawn(dequeue_thread)
    et = eventlet.spawn(enqueue_thread)
    
    
    et.wait()
    dt.wait()
    
    
if __name__=="__main__":
    test()
    