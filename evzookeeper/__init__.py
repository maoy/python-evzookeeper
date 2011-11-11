import eventlet
import zookeeper
import os
import functools

from evzookeeper import utils

class ZKSession(object):
    
    def __init__(self, host, timeout=10):
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
        (clientid, passwd)
        clientid the id of a previously established session that this
        client will be reconnecting to. Clients can access the session id of an 
         established, valid,
        connection by calling zoo_client_id. If
        the specified clientid has expired, or if the clientid is invalid for 
        any reason, the returned zhandle_t will be invalid -- the zhandle_t 
        state will indicate the reason for failure (typically
        EXPIRED_SESSION_STATE).
        
        RETURNS:
        an integer handle. 
        If it fails to create a new zhandle the function throws an exception.
        """
        self._zhandle = None
        pc = utils.PipeCondition()
        def init_cb(handle, type_, state, name):
            '''Fired when connected to ZK, return call from another thread
            '''
            pc.notify()
        #self._zhandle = zookeeper.init(host, initcb, timeout*1000)
        self._zhandle = zookeeper.init(host, init_cb)
        pc.wait(timeout)
        
    def create(self, path, value, acl, flags=0):
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
        results = {}
        pc = utils.PipeCondition()
        def cb(handle, rc, value):
            results["value"] = value
            results["rc"] = rc
            pc.notify()
        ok = zookeeper.acreate(self._zhandle, path, value, acl, flags, cb)
        assert ok == zookeeper.OK
        pc.wait()
        rc = results["rc"]
        value = results["value"]
        if rc == zookeeper.OK:
            return value
        raise self.rc2exception[rc](zookeeper.zerror(rc))
        
    # translate return code to exception
    rc2exception = {zookeeper.APIERROR: zookeeper.ApiErrorException,
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
        

    def exists(self, path, watch=None):
        """checks the existence of a node in zookeeper.
    
        path: the name of the node. Expressed as a file name with slashes 
        separating ancestors of the node.
    
        (Subsequent parameters are optional)
    
        watch: if not None, a watch will be set at the server to notify the 
        client if the node changes. The watch will be set even if the node does not 
        exist. This allows clients to watch for nodes to appear.
    
    completion: the routine to invoke when the request completes. The 
     completion
    will be triggered with one of the following codes passed in as the rc 
     argument:
    OK operation completed successfully
    NONODE the node does not exist.
    NOAUTH the client does not have permission.
    
    data: the data that will be passed to the completion routine when the 
    function completes.
    OK on success or one of the following errcodes on failure:
    BADARGUMENTS - invalid input parameters
    INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE or 
     AUTH_FAILED_STATE
    MARSHALLINGERROR - failed to marshall a request; possibly, out of 
     memory
        """
        zookeeper.aexists()
        
    def get(self, path, watcher=None, bufferlen=1024*1024):
        """
        gets the data associated with a node synchronously.
    PARAMETERS:
    path the name of the node. Expressed as a file name with slashes 
    separating ancestors of the node.
    
    (subsequent parameters are optional)
    watcher if not None, a watch will be set at the server to notify 
    the client if the node changes.
    bufferlen: This value defaults to 1024*1024 - 1Mb. This method 
     returns 
    the minimum of bufferlen and the true length of the znode's 
     data. 
    RETURNS:
    the data associated with the node
    OK operation completed successfully
    NONODE the node does not exist.
    NOAUTH the client does not have permission.
    BADARGUMENTS - invalid input parameters
    INVALIDSTATE - zhandle state is either in 
     SESSION_EXPIRED_STATE or AUTH_FAILED_STATE
    MARSHALLINGERROR - failed to marshall a request; possibly, out 
     of memory
        """
        zookeeper.aget()
        raise zookeeper.NoNodeException
    def get_acl(self, path):
        zookeeper.aget_acl()
        
    def get_children(self, path):
        zookeeper.aget_children()
    
    def set(self):
        zookeeper.aset()
    
    def set_acl(self):
        zookeeper.aset_acl()
        
    def sync(self):
        zookeeper.async()
        zookeeper.get()
        
    def close(self):
        if self._zhandle is not None:
            zookeeper.close(self._zhandle)
            # if closed successfully
            self._zhandle = None
    
    def __del__(self):
        self.close()

        
def test():
    x = ZKSession("localhost:2181", 10)
    print 'done.'
    
if __name__=="__main__":
    test()
    