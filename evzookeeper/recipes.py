'''
Created on Nov 11, 2011

@author: maoy
'''
import functools

from evzookeeper import utils
import zookeeper

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


