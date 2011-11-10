'''
To test the interaction between eventlet and zookeeper
@author: Yun Mao <yunmao@gmail.com>
'''

import eventlet

eventlet.monkey_patch()

import time
import zookeeper
import threading

import sys
import os
################
import logging
logger = logging.getLogger('zk')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
# create console handler and set level to debug
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

class Base(object):
    def __init__(self, addr, path, timeout=10):
        self.addr = addr
        self.path = path
        self.timeout = timeout # connection timeout (in seconds): default 10
        self.connected = False
        self.pipe = os.pipe()
        zookeeper.set_log_stream(open("/dev/null"))
        
        def watcher(handle, type, state, name):
            '''Fired when connected to ZK, return call from another thread
            '''
            logger.info('%s Connected to Zookeeper...' % self.__class__.__name__)
            #self.cv.acquire()
            self.connected = True #race !!
            retv = os.write(self.pipe[1], "connected")

        #self.cv.acquire()
        self.handle = zookeeper.init(self.addr, watcher, self.timeout*1000) # zk in ms
        print os.read(self.pipe[0], 1024)
        if not self.connected:
            logger.error("Connection to ZooKeeper timed out in %s seconds - server running on %s?" % (
                self.timeout, self.addr))
            raise RuntimeError("timeout in connecting to zookeeper")
        
        # create leader tree root node if it does not exist yet
        try:
            zookeeper.create(self.handle, self.path, 
                             '%s top level' % self.__class__.__name__, 
                             [OPEN_ACL_UNSAFE])
        except zookeeper.NodeExistsException:
            logger.info('%s tree root already exists, skip creation' % 
                        self.__class__.__name__)

class Queue(Base):
    '''
    queue model, support concurrent enqueue/dequeue
    '''
    def __init__(self, addr, path, timeout=10):
        Base.__init__(self, addr, path, timeout)

    def enqueue(self, val):
        '''
        concurrent enqueue
        '''
        zookeeper.create(self.handle, self.path + "/item-", val, 
                         [OPEN_ACL_UNSAFE], zookeeper.SEQUENCE)

    def _get_and_delete(self, path):
        '''
        try to get and delete a node from zookeeper
        if not-exist return None
        '''
        try:
            (data, stat) = zookeeper.get(self.handle, path, None)
            zookeeper.delete(self.handle, path, stat["version"])
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
        def watcher(handle, event, state, path):
            print "watcher in dequeue called!"
            os.write(self.pipe[1], "data")

        while True:    
            children = sorted(zookeeper.get_children(self.handle, self.path, watcher))
            for child in children:
                data = self._get_and_delete(self.path + "/" + child)
                if data is not None:
                    return data
            print 'about to block on pipe read'
            print os.read(self.pipe[0], 1024) 
            print 'done pipe read'           



def periodic_print(who):
    while 1:
        print 'sleeping 1 sec', who
        #time.sleep(1)
        eventlet.greenthread.sleep(1)

def twothreads():
    """create two green threads, print stuff
    """
    #handle = zookeeper.init("127.0.0.1:2813", None, 10*1000)
    handle = Base('localhost:2181', "/test-base")
    
    t1 = eventlet.greenthread.spawn(periodic_print, "first thread")
    t2 = eventlet.greenthread.spawn(periodic_print, "second thread")
    t1.wait()
    t2.wait()


queue = None
def dequeue():
    global queue
    logger.info("Consuming all items in queue")
    v = queue.dequeue()
    while v != 'EOF':
        logger.info(v)
        v = queue.dequeue()
    logger.info("Done")
    
def testqueue():
    global queue
    t2 = eventlet.greenthread.spawn(periodic_print, "print thread")
    queue = Queue('localhost:2181', "/test-queue")
    t1 = eventlet.greenthread.spawn(dequeue)
    #time.sleep(10)
    eventlet.greenthread.sleep(5)
    logger.info("Enqueuing three items")
    queue.enqueue("item 1")
    queue.enqueue("item 2")
    queue.enqueue("item 3")
    queue.enqueue('EOF')
    logger.info("Done")
    t1.wait()
    t2.wait()
    

def main():
    #twothreads()
    testqueue()
    
if __name__ == '__main__':
    main()