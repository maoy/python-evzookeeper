'''
Created on Nov 10, 2011

@author: maoy
'''

from eventlet import greenio
import os
import errno
from eventlet.hubs import trampoline
from eventlet.support import get_errno


class SocketDuckForFdTimeout(greenio._SocketDuckForFd):
    """
    enhance SocketDuckForFd with timeout
    """
    def __init__(self, fileno):
        self._timeout = None
        super(SocketDuckForFdTimeout, self).__init__(fileno)
    def recv(self, buflen):
        trampoline(self, read=True, timeout=self._timeout)
        while True:
            try:
                data = os.read(self._fileno, buflen)
                print 'in recv after os.read'
                return data
            except OSError, e:
                if get_errno(e) != errno.EAGAIN:
                    raise IOError(*e.args)


class TimeoutGreenPipe(greenio.GreenPipe):
    """read method with timeout"""

    def __init__(self, f, mode='r', bufsize=-1):
        if not isinstance(f, (basestring, int, file)):
            raise TypeError('f(ile) should be int, str, unicode or file, not %r' % f)

        if isinstance(f, basestring):
            f = open(f, mode, 0)
 
        if isinstance(f, int):
            fileno = f
            self._name = "<fd:%d>" % fileno
        else:
            fileno = os.dup(f.fileno())
            self._name = f.name
            if f.mode != mode:
                raise ValueError('file.mode %r does not match mode parameter %r' % (f.mode, mode))
            self._name = f.name
            f.close()

        greenio._fileobject.__init__(self, SocketDuckForFdTimeout(fileno), 
                                     mode, bufsize)
        greenio.set_nonblocking(self)
        self.softspace = 0
    
    def set_timeout(self, timeout):
        self._sock._timeout = timeout


class PipeCondition(object):
    '''
    A condition-variable like data structure implemented using pipes
    '''

    def __init__(self):
        rfd, wfd = os.pipe()
        self._wfd = wfd
        self._greenpipe = TimeoutGreenPipe(rfd, 'rb', 0)
        
    def notify(self):
        os.write(self._wfd, "X") #write an arbitrary byte
        self._close_wfd()

    def wait(self, timeout=None):
        """
        timeout: in second
        return None if notified within timeout
        or raise an exception eventlet.timeout.Timeout
        """
        self._greenpipe.set_timeout(timeout)
        value = self._greenpipe.read(1)
        assert value=="X"

    def _close_wfd(self):
        if self._wfd is not None:
            os.close(self._wfd)
            self._wfd = None
    
    def _close_rfd(self):
        if self._greenpipe is not None:
            self._greenpipe.close()
            self._greenpipe = None

    def __del__(self):
        try:
            self._close_wfd()
        finally:
            self._close_rfd()
        
