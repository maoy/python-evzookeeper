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


import errno
import os

from eventlet import greenio
from eventlet.hubs import trampoline
from eventlet.support import get_errno


class _SocketDuckForFdTimeout(greenio._SocketDuckForFd):
    """Enhance SocketDuckForFd with timeout"""

    def __init__(self, fileno):
        self._timeout = None
        super(_SocketDuckForFdTimeout, self).__init__(fileno)

    def recv(self, buflen):
        trampoline(self, read=True, timeout=self._timeout)
        while True:
            try:
                return os.read(self._fileno, buflen)
            except OSError, e:
                if get_errno(e) != errno.EAGAIN:
                    raise IOError(*e.args)


class TimeoutGreenPipe(greenio.GreenPipe):
    """Improve the GreenPipe read() method with timeout"""

    def __init__(self, f, mode='r', bufsize=-1):
        if not isinstance(f, (basestring, int, file)):
            raise TypeError('f(ile) should be int, str, unicode or file, '
                            'not %r' % f)
        if isinstance(f, basestring):
            f = open(f, mode, 0)
        if isinstance(f, int):
            fileno = f
            self._name = "<fd:%d>" % fileno
        else:
            fileno = os.dup(f.fileno())
            self._name = f.name
            if f.mode != mode:
                raise \
                    ValueError('file.mode %r does not match mode parameter %r'
                                % (f.mode, mode))
            self._name = f.name
            f.close()

        greenio._fileobject.__init__(self, _SocketDuckForFdTimeout(fileno),
                                     mode, bufsize)
        greenio.set_nonblocking(self)
        self.softspace = 0

    def set_timeout(self, timeout):
        """set timeout in seconds before read"""
        self._sock._timeout = timeout


class PipeConditionClosedError(Exception):
    pass


class PipeCondition(object):
    '''A data structure similar in spirit to condition variable
    implemented using pipes.

    Typical usage with eventlet:

    create the object in the main thread, call wait(), then
    call notify() in another OS thread.

    Right now notify() can only be used once.
    '''

    def __init__(self):
        rfd, wfd = os.pipe()
        self._wfd = wfd
        self._greenpipe = TimeoutGreenPipe(rfd, 'rb', 0)

    def notify(self, quiet=True):
        """Notify the other OS thread.

        @param quiet: if True, do not raise any exceptions.
        """
        try:
            if self._wfd is not None:
                #write an arbitrary byte
                os.write(self._wfd, "X")
        except IOError, e:
            if e.errno == errno.EPIPE:
                # the waiter fd is closed
                pass
            else:
                # TODO(maoy): probably need to retry certain errors
                if not quiet:
                    raise e

    def wait(self, timeout=None):
        """
        @param timeout: in second
        @return: None if notified within timeout
        or raise an exception of eventlet.timeout.Timeout
        """
        try:
            self._greenpipe.set_timeout(timeout)
        except AttributeError:
            raise PipeConditionClosedError()
        value = self._greenpipe.read(1)
        if len(value) == 0:
            raise PipeConditionClosedError()
        assert value == "X"

    def _close_wfd(self):
        if self._wfd is not None:
            os.close(self._wfd)
            self._wfd = None

    def _close_rfd(self):
        if self._greenpipe is not None:
            self._greenpipe.close()
            self._greenpipe = None

    def close(self):
        try:
            self._close_wfd()
        finally:
            self._close_rfd()
        
    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class StatePipeCondition(PipeCondition):
    '''Typical usage with eventlet:
    Create the object in the main thread, call wait_and_get(), then
     in another OS thread, call notify(state=state).
    '''
    def __init__(self):
        PipeCondition.__init__(self)
        self._state = None

    def set_and_notify(self, state, quiet=True):
        """Set state and notify the other OS thread.

        @param quiet: if True, do not raise any exceptions.
        """
        self._state = state
        return PipeCondition.notify(self, quiet=quiet)

    def wait_and_get(self, timeout=None):
        self.wait(timeout)
        return self._state
