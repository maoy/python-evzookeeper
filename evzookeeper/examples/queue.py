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

import eventlet
from evzookeeper import queue
from evzookeeper import ZKSession
from evzookeeper import ZOO_OPEN_ACL_UNSAFE


def demo_queue():
    session = ZKSession("localhost:2181", timeout=10)
    q = queue.ZKQueue(session, "/myqueue", [ZOO_OPEN_ACL_UNSAFE])
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

if __name__ == "__main__":
    demo_queue()
