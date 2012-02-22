import eventlet

from evzookeeper import ZKSession, recipes, ZOO_OPEN_ACL_UNSAFE

def demo_queue():
    session = ZKSession("localhost:2181", timeout=10)
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
    
if __name__ == "__main__":
    demo_queue()
