This is the package for zookeeper python bindings for eventlet threading model.

The default zookeeper python binding use OS threads for async completion 
and watcher notifications. This package uses pipes to communicate between
the main thread that runs eventlet and the completion threads to avoid
any blocking calls in the green threads.


