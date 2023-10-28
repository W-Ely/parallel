# SParallel

Python Library to handle parallelizing I/O bound work with queues and threads.

Simplifies the technique of chaining the output of one function into the input of another function in a parallel fashion.

Any unhandled exception raised a thread stops all work and raises the exception in the main thread.  This is especially import for failing a lambda call on exception.

Library is lambda safe :]
