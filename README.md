# C-Synchronization
Implement a message queue by adding the necessary `locks` and `condition variable` operations to synchronize access to the message queue. You will also implement I/O multiplexing functionality - monitoring multiple queues for events (e.g. new messages) in a single thread - modeled after the poll() system call.

## Introduction
A message queue is very similar to a pipe. The key difference is that a pipe is a stream of bytes, and a message queue stores distinct messages by first storing the message length (as a size_t type), and then the message itself. This means that a receiver retrieves a message by first reading the size of the message, and then reading size bytes from the message queue.
