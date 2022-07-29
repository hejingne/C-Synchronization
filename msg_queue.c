/*
 * This code is provided solely for the personal and private use of students
 * taking the CSC369H course at the University of Toronto. Copying for purposes
 * other than this use is expressly prohibited. All forms of distribution of
 * this code, including but not limited to public repositories on GitHub,
 * GitLab, Bitbucket, or any other online platform, whether as given or with
 * any changes, are expressly prohibited.
 *
 * Authors: Alexey Khrabrov, Andrew Pelegris, Karen Reid
 *
 * All of the files in this directory and all subdirectories are:
 * Copyright (c) 2019, 2020 Karen Reid
 */

/**
 * CSC369 Assignment 2 - Message queue implementation.
 *
 * You may not use the pthread library directly. Instead you must use the
 * functions and types available in sync.h.
 */

#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include "errors.h"
#include "list.h"
#include "msg_queue.h"
#include "ring_buffer.h"


// Message queue implementation backend
typedef struct mq_backend {
	// Ring buffer for storing the messages
	ring_buffer buffer;

	// Reference count
	size_t refs;

	// Number of handles open for reads
	size_t readers;
	// Number of handles open for writes
	size_t writers;

	// Set to true when all the reader handles have been closed. Starts false
	// when they haven't been opened yet.
	bool no_readers;
	// Set to true when all the writer handles have been closed. Starts false
	// when they haven't been opened yet.
	bool no_writers;

	//TODO: add necessary synchronization primitives, as well as data structures
	//      needed to implement the msg_queue_poll() functionality

	cond_t empty;
	cond_t filled;
	mutex_t mutex;

	/**
 * Requested events.
 *
 * A bitwise OR of MQPOLL_* constants. Set by the user before calling
 * msg_queue_poll() and describes what events are subscribed to.
 */
	int flags;

	list_head wait_q_head;

} mq_backend;


typedef struct wait_q_node {
	cond_t pollable;
	mutex_t mutex;
	int flags;
	list_entry entry;
} wait_q_node;


static int mq_init(mq_backend *mq, size_t capacity)
{
	if (ring_buffer_init(&mq->buffer, capacity) < 0) {
		return -1;
	}

	mq->refs = 0;

	mq->readers = 0;
	mq->writers = 0;

	mq->no_readers = false;
	mq->no_writers = false;

	//TODO: initialize remaining fields (synchronization primitives, etc.)
	cond_init(&mq->empty);
	cond_init(&mq->filled);
	mutex_init(&mq->mutex);
	list_init(&mq->wait_q_head);

	return 0;
}

static void mq_destroy(mq_backend *mq)
{
	assert(mq->refs == 0);
	assert(mq->readers == 0);
	assert(mq->writers == 0);

	ring_buffer_destroy(&mq->buffer);

	//TODO: cleanup remaining fields (synchronization primitives, etc.)
	cond_destroy(&mq->empty);
	cond_destroy(&mq->filled);
	mutex_destroy(&mq->mutex);
	list_destroy(&mq->wait_q_head);
}


#define ALL_FLAGS (MSG_QUEUE_READER | MSG_QUEUE_WRITER | MSG_QUEUE_NONBLOCK)

// Message queue handle is a combination of the pointer to the queue backend and
// the handle flags. The pointer is always aligned on 8 bytes - its 3 least
// significant bits are always 0. This allows us to store the flags within the
// same word-sized value as the pointer by ORing the pointer with the flag bits.

// Get queue backend pointer from the queue handle
static mq_backend *get_backend(msg_queue_t queue)
{
	mq_backend *mq = (mq_backend*)(queue & ~ALL_FLAGS);
	assert(mq);
	return mq;
}

// Get handle flags from the queue handle
static int get_flags(msg_queue_t queue)
{
	return (int)(queue & ALL_FLAGS);
}

// Create a queue handle for given backend pointer and handle flags
static msg_queue_t make_handle(mq_backend *mq, int flags)
{
	assert(((uintptr_t)mq & ALL_FLAGS) == 0);
	assert((flags & ~ALL_FLAGS) == 0);
	return (uintptr_t)mq | flags;
}


static msg_queue_t mq_open(mq_backend *mq, int flags)
{
	++mq->refs;

	if (flags & MSG_QUEUE_READER) {
		++mq->readers;
		mq->no_readers = false;
	}
	if (flags & MSG_QUEUE_WRITER) {
		++mq->writers;
		mq->no_writers = false;
	}

	return make_handle(mq, flags);
}

// Returns true if this was the last handle
static bool mq_close(mq_backend *mq, int flags)
{
	assert(mq->refs != 0);
	assert(mq->refs >= mq->readers);
	assert(mq->refs >= mq->writers);

	if ((flags & MSG_QUEUE_READER) && (--mq->readers == 0)) {
		mq->no_readers = true;
	}
	if ((flags & MSG_QUEUE_WRITER) && (--mq->writers == 0)) {
		mq->no_writers = true;
	}

	if (--mq->refs == 0) {
		assert(mq->readers == 0);
		assert(mq->writers == 0);
		return true;
	}
	return false;
}


msg_queue_t msg_queue_create(size_t capacity, int flags)
{
	if (flags & ~ALL_FLAGS) {
		errno = EINVAL;
		report_error("msg_queue_create");
		return MSG_QUEUE_NULL;
	}

	// Refuse to create a message queue without capacity for
	// at least one message (length + 1 byte of message data).
	if (capacity < sizeof(size_t) + 1) {
		errno = EINVAL;
		report_error("msg_queue_create");
		return MSG_QUEUE_NULL;
	}

	mq_backend *mq = (mq_backend*)malloc(sizeof(mq_backend));
	if (!mq) {
		report_error("malloc");
		return MSG_QUEUE_NULL;
	}
	// Result of malloc() is always aligned on 8 bytes, allowing us to use the
	// 3 least significant bits of the handle to store the 3 bits of flags
	assert(((uintptr_t)mq & ALL_FLAGS) == 0);

	if (mq_init(mq, capacity) < 0) {
		// Preserve errno value that can be changed by free()
		int e = errno;
		free(mq);
		errno = e;
		return MSG_QUEUE_NULL;
	}

	return mq_open(mq, flags);
}

msg_queue_t msg_queue_open(msg_queue_t queue, int flags)
{
	if (!queue) {
		errno = EBADF;
		report_error("msg_queue_open");
		return MSG_QUEUE_NULL;
	}

	if (flags & ~ALL_FLAGS) {
		errno = EINVAL;
		report_error("msg_queue_open");
		return MSG_QUEUE_NULL;
	}

	mq_backend *mq = get_backend(queue);

	//TODO: add necessary synchronization
	mutex_lock(&mq->mutex);

	msg_queue_t new_handle = mq_open(mq, flags);

	mutex_unlock(&mq->mutex);

	return new_handle;
}

int msg_queue_close(msg_queue_t *queue)
{
	if (!queue || !*queue) {
		errno = EBADF;
		report_error("msg_queue_close");
		return -1;
	}

	mq_backend *mq = get_backend(*queue);

	//TODO: add necessary synchronization
	mutex_lock(&mq->mutex);

	if (mq_close(mq, get_flags(*queue))) {
		mutex_unlock(&mq->mutex);
		// Closed last handle; destroy the queue
		mq_destroy(mq);
		free(mq);
		*queue = MSG_QUEUE_NULL;
		return 0;
	}

	//TODO: if this is the last reader (or writer) handle, notify all the writer
	//      (or reader) threads currently blocked in msg_queue_write() (or
	//      msg_queue_read()) and msg_queue_poll() calls for this queue.
	if (mq->readers == 0) {
		mq->flags = mq->flags | MQPOLL_WRITABLE | MQPOLL_NOREADERS;
		cond_broadcast(&mq->filled);
	}

	if (mq->writers == 0) {
		mq->flags = mq->flags | MQPOLL_READABLE | MQPOLL_NOWRITERS;
		cond_broadcast(&mq->empty);
	}

	mutex_unlock(&mq->mutex);

	*queue = MSG_QUEUE_NULL;
	return 0;
}

void prep_node_for_poll(mq_backend *mq) {
	list_entry *cur;
	list_for_each(cur, &mq->wait_q_head) {
		struct wait_q_node *node;
		node = container_of(cur, struct wait_q_node, entry);
		if (mq->flags & node->flags) {
			mutex_lock(&node->mutex);
			cond_signal(&node->pollable);
			mutex_unlock(&node->mutex);
		}
	}
}


ssize_t msg_queue_read(msg_queue_t queue, void *buffer, size_t length)
{
	//TODO

	// Error - EBADF  if the message queue handle is not valid for reads
	if (!(get_flags(queue) & MSG_QUEUE_READER)) {
		errno = EBADF;
		report_error("handles error on Read");
		return -1;
	}

	mq_backend *mq = get_backend(queue);

	// 1. Blocks until the queue contains at least one message,
	// 		unless the handle is open in non-blocking mode
	mutex_lock(&mq->mutex);

	while (!(get_flags(queue) & MSG_QUEUE_NONBLOCK) &&
					ring_buffer_used(&mq->buffer) == 0 &&
					!mq->no_writers) {
		cond_wait(&mq->filled, &mq->mutex);
	}

	/* While loop exits */

	// Error - EAGAIN  if the queue handle is non-blocking and the read would block
	//        because there is no message in the queue to read
	if ((get_flags(queue) & MSG_QUEUE_NONBLOCK) &&
			 ring_buffer_used(&mq->buffer) == 0) {
		errno = EAGAIN;
		report_info("blocking error on Read");
		mutex_unlock(&mq->mutex);
		return -1;
	}

	// 2. Read begins

	// Nothing to read - Return 0 if the queue is empty and
	//									 all the writer handles to the queue have been closed (eof)
	if (ring_buffer_used(&mq->buffer) == 0 && mq->no_writers) {
			mq->flags = mq->flags | MQPOLL_NOWRITERS;	// request event
			prep_node_for_poll(mq);	// iterate over wait queue to prepare waiting node for poll
			mutex_unlock(&mq->mutex);
			return 0;
	}

	// Error - EMSGSIZE  if the buffer is not large enough to hold the message
	size_t peak_count;
	if (!ring_buffer_peek(&mq->buffer, &peak_count, sizeof(size_t))) {	// ring buffer sanity check
		errno = EMSGSIZE;
		report_error("error on Ring buffer peek");
		mutex_unlock(&mq->mutex);
		return -1;
	}
	if(length < peak_count) {
		errno = EMSGSIZE;
		report_info("size error on Read");
		mutex_unlock(&mq->mutex);
		return ~peak_count;
	}

	// 2.1. First read the size of the msg
	size_t read_count;
	if (!ring_buffer_read(&mq->buffer, &read_count, sizeof(size_t))) {	// rb sanity check
		errno = EMSGSIZE;
		report_error("error on Ring buffer read count");
		mutex_unlock(&mq->mutex);
		return -1;
	}
	// 2.2. Then read <read_count> bytes from the msg queue
	if (!ring_buffer_read(&mq->buffer, &buffer, read_count)) {	// rb sanity check
		errno = EMSGSIZE;
		report_error("error on Ring buffer read value");
		mutex_unlock(&mq->mutex);
		return -1;
	}

	// 2.3. Signal after successful reads
	cond_signal(&mq->empty);

	// 2.4 Mark the queue as writable after successful reads if
	//			there are writers waiting and if there is empty space in queue
	if (!mq->no_writers && ring_buffer_free(&mq->buffer) > 0) {
		mq->flags = mq->flags | MQPOLL_WRITABLE;
		prep_node_for_poll(mq);
	}

	// 2.5. Finally give up lock and return message size on success
	mutex_unlock(&mq->mutex);
	return read_count;
}


int msg_queue_write(msg_queue_t queue, const void *buffer, size_t length)
{
	//TODO

	// Error - EBADF  if the message queue handle is not valid for writes
	if (!(get_flags(queue) & MSG_QUEUE_WRITER)) {
		errno = EBADF;
		report_error("handles error on Write");
		return -1;
	}

	// Error - EINVAL  if message is of length zero
	if (length == 0) {
		errno = EINVAL;
		report_error("msg length error on Write");
		return -1;
	}

	mq_backend *mq = get_backend(queue);
	size_t total_write_size = sizeof(size_t) + length;
	// Error - EMSGSIZE  if capacity of the queue is not large enough for the message
	if (mq->buffer.size < total_write_size) {
		errno = EMSGSIZE;
		report_error("queue capacity error on Write");
		return -1;
	}

	// 1. Blocks until the queue has enough free space for the message,
	// 		unless the handle is open in non-blocking mode
	mutex_lock(&mq->mutex);

	while (!(get_flags(queue) & MSG_QUEUE_NONBLOCK) &&
					ring_buffer_free(&mq->buffer) < total_write_size &&
					!mq->no_readers) {
		cond_wait(&mq->empty, &mq->mutex);
	}

	/* While loop exits */

	// Error - EAGAIN  if the queue handle is non-blocking and the write would block
  //         because there is not enough space in the queue to write message
	if ((get_flags(queue) & MSG_QUEUE_NONBLOCK) &&
			 ring_buffer_free(&mq->buffer) < total_write_size) {
		errno = EAGAIN;
		report_info("blocking error on Write");
		mutex_unlock(&mq->mutex);
		return -1;
	}

	// Error - EPIPE  if all reader handles to the queue have been closed ("broken pipe")
	if (mq->no_readers) {
			mq->flags = mq->flags | MQPOLL_NOREADERS;
			prep_node_for_poll(mq);
			errno = EPIPE;
			report_info("Broken pipe on Write");
			mutex_unlock(&mq->mutex);
			return -1;
	}

	// 2. Write begins

	// Error - EMSGSIZE  if there is not enough space in the queue to write message
	if (ring_buffer_free(&mq->buffer) < total_write_size) {
		errno = EMSGSIZE;
		report_error("queue leftover space error on Write");
		mutex_unlock(&mq->mutex);
		return -1;
	}


	// 2.1. First write the size of the msg
	if (!ring_buffer_write(&mq->buffer, &length, sizeof(size_t))) {	// rb sanity check
		errno = EMSGSIZE;
		report_error("error on Ring buffer write count");
		mutex_unlock(&mq->mutex);
		return -1;
	}
	// 2.2. Then write <length> bytes from buffer into the msg queue
	if (!ring_buffer_write(&mq->buffer, &buffer, length)) {	// rb sanity check
		errno = EMSGSIZE;
		report_error("error on Ring buffer write value");
		mutex_unlock(&mq->mutex);
		return -1;
	}

	// 2.3. Signal after successful writes
	cond_signal(&mq->filled);

	// 2.4 Mark the queue as readable after successful writes if
	// 		 there are readers waiting and if there is no empty space in queue
	if (!mq->no_readers && ring_buffer_free(&mq->buffer) == 0) {
		mq->flags = mq->flags | MQPOLL_READABLE;
		prep_node_for_poll(mq);
	}

	// 2.5. Finally give up lock and return 0 on success
	mutex_unlock(&mq->mutex);
	return 0;
}

int msg_queue_poll(msg_queue_pollfd *fds, size_t nfds)
{
	//TODO

	int null_q_counter = 0;
	for (int i = 0; i < (int)nfds; i++) {
		msg_queue_pollfd fd = fds[i];

		if (fd.queue == MSG_QUEUE_NULL) {
			fd.revents = 0;
			null_q_counter++;
			continue;
		}

		if (fd.events == 0 || (
			 !(fd.events & MQPOLL_READABLE) &&
			 !(fd.events & MQPOLL_WRITABLE) &&
			 !(fd.events & MQPOLL_NOREADERS) &&
			 !(fd.events & MQPOLL_NOWRITERS) )) {
			errno = EINVAL;
			report_error("invalid events fields");
			return -1;
		}

		if ((fd.events & MQPOLL_READABLE && !(get_flags(fd.queue) & MSG_QUEUE_READER)) ||
				(fd.events & MQPOLL_WRITABLE && !(get_flags(fd.queue) & MSG_QUEUE_WRITER))) {
			errno = EINVAL;
			report_error("invalid events requested for queue handle");
			return -1;
		}
	}
	if ((int)nfds == null_q_counter) {
		errno = EINVAL;
		report_error("no subscribed events");
		return -1;
	}



	wait_q_node *poll_q = (wait_q_node *) malloc(nfds * sizeof(wait_q_node));
	mutex_init(&poll_q->mutex);

	mutex_lock(&poll_q->mutex);
	int ready_q_counter = 0;
	for (int i = 0; i < (int)nfds; i++) {
		msg_queue_pollfd fd = fds[i];
		wait_q_node poll_fd = poll_q[i];

		if (fd.queue != MSG_QUEUE_NULL) {
			mq_backend *mq = get_backend(fd.queue);
			list_entry_init(&poll_fd.entry);

			mutex_lock(&mq->mutex);
			list_add_tail(&mq->wait_q_head, &poll_fd.entry);

			fd.revents = mq->flags & fd.events;
			if (fd.revents != 0) {
				ready_q_counter++;
			}

			list_del(&mq->wait_q_head, &poll_fd.entry);
			mutex_unlock(&mq->mutex);
		}
	}
	mutex_unlock(&poll_q->mutex);

	mutex_destroy(&poll_q->mutex);
	free(poll_q);
	
	return ready_q_counter;
}
