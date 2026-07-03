/* myio backend running blocking Zephyr calls on a fixed pool of k_thread
 * workers - the Zephyr port of the desktop thread-pool backend. Sockets
 * block on the worker (woken through a per-worker eventfd when canceled);
 * file operations are blocking fs_* calls; sleeps are cancelable waits.
 * Workers and their stacks are static, sized by CONFIG_MYIO_POOL_WORKERS
 * and CONFIG_MYIO_POOL_STACK_SIZE: unlike the desktop pool the pool cannot
 * grow, so size the worker count for the maximum number of concurrently
 * blocking operations, like any static RTOS resource. */
#ifndef MYIO_ZEPHYR_POOL_H
#define MYIO_ZEPHYR_POOL_H

#include "myio.h"

/* Returns the (single, static) instance, or NULL if it is already in use
 * or setup failed. Release with myio_destroy() before creating it again. */
myio *myio_zephyr_pool_new(void);

#endif /* MYIO_ZEPHYR_POOL_H */
