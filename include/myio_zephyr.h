/* myio backend for Zephyr RTOS. Sockets and timers are genuinely
 * asynchronous, driven by a hand-rolled zsock_poll() readiness loop inside
 * await/select; file operations complete inside the submit call (Zephyr has
 * no async file IO, and flash-backed filesystem calls are fast), which the
 * myio execution model explicitly sanctions. Tasks come from a fixed
 * k_mem_slab sized by CONFIG_MYIO_MAX_TASKS, so submissions can genuinely
 * return NULL on exhaustion (myio_join maps that to ENOMEM). */
#ifndef MYIO_ZEPHYR_H
#define MYIO_ZEPHYR_H

#include "myio.h"

/* Returns the (single, static) instance, or NULL if it is already in use
 * or setup failed. Release with myio_destroy() before creating it again. */
myio *myio_zephyr_new(void);

#endif /* MYIO_ZEPHYR_H */
