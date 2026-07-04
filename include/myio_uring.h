/* myio backend implemented directly on Linux io_uring (via liburing) — no
 * event-loop library in between. Linux-only. */
#ifndef MYIO_URING_H
#define MYIO_URING_H

#include "myio.h"

/* Returns NULL on allocation failure or when io_uring is unavailable:
 * pre-5.6 kernel, kernel.io_uring_disabled, or a seccomp policy denying the
 * io_uring syscalls (Docker's default profile does). Callers can fall back
 * to another backend. */
myio *myio_uring_new(void);

#endif /* MYIO_URING_H */
