/* myio backend driven by a libxev event loop. Picks the io_uring backend at
 * runtime when the kernel allows it and falls back to epoll otherwise. File
 * opens and DNS lookups run on libxev's thread pool; await/select pump the
 * loop until completion.
 *
 * libxev's C bindings only cover its loop, timers and thread pool, so this
 * backend is implemented in Zig (src/myio_xev.zig) against libxev's native
 * API and exported with the C ABI; programs still see only this header. */
#ifndef MYIO_XEV_H
#define MYIO_XEV_H

#include "myio.h"

/* Returns NULL on allocation/loop-init failure. */
myio *myio_xev_new(void);

#endif /* MYIO_XEV_H */
