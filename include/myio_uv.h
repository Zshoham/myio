/* myio backend driven by a libuv event loop. File operations run on libuv's
 * internal thread pool; await/select pump the loop until completion. */
#ifndef MYIO_UV_H
#define MYIO_UV_H

#include "myio.h"

/* Returns NULL on allocation/loop-init failure. */
myio *myio_uv_new(void);

#endif /* MYIO_UV_H */
