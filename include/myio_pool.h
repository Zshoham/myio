/* myio backend running blocking POSIX calls on a pool of worker threads.
 * The pool starts empty and grows on demand, so a blocked operation never
 * starves a newly submitted one; threads are reclaimed at destroy. */
#ifndef MYIO_POOL_H
#define MYIO_POOL_H

#include "myio.h"

/* Returns NULL on allocation failure. */
myio *myio_pool_new(void);

#endif /* MYIO_POOL_H */
