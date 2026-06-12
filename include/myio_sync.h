/* myio backend using plain blocking POSIX calls. Every operation completes
 * inside the submit call; await and select never block. */
#ifndef MYIO_SYNC_H
#define MYIO_SYNC_H

#include "myio.h"

/* Returns NULL on allocation failure. */
myio *myio_sync_new(void);

#endif /* MYIO_SYNC_H */
