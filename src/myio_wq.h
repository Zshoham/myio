/* myio_wq.h - the intrusive FIFO shared by the C backends. Its defining
 * job is the per-socket write queue, but any order-preserving backlog fits
 * (libuv's accepted-but-unclaimed connection queue rides on it too).
 *
 * The interface's ordering contract (myio.h, "TCP networking") makes every
 * backend serialize the writes of one socket: only the FIFO head is being
 * sent at any moment, successors wait their turn, and cancel/teardown may
 * unlink any member. The pointer discipline behind that - tail append, head
 * pop with successor promotion, arbitrary unlink with tail fixup - is the
 * same in every backend and easy to get subtly wrong, so it lives here
 * once. What "arming a send" means, and all locking, stay with the backend.
 *
 * Usage: embed a myio_wq_node in the task struct and a myio_wq in the
 * socket struct, and recover the task from a node with myio_wq_item. A
 * write is armed in exactly two places: at submit, when myio_wq_push says
 * it became the head, and at completion, for the successor myio_wq_pop
 * returns. Zero-length writes take the same path: the ordering contract
 * covers completion order, not just wire bytes.
 *
 * Deliberately free of OS includes so the POSIX and Zephyr backends can all
 * share it. The Zig backend keeps its own copy of the same three-operation
 * shape (writePush / writePopHead / writeListRemove in myio_xev.zig).
 */
#ifndef MYIO_WQ_H
#define MYIO_WQ_H

#include <stddef.h>

typedef struct myio_wq_node {
    struct myio_wq_node *next;
} myio_wq_node;

typedef struct {
    myio_wq_node *head; /* the write being sent; NULL when the socket is idle */
    myio_wq_node *tail;
} myio_wq;

/* The task embedding `node` (which must be non-NULL) as `member`:
 * myio_wq_item(n, pool_task, wnode). */
#define myio_wq_item(node, type, member) \
    ((type *)(void *)((char *)(node) - offsetof(type, member)))

/* Append a write; returns 1 when it became the FIFO head - nothing was
 * outstanding, and the caller should arm its send now. */
static inline int myio_wq_push(myio_wq *q, myio_wq_node *n) {
    n->next = NULL;
    if (q->tail) {
        q->tail->next = n;
        q->tail = n;
        return 0;
    }
    q->head = q->tail = n;
    return 1;
}

/* Detach the finished (or torn-down) head; returns the successor that just
 * became head - the write to arm next - or NULL when the queue emptied. */
static inline myio_wq_node *myio_wq_pop(myio_wq *q) {
    myio_wq_node *n = q->head;
    q->head = n->next;
    if (!q->head)
        q->tail = NULL;
    n->next = NULL;
    return q->head;
}

/* Unlink an arbitrary member (canceled or freed before its turn); returns 1
 * if it was queued, 0 if not found. Removing the head does NOT arm the
 * successor - that is the caller's decision (a canceled head's successor is
 * armed; a closing socket's is not). */
static inline int myio_wq_remove(myio_wq *q, myio_wq_node *n) {
    myio_wq_node *prev = NULL;
    for (myio_wq_node *c = q->head; c; prev = c, c = c->next) {
        if (c != n)
            continue;
        if (prev)
            prev->next = c->next;
        else
            q->head = c->next;
        if (q->tail == c)
            q->tail = prev;
        n->next = NULL;
        return 1;
    }
    return 0;
}

#endif /* MYIO_WQ_H */
