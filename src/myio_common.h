/* myio_common.h - shared scaffolding for the POSIX C backends (sync, pool,
 * uring). NOT part of the public API and NOT installed: it only collects the
 * byte-for-byte duplicated helpers those backends grew independently - result
 * constructors, the getaddrinfo error mapping, the connect/listen walks, and
 * the local-port and error-string boilerplate. Every helper is `static inline`
 * (the simplest linkage for an internal header compiled into each backend .c).
 *
 * The zsock_* Zephyr backends deliberately do NOT use this - their socket
 * symbols differ - and the libuv/libxev backends have their own error models.
 */
#ifndef MYIO_COMMON_H
#define MYIO_COMMON_H

#include "myio.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ---- result constructors ---- */

static inline myio_result r_ok(int64_t value, void *ptr) {
    myio_result r = { MYIO_OK, value, 0, ptr };
    return r;
}
static inline myio_result r_err(int err) {
    myio_result r = { MYIO_ERROR, 0, err, NULL };
    return r;
}
static inline myio_result r_canceled(void) {
    myio_result r = { MYIO_CANCELED, 0, 0, NULL };
    return r;
}
/* For a raw syscall return: negative means failure, errno carries the code. */
static inline myio_result r_from(int64_t n) {
    return n >= 0 ? r_ok(n, NULL) : r_err(errno);
}

/* ---- getaddrinfo error mapping ---- */

/* getaddrinfo has its own (negative) error namespace; keep it verbatim and let
 * error_str render it honestly. EAI_SYSTEM defers to errno. */
static inline int gai_errno(int rc) {
    return rc == EAI_SYSTEM ? errno : rc;
}

/* ---- POSIX TCP connect / listen ---- */

/* Resolve `host`:`port` and connect to the first address that accepts.
 * Returns a connected fd, or -1 with *err set (errno, or a getaddrinfo EAI_*
 * code kept verbatim). */
static inline int myio_posix_connect(const char *host, int port, int *err) {
    char service[16];
    snprintf(service, sizeof service, "%d", port);
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    int rc = getaddrinfo(host, service, &hints, &res);
    if (rc != 0) {
        *err = gai_errno(rc);
        return -1;
    }
    int fd = -1;
    *err = ECONNREFUSED;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            *err = errno;
            continue;
        }
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        *err = errno;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

/* Resolve a numeric `host`:`port`, create a listening socket (SO_REUSEADDR),
 * bind and listen. Returns the listening fd, or -1 with *err set (errno, or a
 * getaddrinfo EAI_* code kept verbatim). *err is left untouched when err is
 * NULL. */
static inline int myio_posix_listen(const char *host, int port, int backlog,
                                    int *err) {
    char service[16];
    snprintf(service, sizeof service, "%d", port);
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE | AI_NUMERICHOST;
    struct addrinfo *res = NULL;
    int rc = getaddrinfo(host, service, &hints, &res);
    if (rc != 0) {
        if (err)
            *err = gai_errno(rc);
        return -1;
    }
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    int one = 1;
    if (fd >= 0)
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (fd < 0 || bind(fd, res->ai_addr, res->ai_addrlen) != 0 ||
        listen(fd, backlog) != 0) {
        if (err)
            *err = errno;
        if (fd >= 0)
            close(fd);
        freeaddrinfo(res);
        return -1;
    }
    freeaddrinfo(res);
    return fd;
}

/* ---- local port of a socket fd ---- */

/* The local port bound to `fd`, host byte order, or -1 on error / a family
 * that is neither AF_INET nor AF_INET6. */
static inline int myio_local_port(int fd) {
    struct sockaddr_storage ss;
    socklen_t len = sizeof ss;
    if (getsockname(fd, (struct sockaddr *)&ss, &len) != 0)
        return -1;
    if (ss.ss_family == AF_INET)
        return ntohs(((struct sockaddr_in *)&ss)->sin_port);
    if (ss.ss_family == AF_INET6)
        return ntohs(((struct sockaddr_in6 *)&ss)->sin6_port);
    return -1;
}

/* ---- error strings ---- */

/* Negative codes are getaddrinfo's EAI_* range (kept verbatim by connect/
 * listen); everything else is errno-style. */
static inline const char *myio_default_error_str(int err) {
    return err < 0 ? gai_strerror(err) : strerror(err);
}

#endif /* MYIO_COMMON_H */
