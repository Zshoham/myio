//! libxev-backed myio implementation.
//!
//! Implements the `myio_ops` vtable from include/myio.h against libxev's
//! native Zig API (the official C bindings only expose the loop, timers and
//! the thread pool - TCP and file IO are Zig-only). The implementation is
//! generic over the libxev backend type and is instantiated for io_uring and
//! epoll; `myio_xev_new` picks io_uring at runtime when the kernel supports
//! it and falls back to epoll. Each instantiation carries its own vtable, so
//! the choice is invisible to callers.
//!
//! Operations libxev has no event-loop primitive for - open(2) and DNS
//! resolution - run as blocking calls on libxev's thread pool and complete
//! the task through an xev.Async wakeup, mirroring what libuv does for the
//! same operations internally.

const std = @import("std");
const builtin = @import("builtin");
const xevpkg = @import("xev");

const alloc = std.heap.c_allocator;
const E = std.posix.E;

fn eint(e: E) c_int {
    return @intFromEnum(e);
}

// === C ABI mirror of include/myio.h ====================================

const Status = enum(c_int) { pending = 0, ok = 1, err = 2, canceled = 3 };

const Result = extern struct {
    status: Status = .pending,
    value: i64 = 0,
    err: c_int = 0,
    ptr: ?*anyopaque = null,
};

const MyioTask = opaque {};
const MyioSock = opaque {};
const SpawnFn = *const fn (?*anyopaque) callconv(.c) i64;

const Myio = extern struct { ops: *const Ops };

const Ops = extern struct {
    open: *const fn (*Myio, [*:0]const u8, c_int, c_int) callconv(.c) ?*MyioTask,
    close: *const fn (*Myio, i64) callconv(.c) ?*MyioTask,
    read: *const fn (*Myio, i64, ?*anyopaque, usize, i64) callconv(.c) ?*MyioTask,
    write: *const fn (*Myio, i64, ?*const anyopaque, usize, i64) callconv(.c) ?*MyioTask,
    sleep: *const fn (*Myio, u64) callconv(.c) ?*MyioTask,
    spawn: *const fn (*Myio, SpawnFn, ?*anyopaque) callconv(.c) ?*MyioTask,
    tcp_connect: *const fn (*Myio, [*:0]const u8, c_int) callconv(.c) ?*MyioTask,
    tcp_listen: *const fn (*Myio, [*:0]const u8, c_int, c_int, ?*c_int) callconv(.c) ?*MyioSock,
    tcp_accept: *const fn (*Myio, *MyioSock) callconv(.c) ?*MyioTask,
    sock_read: *const fn (*Myio, *MyioSock, ?*anyopaque, usize) callconv(.c) ?*MyioTask,
    sock_write: *const fn (*Myio, *MyioSock, ?*const anyopaque, usize) callconv(.c) ?*MyioTask,
    sock_close: *const fn (*Myio, *MyioSock) callconv(.c) ?*MyioTask,
    sock_port: *const fn (*Myio, *MyioSock) callconv(.c) c_int,
    @"await": *const fn (*Myio, *MyioTask) callconv(.c) Result,
    cancel: *const fn (*Myio, *MyioTask) callconv(.c) c_int,
    select: *const fn (*Myio, [*]?*MyioTask, usize) callconv(.c) isize,
    error_str: *const fn (*Myio, c_int) callconv(.c) [*:0]const u8,
    task_done: *const fn (*Myio, *const MyioTask) callconv(.c) c_int,
    task_free: *const fn (*Myio, *MyioTask) callconv(.c) void,
    task_detach: *const fn (*Myio, *MyioTask) callconv(.c) void,
    destroy: *const fn (*Myio) callconv(.c) void,
};

const MYIO_NO_OFFSET: i64 = -1;

/// Translate a Zig error from libxev/std into an errno-style code.
fn errnoOf(err: anyerror) c_int {
    return eint(switch (err) {
        error.OutOfMemory, error.SystemResources => E.NOMEM,
        error.AccessDenied, error.PermissionDenied => E.ACCES,
        error.AddressInUse => E.ADDRINUSE,
        error.AddressNotAvailable => E.ADDRNOTAVAIL,
        error.AddressFamilyNotSupported => E.AFNOSUPPORT,
        error.WouldBlock => E.AGAIN,
        error.ConnectionRefused => E.CONNREFUSED,
        error.ConnectionResetByPeer => E.CONNRESET,
        error.ConnectionTimedOut => E.TIMEDOUT,
        error.NetworkUnreachable => E.NETUNREACH,
        error.BrokenPipe => E.PIPE,
        error.FileNotFound => E.NOENT,
        error.PathAlreadyExists => E.EXIST,
        error.IsDir => E.ISDIR,
        error.NotDir => E.NOTDIR,
        error.FileTooBig => E.FBIG,
        error.NoSpaceLeft => E.NOSPC,
        error.InvalidArgument, error.InvalidAddress, error.InvalidPort => E.INVAL,
        error.ProcessFdQuotaExceeded => E.MFILE,
        error.SystemFdQuotaExceeded => E.NFILE,
        error.NotOpenForReading, error.NotOpenForWriting => E.BADF,
        error.Unseekable => E.SPIPE,
        else => E.IO,
    });
}

/// Comparison helper: `err` may come from an error set that doesn't contain
/// Canceled at all (libxev error sets differ per backend), which a switch
/// arm would reject at compile time.
fn isCanceled(err: anyerror) bool {
    return err == error.Canceled;
}

fn closeFd(fd: std.posix.fd_t) void {
    _ = std.os.linux.close(fd);
}

extern fn strerror(errnum: c_int) [*:0]const u8;
extern fn gai_strerror(errcode: c_int) [*:0]const u8;

/// Shared by every backend instantiation. Negative codes are getaddrinfo's
/// EAI_* namespace, kept verbatim by the DNS stage; everything else is
/// errno-style (including the codes errnoOf produces from Zig errors).
fn implErrorStr(_: *Myio, err: c_int) callconv(.c) [*:0]const u8 {
    return if (err < 0) gai_strerror(err) else strerror(err);
}

// === backend-generic implementation ====================================

fn Backend(comptime xev: type) type {
    return struct {
        /// Only io_uring can cancel an in-flight file read/write; on epoll
        /// those run as blocking calls on the thread pool, which cannot be
        /// interrupted (same as libuv's behaviour).
        const file_cancelable = xev.backend == .io_uring;

        const Kind = enum {
            open, // blocking open(2) on the thread pool
            file_rw, // read/write/pread/pwrite through xev.File
            file_close,
            timer,
            spawn, // user function on the thread pool
            connect, // DNS on the thread pool, then xev.TCP.connect
            accept,
            sock_read,
            sock_write,
            sock_close,
        };

        const Io = struct {
            base: Myio,
            loop: xev.Loop,
            pool: xevpkg.ThreadPool,
            socks: ?*Sock, // every live socket, for teardown in destroy
            reap: ?*Task, // detached tasks that finished, awaiting reclaim
            detached_live: usize, // detached tasks not yet reclaimed
        };

        const Task = struct {
            io: *Io,
            kind: Kind,
            done: bool = false,
            detached: bool = false, // free on completion, result discarded
            res: Result = .{},
            rnext: ?*Task = null, // io's reap list

            c: xev.Completion = .{}, // the operation itself
            c_cancel: xev.Completion = .{}, // cancellation requests
            c_async: xev.Completion = .{}, // thread-pool completion wakeup

            // Thread-pool stage (open/spawn/connect-DNS).
            pool_task: xevpkg.ThreadPool.Task = undefined,
            notifier: xev.Async = undefined,
            has_notifier: bool = false,
            pool_pending: bool = false, // pool stage still owns the task
            pool_value: i64 = 0,
            pool_errno: c_int = 0,
            cancel_requested: bool = false, // honored at the next stage boundary

            // Operation inputs. The header promises submit copies `path`
            // and `host`; libxev has no such convention, so they are duped
            // here.
            path: ?[:0]u8 = null,
            open_flags: c_int = 0,
            open_mode: std.posix.mode_t = 0,
            spawn_fn: ?SpawnFn = null,
            spawn_arg: ?*anyopaque = null,
            host: ?[:0]u8 = null,
            port: u16 = 0,
            addr: std.Io.net.IpAddress = undefined,
            timer: xev.Timer = undefined,

            sock: ?*Sock = null, // socket the task operates on, if any
            wbuf: []const u8 = &.{}, // full buffer of a sock_write
            wnext: ?*Task = null, // sock's pending-write list
        };

        const Sock = struct {
            io: *Io,
            tcp: xev.TCP,
            read_task: ?*Task = null, // outstanding sock_read (at most one)
            accept_task: ?*Task = null, // outstanding tcp_accept (at most one)
            close_task: ?*Task = null,
            writes: ?*Task = null, // outstanding sock_writes
            c_close: xev.Completion = .{}, // for sockDiscardAsync
            prev: ?*Sock = null,
            next: ?*Sock = null,
        };

        fn ioOf(m: *Myio) *Io {
            return @fieldParentPtr("base", m);
        }
        fn taskOf(h: *MyioTask) *Task {
            return @ptrCast(@alignCast(h));
        }
        fn handleOf(t: *Task) *MyioTask {
            return @ptrCast(t);
        }
        fn sockOf(h: *MyioSock) *Sock {
            return @ptrCast(@alignCast(h));
        }

        fn taskNew(io: *Io, kind: Kind) ?*Task {
            const t = alloc.create(Task) catch return null;
            t.* = .{ .io = io, .kind = kind };
            return t;
        }

        /// A task that failed before it could even be submitted.
        fn taskFailed(io: *Io, kind: Kind, errno: c_int) ?*MyioTask {
            const t = taskNew(io, kind) orelse return null;
            completeErr(t, errno);
            return handleOf(t);
        }

        fn completeOk(t: *Task, value: i64) void {
            t.res.status = .ok;
            t.res.value = value;
            finish(t);
        }

        fn completeErr(t: *Task, errno: c_int) void {
            t.res.status = .err;
            t.res.err = errno;
            finish(t);
        }

        fn completeCanceled(t: *Task) void {
            t.res.status = .canceled;
            finish(t);
        }

        /// Every completion ends here. A detached task's result is never
        /// observable, so a socket it won is closed, and the task joins the
        /// reap list; it is actually freed between loop ticks, once none of
        /// its completions are referenced by the loop anymore.
        fn finish(t: *Task) void {
            t.done = true;
            if (!t.detached) return;
            discardResult(t);
            t.rnext = t.io.reap;
            t.io.reap = t;
        }

        /// Close the socket a detached connect/accept won; nobody will
        /// claim it. Loop-sequenced, since this may run inside a completion
        /// callback whose fd is still registered.
        fn discardResult(t: *Task) void {
            const p = t.res.ptr orelse return;
            t.res.ptr = null;
            sockDiscardAsync(@ptrCast(@alignCast(p)));
        }

        /// True once the loop and thread pool hold no reference to the
        /// task's memory, making it safe to free.
        fn taskIdle(t: *const Task) bool {
            return !t.pool_pending and t.c.state() == .dead and
                t.c_cancel.state() == .dead and t.c_async.state() == .dead;
        }

        /// Release a task's memory and owned inputs.
        fn taskDestroy(io: *Io, t: *Task) void {
            if (t.detached) io.detached_live -= 1;
            if (t.has_notifier) t.notifier.deinit();
            if (t.path) |p| alloc.free(p);
            if (t.host) |h| alloc.free(h);
            alloc.destroy(t);
        }

        /// Free finished detached tasks whose completions have settled.
        /// Called between loop ticks (never from inside a callback, where
        /// the loop may still touch the completion that fired).
        fn reapDetached(io: *Io) void {
            var p = &io.reap;
            while (p.*) |t| {
                if (taskIdle(t)) {
                    p.* = t.rnext;
                    taskDestroy(io, t);
                } else {
                    p = &t.rnext;
                }
            }
        }

        /// Ask the loop to cancel `target`. On io_uring its callback then
        /// fires with error.Canceled; on epoll the completion is killed
        /// without a callback. `cb` runs when the cancellation itself has
        /// been processed. No-op if a cancellation is already in flight.
        fn submitCancel(io: *Io, target: *xev.Completion, c_cancel: *xev.Completion, userdata: ?*anyopaque, cb: xev.Callback) void {
            if (c_cancel.state() == .active) return;
            if (target.state() != .active) return;
            c_cancel.* = .{
                .op = .{ .cancel = .{ .c = target } },
                .userdata = userdata,
                .callback = cb,
            };
            io.loop.add(c_cancel);
        }

        /// Runs when a cancellation submitted for `t.c` has been processed.
        /// On io_uring the canceled operation's own callback fires with
        /// error.Canceled (or with its normal result if the cancel lost the
        /// race) and reports the task's fate, so there is nothing to do
        /// here. On epoll a killed completion's callback never runs: the
        /// task must be completed here, and whatever the dead completion
        /// still held must be released - the fd the epoll backend dup(2)ed
        /// for every TCP completion, and the half-made socket of a connect.
        fn cancelDoneCb(
            ud: ?*anyopaque,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.Result,
        ) xev.CallbackAction {
            if (comptime xev.backend == .epoll) {
                const t: *Task = @ptrCast(@alignCast(ud.?));
                if (t.c.flags.dup) closeFd(t.c.flags.dup_fd);
                if (t.sock) |s| {
                    switch (t.kind) {
                        .sock_read => s.read_task = null,
                        .accept => s.accept_task = null,
                        .sock_write => writeListRemove(s, t),
                        // The fd was deregistered when the completion was
                        // killed, so it can be closed synchronously.
                        .connect => sockDiscard(t.io, s),
                        else => {},
                    }
                    t.sock = null;
                }
                if (!t.done) completeCanceled(t);
            }
            return .disarm;
        }

        fn sockListAdd(io: *Io, s: *Sock) void {
            s.next = io.socks;
            if (io.socks) |head| head.prev = s;
            io.socks = s;
        }

        fn sockListRemove(io: *Io, s: *Sock) void {
            if (s.prev) |p| p.next = s.next else io.socks = s.next;
            if (s.next) |n| n.prev = s.prev;
            s.prev = null;
            s.next = null;
        }

        /// Destroy a socket that was never handed to the caller (failed or
        /// canceled connect/accept). No completions reference it, and its fd
        /// must not be registered with the loop.
        fn sockDiscard(io: *Io, s: *Sock) void {
            sockListRemove(io, s);
            closeFd(s.tcp.fd);
            alloc.destroy(s);
        }

        /// Like sockDiscard, but for use inside a completion callback whose
        /// fd is still registered with the loop: epoll deregisters the fd
        /// after the callback returns, so it cannot be closed synchronously
        /// here. Closing through the loop sequences it correctly.
        fn sockDiscardAsync(s: *Sock) void {
            s.tcp.close(&s.io.loop, &s.c_close, Sock, s, sockReapCb);
        }

        fn sockReapCb(
            ud: ?*Sock,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.TCP,
            r: xev.CloseError!void,
        ) xev.CallbackAction {
            const s = ud.?;
            _ = r catch {};
            sockListRemove(s.io, s);
            alloc.destroy(s);
            return .disarm;
        }

        // ---- thread-pool stage: open / spawn / DNS ----

        fn poolMain(pt: *xevpkg.ThreadPool.Task) void {
            const t: *Task = @fieldParentPtr("pool_task", pt);
            switch (t.kind) {
                .open => {
                    // C open(2) flags pass through verbatim: the platform
                    // bit layout is shared.
                    const flags: std.os.linux.O = @bitCast(@as(u32, @bitCast(t.open_flags)));
                    const rc = std.os.linux.open(t.path.?.ptr, flags, t.open_mode);
                    switch (std.os.linux.errno(rc)) {
                        .SUCCESS => t.pool_value = @intCast(rc),
                        else => |e| t.pool_errno = @intFromEnum(e),
                    }
                },
                .spawn => t.pool_value = t.spawn_fn.?(t.spawn_arg),
                .connect => poolResolve(t),
                else => unreachable,
            }
            // Wake the loop; completion happens on the loop thread.
            t.notifier.notify() catch {};
        }

        fn poolResolve(t: *Task) void {
            var service_buf: [8]u8 = undefined;
            const service = std.fmt.bufPrintZ(&service_buf, "{d}", .{t.port}) catch unreachable;
            var hints = std.mem.zeroes(std.c.addrinfo);
            hints.family = std.c.AF.UNSPEC;
            hints.socktype = std.c.SOCK.STREAM;
            var res: ?*std.c.addrinfo = null;
            const rc = std.c.getaddrinfo(t.host.?.ptr, service.ptr, &hints, &res);
            if (@intFromEnum(rc) != 0) {
                // Resolver failures have no honest errno: keep the
                // (negative) EAI_* code verbatim for implErrorStr, except
                // EAI_SYSTEM, which says the real error is in errno.
                t.pool_errno = if (rc == .SYSTEM)
                    std.c._errno().*
                else
                    @intFromEnum(rc);
                return;
            }
            defer std.c.freeaddrinfo(res.?);
            // Only the first resolved address is tried (as in the libuv
            // backend).
            const sa = res.?.addr orelse {
                t.pool_errno = eint(E.NOENT);
                return;
            };
            switch (sa.family) {
                std.posix.AF.INET => {
                    const sin: *align(1) const std.posix.sockaddr.in = @ptrCast(sa);
                    t.addr = .{ .ip4 = .{
                        .bytes = @bitCast(sin.addr),
                        .port = std.mem.bigToNative(u16, sin.port),
                    } };
                },
                std.posix.AF.INET6 => {
                    const sin6: *align(1) const std.posix.sockaddr.in6 = @ptrCast(sa);
                    t.addr = .{ .ip6 = .{
                        .bytes = sin6.addr,
                        .port = std.mem.bigToNative(u16, sin6.port),
                    } };
                },
                else => t.pool_errno = eint(E.AFNOSUPPORT),
            }
        }

        /// Start the thread-pool stage of `t`. On failure the task is
        /// completed with an error and false is returned.
        fn poolSubmit(t: *Task) void {
            t.notifier = xev.Async.init() catch |err| {
                completeErr(t, errnoOf(err));
                return;
            };
            t.has_notifier = true;
            t.pool_pending = true;
            t.notifier.wait(&t.io.loop, &t.c_async, Task, t, poolDoneCb);
            t.pool_task = .{ .callback = poolMain };
            t.io.pool.schedule(xevpkg.ThreadPool.Batch.from(&t.pool_task));
        }

        fn poolDoneCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            r: xev.Async.WaitError!void,
        ) xev.CallbackAction {
            const t = ud.?;
            t.pool_pending = false;
            r catch |err| {
                if (!t.done) completeErr(t, errnoOf(err));
                return .disarm;
            };
            switch (t.kind) {
                .open => {
                    if (t.done) return .disarm; // canceled: result discarded
                    if (t.pool_errno != 0)
                        completeErr(t, t.pool_errno)
                    else
                        completeOk(t, t.pool_value);
                },
                .spawn => {
                    if (t.done) return .disarm;
                    if (t.pool_value >= 0)
                        completeOk(t, t.pool_value)
                    else
                        completeErr(t, @intCast(-t.pool_value));
                },
                .connect => {
                    if (t.done) return .disarm;
                    if (t.cancel_requested) {
                        // Canceled while the DNS lookup was in flight; the
                        // lookup result is simply discarded.
                        completeCanceled(t);
                    } else if (t.pool_errno != 0)
                        completeErr(t, t.pool_errno)
                    else
                        startConnect(t);
                },
                else => unreachable,
            }
            return .disarm;
        }

        // ---- filesystem operations ----

        fn implOpen(m: *Myio, path: [*:0]const u8, flags: c_int, mode: c_int) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const t = taskNew(io, .open) orelse return null;
            t.path = alloc.dupeZ(u8, std.mem.span(path)) catch {
                alloc.destroy(t);
                return null;
            };
            t.open_flags = flags;
            t.open_mode = @intCast(mode);
            poolSubmit(t);
            return handleOf(t);
        }

        fn implClose(m: *Myio, fd: i64) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const t = taskNew(io, .file_close) orelse return null;
            const file = xev.File.initFd(@intCast(fd));
            file.close(&io.loop, &t.c, Task, t, fileCloseCb);
            return handleOf(t);
        }

        fn fileCloseCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.File,
            r: xev.CloseError!void,
        ) xev.CallbackAction {
            const t = ud.?;
            if (r) |_| completeOk(t, 0) else |err| completeErr(t, errnoOf(err));
            return .disarm;
        }

        fn implRead(m: *Myio, fd: i64, buf: ?*anyopaque, len: usize, offset: i64) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const t = taskNew(io, .file_rw) orelse return null;
            if (len == 0) {
                completeOk(t, 0);
                return handleOf(t);
            }
            const slice = @as([*]u8, @ptrCast(buf.?))[0..len];
            const file = xev.File.initFd(@intCast(fd));
            if (offset == MYIO_NO_OFFSET)
                file.read(&io.loop, &t.c, .{ .slice = slice }, Task, t, fileReadCb)
            else
                file.pread(&io.loop, &t.c, .{ .slice = slice }, @intCast(offset), Task, t, fileReadCb);
            return handleOf(t);
        }

        fn fileReadCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.File,
            _: xev.ReadBuffer,
            r: xev.ReadError!usize,
        ) xev.CallbackAction {
            const t = ud.?;
            if (t.done) return .disarm; // completed elsewhere (loop failure)
            if (r) |n| completeOk(t, @intCast(n)) else |err| {
                if (err == error.EOF)
                    completeOk(t, 0)
                else if (isCanceled(err))
                    completeCanceled(t)
                else
                    completeErr(t, errnoOf(err));
            }
            return .disarm;
        }

        fn implWrite(m: *Myio, fd: i64, buf: ?*const anyopaque, len: usize, offset: i64) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const t = taskNew(io, .file_rw) orelse return null;
            if (len == 0) {
                completeOk(t, 0);
                return handleOf(t);
            }
            const slice = @as([*]const u8, @ptrCast(buf.?))[0..len];
            const file = xev.File.initFd(@intCast(fd));
            if (offset == MYIO_NO_OFFSET)
                file.write(&io.loop, &t.c, .{ .slice = slice }, Task, t, fileWriteCb)
            else
                file.pwrite(&io.loop, &t.c, .{ .slice = slice }, @intCast(offset), Task, t, fileWriteCb);
            return handleOf(t);
        }

        fn fileWriteCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.File,
            _: xev.WriteBuffer,
            r: xev.WriteError!usize,
        ) xev.CallbackAction {
            const t = ud.?;
            if (t.done) return .disarm;
            // Like write(2) and the libuv backend, a file write may be
            // partial; the byte count is the result.
            if (r) |n| completeOk(t, @intCast(n)) else |err| {
                if (isCanceled(err))
                    completeCanceled(t)
                else
                    completeErr(t, errnoOf(err));
            }
            return .disarm;
        }

        // ---- timers ----

        fn implSleep(m: *Myio, ms: u64) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const t = taskNew(io, .timer) orelse return null;
            t.timer = xev.Timer.init() catch |err| {
                completeErr(t, errnoOf(err));
                return handleOf(t);
            };
            t.timer.run(&io.loop, &t.c, ms, Task, t, timerCb);
            return handleOf(t);
        }

        fn timerCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            r: xev.Timer.RunError!void,
        ) xev.CallbackAction {
            const t = ud.?;
            if (t.done) return .disarm; // completed elsewhere (loop failure)
            if (r) |_| completeOk(t, 0) else |err| {
                if (isCanceled(err))
                    completeCanceled(t)
                else
                    completeErr(t, errnoOf(err));
            }
            return .disarm;
        }

        fn timerCancelCb(
            _: ?*void,
            _: *xev.Loop,
            _: *xev.Completion,
            r: xev.Timer.CancelError!void,
        ) xev.CallbackAction {
            _ = r catch {};
            return .disarm;
        }

        // ---- spawned functions (libxev thread pool) ----

        fn implSpawn(m: *Myio, func: SpawnFn, arg: ?*anyopaque) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const t = taskNew(io, .spawn) orelse return null;
            t.spawn_fn = func;
            t.spawn_arg = arg;
            poolSubmit(t);
            return handleOf(t);
        }

        // ---- TCP: connect ----

        fn implTcpConnect(m: *Myio, host: [*:0]const u8, port: c_int) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const t = taskNew(io, .connect) orelse return null;
            if (port < 0 or port > 65535) {
                completeErr(t, eint(E.INVAL));
                return handleOf(t);
            }
            t.port = @intCast(port);
            t.host = alloc.dupeZ(u8, std.mem.span(host)) catch {
                alloc.destroy(t);
                return null;
            };
            poolSubmit(t); // DNS first; startConnect runs on the loop thread
            return handleOf(t);
        }

        fn startConnect(t: *Task) void {
            const io = t.io;
            const s = alloc.create(Sock) catch {
                completeErr(t, eint(E.NOMEM));
                return;
            };
            const tcp = xev.TCP.init(t.addr) catch |err| {
                alloc.destroy(s);
                completeErr(t, errnoOf(err));
                return;
            };
            s.* = .{ .io = io, .tcp = tcp };
            sockListAdd(io, s);
            t.sock = s;
            s.tcp.connect(&io.loop, &t.c, t.addr, Task, t, connectCb);
        }

        fn connectCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.TCP,
            r: xev.ConnectError!void,
        ) xev.CallbackAction {
            const t = ud.?;
            if (t.done) { // completed elsewhere: nobody wants the socket
                if (t.sock) |s| {
                    t.sock = null;
                    sockDiscard(t.io, s);
                }
                return .disarm;
            }
            if (r) |_| {
                t.res.ptr = t.sock;
                t.sock = null; // the caller owns it now
                completeOk(t, 0);
            } else |err| {
                const s = t.sock.?;
                t.sock = null;
                sockDiscardAsync(s); // fd is still registered with the loop
                if (isCanceled(err))
                    completeCanceled(t)
                else
                    completeErr(t, errnoOf(err));
            }
            return .disarm;
        }

        // ---- TCP: listen / accept ----

        fn implTcpListen(m: *Myio, host: [*:0]const u8, port: c_int, backlog: c_int, errp: ?*c_int) callconv(.c) ?*MyioSock {
            const io = ioOf(m);
            const fail = struct {
                fn f(p: ?*c_int, errno: c_int) ?*MyioSock {
                    if (p) |e| e.* = errno;
                    return null;
                }
            }.f;
            if (port < 0 or port > 65535) return fail(errp, eint(E.INVAL));
            const addr = std.Io.net.IpAddress.parse(std.mem.span(host), @intCast(port)) catch
                return fail(errp, eint(E.INVAL));
            const s = alloc.create(Sock) catch return fail(errp, eint(E.NOMEM));
            const tcp = xev.TCP.init(addr) catch |err| {
                alloc.destroy(s);
                return fail(errp, errnoOf(err));
            };
            s.* = .{ .io = io, .tcp = tcp };
            tcp.bind(addr) catch |err| {
                closeFd(tcp.fd);
                alloc.destroy(s);
                return fail(errp, errnoOf(err));
            };
            tcp.listen(@intCast(@max(1, backlog))) catch |err| {
                closeFd(tcp.fd);
                alloc.destroy(s);
                return fail(errp, errnoOf(err));
            };
            sockListAdd(io, s);
            return @ptrCast(s);
        }

        fn implTcpAccept(m: *Myio, listener: *MyioSock) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const ls = sockOf(listener);
            if (ls.accept_task != null)
                return taskFailed(io, .accept, eint(E.BUSY));
            const t = taskNew(io, .accept) orelse return null;
            t.sock = ls;
            ls.accept_task = t;
            ls.tcp.accept(&io.loop, &t.c, Task, t, acceptCb);
            return handleOf(t);
        }

        fn acceptCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            r: xev.AcceptError!xev.TCP,
        ) xev.CallbackAction {
            const t = ud.?;
            if (t.done) { // listener closed; a racing connection is dropped
                if (r) |conn| closeFd(conn.fd) else |_| {}
                return .disarm;
            }
            if (t.sock) |ls| {
                ls.accept_task = null;
                t.sock = null;
            }
            if (r) |conn| {
                const s = alloc.create(Sock) catch {
                    closeFd(conn.fd);
                    completeErr(t, eint(E.NOMEM));
                    return .disarm;
                };
                s.* = .{ .io = t.io, .tcp = conn };
                sockListAdd(t.io, s);
                t.res.ptr = s;
                completeOk(t, 0);
            } else |err| {
                if (isCanceled(err))
                    completeCanceled(t)
                else
                    completeErr(t, errnoOf(err));
            }
            return .disarm;
        }

        // ---- TCP: read / write / close ----

        fn implSockRead(m: *Myio, sock: *MyioSock, buf: ?*anyopaque, len: usize) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const s = sockOf(sock);
            if (s.read_task != null)
                return taskFailed(io, .sock_read, eint(E.BUSY));
            const t = taskNew(io, .sock_read) orelse return null;
            if (len == 0) {
                completeOk(t, 0);
                return handleOf(t);
            }
            t.sock = s;
            s.read_task = t;
            const slice = @as([*]u8, @ptrCast(buf.?))[0..len];
            s.tcp.read(&io.loop, &t.c, .{ .slice = slice }, Task, t, sockReadCb);
            return handleOf(t);
        }

        fn sockReadCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.TCP,
            _: xev.ReadBuffer,
            r: xev.ReadError!usize,
        ) xev.CallbackAction {
            const t = ud.?;
            if (t.done) return .disarm; // socket closed under it
            if (t.sock) |s| {
                s.read_task = null;
                t.sock = null;
            }
            if (r) |n| completeOk(t, @intCast(n)) else |err| {
                if (err == error.EOF)
                    completeOk(t, 0) // peer closed the connection
                else if (isCanceled(err))
                    completeCanceled(t)
                else
                    completeErr(t, errnoOf(err));
            }
            return .disarm;
        }

        fn implSockWrite(m: *Myio, sock: *MyioSock, buf: ?*const anyopaque, len: usize) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const s = sockOf(sock);
            const t = taskNew(io, .sock_write) orelse return null;
            if (len == 0) {
                completeOk(t, 0);
                return handleOf(t);
            }
            t.sock = s;
            t.wbuf = @as([*]const u8, @ptrCast(buf.?))[0..len];
            t.wnext = s.writes;
            s.writes = t;
            s.tcp.write(&io.loop, &t.c, .{ .slice = t.wbuf }, Task, t, sockWriteCb);
            return handleOf(t);
        }

        fn writeListRemove(s: *Sock, t: *Task) void {
            var p = &s.writes;
            while (p.*) |cur| : (p = &cur.wnext) {
                if (cur == t) {
                    p.* = cur.wnext;
                    cur.wnext = null;
                    return;
                }
            }
        }

        fn sockWriteCb(
            ud: ?*Task,
            _: *xev.Loop,
            c: *xev.Completion,
            _: xev.TCP,
            b: xev.WriteBuffer,
            r: xev.WriteError!usize,
        ) xev.CallbackAction {
            const t = ud.?;
            if (t.done) return .disarm; // socket closed under it
            if (r) |n| {
                // myio_sock_write queues the whole buffer: continue from
                // where a partial send stopped.
                if (n < b.slice.len) {
                    c.op.send.buffer = .{ .slice = b.slice[n..] };
                    return .rearm;
                }
                if (t.sock) |s| {
                    writeListRemove(s, t);
                    t.sock = null;
                }
                completeOk(t, @intCast(t.wbuf.len));
            } else |err| {
                if (t.sock) |s| {
                    writeListRemove(s, t);
                    t.sock = null;
                }
                if (isCanceled(err))
                    completeCanceled(t)
                else
                    completeErr(t, errnoOf(err));
            }
            return .disarm;
        }

        fn implSockClose(m: *Myio, sock: *MyioSock) callconv(.c) ?*MyioTask {
            const io = ioOf(m);
            const s = sockOf(sock);
            if (s.close_task != null)
                return taskFailed(io, .sock_close, eint(E.BUSY));
            const t = taskNew(io, .sock_close) orelse return null;
            // Outstanding pull-style tasks would otherwise never complete.
            // They are detached and reported canceled right away (the
            // header promises sock_close cancels them); the cancel requests
            // below reap their loop completions, and cancelDoneCb releases
            // whatever the killed completions still held.
            if (s.read_task) |rt| {
                s.read_task = null;
                rt.sock = null;
                completeCanceled(rt);
                submitCancel(io, &rt.c, &rt.c_cancel, rt, cancelDoneCb);
            }
            if (s.accept_task) |at| {
                s.accept_task = null;
                at.sock = null;
                completeCanceled(at);
                submitCancel(io, &at.c, &at.c_cancel, at, cancelDoneCb);
            }
            while (s.writes) |wt| {
                s.writes = wt.wnext;
                wt.wnext = null;
                wt.sock = null;
                completeCanceled(wt);
                submitCancel(io, &wt.c, &wt.c_cancel, wt, cancelDoneCb);
            }
            s.close_task = t;
            t.sock = s;
            s.tcp.close(&io.loop, &t.c, Task, t, sockCloseCb);
            return handleOf(t);
        }

        fn sockCloseCb(
            ud: ?*Task,
            _: *xev.Loop,
            _: *xev.Completion,
            _: xev.TCP,
            r: xev.CloseError!void,
        ) xev.CallbackAction {
            const t = ud.?;
            const s = t.sock.?;
            t.sock = null;
            sockListRemove(t.io, s);
            alloc.destroy(s);
            if (r) |_| completeOk(t, 0) else |err| completeErr(t, errnoOf(err));
            return .disarm;
        }

        fn implSockPort(_: *Myio, sock: *MyioSock) callconv(.c) c_int {
            const s = sockOf(sock);
            var ss: std.posix.sockaddr.storage = std.mem.zeroes(std.posix.sockaddr.storage);
            var slen: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
            const rc = std.os.linux.getsockname(s.tcp.fd, @ptrCast(&ss), &slen);
            if (std.os.linux.errno(rc) != .SUCCESS) return -1;
            return switch (ss.family) {
                std.posix.AF.INET => std.mem.bigToNative(u16, @as(*const std.posix.sockaddr.in, @ptrCast(&ss)).port),
                std.posix.AF.INET6 => std.mem.bigToNative(u16, @as(*const std.posix.sockaddr.in6, @ptrCast(&ss)).port),
                else => -1,
            };
        }

        // ---- synchronisation ----

        /// True when the loop can make no further progress.
        fn loopDrained(io: *Io) bool {
            return io.loop.active == 0 and io.loop.submissions.empty();
        }

        fn implAwait(m: *Myio, task: *MyioTask) callconv(.c) Result {
            const io = ioOf(m);
            const t = taskOf(task);
            while (!t.done) {
                io.loop.run(.once) catch {
                    completeErr(t, eint(E.IO));
                    break;
                };
                reapDetached(io);
                if (!t.done and loopDrained(io)) {
                    // Loop has nothing left to do; the task can never
                    // complete.
                    completeErr(t, eint(E.DEADLK));
                }
            }
            return t.res;
        }

        fn implCancel(m: *Myio, task: *MyioTask) callconv(.c) c_int {
            const io = ioOf(m);
            const t = taskOf(task);
            if (t.done) return -1;
            switch (t.kind) {
                .timer => {
                    // The timer's own callback fires with error.Canceled on
                    // both backends (epoll special-cases timers) and reports
                    // the outcome.
                    if (t.c_cancel.state() != .active)
                        t.timer.cancel(&io.loop, &t.c, &t.c_cancel, void, null, timerCancelCb);
                    return 0;
                },
                .sock_read, .accept => {
                    submitCancel(io, &t.c, &t.c_cancel, t, cancelDoneCb);
                    return 0;
                },
                .connect => {
                    if (t.pool_pending)
                        // DNS stage: the lookup itself cannot be stopped;
                        // the request is honored when it finishes
                        // (poolDoneCb).
                        t.cancel_requested = true
                    else
                        submitCancel(io, &t.c, &t.c_cancel, t, cancelDoneCb);
                    return 0;
                },
                .file_rw => {
                    if (!file_cancelable) return -1;
                    submitCancel(io, &t.c, &t.c_cancel, t, cancelDoneCb);
                    return 0;
                },
                // open/spawn already run on the pool; writes and closes
                // cannot be taken back.
                else => return -1,
            }
        }

        fn implSelect(m: *Myio, tasks: [*]?*MyioTask, ntasks: usize) callconv(.c) isize {
            const io = ioOf(m);
            while (true) {
                var any = false;
                for (tasks[0..ntasks], 0..) |h, i| {
                    if (h) |handle| {
                        any = true;
                        if (taskOf(handle).done) return @intCast(i);
                    }
                }
                if (!any) return -1;
                io.loop.run(.once) catch return -1;
                reapDetached(io);
                if (loopDrained(io)) {
                    for (tasks[0..ntasks], 0..) |h, i|
                        if (h != null and taskOf(h.?).done) return @intCast(i);
                    return -1; // loop drained without completing any of them
                }
            }
        }

        // ---- lifetime ----

        fn implTaskDone(_: *Myio, task: *const MyioTask) callconv(.c) c_int {
            const t: *const Task = @ptrCast(@alignCast(task));
            return @intFromBool(t.done);
        }

        fn implTaskFree(m: *Myio, task: *MyioTask) callconv(.c) void {
            const io = ioOf(m);
            const t = taskOf(task);
            if (!t.done) {
                // The in-flight operation references this memory: cancel if
                // possible, then wait for completion before releasing it.
                _ = implCancel(m, task);
                _ = implAwait(m, task);
            }
            // A done task may still have loop completions in flight (e.g. a
            // cancellation that was just submitted); drain them so nothing
            // dangles into freed memory.
            while (!taskIdle(t)) {
                if (loopDrained(io)) break;
                io.loop.run(.once) catch break;
                reapDetached(io);
            }
            taskDestroy(io, t);
        }

        fn implTaskDetach(m: *Myio, task: *MyioTask) callconv(.c) void {
            const io = ioOf(m);
            const t = taskOf(task);
            t.detached = true;
            io.detached_live += 1;
            if (!t.done) return; // finish() queues it for reaping
            // Already complete: discard the result now and reclaim the
            // memory as soon as the loop lets go of its completions.
            discardResult(t);
            if (taskIdle(t)) {
                taskDestroy(io, t);
            } else {
                t.rnext = io.reap;
                io.reap = t;
            }
        }

        fn implDestroy(m: *Myio) callconv(.c) void {
            const io = ioOf(m);
            // Detached tasks must not outlive the instance: drive the loop
            // until each completes and its completions settle. May block
            // until their operations can be stopped or complete (as
            // documented for myio_destroy).
            reapDetached(io);
            while (io.detached_live > 0) {
                if (loopDrained(io)) break; // leak rather than free live memory
                io.loop.run(.once) catch break;
                reapDetached(io);
            }
            // Reclaim sockets that were never closed (the listener of a
            // crashed-out program, say); their fds would otherwise leak.
            while (io.socks) |s| {
                io.socks = s.next;
                closeFd(s.tcp.fd);
                alloc.destroy(s);
            }
            io.loop.deinit();
            io.pool.shutdown();
            io.pool.deinit();
            alloc.destroy(io);
        }

        const vtable = Ops{
            .open = implOpen,
            .close = implClose,
            .read = implRead,
            .write = implWrite,
            .sleep = implSleep,
            .spawn = implSpawn,
            .tcp_connect = implTcpConnect,
            .tcp_listen = implTcpListen,
            .tcp_accept = implTcpAccept,
            .sock_read = implSockRead,
            .sock_write = implSockWrite,
            .sock_close = implSockClose,
            .sock_port = implSockPort,
            .@"await" = implAwait,
            .cancel = implCancel,
            .select = implSelect,
            .error_str = implErrorStr,
            .task_done = implTaskDone,
            .task_free = implTaskFree,
            .task_detach = implTaskDetach,
            .destroy = implDestroy,
        };

        fn create() ?*Myio {
            const io = alloc.create(Io) catch return null;
            io.* = .{
                .base = .{ .ops = &vtable },
                .loop = undefined,
                .pool = xevpkg.ThreadPool.init(.{}),
                .socks = null,
                .reap = null,
                .detached_live = 0,
            };
            io.loop = xev.Loop.init(.{ .thread_pool = &io.pool }) catch {
                io.pool.shutdown();
                io.pool.deinit();
                alloc.destroy(io);
                return null;
            };
            return &io.base;
        }
    };
}

export fn myio_xev_new() ?*Myio {
    if (builtin.os.tag == .linux) {
        // Prefer io_uring when the kernel (and its seccomp policy) allows
        // it; epoll works everywhere else.
        if (xevpkg.IO_Uring.available()) {
            if (Backend(xevpkg.IO_Uring).create()) |m| return m;
        }
        return Backend(xevpkg.Epoll).create();
    }
    // Other platforms: libxev's compile-time default backend.
    return Backend(xevpkg).create();
}
