#!/usr/bin/env python3
"""chat_asyncio.py - the persistent peer-to-peer chat from chat.c written
with Python's asyncio, for comparison with the myio version.

usage: python3 chat_asyncio.py <port> [host [peer-port]]

Same behavior as examples/chat.c: always listens on <port>, dials
host:peer-port when a host is given (retrying every 10 seconds, override
with CHAT_RETRY_MS), returns to waiting when the peer leaves, ctrl-d quits.

asyncio expresses the design as cooperating coroutine tasks sharing state:
a server callback, a dialer task and a per-peer reader task, with stdin
driving the main coroutine. There is no select-over-everything loop; each
concern is its own task and they coordinate through the shared peer slot
and an event.
"""
import asyncio
import contextlib
import os
import sys

RETRY_MS = int(os.environ.get("CHAT_RETRY_MS", "10000"))  # test hook
BUFSZ = 512


def status(msg):
    print(f"* {msg}", file=sys.stderr, flush=True)


class Chat:
    def __init__(self, port, host, peer_port):
        self.port, self.host, self.peer_port = port, host, peer_port
        self.writer = None  # current peer, None while disconnected
        self.peer_lost = asyncio.Event()
        self.reader_task = None

    def waiting_msg(self):
        dial = " (and dialing out)" if self.host else ""
        status(f"waiting for a peer on port {self.port}{dial}")

    def adopt(self, reader, writer):
        """Take a new connection as the peer (server callback and dialer
        both land here). One peer at a time: extras are turned away."""
        if self.writer is not None:
            writer.close()
            return
        self.writer = writer
        status("peer connected")
        self.reader_task = asyncio.create_task(self.peer_reader(reader, writer))

    async def peer_reader(self, reader, writer):
        try:
            while data := await reader.read(BUFSZ):
                sys.stdout.write("peer> " + data.decode(errors="replace"))
                sys.stdout.flush()
        except OSError:
            pass
        writer.close()
        if self.writer is writer:
            self.writer = None
            status("peer disconnected")
            self.waiting_msg()
            self.peer_lost.set()

    async def dialer(self):
        if not self.host:
            return
        while True:
            while self.writer is not None:
                await self.peer_lost.wait()
                self.peer_lost.clear()
            try:
                r, w = await asyncio.open_connection(self.host, self.peer_port)
            except OSError as e:
                status(f"connect to {self.host}:{self.peer_port} failed "
                       f"({e}), retrying in {RETRY_MS} ms")
                await asyncio.sleep(RETRY_MS / 1000)
                continue
            self.adopt(r, w)

    async def stdin_lines(self):
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        while line := await reader.readline():
            if self.writer is None:
                status("not connected, message dropped")
                continue
            self.writer.write(line)
            try:
                await self.writer.drain()
            except OSError as e:
                status(f"send failed: {e}")
        status("stdin closed, bye")

    async def run(self):
        server = await asyncio.start_server(self.adopt, "0.0.0.0", self.port)
        status("type messages; ctrl-d quits")
        self.waiting_msg()
        dial_task = asyncio.create_task(self.dialer())

        await self.stdin_lines()

        for task in (dial_task, self.reader_task):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        if self.writer:
            self.writer.close()
        server.close()
        await server.wait_closed()


def main():
    if not 2 <= len(sys.argv) <= 4:
        print(f"usage: {sys.argv[0]} <port> [host [peer-port]]",
              file=sys.stderr)
        sys.exit(2)
    port = int(sys.argv[1])
    host = sys.argv[2] if len(sys.argv) > 2 else None
    peer_port = int(sys.argv[3]) if len(sys.argv) > 3 else port
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(Chat(port, host, peer_port).run())


if __name__ == "__main__":
    main()
