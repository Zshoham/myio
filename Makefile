CC      ?= cc
ZIG     ?= zig
CFLAGS  ?= -O2 -g -Wall -Wextra -std=c11
CPPFLAGS += -Iinclude

UV_CFLAGS := $(shell pkg-config --cflags libuv)
UV_LIBS   := $(shell pkg-config --libs libuv)

# The libxev backend is implemented in Zig (see src/myio_xev.zig) and built
# into a static library that the C programs link against.
XEV_LIB = zig-out/lib/libmyio_xev.a

SRC     = src/myio_uv.c src/myio_sync.c
HEADERS = include/myio.h include/myio_uv.h include/myio_sync.h include/myio_xev.h

all: demo chat chat_uv

$(XEV_LIB): src/myio_xev.zig build.zig build.zig.zon
	$(ZIG) build --release=safe

demo: examples/demo.c $(SRC) $(HEADERS) $(XEV_LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) examples/demo.c $(SRC) $(XEV_LIB) $(UV_LIBS) -o $@

chat: examples/chat.c $(SRC) $(HEADERS) $(XEV_LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) examples/chat.c $(SRC) $(XEV_LIB) $(UV_LIBS) -o $@

# Comparison implementations of the same chat (see README).
chat_uv: examples/chat_uv.c
	$(CC) $(CFLAGS) $(UV_CFLAGS) examples/chat_uv.c $(UV_LIBS) -o $@

chat-rs:
	cd examples/chat-rs && cargo build --release

.PHONY: all test clean chat-rs
test: demo
	./demo uv
	./demo sync
	./demo xev

clean:
	rm -f demo chat chat_uv demo1.tmp demo2.tmp
	rm -rf zig-out
