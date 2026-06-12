CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra -std=c11
CPPFLAGS += -Iinclude

UV_CFLAGS := $(shell pkg-config --cflags libuv)
UV_LIBS   := $(shell pkg-config --libs libuv)

SRC     = src/myio_uv.c src/myio_sync.c
HEADERS = include/myio.h include/myio_uv.h include/myio_sync.h

all: demo chat chat_uv

demo: examples/demo.c $(SRC) $(HEADERS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) examples/demo.c $(SRC) $(UV_LIBS) -o $@

chat: examples/chat.c $(SRC) $(HEADERS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) examples/chat.c $(SRC) $(UV_LIBS) -o $@

# Comparison implementations of the same chat (see README).
chat_uv: examples/chat_uv.c
	$(CC) $(CFLAGS) $(UV_CFLAGS) examples/chat_uv.c $(UV_LIBS) -o $@

chat-rs:
	cd examples/chat-rs && cargo build --release

.PHONY: all test clean chat-rs
test: demo
	./demo uv
	./demo sync

clean:
	rm -f demo chat chat_uv demo1.tmp demo2.tmp
