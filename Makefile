CC      ?= cc
ZIG     ?= zig
CFLAGS  ?= -O2 -g -Wall -Wextra -std=c11
CPPFLAGS += -Iinclude

UV_CFLAGS := $(shell pkg-config --cflags libuv)
UV_LIBS   := $(shell pkg-config --libs libuv)

URING_CFLAGS := $(shell pkg-config --cflags liburing)
URING_LIBS   := $(shell pkg-config --libs liburing)

# The libxev backend is implemented in Zig (see src/myio_xev.zig) and built
# into a static library that the C programs link against.
XEV_LIB = zig-out/lib/libmyio_xev.a

SRC     = src/myio_uv.c src/myio_sync.c src/myio_pool.c src/myio_uring.c
HEADERS = include/myio.h include/myio_uv.h include/myio_sync.h include/myio_xev.h \
          include/myio_pool.h include/myio_uring.h

all: demo chat chat_uv cancel_test concurrency_test

$(XEV_LIB): src/myio_xev.zig build.zig build.zig.zon
	$(ZIG) build --release=safe

demo: examples/demo.c $(SRC) $(HEADERS) $(XEV_LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) $(URING_CFLAGS) examples/demo.c $(SRC) $(XEV_LIB) $(UV_LIBS) $(URING_LIBS) -pthread -o $@

chat: examples/chat.c $(SRC) $(HEADERS) $(XEV_LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) $(URING_CFLAGS) examples/chat.c $(SRC) $(XEV_LIB) $(UV_LIBS) $(URING_LIBS) -pthread -o $@

cancel_test: examples/cancel_test.c examples/test_common.h $(SRC) $(HEADERS) $(XEV_LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) $(URING_CFLAGS) examples/cancel_test.c $(SRC) $(XEV_LIB) $(UV_LIBS) $(URING_LIBS) -pthread -o $@

concurrency_test: examples/concurrency_test.c examples/test_common.h $(SRC) $(HEADERS) $(XEV_LIB)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(UV_CFLAGS) $(URING_CFLAGS) examples/concurrency_test.c $(SRC) $(XEV_LIB) $(UV_LIBS) $(URING_LIBS) -pthread -o $@

# Comparison implementations of the same chat (see README).
chat_uv: examples/chat_uv.c
	$(CC) $(CFLAGS) $(UV_CFLAGS) examples/chat_uv.c $(UV_LIBS) -o $@

chat-rs:
	cd examples/chat-rs && cargo build --release

# The Zephyr backend demo, built for native_sim. The Zephyr tree and
# littlefs module are vendored as submodules (vendor/zephyr,
# vendor/littlefs); override ZEPHYR_BASE/ZEPHYR_LFS to point elsewhere.
# ZEPHYR_VENV is a python venv holding west + Zephyr's python deps; it is
# created on first use (see $(ZEPHYR_VENV)/bin/west below) and prepended
# to PATH for the cmake/ninja invocations.
PYTHON       ?= python3
ZEPHYR_BASE  ?= $(CURDIR)/vendor/zephyr
ZEPHYR_LFS   ?= $(CURDIR)/vendor/littlefs
ZEPHYR_VENV  ?= examples/zephyr_demo/venv
ZEPHYR_BOARD ?= native_sim/native/64
ZEPHYR_BUILD  = examples/zephyr_demo/build

$(ZEPHYR_VENV)/bin/west: $(ZEPHYR_BASE)/scripts/requirements-base.txt
	$(PYTHON) -m venv $(ZEPHYR_VENV)
	$(ZEPHYR_VENV)/bin/pip install -q --upgrade pip
	$(ZEPHYR_VENV)/bin/pip install -q west -r $(ZEPHYR_BASE)/scripts/requirements-base.txt

zephyr_demo: $(ZEPHYR_VENV)/bin/west src/myio_zephyr.c include/myio.h include/myio_zephyr.h \
             examples/zephyr_demo/src/main.c examples/zephyr_demo/prj.conf
	test -f $(ZEPHYR_BUILD)/build.ninja || \
	env PATH="$(ZEPHYR_VENV)/bin:$$PATH" \
	    ZEPHYR_BASE=$(ZEPHYR_BASE) ZEPHYR_TOOLCHAIN_VARIANT=host \
	    cmake -GNinja -B$(ZEPHYR_BUILD) -DBOARD=$(ZEPHYR_BOARD) \
	        "-DZEPHYR_MODULES=$(CURDIR);$(ZEPHYR_LFS)" examples/zephyr_demo
	env PATH="$(ZEPHYR_VENV)/bin:$$PATH" ninja -C $(ZEPHYR_BUILD)

test-zephyr: zephyr_demo
	./$(ZEPHYR_BUILD)/zephyr/zephyr.exe

.PHONY: all test clean chat-rs zephyr_demo test-zephyr
test: demo cancel_test concurrency_test
	./demo uv
	./demo sync
	./demo xev
	./demo pool
	./demo uring
	./cancel_test uv
	./cancel_test xev
	./cancel_test pool
	./cancel_test uring
	./concurrency_test uv
	./concurrency_test xev
	./concurrency_test pool
	./concurrency_test uring

clean:
	rm -f demo chat chat_uv cancel_test concurrency_test demo1.tmp demo2.tmp
	rm -rf zig-out examples/zephyr_demo/build* $(ZEPHYR_VENV)
