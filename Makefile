CC      ?= gcc
CFLAGS  ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -g -pthread
LDFLAGS ?=

SRC := src/server.c src/store.c src/resp.c src/aof.c src/rdb.c src/threadpool.c
OBJ := $(SRC:.c=.o)
BIN := cinder

.PHONY: all clean test run

all: $(BIN)

$(BIN): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(LDFLAGS)

src/%.o: src/%.c
	$(CC) $(CFLAGS) -c $< -o $@

test: $(BIN)
	python3 tests/smoke.py

run: $(BIN)
	./$(BIN) --port 6379 --data-dir ./data

clean:
	rm -f $(OBJ) $(BIN)
	rm -rf data
