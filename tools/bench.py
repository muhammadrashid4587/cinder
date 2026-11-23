#!/usr/bin/env python3
"""Benchmark cinder (or any RESP server) and, optionally, compare it
against a second RESP server on the same machine — normally real Redis.

Speaks raw RESP over one socket per phase, no client library needed.
Measures sequential (one request at a time) and pipelined throughput
for SET, GET, INCR, LPUSH and HSET, then prints a Markdown table.

Usage:
    python3 tools/bench.py --port 7777 [--requests 5000] [--pipeline 50]
    python3 tools/bench.py --port 7777 --compare-port 7780 --label redis

--compare-port runs the same workload against the second server and
adds a column for it, so the two are measured back-to-back on the
same hardware. Exit code 0 when both servers answered every request.
"""
import argparse
import socket
import sys
import time


class Client:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = b""

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.s.recv(65536)
            if not chunk:
                raise EOFError("server closed connection")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _read_line(self):
        while b"\r\n" not in self.buf:
            chunk = self.s.recv(65536)
            if not chunk:
                raise EOFError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_reply(self):
        line = self._read_line()
        kind, payload = line[:1], line[1:]
        if kind == b"+":
            return payload.decode()
        if kind == b"-":
            raise RuntimeError(f"server error: {payload.decode()}")
        if kind == b":":
            return int(payload)
        if kind == b"$":
            n = int(payload)
            if n == -1:
                return None
            return self._read_exact(n + 2)[:-2].decode()
        if kind == b"*":
            n = int(payload)
            if n == -1:
                return None
            return [self._read_reply() for _ in range(n)]
        raise ValueError(f"bad reply kind {kind!r}")

    def send(self, *args):
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = str(a).encode()
            parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
        self.s.sendall(b"".join(parts))

    def cmd(self, *args):
        self.send(*args)
        return self._read_reply()

    def pipeline(self, commands):
        blob = b""
        for args in commands:
            parts = [f"*{len(args)}\r\n".encode()]
            for a in args:
                b = str(a).encode()
                parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
            blob += b"".join(parts)
        self.s.sendall(blob)
        for _ in commands:
            self._read_reply()

    def close(self):
        self.s.close()


def measure_seq(client, op, n):
    """One request at a time, waiting for each reply. Returns ops/sec."""
    start = time.monotonic()
    for i in range(n):
        if op == "SET":
            client.cmd("SET", f"bench:set:{i}", "v" * 16)
        elif op == "GET":
            client.cmd("GET", f"bench:get:{i % 1000}")
        elif op == "INCR":
            client.cmd("INCR", f"bench:ctr:{i % 100}")
        elif op == "LPUSH":
            client.cmd("LPUSH", f"bench:list:{i % 100}", "v")
        elif op == "HSET":
            client.cmd("HSET", f"bench:hash:{i % 100}", f"f{i}", "v")
    return n / (time.monotonic() - start)


def measure_pipe(client, op, n, depth):
    """Pipelined: send `depth` commands, then read `depth` replies."""
    start = time.monotonic()
    done = 0
    while done < n:
        batch = min(depth, n - done)
        if op == "SET":
            cmds = [("SET", f"bench:pset:{done + j}", "v" * 16)
                    for j in range(batch)]
        else:
            cmds = [("GET", f"bench:pget:{(done + j) % 1000}")
                    for j in range(batch)]
        client.pipeline(cmds)
        done += batch
    return n / (time.monotonic() - start)


def run(port, n, depth, label):
    c = Client(port)
    try:
        # Warm up and pre-populate the GET keys.
        for i in range(1000):
            c.cmd("SET", f"bench:get:{i}", "v" * 16)
            c.cmd("SET", f"bench:pget:{i}", "v" * 16)
        results = {}
        for op in ("SET", "GET", "INCR", "LPUSH", "HSET"):
            results[op] = measure_seq(c, op, n)
        results["SET-pipelined"] = measure_pipe(c, "SET", n, depth)
        results["GET-pipelined"] = measure_pipe(c, "GET", n, depth)
        return results
    finally:
        c.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, required=True,
                    help="port of the server to benchmark")
    ap.add_argument("--compare-port", type=int,
                    help="port of a second server to measure the same way")
    ap.add_argument("--label", default="redis",
                    help="column label for the --compare-port server")
    ap.add_argument("--requests", type=int, default=5000,
                    help="requests per operation (default 5000)")
    ap.add_argument("--pipeline", type=int, default=50,
                    help="pipelined batch size (default 50)")
    args = ap.parse_args()

    rows = ["SET", "GET", "INCR", "LPUSH", "HSET",
            "SET-pipelined", "GET-pipelined"]
    first = run(args.port, args.requests, args.pipeline, "cinder")
    second = None
    if args.compare_port:
        second = run(args.compare_port, args.requests, args.pipeline,
                     args.label)

    print(f"# bench: {args.requests} requests/op, pipeline depth "
          f"{args.pipeline}, one client, same machine")
    print()
    print("| Operation | cinder (ops/sec) |"
          + (f" {args.label} (ops/sec) |" if second else ""))
    print("| --- | ---: |" + (" ---: |" if second else ""))
    for op in rows:
        line = f"| `{op}` | {first[op]:,.0f} |"
        if second:
            line += f" {second[op]:,.0f} |"
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
