"""Smoke tests for cinder: speaks raw RESP over a socket, no redis-py needed.

Starts the server on a scratch port with a scratch data dir, runs through
the core commands, checks AOF durability across a restart, and shuts it down.
"""
import os
import random
import shutil
import socket
import subprocess
import sys
import time

PORT = 7777
DATA_DIR = "/tmp/cinder-smoke-data"
BIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cinder")

FAILURES = []


def check(name, got, want):
    if got != want:
        FAILURES.append(f"{name}: got {got!r}, want {want!r}")
        print(f"FAIL {name}: got {got!r}, want {want!r}")
    else:
        print(f"ok   {name}")


class Client:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=5)
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

    def cmd(self, *args):
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
        self.s.sendall(b"".join(parts))
        return self._read_reply()

    def _read_reply(self):
        line = self._read_line()
        kind, payload = line[:1], line[1:]
        if kind == b"+":
            return payload.decode()
        if kind == b"-":
            return ("error", payload.decode())
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

    def close(self):
        self.s.close()


def wait_for_port(port, proc, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start listening in time")


def start_server(extra_args=None, port=PORT, data_dir=DATA_DIR, fresh=True):
    if fresh:
        shutil.rmtree(data_dir, ignore_errors=True)
    proc = subprocess.Popen(
        [BIN, "--port", str(port), "--data-dir", data_dir] + (extra_args or []),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    wait_for_port(port, proc)
    return proc


def main():
    proc = start_server()
    try:
        c = Client(PORT)
        check("PING", c.cmd("PING"), "PONG")
        check("PING msg", c.cmd("PING", "hello"), "hello")
        check("SET", c.cmd("SET", "name", "cinder"), "OK")
        check("GET", c.cmd("GET", "name"), "cinder")
        check("GET missing", c.cmd("GET", "nope"), None)
        check("SET NX existing", c.cmd("SET", "name", "x", "NX"), None)
        check("SET NX new", c.cmd("SET", "fresh", "1", "NX"), "OK")
        check("SET XX missing", c.cmd("SET", "ghost", "1", "XX"), None)
        check("SET XX existing", c.cmd("SET", "fresh", "2", "XX"), "OK")
        check("GET fresh", c.cmd("GET", "fresh"), "2")
        check("INCR new", c.cmd("INCR", "counter"), 1)
        check("INCR", c.cmd("INCR", "counter"), 2)
        check("INCRBY", c.cmd("INCRBY", "counter", "10"), 12)
        check("DECR", c.cmd("DECR", "counter"), 11)
        # INCR preserves the key's TTL and grows the stored value in place.
        check("SET EX ttlctr", c.cmd("SET", "ttlctr", "5", "EX", "100"), "OK")
        check("INCR preserves TTL", c.cmd("INCR", "ttlctr"), 6)
        check("TTL after INCR ~", 99 <= c.cmd("TTL", "ttlctr") <= 100, True)
        check("SET grow", c.cmd("SET", "grow", "99"), "OK")
        check("INCR 99->100", c.cmd("INCR", "grow"), 100)
        check("SET grow 999", c.cmd("SET", "grow", "999"), "OK")
        check("INCR 999->1000", c.cmd("INCR", "grow"), 1000)
        check("DECRBY negative", c.cmd("DECRBY", "grow", "1001"), -1)
        check("SET PX blinkctr", c.cmd("SET", "blinkctr", "41", "PX", "50"), "OK")
        time.sleep(0.1)
        check("INCR expired restarts", c.cmd("INCR", "blinkctr"), 1)
        check("INCR non-int", c.cmd("INCR", "name"), ("error", "ERR value is not an integer or out of range"))
        check("SET EX", c.cmd("SET", "temp", "v", "EX", "100"), "OK")
        check("TTL temp ~", 99 <= c.cmd("TTL", "temp") <= 100, True)
        check("PTTL temp ~", 99000 <= c.cmd("PTTL", "temp") <= 100000, True)
        check("SET PX short", c.cmd("SET", "blink", "v", "PX", "50"), "OK")
        time.sleep(0.1)
        check("GET expired", c.cmd("GET", "blink"), None)
        check("TTL expired", c.cmd("TTL", "blink"), -2)
        check("TTL no expiry", c.cmd("TTL", "name"), -1)
        check("EXPIRE", c.cmd("EXPIRE", "name", "100"), 1)
        check("EXPIRE missing", c.cmd("EXPIRE", "ghost", "100"), 0)
        check("EXISTS", c.cmd("EXISTS", "name", "fresh", "ghost"), 2)
        check("DEL", c.cmd("DEL", "fresh", "ghost"), 1)
        check("DEL again", c.cmd("DEL", "fresh"), 0)
        check("DEL incr extras", c.cmd("DEL", "ttlctr", "grow", "blinkctr"), 3)
        check("KEYS *", sorted(c.cmd("KEYS", "*")), ["counter", "name", "temp"])
        check("KEYS c*", c.cmd("KEYS", "c*"), ["counter"])
        # SCAN: cursor-based iteration with MATCH and COUNT. The helper
        # drives the cursor to 0 and returns every key seen plus the
        # number of round trips it took.
        def scan_all(*args):
            seen = []
            cursor = "0"
            rounds = 0
            while True:
                cursor, keys = c.cmd("SCAN", cursor, *args)
                seen.extend(keys)
                rounds += 1
                if rounds > 10000:
                    raise AssertionError("SCAN did not terminate")
                if cursor == "0":
                    break
            return seen, rounds
        keys, rounds = scan_all()
        check("SCAN all keys", sorted(keys), ["counter", "name", "temp"])
        check("SCAN default COUNT one round", rounds, 1)
        keys, _ = scan_all("MATCH", "c*")
        check("SCAN MATCH c*", sorted(keys), ["counter"])
        keys, _ = scan_all("MATCH", "zzz*")
        check("SCAN MATCH none", keys, [])
        keys, rounds = scan_all("COUNT", "1")
        check("SCAN COUNT 1 all keys", sorted(keys),
              ["counter", "name", "temp"])
        check("SCAN COUNT 1 took >1 round", rounds > 1, True)
        keys, _ = scan_all("MATCH", "c*", "COUNT", "1")
        check("SCAN MATCH+COUNT", sorted(keys), ["counter"])
        check("SCAN bad cursor", c.cmd("SCAN", "abc"),
              ("error", "ERR invalid cursor"))
        check("SCAN negative cursor", c.cmd("SCAN", "-1"),
              ("error", "ERR invalid cursor"))
        check("SCAN bad option", c.cmd("SCAN", "0", "NOPE"),
              ("error", "ERR syntax error"))
        check("SCAN COUNT missing value", c.cmd("SCAN", "0", "COUNT"),
              ("error", "ERR syntax error"))
        check("SCAN COUNT bad value", c.cmd("SCAN", "0", "COUNT", "x"),
              ("error", "ERR value is not an integer or out of range"))
        check("SCAN COUNT negative", c.cmd("SCAN", "0", "COUNT", "-1"),
              ("error", "ERR syntax error"))
        check("SCAN duplicate MATCH",
              c.cmd("SCAN", "0", "MATCH", "*", "MATCH", "*"),
              ("error", "ERR syntax error"))
        check("SCAN no args", c.cmd("SCAN"),
              ("error", "ERR wrong number of arguments for 'scan' command"))
        # SCAN across a table resize: 800 keys push the table 1024 ->
        # 2048 before the scan starts, then 800 more mid-iteration push
        # it to 4096. Keys present for the whole iteration must all
        # come back, even though the buckets moved under the cursor.
        for i in range(800):
            c.cmd("SET", f"scan:{i}", str(i))
        seen = []
        cursor = "0"
        rounds = 0
        grown = False
        while True:
            cursor, keys = c.cmd("SCAN", cursor, "MATCH", "scan:*",
                                 "COUNT", "1")
            seen.extend(keys)
            rounds += 1
            if rounds > 10000:
                raise AssertionError("SCAN did not terminate")
            if not grown and rounds == 5:
                for i in range(800):
                    c.cmd("SET", f"scan2:{i}", str(i))
                grown = True
            if cursor == "0":
                break
        check("SCAN resize all 800 keys", sorted(set(seen)),
              sorted(f"scan:{i}" for i in range(800)))
        check("SCAN resize terminated in bounds", rounds < 5000, True)
        # Cleanup in two DELs: one command may carry at most 1024
        # RESP array elements, so 1600 keys do not fit in a single DEL.
        check("SCAN resize cleanup 1",
              c.cmd("DEL", *[f"scan:{i}" for i in range(800)]), 800)
        check("SCAN resize cleanup 2",
              c.cmd("DEL", *[f"scan2:{i}" for i in range(800)]), 800)
        check("DBSIZE", c.cmd("DBSIZE"), 3)
        # GETDEL / GETEX: get-and-delete and get-and-retime a string key.
        check("SET getdel", c.cmd("SET", "gd", "gone"), "OK")
        check("GETDEL", c.cmd("GETDEL", "gd"), "gone")
        check("GET after GETDEL", c.cmd("GET", "gd"), None)
        check("GETDEL missing", c.cmd("GETDEL", "gd"), None)
        check("SET getex", c.cmd("SET", "ge", "val", "EX", "100"), "OK")
        check("GETEX no option", c.cmd("GETEX", "ge"), "val")
        check("TTL after GETEX no-op ~", 99 <= c.cmd("TTL", "ge") <= 100, True)
        check("GETEX EX", c.cmd("GETEX", "ge", "EX", "200"), "val")
        check("TTL after GETEX EX ~", 199 <= c.cmd("TTL", "ge") <= 200, True)
        check("GETEX PX", c.cmd("GETEX", "ge", "PX", "150000"), "val")
        check("PTTL after GETEX PX ~", 149000 <= c.cmd("PTTL", "ge") <= 150000, True)
        check("GETEX PERSIST", c.cmd("GETEX", "ge", "PERSIST"), "val")
        check("TTL after GETEX PERSIST", c.cmd("TTL", "ge"), -1)
        check("GETEX PERSIST no TTL", c.cmd("GETEX", "ge", "PERSIST"), "val")
        check("TTL still -1", c.cmd("TTL", "ge"), -1)
        check("GETEX missing", c.cmd("GETEX", "ghost"), None)
        check("GETEX bad expire", c.cmd("GETEX", "ge", "EX", "0"),
              ("error", "ERR invalid expire time in 'getex' command"))
        check("GETEX bad option", c.cmd("GETEX", "ge", "XX"), ("error", "ERR syntax error"))
        check("GETEX too many args", c.cmd("GETEX", "ge", "EX", "10", "PX"),
              ("error", "ERR wrong number of arguments for 'getex' command"))
        check("RPUSH getdel list", c.cmd("RPUSH", "gdl", "a"), 1)
        check("GETDEL wrongtype", c.cmd("GETDEL", "gdl"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("GETEX wrongtype", c.cmd("GETEX", "gdl"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("GETDEL/GETEX cleanup", c.cmd("DEL", "ge", "gdl"), 2)
        # String misc: SETNX / SETEX / GETSET / APPEND / STRLEN.
        check("SETNX new", c.cmd("SETNX", "nx", "one"), 1)
        check("GET after SETNX", c.cmd("GET", "nx"), "one")
        check("SETNX existing", c.cmd("SETNX", "nx", "two"), 0)
        check("GET after SETNX existing", c.cmd("GET", "nx"), "one")
        check("SETNX wrong args", c.cmd("SETNX", "nx"),
              ("error", "ERR wrong number of arguments for 'setnx' command"))
        check("RPUSH setnx list", c.cmd("RPUSH", "nxl", "a"), 1)
        check("SETNX over list is 0", c.cmd("SETNX", "nxl", "x"), 0)
        check("SETNX leaves list alone", c.cmd("LRANGE", "nxl", "0", "-1"), ["a"])
        check("SETEX", c.cmd("SETEX", "sx", "100", "val"), "OK")
        check("GET after SETEX", c.cmd("GET", "sx"), "val")
        check("TTL after SETEX ~", 99 <= c.cmd("TTL", "sx") <= 100, True)
        check("SETEX bad ttl 0", c.cmd("SETEX", "sx", "0", "val"),
              ("error", "ERR invalid expire time in 'setex' command"))
        check("SETEX bad ttl text", c.cmd("SETEX", "sx", "ten", "val"),
              ("error", "ERR invalid expire time in 'setex' command"))
        check("SETEX wrong args", c.cmd("SETEX", "sx", "100"),
              ("error", "ERR wrong number of arguments for 'setex' command"))
        check("SETEX overwrites list", c.cmd("SETEX", "nxl", "100", "now-string"), "OK")
        check("GET overwritten list", c.cmd("GET", "nxl"), "now-string")
        check("GETSET missing", c.cmd("GETSET", "gs", "first"), None)
        check("GET after GETSET missing", c.cmd("GET", "gs"), "first")
        check("GETSET", c.cmd("GETSET", "gs", "second"), "first")
        check("GET after GETSET", c.cmd("GET", "gs"), "second")
        check("SET EX getset ttl", c.cmd("SET", "gs", "v", "EX", "100"), "OK")
        check("GETSET clears TTL", c.cmd("GETSET", "gs", "w"), "v")
        check("TTL after GETSET", c.cmd("TTL", "gs"), -1)
        check("GETSET wrong args", c.cmd("GETSET", "gs"),
              ("error", "ERR wrong number of arguments for 'getset' command"))
        check("RPUSH getset list", c.cmd("RPUSH", "gsl", "a"), 1)
        check("GETSET wrongtype", c.cmd("GETSET", "gsl", "x"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("APPEND new", c.cmd("APPEND", "ap", "hello"), 5)
        check("GET after APPEND new", c.cmd("GET", "ap"), "hello")
        check("APPEND", c.cmd("APPEND", "ap", " world"), 11)
        check("GET after APPEND", c.cmd("GET", "ap"), "hello world")
        check("APPEND empty", c.cmd("APPEND", "ap", ""), 11)
        check("SET EX append ttl", c.cmd("SET", "ap", "x", "EX", "100"), "OK")
        check("APPEND preserves TTL", c.cmd("APPEND", "ap", "y"), 2)
        check("TTL after APPEND ~", 99 <= c.cmd("TTL", "ap") <= 100, True)
        check("APPEND wrong args", c.cmd("APPEND", "ap"),
              ("error", "ERR wrong number of arguments for 'append' command"))
        check("APPEND wrongtype", c.cmd("APPEND", "gsl", "x"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("STRLEN", c.cmd("STRLEN", "ap"), 2)
        check("STRLEN missing", c.cmd("STRLEN", "ghost"), 0)
        check("STRLEN wrong args", c.cmd("STRLEN"),
              ("error", "ERR wrong number of arguments for 'strlen' command"))
        check("STRLEN wrongtype", c.cmd("STRLEN", "gsl"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("COMMAND lists SETNX", "SETNX" in c.cmd("COMMAND"), True)
        check("COMMAND lists SETEX", "SETEX" in c.cmd("COMMAND"), True)
        check("COMMAND lists GETSET", "GETSET" in c.cmd("COMMAND"), True)
        check("COMMAND lists APPEND", "APPEND" in c.cmd("COMMAND"), True)
        check("COMMAND lists STRLEN", "STRLEN" in c.cmd("COMMAND"), True)
        check("string misc cleanup", c.cmd("DEL", "nx", "sx", "gs", "gsl", "ap", "nxl"), 6)
        # String ranges: GETRANGE / SETRANGE.
        check("SET grange", c.cmd("SET", "gr", "Hello World"), "OK")
        check("RPUSH grange list", c.cmd("RPUSH", "grl", "a"), 1)
        check("GETRANGE 0 4", c.cmd("GETRANGE", "gr", "0", "4"), "Hello")
        check("GETRANGE 6 -1", c.cmd("GETRANGE", "gr", "6", "-1"), "World")
        check("GETRANGE -5 -1", c.cmd("GETRANGE", "gr", "-5", "-1"), "World")
        check("GETRANGE 0 -1", c.cmd("GETRANGE", "gr", "0", "-1"), "Hello World")
        check("GETRANGE clamp stop", c.cmd("GETRANGE", "gr", "0", "100"), "Hello World")
        check("GETRANGE clamp start", c.cmd("GETRANGE", "gr", "-100", "2"), "Hel")
        check("GETRANGE start > stop", c.cmd("GETRANGE", "gr", "5", "2"), "")
        check("GETRANGE past end", c.cmd("GETRANGE", "gr", "20", "30"), "")
        check("GETRANGE missing", c.cmd("GETRANGE", "ghost", "0", "-1"), "")
        check("GETRANGE bad index", c.cmd("GETRANGE", "gr", "x", "1"),
              ("error", "ERR value is not an integer or out of range"))
        check("GETRANGE wrong args", c.cmd("GETRANGE", "gr", "0"),
              ("error", "ERR wrong number of arguments for 'getrange' command"))
        check("GETRANGE wrongtype", c.cmd("GETRANGE", "grl", "0", "-1"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("SETRANGE overwrite", c.cmd("SETRANGE", "gr", "6", "Redis"), 11)
        check("GET after SETRANGE", c.cmd("GET", "gr"), "Hello Redis")
        check("SETRANGE at end", c.cmd("SETRANGE", "gr", "11", "!"), 12)
        check("GET after SETRANGE at end", c.cmd("GET", "gr"), "Hello Redis!")
        # A missing key is created zero-padded up to the offset.
        check("SETRANGE missing creates", c.cmd("SETRANGE", "sr", "6", "Redis"), 11)
        check("STRLEN padded", c.cmd("STRLEN", "sr"), 11)
        check("GET padded", c.cmd("GET", "sr"), "\x00" * 6 + "Redis")
        check("GETRANGE padded head", c.cmd("GETRANGE", "sr", "0", "5"), "\x00" * 6)
        check("GETRANGE padded tail", c.cmd("GETRANGE", "sr", "6", "-1"), "Redis")
        check("SETRANGE into padding", c.cmd("SETRANGE", "sr", "0", "Hello"), 11)
        check("GET after SETRANGE into padding", c.cmd("GET", "sr"), "Hello\x00Redis")
        check("APPEND after padding", c.cmd("APPEND", "sr", "!"), 12)
        check("GET after APPEND padded", c.cmd("GET", "sr"), "Hello\x00Redis!")
        check("INCR padded is not an integer", c.cmd("INCR", "sr"),
              ("error", "ERR value is not an integer or out of range"))
        # SETRANGE preserves the TTL.
        check("SET EX setrange ttl", c.cmd("SET", "srt", "hello", "EX", "100"), "OK")
        check("SETRANGE preserves TTL", c.cmd("SETRANGE", "srt", "5", " world"), 11)
        check("TTL after SETRANGE ~", 99 <= c.cmd("TTL", "srt") <= 100, True)
        # An empty value changes nothing and creates nothing.
        check("SETRANGE empty missing", c.cmd("SETRANGE", "ghost", "5", ""), 0)
        check("EXISTS after empty SETRANGE", c.cmd("EXISTS", "ghost"), 0)
        check("SETRANGE empty existing", c.cmd("SETRANGE", "gr", "0", ""), 12)
        check("GET after empty SETRANGE", c.cmd("GET", "gr"), "Hello Redis!")
        check("SETRANGE negative offset", c.cmd("SETRANGE", "gr", "-1", "x"),
              ("error", "ERR offset is out of range"))
        check("SETRANGE bad offset", c.cmd("SETRANGE", "gr", "x", "y"),
              ("error", "ERR value is not an integer or out of range"))
        check("SETRANGE wrong args", c.cmd("SETRANGE", "gr", "0"),
              ("error", "ERR wrong number of arguments for 'setrange' command"))
        check("SETRANGE wrongtype", c.cmd("SETRANGE", "grl", "0", "x"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("COMMAND lists GETRANGE", "GETRANGE" in c.cmd("COMMAND"), True)
        check("COMMAND lists SETRANGE", "SETRANGE" in c.cmd("COMMAND"), True)
        check("string ranges cleanup", c.cmd("DEL", "gr", "sr", "srt", "grl"), 4)
        # String bits: SETBIT / GETBIT / BITCOUNT.
        check("GETBIT missing", c.cmd("GETBIT", "ghost", "7"), 0)
        check("BITCOUNT missing", c.cmd("BITCOUNT", "ghost"), 0)
        check("SETBIT new returns old 0", c.cmd("SETBIT", "bits", "7", "1"), 0)
        check("GETBIT set bit", c.cmd("GETBIT", "bits", "7"), 1)
        check("GETBIT unset bit", c.cmd("GETBIT", "bits", "0"), 0)
        check("BITCOUNT one bit", c.cmd("BITCOUNT", "bits"), 1)
        check("SETBIT same bit returns old 1", c.cmd("SETBIT", "bits", "7", "1"), 1)
        check("SETBIT clear returns old 1", c.cmd("SETBIT", "bits", "7", "0"), 1)
        check("GETBIT cleared", c.cmd("GETBIT", "bits", "7"), 0)
        check("BITCOUNT after clear", c.cmd("BITCOUNT", "bits"), 0)
        # Bit 0 is the high bit of byte 0: SETBIT 0 makes 0x80.
        check("SETBIT high bit", c.cmd("SETBIT", "bits", "0", "1"), 0)
        check("GETBIT high bit", c.cmd("GETBIT", "bits", "0"), 1)
        check("BITCOUNT high bit", c.cmd("BITCOUNT", "bits"), 1)
        # A bit past the end grows the value zero-padded.
        check("SETBIT far bit", c.cmd("SETBIT", "bits", "15", "1"), 0)
        check("STRLEN after far SETBIT", c.cmd("STRLEN", "bits"), 2)
        check("BITCOUNT two bits", c.cmd("BITCOUNT", "bits"), 2)
        check("BITCOUNT byte 0", c.cmd("BITCOUNT", "bits", "0", "0"), 1)
        check("BITCOUNT byte 1", c.cmd("BITCOUNT", "bits", "1", "1"), 1)
        check("BITCOUNT negative range", c.cmd("BITCOUNT", "bits", "-1", "-1"), 1)
        check("BITCOUNT empty range", c.cmd("BITCOUNT", "bits", "1", "0"), 0)
        check("BITCOUNT past end", c.cmd("BITCOUNT", "bits", "5", "10"), 0)
        check("BITCOUNT clamp end", c.cmd("BITCOUNT", "bits", "0", "100"), 2)
        check("GETBIT past end", c.cmd("GETBIT", "bits", "100"), 0)
        # SETBIT with value 0 still creates the key, zero-padded.
        check("SETBIT zero creates", c.cmd("SETBIT", "zb", "7", "0"), 0)
        check("EXISTS after zero SETBIT", c.cmd("EXISTS", "zb"), 1)
        check("STRLEN zero SETBIT", c.cmd("STRLEN", "zb"), 1)
        check("BITCOUNT zero SETBIT", c.cmd("BITCOUNT", "zb"), 0)
        check("GET zero SETBIT", c.cmd("GET", "zb"), "\x00")
        # SETBIT preserves the TTL.
        check("SET EX bit ttl", c.cmd("SET", "bt", "x", "EX", "100"), "OK")
        check("SETBIT preserves TTL", c.cmd("SETBIT", "bt", "7", "1"), 0)
        check("TTL after SETBIT ~", 99 <= c.cmd("TTL", "bt") <= 100, True)
        # Errors.
        check("SETBIT bad offset", c.cmd("SETBIT", "bits", "x", "1"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("SETBIT negative offset", c.cmd("SETBIT", "bits", "-1", "1"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("SETBIT huge offset", c.cmd("SETBIT", "bits", "4294967296", "1"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("SETBIT bad bit", c.cmd("SETBIT", "bits", "0", "2"),
              ("error", "ERR bit is not an integer or out of range"))
        check("SETBIT wrong args", c.cmd("SETBIT", "bits", "0"),
              ("error", "ERR wrong number of arguments for 'setbit' command"))
        check("GETBIT bad offset", c.cmd("GETBIT", "bits", "x"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("GETBIT negative offset", c.cmd("GETBIT", "bits", "-1"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("GETBIT wrong args", c.cmd("GETBIT", "bits"),
              ("error", "ERR wrong number of arguments for 'getbit' command"))
        check("BITCOUNT bad range", c.cmd("BITCOUNT", "bits", "x", "1"),
              ("error", "ERR value is not an integer or out of range"))
        check("BITCOUNT wrong args", c.cmd("BITCOUNT", "bits", "0"),
              ("error", "ERR wrong number of arguments for 'bitcount' command"))
        check("RPUSH bit list", c.cmd("RPUSH", "bl", "a"), 1)
        check("SETBIT wrongtype", c.cmd("SETBIT", "bl", "0", "1"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("GETBIT wrongtype", c.cmd("GETBIT", "bl", "0"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("BITCOUNT wrongtype", c.cmd("BITCOUNT", "bl"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("COMMAND lists SETBIT", "SETBIT" in c.cmd("COMMAND"), True)
        check("COMMAND lists GETBIT", "GETBIT" in c.cmd("COMMAND"), True)
        check("COMMAND lists BITCOUNT", "BITCOUNT" in c.cmd("COMMAND"), True)
        # BITOP: bitwise AND / OR / XOR / NOT across string keys.
        check("SET bitop a", c.cmd("SET", "ba", "foobar"), "OK")
        check("SET bitop b", c.cmd("SET", "bb", "abcdef"), "OK")
        check("BITOP AND", c.cmd("BITOP", "AND", "bd", "ba", "bb"), 6)
        check("GET BITOP AND", c.cmd("GET", "bd"), "`bc`ab")
        check("BITOP OR", c.cmd("BITOP", "OR", "bd", "ba", "bb"), 6)
        check("GET BITOP OR", c.cmd("GET", "bd"), "goofev")
        check("BITOP XOR", c.cmd("BITOP", "XOR", "bd", "ba", "bb"), 6)
        check("GET BITOP XOR", c.cmd("GET", "bd"), "\x07\r\x0c\x06\x04\x14")
        # A missing source counts as an empty string: AND with it is all zeros.
        check("BITOP AND missing", c.cmd("BITOP", "AND", "bd", "ba", "ghost"), 6)
        check("GET BITOP AND missing", c.cmd("GET", "bd"), "\x00\x00\x00\x00\x00\x00")
        # Every source missing: empty result, the destination is deleted.
        check("BITOP all missing", c.cmd("BITOP", "AND", "bd", "ghost1", "ghost2"), 0)
        check("EXISTS after empty BITOP", c.cmd("EXISTS", "bd"), 0)
        check("BITOP all missing no dest", c.cmd("BITOP", "OR", "bd", "ghost1", "ghost2"), 0)
        # NOT inverts the single source; checked by length and bit count
        # (the bytes themselves are not valid UTF-8, so GET is not compared).
        check("BITOP NOT", c.cmd("BITOP", "NOT", "bd", "ba"), 6)
        check("STRLEN BITOP NOT", c.cmd("STRLEN", "bd"), 6)
        check("BITCOUNT BITOP NOT", c.cmd("BITCOUNT", "bd"), 22)
        check("BITOP NOT missing", c.cmd("BITOP", "NOT", "bd", "ghost"), 0)
        check("EXISTS after NOT missing", c.cmd("EXISTS", "bd"), 0)
        # BITOP overwrites the destination and clears its TTL, like SET.
        check("SET EX bitop ttl", c.cmd("SET", "bd", "x", "EX", "100"), "OK")
        check("BITOP clears TTL", c.cmd("BITOP", "AND", "bd", "ba", "bb"), 6)
        check("TTL after BITOP", c.cmd("TTL", "bd"), -1)
        # The destination may also be a source key.
        check("BITOP dest is source", c.cmd("BITOP", "AND", "ba", "ba", "bb"), 6)
        check("GET dest is source", c.cmd("GET", "ba"), "`bc`ab")
        # Errors.
        check("BITOP bad op", c.cmd("BITOP", "NAND", "bd", "ba", "bb"),
              ("error", "ERR syntax error"))
        check("BITOP AND one key", c.cmd("BITOP", "AND", "bd", "ba"),
              ("error", "ERR wrong number of arguments for 'bitop' command"))
        check("BITOP NOT two keys", c.cmd("BITOP", "NOT", "bd", "ba", "bb"),
              ("error", "ERR BITOP NOT must be called with a single source key."))
        check("BITOP wrong args", c.cmd("BITOP", "AND", "bd"),
              ("error", "ERR wrong number of arguments for 'bitop' command"))
        check("BITOP wrongtype source", c.cmd("BITOP", "AND", "bd", "ba", "bl"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("BITOP wrongtype dest", c.cmd("BITOP", "AND", "bl", "ba", "bb"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("COMMAND lists BITOP", "BITOP" in c.cmd("COMMAND"), True)
        check("string bits cleanup", c.cmd("DEL", "bits", "zb", "bt", "bl", "ba", "bb", "bd"), 7)
        # BITFIELD: packed integer fields in a string key.
        # GET on a missing key reads 0 and creates nothing.
        check("BITFIELD GET missing", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [0])
        check("BITFIELD GET missing no key", c.cmd("EXISTS", "bf"), 0)
        # SET returns the old value; GET reads it back, signed and unsigned.
        check("BITFIELD SET i8", c.cmd("BITFIELD", "bf", "SET", "i8", "0", "65"), [0])
        check("BITFIELD GET i8", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [65])
        check("BITFIELD GET u8", c.cmd("BITFIELD", "bf", "GET", "u8", "0"), [65])
        check("BITFIELD STRLEN", c.cmd("STRLEN", "bf"), 1)
        # A negative value in a signed field reads back unsigned as 255.
        check("BITFIELD SET negative", c.cmd("BITFIELD", "bf", "SET", "i8", "0", "-1"), [65])
        check("BITFIELD GET i8 negative", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [-1])
        check("BITFIELD GET u8 negative", c.cmd("BITFIELD", "bf", "GET", "u8", "0"), [255])
        # SET wraps a value that does not fit the width: 200 in i8 is -56.
        check("BITFIELD SET wraps", c.cmd("BITFIELD", "bf", "SET", "i8", "0", "200"), [-1])
        check("BITFIELD GET wrapped", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [-56])
        # A second byte field: the u16 across both bytes is 0xC8FF.
        check("BITFIELD SET second byte", c.cmd("BITFIELD", "bf", "SET", "u8", "8", "255"), [0])
        check("BITFIELD GET u16", c.cmd("BITFIELD", "bf", "GET", "u16", "0"), [51455])
        check("BITFIELD GET i16", c.cmd("BITFIELD", "bf", "GET", "i16", "0"), [-14081])
        check("BITFIELD STRLEN two bytes", c.cmd("STRLEN", "bf"), 2)
        # #n addresses the n-th field of the encoding's width.
        check("BITFIELD SET hash offset", c.cmd("BITFIELD", "bf", "SET", "u8", "#1", "66"), [255])
        check("BITFIELD GET hash u8", c.cmd("BITFIELD", "bf", "GET", "u8", "#1"), [66])
        check("BITFIELD GET hash u16", c.cmd("BITFIELD", "bf", "GET", "u16", "0"), [51266])
        # INCRBY returns the new value, including negative increments.
        check("BITFIELD INCRBY", c.cmd("BITFIELD", "bf", "INCRBY", "i8", "0", "10"), [-46])
        check("BITFIELD INCRBY negative", c.cmd("BITFIELD", "bf", "INCRBY", "i8", "0", "-5"), [-51])
        # Several operations in one call run in order, one reply each.
        check("BITFIELD multi op", c.cmd("BITFIELD", "bf", "GET", "i8", "0", "SET", "i8", "8", "42", "INCRBY", "i8", "16", "5"), [-51, 66, 5])
        check("BITFIELD STRLEN three bytes", c.cmd("STRLEN", "bf"), 3)
        # OVERFLOW WRAP (the default) wraps around: -51 + 200 = 149 -> -107.
        check("BITFIELD OVERFLOW WRAP", c.cmd("BITFIELD", "bf", "OVERFLOW", "WRAP", "INCRBY", "i8", "0", "200"), [-107])
        # OVERFLOW SAT clamps at the type's range, both ends.
        check("BITFIELD OVERFLOW SAT", c.cmd("BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "300"), [127])
        check("BITFIELD OVERFLOW SAT low", c.cmd("BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "-300"), [-128])
        # OVERFLOW FAIL answers nil and leaves the field unchanged.
        check("BITFIELD OVERFLOW FAIL", c.cmd("BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "i8", "0", "-1"), [None])
        check("BITFIELD FAIL unchanged", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [-128])
        # OVERFLOW applies to SET too: FAIL refuses, SAT clamps.
        check("BITFIELD SET FAIL overflow", c.cmd("BITFIELD", "bf", "OVERFLOW", "FAIL", "SET", "i8", "0", "200"), [None])
        check("BITFIELD SET FAIL unchanged", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [-128])
        check("BITFIELD SET SAT overflow", c.cmd("BITFIELD", "bf", "OVERFLOW", "SAT", "SET", "i8", "0", "200"), [-128])
        check("BITFIELD SET SAT clamped", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [127])
        # The overflow policy resets to WRAP on the next call.
        check("BITFIELD overflow resets", c.cmd("BITFIELD", "bf", "INCRBY", "i8", "0", "1"), [-128])
        # 64-bit fields on a fresh key; u64 is rejected, u63 is the max.
        check("BITFIELD SET i64", c.cmd("BITFIELD", "bf64", "SET", "i64", "0", "-1"), [0])
        check("BITFIELD GET i64", c.cmd("BITFIELD", "bf64", "GET", "i64", "0"), [-1])
        check("BITFIELD GET u63", c.cmd("BITFIELD", "bf64", "GET", "u63", "0"), [9223372036854775807])
        check("BITFIELD SET u63", c.cmd("BITFIELD", "bf64", "SET", "u63", "0", "9223372036854775807"), [9223372036854775807])
        check("BITFIELD GET u63 max", c.cmd("BITFIELD", "bf64", "GET", "u63", "0"), [9223372036854775807])
        check("BITFIELD INCRBY u63 wrap", c.cmd("BITFIELD", "bf64", "INCRBY", "u63", "0", "1"), [0])
        check("BITFIELD STRLEN i64", c.cmd("STRLEN", "bf64"), 8)
        check("BITFIELD u64 rejected", c.cmd("BITFIELD", "bf", "GET", "u64", "0"),
              ("error", "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is the only unsigned 64 bit type."))
        # BITFIELD preserves the TTL.
        check("SET EX bitfield ttl", c.cmd("SET", "bft", "x", "EX", "100"), "OK")
        check("BITFIELD preserves TTL", c.cmd("BITFIELD", "bft", "SET", "i8", "0", "1"), [120])
        check("TTL after BITFIELD ~", 99 <= c.cmd("TTL", "bft") <= 100, True)
        # A syntax error anywhere means nothing executes.
        check("BITFIELD syntax error atomic", c.cmd("BITFIELD", "bf", "SET", "i8", "0", "99", "BOGUS", "i8", "0"),
              ("error", "ERR syntax error"))
        check("BITFIELD atomic GET", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [-128])
        # Errors.
        check("BITFIELD bad type", c.cmd("BITFIELD", "bf", "GET", "x8", "0"),
              ("error", "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is the only unsigned 64 bit type."))
        check("BITFIELD i0", c.cmd("BITFIELD", "bf", "GET", "i0", "0"),
              ("error", "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is the only unsigned 64 bit type."))
        check("BITFIELD bad subcommand", c.cmd("BITFIELD", "bf", "BOGUS", "i8", "0"),
              ("error", "ERR syntax error"))
        check("BITFIELD bad overflow", c.cmd("BITFIELD", "bf", "OVERFLOW", "MAYBE", "GET", "i8", "0"),
              ("error", "ERR Invalid OVERFLOW type"))
        check("BITFIELD bad offset", c.cmd("BITFIELD", "bf", "GET", "i8", "x"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("BITFIELD negative offset", c.cmd("BITFIELD", "bf", "GET", "i8", "-1"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("BITFIELD offset past max", c.cmd("BITFIELD", "bf", "GET", "i8", "4294967289"),
              ("error", "ERR bit offset is not an integer or out of range"))
        check("BITFIELD max offset GET", c.cmd("BITFIELD", "bf", "GET", "i8", "4294967288"), [0])
        check("BITFIELD bad value", c.cmd("BITFIELD", "bf", "SET", "i8", "0", "x"),
              ("error", "ERR value is not an integer or out of range"))
        check("BITFIELD wrong args", c.cmd("BITFIELD"),
              ("error", "ERR wrong number of arguments for 'bitfield' command"))
        check("BITFIELD no ops", c.cmd("BITFIELD", "bf"), [])
        check("RPUSH bitfield list", c.cmd("RPUSH", "bfl", "a"), 1)
        check("BITFIELD wrongtype", c.cmd("BITFIELD", "bfl", "GET", "i8", "0"),
              ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value"))
        check("COMMAND lists BITFIELD", "BITFIELD" in c.cmd("COMMAND"), True)
        # A BITFIELD write invalidates a WATCH; a no-op write does not.
        c2 = Client(PORT)
        check("BITFIELD watch", c2.cmd("WATCH", "bf"), "OK")
        check("BITFIELD watch multi", c2.cmd("MULTI"), "OK")
        check("BITFIELD watch queued", c2.cmd("SET", "bf", "z"), "QUEUED")
        check("BITFIELD watch write", c.cmd("BITFIELD", "bf", "SET", "i8", "0", "7"), [-128])
        check("BITFIELD watch exec aborts", c2.cmd("EXEC"), None)
        check("BITFIELD watch value kept", c.cmd("BITFIELD", "bf", "GET", "i8", "0"), [7])
        check("BITFIELD watch noop", c2.cmd("WATCH", "bf"), "OK")
        check("BITFIELD watch noop multi", c2.cmd("MULTI"), "OK")
        check("BITFIELD watch noop queued", c2.cmd("SET", "bf", "z"), "QUEUED")
        check("BITFIELD noop write", c.cmd("BITFIELD", "bf", "SET", "i8", "0", "7"), [7])
        check("BITFIELD watch noop exec runs", c2.cmd("EXEC"), ["OK"])
        check("BITFIELD watch noop value", c.cmd("GET", "bf"), "z")
        c2.close()
        check("bitfield cleanup", c.cmd("DEL", "bf", "bf64", "bft", "bfl"), 4)
        # MSET / MGET: set and get several string keys at once.
        check("MSET", c.cmd("MSET", "m1", "one", "m2", "two", "m3", "three"), "OK")
        check("MGET all", c.cmd("MGET", "m1", "m2", "m3"), ["one", "two", "three"])
        check("MGET missing", c.cmd("MGET", "m1", "ghost", "m3"), ["one", None, "three"])
        check("MSET odd args", c.cmd("MSET", "m1", "one", "m2"),
              ("error", "ERR wrong number of arguments for 'mset' command"))
        check("MSET no args", c.cmd("MSET"),
              ("error", "ERR wrong number of arguments for 'mset' command"))
        check("MGET no args", c.cmd("MGET"),
              ("error", "ERR wrong number of arguments for 'mget' command"))
        check("RPUSH mget list", c.cmd("RPUSH", "ml", "a"), 1)
        check("MGET wrongtype is nil", c.cmd("MGET", "m1", "ml"), ["one", None])
        check("MSET overwrites list", c.cmd("MSET", "ml", "now-string"), "OK")
        check("GET overwritten list", c.cmd("GET", "ml"), "now-string")
        check("MSET/MGET cleanup", c.cmd("DEL", "m1", "m2", "m3", "ml"), 4)
        check("SET persist", c.cmd("SET", "durable", "yes"), "OK")
        info = c.cmd("INFO")
        check("INFO has keys", "keys:4" in info, True)
        check("INFO default fsync policy", "aof_fsync_policy:always" in info, True)
        check("unknown cmd", c.cmd("NOPE"), ("error", "ERR unknown command"))
        check("FLUSHALL", c.cmd("FLUSHALL"), "OK")
        check("DBSIZE after flush", c.cmd("DBSIZE"), 0)
        check("SET after flush", c.cmd("SET", "after", "flush"), "OK")
        # Many concurrent clients at once: exercises the epoll loop with
        # dozens of ready fds in a single epoll_wait batch, and the
        # fd-to-client lookup when the client array is large.
        conc = [Client(PORT) for _ in range(64)]
        try:
            for i, cc in enumerate(conc):
                check(f"concurrent SET {i}", cc.cmd("SET", f"conc:{i}", str(i)), "OK")
            ok = all(cc.cmd("GET", f"conc:{i}") == str(i) for i, cc in enumerate(conc))
            check("concurrent GET all", ok, True)
            check("DBSIZE after concurrent", c.cmd("DBSIZE"), 65)
            # Clean up: later tests (AOF restart) count exact keys.
            check("concurrent DEL all", c.cmd("DEL", *[f"conc:{i}" for i in range(64)]), 64)
            check("DBSIZE after concurrent cleanup", c.cmd("DBSIZE"), 1)
        finally:
            for cc in conc:
                cc.close()
        # Lists: push/pop at both ends, length, range, wrong type, TTL.
        check("RPUSH", c.cmd("RPUSH", "mylist", "a", "b", "c"), 3)
        check("LLEN", c.cmd("LLEN", "mylist"), 3)
        check("LRANGE all", c.cmd("LRANGE", "mylist", "0", "-1"), ["a", "b", "c"])
        check("LPUSH", c.cmd("LPUSH", "mylist", "z"), 4)
        check("LRANGE after LPUSH", c.cmd("LRANGE", "mylist", "0", "-1"), ["z", "a", "b", "c"])
        check("LRANGE neg", c.cmd("LRANGE", "mylist", "-2", "-1"), ["b", "c"])
        check("LRANGE empty range", c.cmd("LRANGE", "mylist", "2", "1"), [])
        check("LRANGE out of range", c.cmd("LRANGE", "mylist", "10", "20"), [])
        check("LPOP", c.cmd("LPOP", "mylist"), "z")
        check("RPOP", c.cmd("RPOP", "mylist"), "c")
        check("LLEN after pops", c.cmd("LLEN", "mylist"), 2)
        check("LPOP missing", c.cmd("LPOP", "ghostlist"), None)
        check("RPOP missing", c.cmd("RPOP", "ghostlist"), None)
        check("LLEN missing", c.cmd("LLEN", "ghostlist"), 0)
        check("LRANGE missing", c.cmd("LRANGE", "ghostlist", "0", "-1"), [])
        wrongtype = ("error", "ERR WRONGTYPE Operation against a key holding the wrong kind of value")
        check("LPUSH wrongtype", c.cmd("LPUSH", "after", "x"), wrongtype)
        check("RPUSH wrongtype", c.cmd("RPUSH", "after", "x"), wrongtype)
        check("LPOP wrongtype", c.cmd("LPOP", "after"), wrongtype)
        check("LLEN wrongtype", c.cmd("LLEN", "after"), wrongtype)
        check("LRANGE wrongtype", c.cmd("LRANGE", "after", "0", "-1"), wrongtype)
        check("GET list wrongtype", c.cmd("GET", "mylist"), wrongtype)
        check("INCR list wrongtype", c.cmd("INCR", "mylist"), wrongtype)
        check("LPUSH ttl list", c.cmd("LPUSH", "blinklist", "a"), 1)
        check("PEXPIRE list", c.cmd("PEXPIRE", "blinklist", "50"), 1)
        time.sleep(0.1)
        check("LLEN expired list", c.cmd("LLEN", "blinklist"), 0)
        check("LRANGE expired list", c.cmd("LRANGE", "blinklist", "0", "-1"), [])
        check("RPUSH solo", c.cmd("RPUSH", "solo", "only"), 1)
        check("LPOP solo", c.cmd("LPOP", "solo"), "only")
        check("EXISTS after last pop", c.cmd("EXISTS", "solo"), 0)
        check("DEL list", c.cmd("DEL", "mylist"), 1)
        # List index commands: LINDEX / LSET / LINSERT.
        check("RPUSH idxlist", c.cmd("RPUSH", "idxlist", "a", "b", "c", "d"), 4)
        check("LINDEX 0", c.cmd("LINDEX", "idxlist", "0"), "a")
        check("LINDEX -1", c.cmd("LINDEX", "idxlist", "-1"), "d")
        check("LINDEX 2", c.cmd("LINDEX", "idxlist", "2"), "c")
        check("LINDEX out of range", c.cmd("LINDEX", "idxlist", "4"), None)
        check("LINDEX neg out of range", c.cmd("LINDEX", "idxlist", "-5"), None)
        check("LINDEX missing", c.cmd("LINDEX", "ghostlist", "0"), None)
        check("LSET 1", c.cmd("LSET", "idxlist", "1", "B"), "OK")
        check("LRANGE after LSET", c.cmd("LRANGE", "idxlist", "0", "-1"), ["a", "B", "c", "d"])
        check("LSET -1", c.cmd("LSET", "idxlist", "-1", "D"), "OK")
        check("LINDEX after LSET", c.cmd("LINDEX", "idxlist", "3"), "D")
        check("LSET no-op same value", c.cmd("LSET", "idxlist", "0", "a"), "OK")
        check("LRANGE after no-op LSET", c.cmd("LRANGE", "idxlist", "0", "-1"), ["a", "B", "c", "D"])
        check("LSET missing", c.cmd("LSET", "ghostlist", "0", "x"),
              ("error", "ERR no such key"))
        check("LSET out of range", c.cmd("LSET", "idxlist", "4", "x"),
              ("error", "ERR index out of range"))
        check("LSET wrongtype", c.cmd("LSET", "after", "0", "x"), wrongtype)
        check("LINDEX wrongtype", c.cmd("LINDEX", "after", "0"), wrongtype)
        check("LINSERT BEFORE", c.cmd("LINSERT", "idxlist", "BEFORE", "B", "X"), 5)
        check("LRANGE after LINSERT BEFORE", c.cmd("LRANGE", "idxlist", "0", "-1"),
              ["a", "X", "B", "c", "D"])
        check("LINSERT AFTER", c.cmd("LINSERT", "idxlist", "AFTER", "D", "Y"), 6)
        check("LRANGE after LINSERT AFTER", c.cmd("LRANGE", "idxlist", "0", "-1"),
              ["a", "X", "B", "c", "D", "Y"])
        check("LINSERT pivot absent", c.cmd("LINSERT", "idxlist", "BEFORE", "zzz", "Q"), 0)
        check("LINSERT missing", c.cmd("LINSERT", "ghostlist", "BEFORE", "a", "x"), -1)
        check("LINSERT bad direction", c.cmd("LINSERT", "idxlist", "SIDEWAYS", "a", "x"),
              ("error", "ERR syntax error"))
        check("LINSERT wrongtype", c.cmd("LINSERT", "after", "BEFORE", "a", "x"), wrongtype)
        check("RPUSH ttllist", c.cmd("RPUSH", "ttllist", "a", "b"), 2)
        check("PEXPIRE ttllist", c.cmd("PEXPIRE", "ttllist", "10000"), 1)
        check("LSET ttl list", c.cmd("LSET", "ttllist", "0", "z"), "OK")
        check("TTL survives LSET", c.cmd("TTL", "ttllist") > 0, True)
        check("LINSERT ttl list", c.cmd("LINSERT", "ttllist", "AFTER", "z", "w"), 3)
        check("TTL survives LINSERT", c.cmd("TTL", "ttllist") > 0, True)
        # LPUSHX / RPUSHX / LTRIM / LMOVE.
        check("RPUSH trimlist", c.cmd("RPUSH", "trimlist", "a", "b", "c", "d", "e"), 5)
        check("LPUSHX existing", c.cmd("LPUSHX", "trimlist", "z"), 6)
        check("RPUSHX existing", c.cmd("RPUSHX", "trimlist", "f"), 7)
        check("LRANGE after LPUSHX/RPUSHX", c.cmd("LRANGE", "trimlist", "0", "-1"),
              ["z", "a", "b", "c", "d", "e", "f"])
        check("LPUSHX missing", c.cmd("LPUSHX", "ghostlist", "x"), 0)
        check("RPUSHX missing", c.cmd("RPUSHX", "ghostlist", "x"), 0)
        check("EXISTS after LPUSHX missing", c.cmd("EXISTS", "ghostlist"), 0)
        check("LPUSHX multi", c.cmd("LPUSHX", "trimlist", "y", "x"), 9)
        check("LRANGE after LPUSHX multi", c.cmd("LRANGE", "trimlist", "0", "-1"),
              ["x", "y", "z", "a", "b", "c", "d", "e", "f"])
        check("LPUSHX wrongtype", c.cmd("LPUSHX", "after", "x"), wrongtype)
        check("RPUSHX wrongtype", c.cmd("RPUSHX", "after", "x"), wrongtype)
        check("LTRIM middle", c.cmd("LTRIM", "trimlist", "2", "4"), "OK")
        check("LRANGE after LTRIM", c.cmd("LRANGE", "trimlist", "0", "-1"), ["z", "a", "b"])
        check("LTRIM negative", c.cmd("LTRIM", "trimlist", "-2", "-1"), "OK")
        check("LRANGE after LTRIM neg", c.cmd("LRANGE", "trimlist", "0", "-1"), ["a", "b"])
        check("LTRIM whole list", c.cmd("LTRIM", "trimlist", "0", "-1"), "OK")
        check("LRANGE after whole LTRIM", c.cmd("LRANGE", "trimlist", "0", "-1"), ["a", "b"])
        check("LTRIM empty range deletes", c.cmd("LTRIM", "trimlist", "5", "2"), "OK")
        check("EXISTS after empty LTRIM", c.cmd("EXISTS", "trimlist"), 0)
        check("LTRIM missing", c.cmd("LTRIM", "ghostlist", "0", "-1"), "OK")
        check("LTRIM wrongtype", c.cmd("LTRIM", "after", "0", "-1"), wrongtype)
        check("LTRIM ttl list", c.cmd("LTRIM", "ttllist", "0", "1"), "OK")
        check("LRANGE after LTRIM ttl", c.cmd("LRANGE", "ttllist", "0", "-1"), ["z", "w"])
        check("TTL survives LTRIM", c.cmd("TTL", "ttllist") > 0, True)
        check("RPUSH srclist", c.cmd("RPUSH", "srclist", "a", "b", "c"), 3)
        check("LMOVE left left", c.cmd("LMOVE", "srclist", "dstlist", "LEFT", "LEFT"), "a")
        check("LRANGE source after LMOVE", c.cmd("LRANGE", "srclist", "0", "-1"), ["b", "c"])
        check("LRANGE dest after LMOVE", c.cmd("LRANGE", "dstlist", "0", "-1"), ["a"])
        check("LMOVE right right", c.cmd("LMOVE", "srclist", "dstlist", "RIGHT", "RIGHT"), "c")
        check("LRANGE dest after LMOVE RR", c.cmd("LRANGE", "dstlist", "0", "-1"), ["a", "c"])
        check("LMOVE empties source", c.cmd("LMOVE", "srclist", "dstlist", "LEFT", "RIGHT"), "b")
        check("EXISTS after LMOVE empties", c.cmd("EXISTS", "srclist"), 0)
        check("LRANGE dest after emptying", c.cmd("LRANGE", "dstlist", "0", "-1"), ["a", "c", "b"])
        check("LMOVE missing source", c.cmd("LMOVE", "ghostlist", "dstlist", "LEFT", "RIGHT"), None)
        check("LMOVE same key", c.cmd("LMOVE", "dstlist", "dstlist", "LEFT", "RIGHT"), "a")
        check("LRANGE after same-key LMOVE", c.cmd("LRANGE", "dstlist", "0", "-1"), ["c", "b", "a"])
        check("LMOVE bad direction", c.cmd("LMOVE", "dstlist", "dstlist", "UP", "RIGHT"),
              ("error", "ERR syntax error"))
        check("LMOVE wrongtype source", c.cmd("LMOVE", "after", "dstlist", "LEFT", "RIGHT"), wrongtype)
        check("LMOVE wrongtype dest", c.cmd("LMOVE", "dstlist", "after", "LEFT", "RIGHT"), wrongtype)
        check("LRANGE dest after wrongtype LMOVE", c.cmd("LRANGE", "dstlist", "0", "-1"),
              ["c", "b", "a"])
        cmds = c.cmd("COMMAND")
        check("COMMAND lists LINDEX", "LINDEX" in cmds, True)
        check("COMMAND lists LSET", "LSET" in cmds, True)
        check("COMMAND lists LINSERT", "LINSERT" in cmds, True)
        check("COMMAND lists LPUSHX", "LPUSHX" in cmds, True)
        check("COMMAND lists RPUSHX", "RPUSHX" in cmds, True)
        check("COMMAND lists LTRIM", "LTRIM" in cmds, True)
        check("COMMAND lists LMOVE", "LMOVE" in cmds, True)
        check("DEL idxlist", c.cmd("DEL", "idxlist", "ttllist"), 2)
        check("DEL dstlist", c.cmd("DEL", "dstlist"), 1)
        # Sets: add/dedup, membership, remove, wrong type, TTL, emptying.
        check("SADD", c.cmd("SADD", "myset", "a", "b", "c"), 3)
        check("SADD dup", c.cmd("SADD", "myset", "b", "d"), 1)
        check("SCARD", c.cmd("SCARD", "myset"), 4)
        check("SISMEMBER yes", c.cmd("SISMEMBER", "myset", "a"), 1)
        check("SISMEMBER no", c.cmd("SISMEMBER", "myset", "zzz"), 0)
        check("SMEMBERS", sorted(c.cmd("SMEMBERS", "myset")), ["a", "b", "c", "d"])
        check("SREM", c.cmd("SREM", "myset", "a", "zzz"), 1)
        check("SCARD after SREM", c.cmd("SCARD", "myset"), 3)
        check("SREM missing", c.cmd("SREM", "ghostset", "a"), 0)
        check("SCARD missing", c.cmd("SCARD", "ghostset"), 0)
        check("SISMEMBER missing", c.cmd("SISMEMBER", "ghostset", "a"), 0)
        check("SMEMBERS missing", c.cmd("SMEMBERS", "ghostset"), [])
        check("SADD wrongtype", c.cmd("SADD", "after", "x"), wrongtype)
        check("SREM wrongtype", c.cmd("SREM", "after", "x"), wrongtype)
        check("SCARD wrongtype", c.cmd("SCARD", "after"), wrongtype)
        check("SISMEMBER wrongtype", c.cmd("SISMEMBER", "after", "x"), wrongtype)
        check("SMEMBERS wrongtype", c.cmd("SMEMBERS", "after"), wrongtype)
        check("GET set wrongtype", c.cmd("GET", "myset"), wrongtype)
        check("INCR set wrongtype", c.cmd("INCR", "myset"), wrongtype)
        check("LPUSH set wrongtype", c.cmd("LPUSH", "myset", "x"), wrongtype)
        check("SADD ttl set", c.cmd("SADD", "blinkset", "a"), 1)
        check("PEXPIRE set", c.cmd("PEXPIRE", "blinkset", "50"), 1)
        time.sleep(0.1)
        check("SCARD expired set", c.cmd("SCARD", "blinkset"), 0)
        check("SREM empties set", c.cmd("SREM", "myset", "b", "c", "d"), 3)
        check("EXISTS after set emptied", c.cmd("EXISTS", "myset"), 0)
        check("SADD persist set", c.cmd("SADD", "pset", "x", "y", "z"), 3)
        # Large-set dict checks: 100 members force the member dict to
        # grow several times, and removing a middle member swaps the
        # last member into its hole, so the swapped member must still
        # be found (its dict index moved with it).
        big = ["m%d" % i for i in range(100)]
        check("SADD big set", c.cmd("SADD", "bigset", *big), 100)
        check("SCARD big set", c.cmd("SCARD", "bigset"), 100)
        check("SISMEMBER big first", c.cmd("SISMEMBER", "bigset", "m0"), 1)
        check("SISMEMBER big mid", c.cmd("SISMEMBER", "bigset", "m50"), 1)
        check("SISMEMBER big last", c.cmd("SISMEMBER", "bigset", "m99"), 1)
        check("SADD big dup", c.cmd("SADD", "bigset", "m50", "new"), 1)
        check("SREM big middle", c.cmd("SREM", "bigset", "m0"), 1)
        check("SISMEMBER swapped-in last", c.cmd("SISMEMBER", "bigset", "m99"), 1)
        check("SISMEMBER removed", c.cmd("SISMEMBER", "bigset", "m0"), 0)
        check("SCARD big after SREM", c.cmd("SCARD", "bigset"), 100)
        check("SREM big last", c.cmd("SREM", "bigset", "m99"), 1)
        check("SISMEMBER big last gone", c.cmd("SISMEMBER", "bigset", "m99"), 0)
        check("SADD big re-add", c.cmd("SADD", "bigset", "m0"), 1)
        check("SMEMBERS big count", len(c.cmd("SMEMBERS", "bigset")), 100)
        check("SMEMBERS big contents", sorted(c.cmd("SMEMBERS", "bigset")),
              sorted([m for m in big if m != "m99"] + ["new"]))
        check("DEL bigset", c.cmd("DEL", "bigset"), 1)
        check("RPUSH persist list", c.cmd("RPUSH", "plist", "x", "y", "z"), 3)
        # Hashes: set/get multiple fields, update, delete, wrong type,
        # TTL, emptying, and AOF persistence.
        check("HSET", c.cmd("HSET", "myhash", "f1", "v1", "f2", "v2"), 2)
        check("HSET update", c.cmd("HSET", "myhash", "f1", "v1b"), 0)
        check("HGET", c.cmd("HGET", "myhash", "f1"), "v1b")
        check("HGET missing field", c.cmd("HGET", "myhash", "ghost"), None)
        check("HGET missing key", c.cmd("HGET", "ghosthash", "f1"), None)
        check("HLEN", c.cmd("HLEN", "myhash"), 2)
        check("HLEN missing", c.cmd("HLEN", "ghosthash"), 0)
        check("HEXISTS yes", c.cmd("HEXISTS", "myhash", "f1"), 1)
        check("HEXISTS no", c.cmd("HEXISTS", "myhash", "ghost"), 0)
        check("HEXISTS missing", c.cmd("HEXISTS", "ghosthash", "f1"), 0)
        check("HGETALL", c.cmd("HGETALL", "myhash"), ["f1", "v1b", "f2", "v2"])
        check("HGETALL missing", c.cmd("HGETALL", "ghosthash"), [])
        check("HDEL", c.cmd("HDEL", "myhash", "f2", "ghost"), 1)
        check("HLEN after HDEL", c.cmd("HLEN", "myhash"), 1)
        check("HGETALL after HDEL", c.cmd("HGETALL", "myhash"), ["f1", "v1b"])
        check("HSET wrongtype", c.cmd("HSET", "after", "f", "v"), wrongtype)
        check("HGET wrongtype", c.cmd("HGET", "after", "f"), wrongtype)
        check("HDEL wrongtype", c.cmd("HDEL", "after", "f"), wrongtype)
        check("HLEN wrongtype", c.cmd("HLEN", "after"), wrongtype)
        check("HEXISTS wrongtype", c.cmd("HEXISTS", "after", "f"), wrongtype)
        check("HGETALL wrongtype", c.cmd("HGETALL", "after"), wrongtype)
        check("GET hash wrongtype", c.cmd("GET", "myhash"), wrongtype)
        check("LPUSH hash wrongtype", c.cmd("LPUSH", "myhash", "x"), wrongtype)
        check("SADD hash wrongtype", c.cmd("SADD", "myhash", "x"), wrongtype)
        check("HSET ttl hash", c.cmd("HSET", "blinkhash", "f", "v"), 1)
        check("PEXPIRE hash", c.cmd("PEXPIRE", "blinkhash", "50"), 1)
        time.sleep(0.1)
        check("HLEN expired hash", c.cmd("HLEN", "blinkhash"), 0)
        check("HGETALL expired hash", c.cmd("HGETALL", "blinkhash"), [])
        check("HDEL empties hash", c.cmd("HDEL", "myhash", "f1"), 1)
        check("EXISTS after hash emptied", c.cmd("EXISTS", "myhash"), 0)
        check("HSET persist hash", c.cmd("HSET", "phash", "a", "1", "b", "2"), 2)
        # HMGET / HSETNX / HINCRBY: multi-field read, conditional set,
        # integer fields, wrong type, TTL, and WATCH behaviour (the
        # WATCH checks live in the transaction section below).
        check("HSET hmhash", c.cmd("HSET", "hmhash", "a", "1", "b", "2", "s", "txt"), 3)
        check("HMGET", c.cmd("HMGET", "hmhash", "a", "b", "ghost"), ["1", "2", None])
        check("HMGET missing key", c.cmd("HMGET", "ghosthash", "a"), [None])
        check("HMGET wrongtype", c.cmd("HMGET", "after", "a"), wrongtype)
        check("HMGET wrong args", c.cmd("HMGET", "hmhash"),
              ("error", "ERR wrong number of arguments for 'hmget' command"))
        check("HSETNX new", c.cmd("HSETNX", "hmhash", "c", "3"), 1)
        check("HSETNX existing", c.cmd("HSETNX", "hmhash", "a", "9"), 0)
        check("HGET after HSETNX noop", c.cmd("HGET", "hmhash", "a"), "1")
        check("HSETNX missing key", c.cmd("HSETNX", "nxhash", "f", "v"), 1)
        check("HSETNX wrongtype", c.cmd("HSETNX", "after", "f", "v"), wrongtype)
        check("HSETNX wrong args", c.cmd("HSETNX", "hmhash", "a"),
              ("error", "ERR wrong number of arguments for 'hsetnx' command"))
        check("HINCRBY", c.cmd("HINCRBY", "hmhash", "a", "4"), 5)
        check("HINCRBY new field", c.cmd("HINCRBY", "hmhash", "ctr", "10"), 10)
        check("HINCRBY negative", c.cmd("HINCRBY", "hmhash", "ctr", "-3"), 7)
        check("HINCRBY zero noop", c.cmd("HINCRBY", "hmhash", "a", "0"), 5)
        check("HINCRBY missing key", c.cmd("HINCRBY", "incrhash", "n", "2"), 2)
        check("HINCRBY non-integer field", c.cmd("HINCRBY", "hmhash", "s", "1"),
              ("error", "ERR hash value is not an integer"))
        check("HINCRBY bad increment", c.cmd("HINCRBY", "hmhash", "a", "xx"),
              ("error", "ERR value is not an integer or out of range"))
        check("HINCRBY wrongtype", c.cmd("HINCRBY", "after", "f", "1"), wrongtype)
        check("HINCRBY wrong args", c.cmd("HINCRBY", "hmhash", "a"),
              ("error", "ERR wrong number of arguments for 'hincrby' command"))
        check("HSET hmhash big", c.cmd("HSET", "hmhash", "big", "9223372036854775807"), 1)
        check("HINCRBY overflow", c.cmd("HINCRBY", "hmhash", "big", "1"),
              ("error", "ERR increment or decrement would overflow"))
        check("HGET big unchanged after overflow", c.cmd("HGET", "hmhash", "big"), "9223372036854775807")
        check("HSET ttl hincrby", c.cmd("HSET", "blinkhincr", "n", "1"), 1)
        check("PEXPIRE hincrby", c.cmd("PEXPIRE", "blinkhincr", "100000"), 1)
        check("HINCRBY preserves TTL", c.cmd("HINCRBY", "blinkhincr", "n", "1"), 2)
        check("TTL after HINCRBY ~", 99 <= c.cmd("TTL", "blinkhincr") <= 100, True)
        # HKEYS / HVALS: insertion order, missing key, wrong type.
        check("HKEYS", c.cmd("HKEYS", "hmhash"),
              ["a", "b", "s", "c", "ctr", "big"])
        check("HVALS", c.cmd("HVALS", "hmhash"),
              ["5", "2", "txt", "3", "7", "9223372036854775807"])
        check("HKEYS missing key", c.cmd("HKEYS", "ghosthash"), [])
        check("HVALS missing key", c.cmd("HVALS", "ghosthash"), [])
        check("HKEYS wrongtype", c.cmd("HKEYS", "after"), wrongtype)
        check("HVALS wrongtype", c.cmd("HVALS", "after"), wrongtype)
        check("HKEYS wrong args", c.cmd("HKEYS", "hmhash", "x"),
              ("error", "ERR wrong number of arguments for 'hkeys' command"))
        check("HVALS wrong args", c.cmd("HVALS"),
              ("error", "ERR wrong number of arguments for 'hvals' command"))
        # HINCRBYFLOAT: float fields, new fields and keys, errors,
        # overflow, zero no-op, TTL.
        check("HSET floathash", c.cmd("HSET", "floathash", "x", "1.5", "s", "txt"), 2)
        check("HINCRBYFLOAT", c.cmd("HINCRBYFLOAT", "floathash", "x", "0.25"), "1.75")
        check("HINCRBYFLOAT new field", c.cmd("HINCRBYFLOAT", "floathash", "y", "2"), "2")
        check("HINCRBYFLOAT negative", c.cmd("HINCRBYFLOAT", "floathash", "y", "-0.5"), "1.5")
        check("HINCRBYFLOAT zero noop", c.cmd("HINCRBYFLOAT", "floathash", "x", "0"), "1.75")
        check("HINCRBYFLOAT missing key", c.cmd("HINCRBYFLOAT", "floatnew", "n", "3"), "3")
        check("HINCRBYFLOAT non-float field", c.cmd("HINCRBYFLOAT", "floathash", "s", "1"),
              ("error", "ERR hash value is not a float"))
        check("HINCRBYFLOAT bad increment", c.cmd("HINCRBYFLOAT", "floathash", "x", "xx"),
              ("error", "ERR value is not a valid float"))
        check("HINCRBYFLOAT inf increment", c.cmd("HINCRBYFLOAT", "floathash", "x", "inf"),
              ("error", "ERR increment or decrement would overflow"))
        check("HGET unchanged after inf increment", c.cmd("HGET", "floathash", "x"), "1.75")
        check("HSET floathash big", c.cmd("HSET", "floathash", "big", "1.7976931348623157e308"), 1)
        check("HINCRBYFLOAT overflow", c.cmd("HINCRBYFLOAT", "floathash", "big", "1.7976931348623157e308"),
              ("error", "ERR increment or decrement would overflow"))
        check("HGET big unchanged after float overflow", c.cmd("HGET", "floathash", "big"), "1.7976931348623157e308")
        check("HINCRBYFLOAT wrongtype", c.cmd("HINCRBYFLOAT", "after", "f", "1"), wrongtype)
        check("HINCRBYFLOAT wrong args", c.cmd("HINCRBYFLOAT", "floathash", "x"),
              ("error", "ERR wrong number of arguments for 'hincrbyfloat' command"))
        check("HSET ttl hincrbyfloat", c.cmd("HSET", "blinkfloat", "n", "1.5"), 1)
        check("PEXPIRE hincrbyfloat", c.cmd("PEXPIRE", "blinkfloat", "100000"), 1)
        check("HINCRBYFLOAT preserves TTL", c.cmd("HINCRBYFLOAT", "blinkfloat", "n", "0.5"), "2")
        check("TTL after HINCRBYFLOAT ~", 99 <= c.cmd("TTL", "blinkfloat") <= 100, True)
        # HSCAN: full iteration, MATCH, COUNT pagination, errors.
        check("HSET scanhash", c.cmd("HSET", "scanhash", "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5"), 5)
        check("HSCAN all", c.cmd("HSCAN", "scanhash", "0"),
              ["0", ["f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5"]])
        check("HSCAN MATCH f1*", c.cmd("HSCAN", "scanhash", "0", "MATCH", "f1*"),
              ["0", ["f1", "v1"]])
        check("HSCAN MATCH none", c.cmd("HSCAN", "scanhash", "0", "MATCH", "zzz*"),
              ["0", []])
        check("HSCAN COUNT 2 page 1", c.cmd("HSCAN", "scanhash", "0", "COUNT", "2"),
              ["2", ["f1", "v1", "f2", "v2"]])
        check("HSCAN COUNT 2 page 2", c.cmd("HSCAN", "scanhash", "2", "COUNT", "2"),
              ["4", ["f3", "v3", "f4", "v4"]])
        check("HSCAN COUNT 2 page 3", c.cmd("HSCAN", "scanhash", "4", "COUNT", "2"),
              ["0", ["f5", "v5"]])
        check("HSCAN missing key", c.cmd("HSCAN", "ghosthash", "0"), ["0", []])
        check("HSCAN wrongtype", c.cmd("HSCAN", "after", "0"), wrongtype)
        check("HSCAN bad cursor", c.cmd("HSCAN", "scanhash", "abc"),
              ("error", "ERR invalid cursor"))
        check("HSCAN wrong args", c.cmd("HSCAN", "scanhash"),
              ("error", "ERR wrong number of arguments for 'hscan' command"))
        check("HSCAN bad option", c.cmd("HSCAN", "scanhash", "0", "NOPE"),
              ("error", "ERR syntax error"))
        # SSCAN: full iteration, MATCH, COUNT pagination, errors.
        check("SADD scanset", c.cmd("SADD", "scanset", "m1", "m2", "m3", "m4", "m5"), 5)
        check("SSCAN all", c.cmd("SSCAN", "scanset", "0"),
              ["0", ["m1", "m2", "m3", "m4", "m5"]])
        check("SSCAN MATCH m1*", c.cmd("SSCAN", "scanset", "0", "MATCH", "m1*"),
              ["0", ["m1"]])
        check("SSCAN MATCH none", c.cmd("SSCAN", "scanset", "0", "MATCH", "zzz*"),
              ["0", []])
        check("SSCAN COUNT 2 page 1", c.cmd("SSCAN", "scanset", "0", "COUNT", "2"),
              ["2", ["m1", "m2"]])
        check("SSCAN COUNT 2 page 2", c.cmd("SSCAN", "scanset", "2", "COUNT", "2"),
              ["4", ["m3", "m4"]])
        check("SSCAN COUNT 2 page 3", c.cmd("SSCAN", "scanset", "4", "COUNT", "2"),
              ["0", ["m5"]])
        check("SSCAN COUNT 0", c.cmd("SSCAN", "scanset", "0", "COUNT", "0"),
              ["0", []])
        check("SSCAN cursor past end", c.cmd("SSCAN", "scanset", "99"),
              ["0", []])
        check("SSCAN missing key", c.cmd("SSCAN", "ghostset", "0"), ["0", []])
        check("SSCAN wrongtype", c.cmd("SSCAN", "after", "0"), wrongtype)
        check("SSCAN bad cursor", c.cmd("SSCAN", "scanset", "abc"),
              ("error", "ERR invalid cursor"))
        check("SSCAN wrong args", c.cmd("SSCAN", "scanset"),
              ("error", "ERR wrong number of arguments for 'sscan' command"))
        check("SSCAN bad option", c.cmd("SSCAN", "scanset", "0", "NOPE"),
              ("error", "ERR syntax error"))
        check("COMMAND lists SSCAN", "SSCAN" in c.cmd("COMMAND"), True)
        check("COMMAND lists HMGET", "HMGET" in c.cmd("COMMAND"), True)
        check("COMMAND lists HSETNX", "HSETNX" in c.cmd("COMMAND"), True)
        check("COMMAND lists HINCRBY", "HINCRBY" in c.cmd("COMMAND"), True)
        check("COMMAND lists HKEYS", "HKEYS" in c.cmd("COMMAND"), True)
        check("COMMAND lists HVALS", "HVALS" in c.cmd("COMMAND"), True)
        check("COMMAND lists HINCRBYFLOAT", "HINCRBYFLOAT" in c.cmd("COMMAND"), True)
        check("COMMAND lists HSCAN", "HSCAN" in c.cmd("COMMAND"), True)
        # Hash dict: a hash big enough to grow the field table, with
        # deletes in the middle, so lookups after the delete exercise
        # the re-indexing of the fields that moved down a slot.
        big_fields = ["f%02d" % i for i in range(50)]
        big_args = []
        for f in big_fields:
            big_args += [f, "v" + f[1:]]
        check("HSET bighash 50 fields", c.cmd("HSET", "bighash", *big_args), 50)
        check("HLEN bighash", c.cmd("HLEN", "bighash"), 50)
        check("HGET bighash late field", c.cmd("HGET", "bighash", "f49"), "v49")
        check("HDEL bighash middle", c.cmd("HDEL", "bighash", "f10", "f25"), 2)
        check("HLEN bighash after HDEL", c.cmd("HLEN", "bighash"), 48)
        check("HGET bighash after deleted field", c.cmd("HGET", "bighash", "f26"), "v26")
        check("HGET bighash after deleted field 2", c.cmd("HGET", "bighash", "f11"), "v11")
        check("HGET bighash deleted field", c.cmd("HGET", "bighash", "f10"), None)
        check("HSET bighash update after delete", c.cmd("HSET", "bighash", "f26", "v26b"), 0)
        check("HGET bighash updated", c.cmd("HGET", "bighash", "f26"), "v26b")
        check("HSETNX bighash re-add deleted", c.cmd("HSETNX", "bighash", "f10", "new"), 1)
        check("HINCRBY bighash", c.cmd("HINCRBY", "bighash", "ctr", "5"), 5)
        big_keys = (["f%02d" % i for i in range(10)]
                    + ["f%02d" % i for i in range(11, 25)]
                    + ["f%02d" % i for i in range(26, 50)]
                    + ["f10", "ctr"])
        check("HKEYS bighash order", c.cmd("HKEYS", "bighash"), big_keys)
        big_vals = {"f%02d" % i: "v%02d" % i for i in range(50)}
        del big_vals["f25"]
        big_vals["f10"] = "new"
        big_vals["f26"] = "v26b"
        big_vals["ctr"] = "5"
        big_flat = [x for k in big_keys for x in (k, big_vals[k])]
        check("HGETALL bighash order", c.cmd("HGETALL", "bighash"), big_flat)
        check("HSCAN bighash all", c.cmd("HSCAN", "bighash", "0", "COUNT", "100"),
              ["0", big_flat])
        # Sorted sets: add/update, scores, wrong type, TTL, AOF
        # persistence. Members are ordered by (score, member); range
        # commands are not implemented yet.
        check("ZADD", c.cmd("ZADD", "myzset", "1.5", "a", "2", "b", "0.5", "c"), 3)
        check("ZADD update score", c.cmd("ZADD", "myzset", "3", "a"), 0)
        check("ZADD same score", c.cmd("ZADD", "myzset", "3", "a"), 0)
        check("ZADD dup member one call", c.cmd("ZADD", "myzset", "4", "b", "5", "b"), 0)
        check("ZCARD", c.cmd("ZCARD", "myzset"), 3)
        check("ZCARD missing", c.cmd("ZCARD", "ghostzset"), 0)
        check("ZSCORE updated", c.cmd("ZSCORE", "myzset", "a"), "3")
        check("ZSCORE float", c.cmd("ZSCORE", "myzset", "b"), "5")
        check("ZSCORE low", c.cmd("ZSCORE", "myzset", "c"), "0.5")
        check("ZSCORE missing member", c.cmd("ZSCORE", "myzset", "ghost"), None)
        check("ZSCORE missing key", c.cmd("ZSCORE", "ghostzset", "a"), None)
        check("ZADD bad score", c.cmd("ZADD", "myzset", "nan", "x"),
              ("error", "ERR value is not a valid float"))
        check("ZADD bad score second pair", c.cmd("ZADD", "myzset", "1", "ok", "xx", "x"),
              ("error", "ERR value is not a valid float"))
        check("ZSCORE unchanged after bad ZADD", c.cmd("ZSCORE", "myzset", "ok"), None)
        check("ZADD wrong args", c.cmd("ZADD", "myzset", "1"),
              ("error", "ERR wrong number of arguments for 'zadd' command"))
        check("ZADD wrongtype", c.cmd("ZADD", "after", "1", "x"), wrongtype)
        check("ZSCORE wrongtype", c.cmd("ZSCORE", "after", "x"), wrongtype)
        check("ZCARD wrongtype", c.cmd("ZCARD", "after"), wrongtype)
        check("GET zset wrongtype", c.cmd("GET", "myzset"), wrongtype)
        check("SADD zset wrongtype", c.cmd("SADD", "myzset", "x"), wrongtype)
        check("HSET zset wrongtype", c.cmd("HSET", "myzset", "f", "v"), wrongtype)
        check("ZADD ttl zset", c.cmd("ZADD", "blinkzset", "1", "a"), 1)
        check("PEXPIRE zset", c.cmd("PEXPIRE", "blinkzset", "50"), 1)
        time.sleep(0.1)
        check("ZCARD expired zset", c.cmd("ZCARD", "blinkzset"), 0)
        check("ZSCORE expired zset", c.cmd("ZSCORE", "blinkzset", "a"), None)
        check("ZADD persist zset", c.cmd("ZADD", "pzset", "1.5", "x", "2.25", "y", "10", "z"), 3)
        # ZADD options: NX/XX/GT/LT/CH/INCR, on a fresh key so the
        # range checks below keep seeing myzset as they expect.
        check("ZADD NX new", c.cmd("ZADD", "optzset", "NX", "1", "a", "2", "b"), 2)
        check("ZADD NX lowercase", c.cmd("ZADD", "optzset", "nx", "1", "low"), 1)
        check("ZADD NX skips existing", c.cmd("ZADD", "optzset", "NX", "9", "a", "3", "c"), 1)
        check("ZSCORE after NX skip", c.cmd("ZSCORE", "optzset", "a"), "1")
        check("ZADD XX updates only", c.cmd("ZADD", "optzset", "XX", "5", "a", "9", "d"), 0)
        check("ZSCORE after XX update", c.cmd("ZSCORE", "optzset", "a"), "5")
        check("ZSCORE after XX skip", c.cmd("ZSCORE", "optzset", "d"), None)
        check("ZADD XX missing member", c.cmd("ZADD", "optzset", "XX", "7", "ghost"), 0)
        check("ZADD GT rejects lower", c.cmd("ZADD", "optzset", "GT", "4", "a"), 0)
        check("ZSCORE after GT reject", c.cmd("ZSCORE", "optzset", "a"), "5")
        check("ZADD GT accepts higher", c.cmd("ZADD", "optzset", "GT", "6", "a"), 0)
        check("ZSCORE after GT accept", c.cmd("ZSCORE", "optzset", "a"), "6")
        check("ZADD GT adds new", c.cmd("ZADD", "optzset", "GT", "10", "new"), 1)
        check("ZADD LT rejects higher", c.cmd("ZADD", "optzset", "LT", "100", "a"), 0)
        check("ZSCORE after LT reject", c.cmd("ZSCORE", "optzset", "a"), "6")
        check("ZADD LT accepts lower", c.cmd("ZADD", "optzset", "LT", "2", "a"), 0)
        check("ZSCORE after LT accept", c.cmd("ZSCORE", "optzset", "a"), "2")
        check("ZADD LT adds new", c.cmd("ZADD", "optzset", "LT", "0", "new2"), 1)
        check("ZADD XX GT combined", c.cmd("ZADD", "optzset", "XX", "GT", "9", "a"), 0)
        check("ZSCORE after XX GT", c.cmd("ZSCORE", "optzset", "a"), "9")
        check("ZADD XX GT rejects lower", c.cmd("ZADD", "optzset", "XX", "GT", "1", "a"), 0)
        check("ZSCORE after XX GT reject", c.cmd("ZSCORE", "optzset", "a"), "9")
        check("ZADD CH counts updates", c.cmd("ZADD", "optzset", "CH", "9", "a", "3", "b"), 1)
        check("ZADD CH counts adds", c.cmd("ZADD", "optzset", "CH", "7", "e", "8", "f"), 2)
        check("ZADD CH NX skips", c.cmd("ZADD", "optzset", "CH", "NX", "99", "a"), 0)
        check("ZADD INCR existing", c.cmd("ZADD", "optzset", "INCR", "1.5", "a"), "10.5")
        check("ZADD INCR new member", c.cmd("ZADD", "optzset", "INCR", "2", "g"), "2")
        check("ZADD INCR NX blocked", c.cmd("ZADD", "optzset", "NX", "INCR", "10", "a"), None)
        check("ZSCORE after INCR NX block", c.cmd("ZSCORE", "optzset", "a"), "10.5")
        check("ZADD INCR XX missing", c.cmd("ZADD", "optzset", "XX", "INCR", "10", "ghost2"), None)
        check("ZSCORE after INCR XX miss", c.cmd("ZSCORE", "optzset", "ghost2"), None)
        check("ZADD INCR XX updates", c.cmd("ZADD", "optzset", "XX", "INCR", "1", "a"), "11.5")
        check("ZADD INCR GT accepts", c.cmd("ZADD", "optzset", "GT", "INCR", "1", "a"), "12.5")
        check("ZADD INCR GT rejects", c.cmd("ZADD", "optzset", "GT", "INCR", "-10", "a"), None)
        check("ZSCORE after INCR GT reject", c.cmd("ZSCORE", "optzset", "a"), "12.5")
        check("ZADD INCR LT accepts", c.cmd("ZADD", "optzset", "LT", "INCR", "-1", "a"), "11.5")
        check("ZADD INCR zero noop", c.cmd("ZADD", "optzset", "INCR", "0", "a"), "11.5")
        check("ZADD INCR inf member", c.cmd("ZADD", "optzset", "inf", "h"), 1)
        check("ZADD INCR NaN result", c.cmd("ZADD", "optzset", "INCR", "-inf", "h"),
              ("error", "ERR resulting score is not a number (NaN)"))
        check("ZSCORE after INCR NaN", c.cmd("ZSCORE", "optzset", "h"), "inf")
        check("ZADD INCR multi pair", c.cmd("ZADD", "optzset", "INCR", "1", "a", "2", "b"),
              ("error", "ERR INCR option supports a single increment-element pair"))
        check("ZADD NX XX conflict", c.cmd("ZADD", "optzset", "NX", "XX", "1", "a"),
              ("error", "ERR XX and NX options at the same time are not compatible"))
        check("ZADD GT LT conflict", c.cmd("ZADD", "optzset", "GT", "LT", "1", "a"),
              ("error", "ERR GT, LT, and/or NX options at the same time are not compatible"))
        check("ZADD NX GT conflict", c.cmd("ZADD", "optzset", "NX", "GT", "1", "a"),
              ("error", "ERR GT, LT, and/or NX options at the same time are not compatible"))
        check("ZADD option after pair", c.cmd("ZADD", "optzset", "1", "x", "NX"),
              ("error", "ERR wrong number of arguments for 'zadd' command"))
        check("ZADD options wrongtype", c.cmd("ZADD", "after", "NX", "1", "x"), wrongtype)
        check("ZADD options cleanup", c.cmd("DEL", "optzset"), 1)
        # A ZADD with more than 1024 arguments used to kill the
        # connection: the RESP parser capped the argument count at
        # 1024, so a ZADD of 1001 members (2004 args) was a protocol
        # error and the server closed the connection with no reply.
        # A throwaway client keeps a regression here from taking the
        # main client down with it.
        big = Client(PORT)
        try:
            pairs = [x for i in range(1001) for x in (str(i), f"m{i}")]
            check("ZADD 1001 members", big.cmd("ZADD", "manyargszset", *pairs), 1001)
            check("ZCARD 1001 members", big.cmd("ZCARD", "manyargszset"), 1001)
            check("ZSCORE of last big member", big.cmd("ZSCORE", "manyargszset", "m1000"), "1000")
            check("connection alive after big ZADD", big.cmd("PING"), "PONG")
            check("ZADD 1001 members cleanup", big.cmd("DEL", "manyargszset"), 1)
        except EOFError:
            check("ZADD 1001 members (connection closed)", "closed", 1001)
        finally:
            big.close()
        # Sorted-set ranges, rank, removal and increment. myzset holds
        # c=0.5, a=3, b=5 at this point.
        check("ZRANGE all", c.cmd("ZRANGE", "myzset", "0", "-1"), ["c", "a", "b"])
        check("ZRANGE first two", c.cmd("ZRANGE", "myzset", "0", "1"), ["c", "a"])
        check("ZRANGE negative", c.cmd("ZRANGE", "myzset", "-2", "-1"), ["a", "b"])
        check("ZRANGE withscores", c.cmd("ZRANGE", "myzset", "0", "-1", "WITHSCORES"),
              ["c", "0.5", "a", "3", "b", "5"])
        check("ZRANGE empty range", c.cmd("ZRANGE", "myzset", "2", "1"), [])
        check("ZRANGE out of range", c.cmd("ZRANGE", "myzset", "5", "10"), [])
        check("ZRANGE missing", c.cmd("ZRANGE", "ghostzset", "0", "-1"), [])
        check("ZREVRANGE all", c.cmd("ZREVRANGE", "myzset", "0", "-1"), ["b", "a", "c"])
        check("ZREVRANGE withscores", c.cmd("ZREVRANGE", "myzset", "0", "-1", "WITHSCORES"),
              ["b", "5", "a", "3", "c", "0.5"])
        check("ZREVRANGE negative", c.cmd("ZREVRANGE", "myzset", "-2", "-1"), ["a", "c"])
        check("ZRANK lowest", c.cmd("ZRANK", "myzset", "c"), 0)
        check("ZRANK middle", c.cmd("ZRANK", "myzset", "a"), 1)
        check("ZRANK highest", c.cmd("ZRANK", "myzset", "b"), 2)
        check("ZRANK missing member", c.cmd("ZRANK", "myzset", "ghost"), None)
        check("ZRANK missing key", c.cmd("ZRANK", "ghostzset", "a"), None)
        check("ZREVRANK highest", c.cmd("ZREVRANK", "myzset", "b"), 0)
        check("ZREVRANK middle", c.cmd("ZREVRANK", "myzset", "a"), 1)
        check("ZREVRANK lowest", c.cmd("ZREVRANK", "myzset", "c"), 2)
        check("ZREVRANK missing member", c.cmd("ZREVRANK", "myzset", "ghost"), None)
        check("ZREVRANK missing key", c.cmd("ZREVRANK", "ghostzset", "a"), None)
        check("ZINCRBY existing", c.cmd("ZINCRBY", "myzset", "2", "a"), "5")
        check("ZRANGE after tie", c.cmd("ZRANGE", "myzset", "0", "-1"), ["c", "a", "b"])
        check("ZINCRBY new member", c.cmd("ZINCRBY", "myzset", "1.5", "d"), "1.5")
        check("ZCARD after ZINCRBY", c.cmd("ZCARD", "myzset"), 4)
        check("ZINCRBY zero noop", c.cmd("ZINCRBY", "myzset", "0", "a"), "5")
        check("ZINCRBY new key", c.cmd("ZINCRBY", "newzset", "-2.5", "m"), "-2.5")
        check("ZINCRBY bad float", c.cmd("ZINCRBY", "myzset", "xx", "a"),
              ("error", "ERR value is not a valid float"))
        # Ranges by score. myzset holds c=0.5, d=1.5, a=5, b=5 here
        # (a before b on the score tie, member order).
        check("ZRANGEBYSCORE all", c.cmd("ZRANGEBYSCORE", "myzset", "0", "10"),
              ["c", "d", "a", "b"])
        check("ZRANGEBYSCORE slice", c.cmd("ZRANGEBYSCORE", "myzset", "1", "5"),
              ["d", "a", "b"])
        check("ZRANGEBYSCORE excl min", c.cmd("ZRANGEBYSCORE", "myzset", "(1", "5"),
              ["d", "a", "b"])
        check("ZRANGEBYSCORE excl max", c.cmd("ZRANGEBYSCORE", "myzset", "0", "(5"),
              ["c", "d"])
        check("ZRANGEBYSCORE excl both", c.cmd("ZRANGEBYSCORE", "myzset", "(0.5", "(5"),
              ["d"])
        check("ZRANGEBYSCORE infinities", c.cmd("ZRANGEBYSCORE", "myzset", "-inf", "+inf"),
              ["c", "d", "a", "b"])
        check("ZRANGEBYSCORE withscores",
              c.cmd("ZRANGEBYSCORE", "myzset", "-inf", "+inf", "WITHSCORES"),
              ["c", "0.5", "d", "1.5", "a", "5", "b", "5"])
        check("ZRANGEBYSCORE limit", c.cmd("ZRANGEBYSCORE", "myzset", "0", "10",
                                           "LIMIT", "1", "2"), ["d", "a"])
        check("ZRANGEBYSCORE limit all", c.cmd("ZRANGEBYSCORE", "myzset", "0", "10",
                                               "LIMIT", "0", "-1"),
              ["c", "d", "a", "b"])
        check("ZRANGEBYSCORE limit past end", c.cmd("ZRANGEBYSCORE", "myzset", "0", "10",
                                                    "LIMIT", "10", "5"), [])
        check("ZRANGEBYSCORE limit withscores",
              c.cmd("ZRANGEBYSCORE", "myzset", "0", "10", "WITHSCORES", "LIMIT", "2", "1"),
              ["a", "5"])
        check("ZRANGEBYSCORE reversed bounds", c.cmd("ZRANGEBYSCORE", "myzset", "10", "0"), [])
        check("ZRANGEBYSCORE empty slice", c.cmd("ZRANGEBYSCORE", "myzset", "2", "3"), [])
        check("ZRANGEBYSCORE missing", c.cmd("ZRANGEBYSCORE", "ghostzset", "-inf", "+inf"), [])
        check("ZRANGEBYSCORE wrong args", c.cmd("ZRANGEBYSCORE", "myzset", "0"),
              ("error", "ERR wrong number of arguments for 'zrangebyscore' command"))
        check("ZRANGEBYSCORE bad bound", c.cmd("ZRANGEBYSCORE", "myzset", "xx", "10"),
              ("error", "ERR value is not a valid float"))
        check("ZRANGEBYSCORE bad bound paren", c.cmd("ZRANGEBYSCORE", "myzset", "(", "10"),
              ("error", "ERR value is not a valid float"))
        check("ZRANGEBYSCORE bad option", c.cmd("ZRANGEBYSCORE", "myzset", "0", "10", "BOGUS"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYSCORE limit missing count",
              c.cmd("ZRANGEBYSCORE", "myzset", "0", "10", "LIMIT", "1"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYSCORE limit negative offset",
              c.cmd("ZRANGEBYSCORE", "myzset", "0", "10", "LIMIT", "-1", "5"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYSCORE wrongtype", c.cmd("ZRANGEBYSCORE", "after", "0", "10"), wrongtype)
        check("COMMAND lists ZRANGEBYSCORE", "ZRANGEBYSCORE" in c.cmd("COMMAND"), True)
        # Reversed ranges by score. Same myzset (c=0.5, d=1.5, a=5, b=5);
        # bounds are max-first, as in Redis, and ties come back in
        # reverse member order (b before a).
        check("ZREVRANGEBYSCORE all", c.cmd("ZREVRANGEBYSCORE", "myzset", "10", "0"),
              ["b", "a", "d", "c"])
        check("ZREVRANGEBYSCORE slice", c.cmd("ZREVRANGEBYSCORE", "myzset", "5", "1"),
              ["b", "a", "d"])
        check("ZREVRANGEBYSCORE excl max", c.cmd("ZREVRANGEBYSCORE", "myzset", "(5", "1"),
              ["d"])
        check("ZREVRANGEBYSCORE excl min", c.cmd("ZREVRANGEBYSCORE", "myzset", "5", "(1"),
              ["b", "a", "d"])
        check("ZREVRANGEBYSCORE excl both",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "(5", "(0.5"), ["d"])
        check("ZREVRANGEBYSCORE infinities",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "+inf", "-inf"),
              ["b", "a", "d", "c"])
        check("ZREVRANGEBYSCORE withscores",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "+inf", "-inf", "WITHSCORES"),
              ["b", "5", "a", "5", "d", "1.5", "c", "0.5"])
        check("ZREVRANGEBYSCORE limit", c.cmd("ZREVRANGEBYSCORE", "myzset", "10", "0",
                                               "LIMIT", "1", "2"), ["a", "d"])
        check("ZREVRANGEBYSCORE limit all", c.cmd("ZREVRANGEBYSCORE", "myzset", "10", "0",
                                                   "LIMIT", "0", "-1"),
              ["b", "a", "d", "c"])
        check("ZREVRANGEBYSCORE limit past end",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "10", "0", "LIMIT", "10", "5"), [])
        check("ZREVRANGEBYSCORE limit withscores",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "10", "0", "WITHSCORES", "LIMIT", "2", "1"),
              ["d", "1.5"])
        check("ZREVRANGEBYSCORE reversed bounds",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "0", "10"), [])
        check("ZREVRANGEBYSCORE empty slice",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "3", "2"), [])
        check("ZREVRANGEBYSCORE missing",
              c.cmd("ZREVRANGEBYSCORE", "ghostzset", "+inf", "-inf"), [])
        check("ZREVRANGEBYSCORE wrong args", c.cmd("ZREVRANGEBYSCORE", "myzset", "10"),
              ("error", "ERR wrong number of arguments for 'zrevrangebyscore' command"))
        check("ZREVRANGEBYSCORE bad bound", c.cmd("ZREVRANGEBYSCORE", "myzset", "xx", "0"),
              ("error", "ERR value is not a valid float"))
        check("ZREVRANGEBYSCORE bad option",
              c.cmd("ZREVRANGEBYSCORE", "myzset", "10", "0", "BOGUS"),
              ("error", "ERR syntax error"))
        check("ZREVRANGEBYSCORE wrongtype",
              c.cmd("ZREVRANGEBYSCORE", "after", "10", "0"), wrongtype)
        check("COMMAND lists ZREVRANGEBYSCORE", "ZREVRANGEBYSCORE" in c.cmd("COMMAND"), True)
        # Lexicographic ranges. myzset holds c=0.5, d=1.5, a=5, b=5
        # here, so score order (c, d, a, b) and member order (a, b,
        # c, d) differ — a lex range must follow member order.
        check("ZRANGEBYLEX all", c.cmd("ZRANGEBYLEX", "myzset", "-", "+"),
              ["a", "b", "c", "d"])
        check("ZRANGEBYLEX slice", c.cmd("ZRANGEBYLEX", "myzset", "[a", "[c"),
              ["a", "b", "c"])
        check("ZRANGEBYLEX excl min", c.cmd("ZRANGEBYLEX", "myzset", "(a", "[c"),
              ["b", "c"])
        check("ZRANGEBYLEX excl max", c.cmd("ZRANGEBYLEX", "myzset", "[a", "(c"),
              ["a", "b"])
        check("ZRANGEBYLEX excl both", c.cmd("ZRANGEBYLEX", "myzset", "(a", "(d"),
              ["b", "c"])
        check("ZRANGEBYLEX from start", c.cmd("ZRANGEBYLEX", "myzset", "-", "[b"),
              ["a", "b"])
        check("ZRANGEBYLEX to end", c.cmd("ZRANGEBYLEX", "myzset", "[c", "+"),
              ["c", "d"])
        check("ZRANGEBYLEX rev all", c.cmd("ZRANGEBYLEX", "myzset", "+", "-",
                                           "REV"), ["d", "c", "b", "a"])
        check("ZRANGEBYLEX rev slice", c.cmd("ZRANGEBYLEX", "myzset", "[c", "[a",
                                             "REV"), ["c", "b", "a"])
        check("ZRANGEBYLEX rev excl", c.cmd("ZRANGEBYLEX", "myzset", "+", "(b",
                                            "REV"), ["d", "c"])
        check("ZRANGEBYLEX limit", c.cmd("ZRANGEBYLEX", "myzset", "-", "+",
                                         "LIMIT", "1", "2"), ["b", "c"])
        check("ZRANGEBYLEX limit all", c.cmd("ZRANGEBYLEX", "myzset", "-", "+",
                                             "LIMIT", "0", "-1"),
              ["a", "b", "c", "d"])
        check("ZRANGEBYLEX limit past end", c.cmd("ZRANGEBYLEX", "myzset", "-",
                                                  "+", "LIMIT", "10", "5"), [])
        check("ZRANGEBYLEX limit rev", c.cmd("ZRANGEBYLEX", "myzset", "+", "-",
                                             "LIMIT", "1", "2", "REV"),
              ["c", "b"])
        check("ZRANGEBYLEX rev before limit",
              c.cmd("ZRANGEBYLEX", "myzset", "+", "-", "REV", "LIMIT", "1", "2"),
              ["c", "b"])
        check("ZRANGEBYLEX min above max",
              c.cmd("ZRANGEBYLEX", "myzset", "[d", "[a"), [])
        check("ZRANGEBYLEX empty slice",
              c.cmd("ZRANGEBYLEX", "myzset", "[x", "[z"), [])
        check("ZRANGEBYLEX missing",
              c.cmd("ZRANGEBYLEX", "ghostzset", "-", "+"), [])
        check("ZRANGEBYLEX wrong args", c.cmd("ZRANGEBYLEX", "myzset", "[a"),
              ("error", "ERR wrong number of arguments for 'zrangebylex' command"))
        check("ZRANGEBYLEX bare bound", c.cmd("ZRANGEBYLEX", "myzset", "a", "[z"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX plus as min", c.cmd("ZRANGEBYLEX", "myzset", "+", "[z"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX minus as max", c.cmd("ZRANGEBYLEX", "myzset", "[a", "-"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX empty bracket", c.cmd("ZRANGEBYLEX", "myzset", "[", "+"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX bad option", c.cmd("ZRANGEBYLEX", "myzset", "-", "+",
                                              "WITHSCORES"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX limit missing count",
              c.cmd("ZRANGEBYLEX", "myzset", "-", "+", "LIMIT", "1"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX limit negative offset",
              c.cmd("ZRANGEBYLEX", "myzset", "-", "+", "LIMIT", "-1", "5"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX duplicate limit",
              c.cmd("ZRANGEBYLEX", "myzset", "-", "+", "LIMIT", "0", "1",
                    "LIMIT", "0", "1"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX duplicate rev",
              c.cmd("ZRANGEBYLEX", "myzset", "-", "+", "REV", "REV"),
              ("error", "ERR syntax error"))
        check("ZRANGEBYLEX wrongtype", c.cmd("ZRANGEBYLEX", "after", "-", "+"),
              wrongtype)
        # Lexicographic counts. Same myzset (a, b, c, d), same
        # bounds as ZRANGEBYLEX, but a count instead of the members.
        check("ZLEXCOUNT all", c.cmd("ZLEXCOUNT", "myzset", "-", "+"), 4)
        check("ZLEXCOUNT slice", c.cmd("ZLEXCOUNT", "myzset", "[a", "[c"), 3)
        check("ZLEXCOUNT excl min", c.cmd("ZLEXCOUNT", "myzset", "(a", "[c"), 2)
        check("ZLEXCOUNT excl max", c.cmd("ZLEXCOUNT", "myzset", "[a", "(c"), 2)
        check("ZLEXCOUNT excl both", c.cmd("ZLEXCOUNT", "myzset", "(a", "(d"), 2)
        check("ZLEXCOUNT from start", c.cmd("ZLEXCOUNT", "myzset", "-", "[b"), 2)
        check("ZLEXCOUNT to end", c.cmd("ZLEXCOUNT", "myzset", "[c", "+"), 2)
        check("ZLEXCOUNT min above max",
              c.cmd("ZLEXCOUNT", "myzset", "[d", "[a"), 0)
        check("ZLEXCOUNT empty slice",
              c.cmd("ZLEXCOUNT", "myzset", "[x", "[z"), 0)
        check("ZLEXCOUNT missing",
              c.cmd("ZLEXCOUNT", "ghostzset", "-", "+"), 0)
        check("ZLEXCOUNT wrong args", c.cmd("ZLEXCOUNT", "myzset", "[a"),
              ("error", "ERR wrong number of arguments for 'zlexcount' command"))
        check("ZLEXCOUNT bare bound", c.cmd("ZLEXCOUNT", "myzset", "a", "[z"),
              ("error", "ERR syntax error"))
        check("ZLEXCOUNT plus as min", c.cmd("ZLEXCOUNT", "myzset", "+", "[z"),
              ("error", "ERR syntax error"))
        check("ZLEXCOUNT minus as max", c.cmd("ZLEXCOUNT", "myzset", "[a", "-"),
              ("error", "ERR syntax error"))
        check("ZLEXCOUNT empty bracket", c.cmd("ZLEXCOUNT", "myzset", "[", "+"),
              ("error", "ERR syntax error"))
        check("ZLEXCOUNT wrongtype", c.cmd("ZLEXCOUNT", "after", "-", "+"),
              wrongtype)
        check("COMMAND lists ZLEXCOUNT", "ZLEXCOUNT" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZRANGEBYLEX", "ZRANGEBYLEX" in c.cmd("COMMAND"), True)
        # ZREMRANGEBYSCORE. rrzset holds a=1, b=2, c=3, d=4, e=5.
        check("ZADD rrzset", c.cmd("ZADD", "rrzset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"), 5)
        check("ZREMRANGEBYSCORE mid", c.cmd("ZREMRANGEBYSCORE", "rrzset", "2", "4"), 3)
        check("ZRANGE after ZREMRANGEBYSCORE", c.cmd("ZRANGE", "rrzset", "0", "-1"), ["a", "e"])
        check("ZREMRANGEBYSCORE excl both empty",
              c.cmd("ZREMRANGEBYSCORE", "rrzset", "(1", "(5"), 0)
        check("ZCARD after excl empty", c.cmd("ZCARD", "rrzset"), 2)
        check("ZREMRANGEBYSCORE excl max", c.cmd("ZREMRANGEBYSCORE", "rrzset", "1", "(5"), 1)
        check("ZRANGE after excl max", c.cmd("ZRANGE", "rrzset", "0", "-1"), ["e"])
        check("ZREMRANGEBYSCORE all", c.cmd("ZREMRANGEBYSCORE", "rrzset", "-inf", "+inf"), 1)
        check("EXISTS emptied by ZREMRANGEBYSCORE", c.cmd("EXISTS", "rrzset"), 0)
        check("ZREMRANGEBYSCORE missing", c.cmd("ZREMRANGEBYSCORE", "ghostzset", "0", "10"), 0)
        check("ZADD rrzset2", c.cmd("ZADD", "rrzset2", "1", "a", "2", "b"), 2)
        check("ZREMRANGEBYSCORE min above max",
              c.cmd("ZREMRANGEBYSCORE", "rrzset2", "5", "1"), 0)
        check("ZCARD after reversed range", c.cmd("ZCARD", "rrzset2"), 2)
        check("ZREMRANGEBYSCORE empty point",
              c.cmd("ZREMRANGEBYSCORE", "rrzset2", "(2", "2"), 0)
        check("ZREMRANGEBYSCORE bad float",
              c.cmd("ZREMRANGEBYSCORE", "rrzset2", "xx", "5"),
              ("error", "ERR value is not a valid float"))
        check("ZREMRANGEBYSCORE NaN",
              c.cmd("ZREMRANGEBYSCORE", "rrzset2", "nan", "5"),
              ("error", "ERR value is not a valid float"))
        check("ZREMRANGEBYSCORE wrong args", c.cmd("ZREMRANGEBYSCORE", "rrzset2", "1"),
              ("error", "ERR wrong number of arguments for 'zremrangebyscore' command"))
        check("ZREMRANGEBYSCORE wrongtype", c.cmd("ZREMRANGEBYSCORE", "after", "0", "10"),
              wrongtype)
        check("COMMAND lists ZREMRANGEBYSCORE", "ZREMRANGEBYSCORE" in c.cmd("COMMAND"), True)
        # ZREMRANGEBYRANK. rrzset holds a=1, b=2, c=3, d=4, e=5.
        check("ZADD rrzset", c.cmd("ZADD", "rrzset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"), 5)
        check("ZREMRANGEBYRANK mid", c.cmd("ZREMRANGEBYRANK", "rrzset", "1", "3"), 3)
        check("ZRANGE after ZREMRANGEBYRANK", c.cmd("ZRANGE", "rrzset", "0", "-1"), ["a", "e"])
        check("ZREMRANGEBYRANK negative", c.cmd("ZREMRANGEBYRANK", "rrzset", "-1", "-1"), 1)
        check("ZRANGE after negative ZREMRANGEBYRANK", c.cmd("ZRANGE", "rrzset", "0", "-1"), ["a"])
        check("ZREMRANGEBYRANK last one", c.cmd("ZREMRANGEBYRANK", "rrzset", "0", "0"), 1)
        check("EXISTS emptied by ZREMRANGEBYRANK", c.cmd("EXISTS", "rrzset"), 0)
        check("ZREMRANGEBYRANK missing", c.cmd("ZREMRANGEBYRANK", "ghostzset", "0", "-1"), 0)
        check("ZREMRANGEBYRANK reversed", c.cmd("ZREMRANGEBYRANK", "rrzset2", "1", "0"), 0)
        check("ZCARD after reversed rank range", c.cmd("ZCARD", "rrzset2"), 2)
        check("ZREMRANGEBYRANK out of range", c.cmd("ZREMRANGEBYRANK", "rrzset2", "5", "9"), 0)
        check("ZCARD after out-of-range rank range", c.cmd("ZCARD", "rrzset2"), 2)
        check("ZREMRANGEBYRANK clamped stop", c.cmd("ZREMRANGEBYRANK", "rrzset2", "0", "99"), 2)
        check("EXISTS emptied by clamped ZREMRANGEBYRANK", c.cmd("EXISTS", "rrzset2"), 0)
        check("ZREMRANGEBYRANK bad int",
              c.cmd("ZREMRANGEBYRANK", "myzset", "xx", "5"),
              ("error", "ERR value is not an integer or out of range"))
        check("ZREMRANGEBYRANK wrong args", c.cmd("ZREMRANGEBYRANK", "myzset", "1"),
              ("error", "ERR wrong number of arguments for 'zremrangebyrank' command"))
        check("ZREMRANGEBYRANK wrongtype", c.cmd("ZREMRANGEBYRANK", "after", "0", "-1"),
              wrongtype)
        check("COMMAND lists ZREMRANGEBYRANK", "ZREMRANGEBYRANK" in c.cmd("COMMAND"), True)
        # ZREMRANGEBYLEX. rlzset holds a=1, b=2, c=3, d=4, e=5.
        check("ZADD rlzset", c.cmd("ZADD", "rlzset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"), 5)
        check("ZREMRANGEBYLEX mid", c.cmd("ZREMRANGEBYLEX", "rlzset", "[b", "[d"), 3)
        check("ZRANGE after ZREMRANGEBYLEX", c.cmd("ZRANGE", "rlzset", "0", "-1"), ["a", "e"])
        check("ZREMRANGEBYLEX excl both empty",
              c.cmd("ZREMRANGEBYLEX", "rlzset", "(a", "(e"), 0)
        check("ZCARD after excl empty", c.cmd("ZCARD", "rlzset"), 2)
        check("ZREMRANGEBYLEX excl max", c.cmd("ZREMRANGEBYLEX", "rlzset", "[a", "(e"), 1)
        check("ZRANGE after excl max", c.cmd("ZRANGE", "rlzset", "0", "-1"), ["e"])
        check("ZREMRANGEBYLEX from start", c.cmd("ZREMRANGEBYLEX", "rlzset", "-", "(e"), 0)
        check("ZREMRANGEBYLEX last one", c.cmd("ZREMRANGEBYLEX", "rlzset", "[e", "+"), 1)
        check("EXISTS emptied by ZREMRANGEBYLEX", c.cmd("EXISTS", "rlzset"), 0)
        check("ZREMRANGEBYLEX missing", c.cmd("ZREMRANGEBYLEX", "ghostzset", "-", "+"), 0)
        check("ZREMRANGEBYLEX min above max",
              c.cmd("ZREMRANGEBYLEX", "rlzset2", "[d", "[a"), 0)
        check("ZREMRANGEBYLEX empty slice",
              c.cmd("ZREMRANGEBYLEX", "rlzset2", "[x", "[z"), 0)
        check("ZREMRANGEBYLEX wrong args", c.cmd("ZREMRANGEBYLEX", "rlzset2", "[a"),
              ("error", "ERR wrong number of arguments for 'zremrangebylex' command"))
        check("ZREMRANGEBYLEX bare bound", c.cmd("ZREMRANGEBYLEX", "rlzset2", "a", "[z"),
              ("error", "ERR syntax error"))
        check("ZREMRANGEBYLEX plus as min", c.cmd("ZREMRANGEBYLEX", "rlzset2", "+", "[z"),
              ("error", "ERR syntax error"))
        check("ZREMRANGEBYLEX minus as max", c.cmd("ZREMRANGEBYLEX", "rlzset2", "[a", "-"),
              ("error", "ERR syntax error"))
        check("ZREMRANGEBYLEX empty bracket", c.cmd("ZREMRANGEBYLEX", "rlzset2", "[", "+"),
              ("error", "ERR syntax error"))
        # ZMSCORE. mszset holds a=1, b=2.5, c=-3.
        check("ZADD mszset", c.cmd("ZADD", "mszset", "1", "a", "2.5", "b", "-3", "c"), 3)
        check("ZMSCORE all", c.cmd("ZMSCORE", "mszset", "a", "b", "c"),
              ["1", "2.5", "-3"])
        check("ZMSCORE order", c.cmd("ZMSCORE", "mszset", "c", "a"), ["-3", "1"])
        check("ZMSCORE missing member", c.cmd("ZMSCORE", "mszset", "a", "ghost", "c"),
              ["1", None, "-3"])
        check("ZMSCORE missing key", c.cmd("ZMSCORE", "ghostzset", "a", "b"),
              [None, None])
        check("ZMSCORE one", c.cmd("ZMSCORE", "mszset", "b"), ["2.5"])
        check("ZMSCORE wrongtype", c.cmd("ZMSCORE", "after", "x"), wrongtype)
        check("ZMSCORE wrong args", c.cmd("ZMSCORE", "mszset"),
              ("error", "ERR wrong number of arguments for 'zmscore' command"))
        check("COMMAND lists ZMSCORE", "ZMSCORE" in c.cmd("COMMAND"), True)
        # ZDIFF. dzset1 holds a=1, b=2, c=3; dzset2 holds b=2, d=4.
        check("ZADD dzset1", c.cmd("ZADD", "dzset1", "1", "a", "2", "b", "3", "c"), 3)
        check("ZADD dzset2", c.cmd("ZADD", "dzset2", "2", "b", "4", "d"), 2)
        check("ZDIFF one", c.cmd("ZDIFF", "1", "dzset1"), ["a", "b", "c"])
        check("ZDIFF two", c.cmd("ZDIFF", "2", "dzset1", "dzset2"), ["a", "c"])
        check("ZDIFF withscores", c.cmd("ZDIFF", "2", "dzset1", "dzset2", "WITHSCORES"),
              ["a", "1", "c", "3"])
        check("ZDIFF missing other", c.cmd("ZDIFF", "2", "dzset1", "ghostzset"),
              ["a", "b", "c"])
        check("ZDIFF missing first", c.cmd("ZDIFF", "1", "ghostzset"), [])
        check("ZDIFF missing first two", c.cmd("ZDIFF", "2", "ghostzset", "dzset1"), [])
        check("ZDIFF wrongtype", c.cmd("ZDIFF", "2", "dzset1", "after"), wrongtype)
        check("ZDIFF wrong args", c.cmd("ZDIFF", "dzset1"),
              ("error", "ERR wrong number of arguments for 'zdiff' command"))
        check("ZDIFF too few keys", c.cmd("ZDIFF", "2", "dzset1"),
              ("error", "ERR wrong number of arguments for 'zdiff' command"))
        check("ZDIFF zero keys", c.cmd("ZDIFF", "0", "dzset1"),
              ("error", "ERR at least 1 input key is needed for 'zdiff' command"))
        check("ZDIFF bad option", c.cmd("ZDIFF", "1", "dzset1", "BOGUS"),
              ("error", "ERR syntax error"))
        check("COMMAND lists ZDIFF", "ZDIFF" in c.cmd("COMMAND"), True)
        check("ZDIFF cleanup", c.cmd("DEL", "dzset1", "dzset2"), 2)
        # ZUNION. uzset1 holds a=1, b=2, c=3; uzset2 holds b=10, d=4,
        # so b scores 12 in the union and the members order a, c, d, b.
        check("ZADD uzset1", c.cmd("ZADD", "uzset1", "1", "a", "2", "b", "3", "c"), 3)
        check("ZADD uzset2", c.cmd("ZADD", "uzset2", "10", "b", "4", "d"), 2)
        check("ZUNION one", c.cmd("ZUNION", "1", "uzset1"), ["a", "b", "c"])
        check("ZUNION two", c.cmd("ZUNION", "2", "uzset1", "uzset2"),
              ["a", "c", "d", "b"])
        check("ZUNION withscores", c.cmd("ZUNION", "2", "uzset1", "uzset2", "WITHSCORES"),
              ["a", "1", "c", "3", "d", "4", "b", "12"])
        check("ZUNION missing other", c.cmd("ZUNION", "2", "uzset1", "ghostzset"),
              ["a", "b", "c"])
        check("ZUNION missing first", c.cmd("ZUNION", "1", "ghostzset"), [])
        check("ZUNION missing first two", c.cmd("ZUNION", "2", "ghostzset", "uzset1"),
              ["a", "b", "c"])
        check("ZUNION wrongtype", c.cmd("ZUNION", "2", "uzset1", "after"), wrongtype)
        check("ZUNION wrong args", c.cmd("ZUNION", "uzset1"),
              ("error", "ERR wrong number of arguments for 'zunion' command"))
        check("ZUNION too few keys", c.cmd("ZUNION", "2", "uzset1"),
              ("error", "ERR wrong number of arguments for 'zunion' command"))
        check("ZUNION zero keys", c.cmd("ZUNION", "0", "uzset1"),
              ("error", "ERR at least 1 input key is needed for 'zunion' command"))
        check("ZUNION bad option", c.cmd("ZUNION", "1", "uzset1", "BOGUS"),
              ("error", "ERR syntax error"))
        check("COMMAND lists ZUNION", "ZUNION" in c.cmd("COMMAND"), True)
        check("ZUNION cleanup", c.cmd("DEL", "uzset1", "uzset2"), 2)
        # ZINTER. izset1 holds a=1, b=2, c=3; izset2 holds b=10, c=30, d=4,
        # so the intersection is b and c: SUM gives b=12, c=33.
        check("ZADD izset1", c.cmd("ZADD", "izset1", "1", "a", "2", "b", "3", "c"), 3)
        check("ZADD izset2", c.cmd("ZADD", "izset2", "10", "b", "30", "c", "4", "d"), 3)
        check("ZINTER one", c.cmd("ZINTER", "1", "izset1"), ["a", "b", "c"])
        check("ZINTER two", c.cmd("ZINTER", "2", "izset1", "izset2"), ["b", "c"])
        check("ZINTER withscores", c.cmd("ZINTER", "2", "izset1", "izset2", "WITHSCORES"),
              ["b", "12", "c", "33"])
        check("ZINTER weights", c.cmd("ZINTER", "2", "izset1", "izset2",
                                       "WEIGHTS", "2", "3", "WITHSCORES"),
              ["b", "34", "c", "96"])
        check("ZINTER aggregate min", c.cmd("ZINTER", "2", "izset1", "izset2",
                                            "AGGREGATE", "MIN", "WITHSCORES"),
              ["b", "2", "c", "3"])
        check("ZINTER aggregate max", c.cmd("ZINTER", "2", "izset1", "izset2",
                                            "AGGREGATE", "MAX", "WITHSCORES"),
              ["b", "10", "c", "30"])
        check("ZINTER weights min", c.cmd("ZINTER", "2", "izset1", "izset2",
                                          "WEIGHTS", "2", "1", "AGGREGATE", "MIN",
                                          "WITHSCORES"),
              ["b", "4", "c", "6"])
        check("ZINTER options any order", c.cmd("ZINTER", "2", "izset1", "izset2",
                                                "WITHSCORES", "AGGREGATE", "MAX"),
              ["b", "10", "c", "30"])
        check("ZINTER missing other", c.cmd("ZINTER", "2", "izset1", "ghostzset"), [])
        check("ZINTER missing first", c.cmd("ZINTER", "1", "ghostzset"), [])
        check("ZINTER missing first two", c.cmd("ZINTER", "2", "ghostzset", "izset1"), [])
        check("ZINTER wrongtype", c.cmd("ZINTER", "2", "izset1", "after"), wrongtype)
        check("ZINTER wrong args", c.cmd("ZINTER", "izset1"),
              ("error", "ERR wrong number of arguments for 'zinter' command"))
        check("ZINTER too few keys", c.cmd("ZINTER", "2", "izset1"),
              ("error", "ERR wrong number of arguments for 'zinter' command"))
        check("ZINTER zero keys", c.cmd("ZINTER", "0", "izset1"),
              ("error", "ERR at least 1 input key is needed for 'zinter' command"))
        check("ZINTER bad weight", c.cmd("ZINTER", "2", "izset1", "izset2",
                                         "WEIGHTS", "x", "1"),
              ("error", "ERR value is not a valid float"))
        check("ZINTER weights short", c.cmd("ZINTER", "2", "izset1", "izset2",
                                            "WEIGHTS", "1"),
              ("error", "ERR syntax error"))
        check("ZINTER bad aggregate", c.cmd("ZINTER", "2", "izset1", "izset2",
                                            "AGGREGATE", "MEAN"),
              ("error", "ERR syntax error"))
        check("ZINTER duplicate weights", c.cmd("ZINTER", "2", "izset1", "izset2",
                                                "WEIGHTS", "1", "1",
                                                "WEIGHTS", "1", "1"),
              ("error", "ERR syntax error"))
        check("ZINTER duplicate withscores", c.cmd("ZINTER", "2", "izset1", "izset2",
                                                   "WITHSCORES", "WITHSCORES"),
              ("error", "ERR syntax error"))
        check("ZINTER bad option", c.cmd("ZINTER", "1", "izset1", "BOGUS"),
              ("error", "ERR syntax error"))
        check("COMMAND lists ZINTER", "ZINTER" in c.cmd("COMMAND"), True)
        check("ZINTER cleanup", c.cmd("DEL", "izset1", "izset2"), 2)
        # ZUNIONSTORE. stzset1 holds a=1, b=2, c=3; stzset2 holds
        # b=10, d=4, so the union is a=1, b=12, c=3, d=4.
        check("ZADD stzset1", c.cmd("ZADD", "stzset1", "1", "a", "2", "b", "3", "c"), 3)
        check("ZADD stzset2", c.cmd("ZADD", "stzset2", "10", "b", "4", "d"), 2)
        check("ZUNIONSTORE one", c.cmd("ZUNIONSTORE", "stdest", "1", "stzset1"), 3)
        check("ZUNIONSTORE one range", c.cmd("ZRANGE", "stdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "b", "2", "c", "3"])
        check("ZUNIONSTORE two", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2"), 4)
        check("ZUNIONSTORE two range", c.cmd("ZRANGE", "stdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "c", "3", "d", "4", "b", "12"])
        check("ZUNIONSTORE weights", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                           "WEIGHTS", "2", "3"), 4)
        check("ZUNIONSTORE weights range", c.cmd("ZRANGE", "stdest", "0", "-1", "WITHSCORES"),
              ["a", "2", "c", "6", "d", "12", "b", "34"])
        check("ZUNIONSTORE aggregate min", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                                 "AGGREGATE", "MIN"), 4)
        check("ZUNIONSTORE aggregate min range", c.cmd("ZRANGE", "stdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "b", "2", "c", "3", "d", "4"])
        check("ZUNIONSTORE aggregate max", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                                 "AGGREGATE", "MAX"), 4)
        check("ZUNIONSTORE aggregate max range", c.cmd("ZRANGE", "stdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "c", "3", "d", "4", "b", "10"])
        check("ZUNIONSTORE weights min", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                               "WEIGHTS", "2", "1", "AGGREGATE", "MIN"), 4)
        check("ZUNIONSTORE weights min range", c.cmd("ZRANGE", "stdest", "0", "-1", "WITHSCORES"),
              ["a", "2", "b", "4", "d", "4", "c", "6"])
        check("ZUNIONSTORE options any order", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                                     "AGGREGATE", "MAX", "WEIGHTS", "1", "1"), 4)
        check("ZUNIONSTORE options any order range", c.cmd("ZRANGE", "stdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "c", "3", "d", "4", "b", "10"])
        # Overwriting a string destination replaces it, and the
        # destination's TTL is cleared by the overwrite.
        check("ZUNIONSTORE string dest setup", c.cmd("SET", "strdest", "old"), "OK")
        check("ZUNIONSTORE overwrites string", c.cmd("ZUNIONSTORE", "strdest", "1", "stzset1"), 3)
        check("ZUNIONSTORE string dest is zset", c.cmd("ZSCORE", "strdest", "a"), "1")
        check("ZUNIONSTORE string dest GET wrongtype", c.cmd("GET", "strdest"), wrongtype)
        check("ZUNIONSTORE TTL setup", c.cmd("EXPIRE", "stdest", "100"), 1)
        check("ZUNIONSTORE overwrites TTL", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2"), 4)
        check("ZUNIONSTORE TTL cleared", c.cmd("TTL", "stdest"), -1)
        # An empty result deletes the destination instead of leaving
        # an empty set, and a missing destination stays missing.
        check("ZUNIONSTORE empty deletes", c.cmd("ZUNIONSTORE", "stdest", "1", "ghostzset"), 0)
        check("ZUNIONSTORE empty dest gone", c.cmd("EXISTS", "stdest"), 0)
        check("ZUNIONSTORE empty missing dest", c.cmd("ZUNIONSTORE", "newdest", "1", "ghostzset"), 0)
        check("ZUNIONSTORE empty missing dest gone", c.cmd("EXISTS", "newdest"), 0)
        check("ZUNIONSTORE missing other", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "ghostzset"), 3)
        check("ZUNIONSTORE missing other range", c.cmd("ZRANGE", "stdest", "0", "-1"),
              ["a", "b", "c"])
        check("ZUNIONSTORE wrongtype", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "after"), wrongtype)
        check("ZUNIONSTORE wrong args", c.cmd("ZUNIONSTORE", "stdest"),
              ("error", "ERR wrong number of arguments for 'zunionstore' command"))
        check("ZUNIONSTORE too few keys", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1"),
              ("error", "ERR wrong number of arguments for 'zunionstore' command"))
        check("ZUNIONSTORE zero keys", c.cmd("ZUNIONSTORE", "stdest", "0", "stzset1"),
              ("error", "ERR at least 1 input key is needed for 'zunionstore' command"))
        check("ZUNIONSTORE bad weight", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                              "WEIGHTS", "x", "1"),
              ("error", "ERR value is not a valid float"))
        check("ZUNIONSTORE weights short", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                                 "WEIGHTS", "1"),
              ("error", "ERR syntax error"))
        check("ZUNIONSTORE bad aggregate", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                                 "AGGREGATE", "MEAN"),
              ("error", "ERR syntax error"))
        check("ZUNIONSTORE duplicate weights", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                                     "WEIGHTS", "1", "1",
                                                     "WEIGHTS", "1", "1"),
              ("error", "ERR syntax error"))
        check("ZUNIONSTORE duplicate aggregate", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                                       "AGGREGATE", "SUM",
                                                       "AGGREGATE", "MIN"),
              ("error", "ERR syntax error"))
        check("ZUNIONSTORE withscores rejected", c.cmd("ZUNIONSTORE", "stdest", "1", "stzset1",
                                                       "WITHSCORES"),
              ("error", "ERR syntax error"))
        check("ZUNIONSTORE bad option", c.cmd("ZUNIONSTORE", "stdest", "1", "stzset1", "BOGUS"),
              ("error", "ERR syntax error"))
        check("COMMAND lists ZUNIONSTORE", "ZUNIONSTORE" in c.cmd("COMMAND"), True)
        # A ZUNIONSTORE that reproduces the destination exactly is a
        # no-op and does not invalidate a WATCH; one that changes it
        # does. Restore the plain union into stdest first.
        check("ZUNIONSTORE restore union", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2"), 4)
        check("WATCH zunionstore noop", c.cmd("WATCH", "stdest"), "OK")
        check("ZUNIONSTORE noop", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2"), 4)
        check("MULTI zunionstore noop watch", c.cmd("MULTI"), "OK")
        check("MULTI zunionstore noop SET queued", c.cmd("SET", "wtestkey", "v1"), "QUEUED")
        check("EXEC zunionstore noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wtestkey after zunionstore noop EXEC", c.cmd("GET", "wtestkey"), "v1")
        check("WATCH zunionstore change", c.cmd("WATCH", "stdest"), "OK")
        check("ZUNIONSTORE change", c.cmd("ZUNIONSTORE", "stdest", "2", "stzset1", "stzset2",
                                          "WEIGHTS", "2", "1"), 4)
        check("MULTI zunionstore change watch", c.cmd("MULTI"), "OK")
        check("MULTI zunionstore change SET queued", c.cmd("SET", "wtestkey", "v2"), "QUEUED")
        check("EXEC zunionstore change watch aborts", c.cmd("EXEC"), None)
        check("wtestkey unchanged after zunionstore abort", c.cmd("GET", "wtestkey"), "v1")
        # The destination may be one of the inputs: the union is
        # computed before the destination is overwritten.
        check("ZUNIONSTORE dest is input", c.cmd("ZUNIONSTORE", "stzset1", "2", "stzset1", "stzset2"), 4)
        check("ZUNIONSTORE dest is input range", c.cmd("ZRANGE", "stzset1", "0", "-1", "WITHSCORES"),
              ["a", "1", "c", "3", "d", "4", "b", "12"])
        check("ZUNIONSTORE cleanup", c.cmd("DEL", "stzset1", "stzset2", "stdest", "strdest", "wtestkey"), 5)
        # ZINTERSTORE. itzset1 holds a=1, b=2, c=3; itzset2 holds
        # b=10, c=30, d=4, so the intersection is b=12, c=33.
        check("ZADD itzset1", c.cmd("ZADD", "itzset1", "1", "a", "2", "b", "3", "c"), 3)
        check("ZADD itzset2", c.cmd("ZADD", "itzset2", "10", "b", "30", "c", "4", "d"), 3)
        check("ZINTERSTORE one", c.cmd("ZINTERSTORE", "itdest", "1", "itzset1"), 3)
        check("ZINTERSTORE one range", c.cmd("ZRANGE", "itdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "b", "2", "c", "3"])
        check("ZINTERSTORE two", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2"), 2)
        check("ZINTERSTORE two range", c.cmd("ZRANGE", "itdest", "0", "-1", "WITHSCORES"),
              ["b", "12", "c", "33"])
        check("ZINTERSTORE weights", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                           "WEIGHTS", "2", "3"), 2)
        check("ZINTERSTORE weights range", c.cmd("ZRANGE", "itdest", "0", "-1", "WITHSCORES"),
              ["b", "34", "c", "96"])
        check("ZINTERSTORE aggregate min", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                                 "AGGREGATE", "MIN"), 2)
        check("ZINTERSTORE aggregate min range", c.cmd("ZRANGE", "itdest", "0", "-1", "WITHSCORES"),
              ["b", "2", "c", "3"])
        check("ZINTERSTORE aggregate max", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                                 "AGGREGATE", "MAX"), 2)
        check("ZINTERSTORE aggregate max range", c.cmd("ZRANGE", "itdest", "0", "-1", "WITHSCORES"),
              ["b", "10", "c", "30"])
        check("ZINTERSTORE weights min", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                               "WEIGHTS", "2", "1", "AGGREGATE", "MIN"), 2)
        check("ZINTERSTORE weights min range", c.cmd("ZRANGE", "itdest", "0", "-1", "WITHSCORES"),
              ["b", "4", "c", "6"])
        # Overwriting a string destination replaces it, and the
        # destination's TTL is cleared by the overwrite.
        check("ZINTERSTORE string dest setup", c.cmd("SET", "strdest2", "old"), "OK")
        check("ZINTERSTORE overwrites string", c.cmd("ZINTERSTORE", "strdest2", "2", "itzset1", "itzset2"), 2)
        check("ZINTERSTORE string dest is zset", c.cmd("ZSCORE", "strdest2", "b"), "12")
        check("ZINTERSTORE TTL setup", c.cmd("EXPIRE", "itdest", "100"), 1)
        check("ZINTERSTORE overwrites TTL", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2"), 2)
        check("ZINTERSTORE TTL cleared", c.cmd("TTL", "itdest"), -1)
        # An empty result deletes the destination instead of leaving
        # an empty set; a missing first input set is empty too.
        check("ZINTERSTORE empty deletes", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "ghostzset"), 0)
        check("ZINTERSTORE empty dest gone", c.cmd("EXISTS", "itdest"), 0)
        check("ZINTERSTORE missing first", c.cmd("ZINTERSTORE", "itdest", "2", "ghostzset", "itzset2"), 0)
        check("ZINTERSTORE wrongtype", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "after"), wrongtype)
        check("ZINTERSTORE wrong args", c.cmd("ZINTERSTORE", "itdest"),
              ("error", "ERR wrong number of arguments for 'zinterstore' command"))
        check("ZINTERSTORE too few keys", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1"),
              ("error", "ERR wrong number of arguments for 'zinterstore' command"))
        check("ZINTERSTORE zero keys", c.cmd("ZINTERSTORE", "itdest", "0", "itzset1"),
              ("error", "ERR at least 1 input key is needed for 'zinterstore' command"))
        check("ZINTERSTORE bad weight", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                              "WEIGHTS", "x", "1"),
              ("error", "ERR value is not a valid float"))
        check("ZINTERSTORE weights short", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                                 "WEIGHTS", "1"),
              ("error", "ERR syntax error"))
        check("ZINTERSTORE bad aggregate", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                                 "AGGREGATE", "MEAN"),
              ("error", "ERR syntax error"))
        check("ZINTERSTORE duplicate weights", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                                     "WEIGHTS", "1", "1",
                                                     "WEIGHTS", "1", "1"),
              ("error", "ERR syntax error"))
        check("ZINTERSTORE duplicate aggregate", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                                       "AGGREGATE", "SUM",
                                                       "AGGREGATE", "MIN"),
              ("error", "ERR syntax error"))
        check("ZINTERSTORE withscores rejected", c.cmd("ZINTERSTORE", "itdest", "1", "itzset1",
                                                       "WITHSCORES"),
              ("error", "ERR syntax error"))
        check("ZINTERSTORE bad option", c.cmd("ZINTERSTORE", "itdest", "1", "itzset1", "BOGUS"),
              ("error", "ERR syntax error"))
        check("COMMAND lists ZINTERSTORE", "ZINTERSTORE" in c.cmd("COMMAND"), True)
        # A ZINTERSTORE that reproduces the destination exactly is a
        # no-op and does not invalidate a WATCH; one that changes it
        # does. Restore the plain intersection into itdest first.
        check("ZINTERSTORE restore intersection", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2"), 2)
        check("WATCH zinterstore noop", c.cmd("WATCH", "itdest"), "OK")
        check("ZINTERSTORE noop", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2"), 2)
        check("MULTI zinterstore noop watch", c.cmd("MULTI"), "OK")
        check("MULTI zinterstore noop SET queued", c.cmd("SET", "wtestkey", "v1"), "QUEUED")
        check("EXEC zinterstore noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wtestkey after zinterstore noop EXEC", c.cmd("GET", "wtestkey"), "v1")
        check("WATCH zinterstore change", c.cmd("WATCH", "itdest"), "OK")
        check("ZINTERSTORE change", c.cmd("ZINTERSTORE", "itdest", "2", "itzset1", "itzset2",
                                          "WEIGHTS", "2", "1"), 2)
        check("MULTI zinterstore change watch", c.cmd("MULTI"), "OK")
        check("MULTI zinterstore change SET queued", c.cmd("SET", "wtestkey", "v2"), "QUEUED")
        check("EXEC zinterstore change watch aborts", c.cmd("EXEC"), None)
        check("wtestkey unchanged after zinterstore abort", c.cmd("GET", "wtestkey"), "v1")
        # The destination may be one of the inputs: the intersection
        # is computed before the destination is overwritten.
        check("ZINTERSTORE dest is input", c.cmd("ZINTERSTORE", "itzset1", "2", "itzset1", "itzset2"), 2)
        check("ZINTERSTORE dest is input range", c.cmd("ZRANGE", "itzset1", "0", "-1", "WITHSCORES"),
              ["b", "12", "c", "33"])
        check("ZINTERSTORE cleanup", c.cmd("DEL", "itzset1", "itzset2", "itdest", "strdest2", "wtestkey"), 5)
        # ZDIFFSTORE. dfzset1 holds a=1, b=2, c=3, e=5; dfzset2 holds
        # b=10, c=30, d=4, so the difference is a=1, e=5, with the
        # scores from the first set.
        check("ZADD dfzset1", c.cmd("ZADD", "dfzset1", "1", "a", "2", "b", "3", "c", "5", "e"), 4)
        check("ZADD dfzset2", c.cmd("ZADD", "dfzset2", "10", "b", "30", "c", "4", "d"), 3)
        check("ZDIFFSTORE one", c.cmd("ZDIFFSTORE", "dfdest", "1", "dfzset1"), 4)
        check("ZDIFFSTORE one range", c.cmd("ZRANGE", "dfdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "b", "2", "c", "3", "e", "5"])
        check("ZDIFFSTORE two", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "dfzset2"), 2)
        check("ZDIFFSTORE two range", c.cmd("ZRANGE", "dfdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "e", "5"])
        check("ZDIFFSTORE missing other", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "ghostzset"), 4)
        check("ZDIFFSTORE missing other range", c.cmd("ZRANGE", "dfdest", "0", "-1", "WITHSCORES"),
              ["a", "1", "b", "2", "c", "3", "e", "5"])
        check("ZDIFFSTORE missing first", c.cmd("ZDIFFSTORE", "dfdest", "2", "ghostzset", "dfzset2"), 0)
        check("ZDIFFSTORE missing first dest gone", c.cmd("EXISTS", "dfdest"), 0)
        # Overwriting a string destination replaces it, and the
        # destination's TTL is cleared by the overwrite.
        check("ZDIFFSTORE string dest setup", c.cmd("SET", "strdest3", "old"), "OK")
        check("ZDIFFSTORE overwrites string", c.cmd("ZDIFFSTORE", "strdest3", "2", "dfzset1", "dfzset2"), 2)
        check("ZDIFFSTORE string dest is zset", c.cmd("ZSCORE", "strdest3", "a"), "1")
        check("ZDIFFSTORE string dest GET wrongtype", c.cmd("GET", "strdest3"), wrongtype)
        check("ZDIFFSTORE restore diff", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "dfzset2"), 2)
        check("ZDIFFSTORE TTL setup", c.cmd("EXPIRE", "dfdest", "100"), 1)
        check("ZDIFFSTORE overwrites TTL", c.cmd("ZDIFFSTORE", "dfdest", "1", "dfzset1"), 4)
        check("ZDIFFSTORE TTL cleared", c.cmd("TTL", "dfdest"), -1)
        # An empty result deletes the destination instead of leaving
        # an empty set, and a missing destination stays missing.
        check("ZDIFFSTORE empty deletes", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "dfzset1"), 0)
        check("ZDIFFSTORE empty dest gone", c.cmd("EXISTS", "dfdest"), 0)
        check("ZDIFFSTORE empty missing dest", c.cmd("ZDIFFSTORE", "newdest2", "1", "ghostzset"), 0)
        check("ZDIFFSTORE empty missing dest gone", c.cmd("EXISTS", "newdest2"), 0)
        check("ZDIFFSTORE wrongtype", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "after"), wrongtype)
        check("ZDIFFSTORE wrong args", c.cmd("ZDIFFSTORE", "dfdest"),
              ("error", "ERR wrong number of arguments for 'zdiffstore' command"))
        check("ZDIFFSTORE too few keys", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1"),
              ("error", "ERR wrong number of arguments for 'zdiffstore' command"))
        check("ZDIFFSTORE zero keys", c.cmd("ZDIFFSTORE", "dfdest", "0", "dfzset1"),
              ("error", "ERR at least 1 input key is needed for 'zdiffstore' command"))
        # ZDIFFSTORE takes no options: anything past the input keys
        # is a syntax error, including WEIGHTS and WITHSCORES.
        check("ZDIFFSTORE withscores rejected", c.cmd("ZDIFFSTORE", "dfdest", "1", "dfzset1",
                                                      "WITHSCORES"),
              ("error", "ERR syntax error"))
        check("ZDIFFSTORE weights rejected", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "dfzset2",
                                                   "WEIGHTS", "1", "1"),
              ("error", "ERR syntax error"))
        check("ZDIFFSTORE bad option", c.cmd("ZDIFFSTORE", "dfdest", "1", "dfzset1", "BOGUS"),
              ("error", "ERR syntax error"))
        check("COMMAND lists ZDIFFSTORE", "ZDIFFSTORE" in c.cmd("COMMAND"), True)
        # A ZDIFFSTORE that reproduces the destination exactly is a
        # no-op and does not invalidate a WATCH; one that changes it
        # does. Restore the plain difference into dfdest first.
        check("ZDIFFSTORE restore difference", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "dfzset2"), 2)
        check("WATCH zdiffstore noop", c.cmd("WATCH", "dfdest"), "OK")
        check("ZDIFFSTORE noop", c.cmd("ZDIFFSTORE", "dfdest", "2", "dfzset1", "dfzset2"), 2)
        check("MULTI zdiffstore noop watch", c.cmd("MULTI"), "OK")
        check("MULTI zdiffstore noop SET queued", c.cmd("SET", "wtestkey", "v1"), "QUEUED")
        check("EXEC zdiffstore noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wtestkey after zdiffstore noop EXEC", c.cmd("GET", "wtestkey"), "v1")
        check("WATCH zdiffstore change", c.cmd("WATCH", "dfdest"), "OK")
        check("ZDIFFSTORE change", c.cmd("ZDIFFSTORE", "dfdest", "1", "dfzset1"), 4)
        check("MULTI zdiffstore change watch", c.cmd("MULTI"), "OK")
        check("MULTI zdiffstore change SET queued", c.cmd("SET", "wtestkey", "v2"), "QUEUED")
        check("EXEC zdiffstore change watch aborts", c.cmd("EXEC"), None)
        check("wtestkey unchanged after zdiffstore abort", c.cmd("GET", "wtestkey"), "v1")
        # The destination may be one of the inputs: the difference
        # is computed before the destination is overwritten.
        check("ZDIFFSTORE dest is input", c.cmd("ZDIFFSTORE", "dfzset1", "2", "dfzset1", "dfzset2"), 2)
        check("ZDIFFSTORE dest is input range", c.cmd("ZRANGE", "dfzset1", "0", "-1", "WITHSCORES"),
              ["a", "1", "e", "5"])
        check("ZDIFFSTORE cleanup", c.cmd("DEL", "dfzset1", "dfzset2", "dfdest", "strdest3", "wtestkey"), 5)
        # Deleted again: the AOF restart below counts exactly 11 keys.
        check("ZMSCORE cleanup", c.cmd("DEL", "mszset"), 1)
        # HyperLogLog: PFADD / PFCOUNT. A new key takes the elements
        # and reports 1; re-adding the same elements moves no register
        # and reports 0; a new element reports 1 again. Small counts
        # come back exact through linear counting.
        check("PFADD new", c.cmd("PFADD", "hll", "a", "b", "c"), 1)
        check("PFCOUNT three", c.cmd("PFCOUNT", "hll"), 3)
        check("PFADD same", c.cmd("PFADD", "hll", "a", "b", "c"), 0)
        check("PFADD new element", c.cmd("PFADD", "hll", "d"), 1)
        check("PFCOUNT four", c.cmd("PFCOUNT", "hll"), 4)
        check("PFADD dup in one call", c.cmd("PFADD", "hlldup", "a", "a", "a"), 1)
        check("PFCOUNT dup in one call", c.cmd("PFCOUNT", "hlldup"), 1)
        check("PFCOUNT missing", c.cmd("PFCOUNT", "ghosthll"), 0)
        check("PFADD no elements", c.cmd("PFADD", "hll"),
              ("error", "ERR wrong number of arguments for 'pfadd' command"))
        check("PFCOUNT no keys", c.cmd("PFCOUNT"),
              ("error", "ERR wrong number of arguments for 'pfcount' command"))
        # A plain string is not a HyperLogLog value, and a list is the
        # usual wrong type; both errors name the problem.
        check("PFADD plain string", c.cmd("PFADD", "after", "x"),
              ("error", "ERR WRONGTYPE Key is not a valid HyperLogLog string value."))
        check("PFCOUNT plain string", c.cmd("PFCOUNT", "after"),
              ("error", "ERR WRONGTYPE Key is not a valid HyperLogLog string value."))
        check("PFADD list wrongtype", c.cmd("PFADD", "plist", "x"), wrongtype)
        check("PFCOUNT list wrongtype", c.cmd("PFCOUNT", "plist"), wrongtype)
        # PFADD preserves the TTL, as in Redis.
        check("PFADD TTL setup", c.cmd("EXPIRE", "hll", "100"), 1)
        check("PFADD TTL add", c.cmd("PFADD", "hll", "e"), 1)
        check("PFCOUNT five after TTL add", c.cmd("PFCOUNT", "hll"), 5)
        check("TTL survives PFADD", 99 <= c.cmd("TTL", "hll") <= 100, True)
        # PFCOUNT over several keys merges the registers, so the
        # union of two overlapping sets counts each element once.
        # 1000 distinct elements estimate within 5% (the standard
        # error at 2^14 registers is under 1%).
        check("PFADD hll1 thousand",
              c.cmd("PFADD", "hll1", *["user:%d" % i for i in range(1000)]), 1)
        check("PFCOUNT hll1 thousand",
              950 <= c.cmd("PFCOUNT", "hll1") <= 1050, True)
        check("PFADD hll2 thousand overlap",
              c.cmd("PFADD", "hll2", *["user:%d" % i for i in range(500, 1500)]), 1)
        check("PFCOUNT hll2 thousand",
              950 <= c.cmd("PFCOUNT", "hll2") <= 1050, True)
        check("PFCOUNT union fifteen hundred",
              1425 <= c.cmd("PFCOUNT", "hll1", "hll2") <= 1575, True)
        check("PFCOUNT union with missing", c.cmd("PFCOUNT", "hll1", "ghosthll"),
              c.cmd("PFCOUNT", "hll1"))
        # PFMERGE: the registers merge by maximum into the
        # destination, which is created when missing and overwritten
        # when it exists; a missing source counts as empty, and a
        # merge that reproduces the destination exactly is a no-op.
        check("PFADD mhll1", c.cmd("PFADD", "mhll1", "a", "b", "c"), 1)
        check("PFADD mhll2", c.cmd("PFADD", "mhll2", "c", "d", "e"), 1)
        check("PFMERGE new dest", c.cmd("PFMERGE", "mdest", "mhll1", "mhll2"), "OK")
        check("PFCOUNT merged five", c.cmd("PFCOUNT", "mdest"), 5)
        check("PFMERGE dest length", c.cmd("STRLEN", "mdest"), 16388)
        check("PFMERGE sources untouched", c.cmd("PFCOUNT", "mhll1"), 3)
        check("PFMERGE missing source", c.cmd("PFMERGE", "mdest2", "mhll1", "ghosthll"), "OK")
        check("PFCOUNT merged missing source", c.cmd("PFCOUNT", "mdest2"), 3)
        check("PFMERGE all missing", c.cmd("PFMERGE", "mdest3", "ghosthll", "ghosthll2"), "OK")
        check("PFCOUNT merged all missing", c.cmd("PFCOUNT", "mdest3"), 0)
        check("PFMERGE no sources", c.cmd("PFMERGE", "mdest"),
              ("error", "ERR wrong number of arguments for 'pfmerge' command"))
        # A plain string at the destination or a source is an error,
        # as is a list key at either end.
        check("PFMERGE plain string dest", c.cmd("PFMERGE", "after", "mhll1"),
              ("error", "ERR WRONGTYPE Key is not a valid HyperLogLog string value."))
        check("PFMERGE plain string source", c.cmd("PFMERGE", "mdest4", "after"),
              ("error", "ERR WRONGTYPE Key is not a valid HyperLogLog string value."))
        check("PFMERGE list dest", c.cmd("PFMERGE", "plist", "mhll1"), wrongtype)
        check("PFMERGE list source", c.cmd("PFMERGE", "mdest4", "plist"), wrongtype)
        # Merging a destination into itself changes nothing, and a
        # merge that reproduces the destination does not invalidate
        # a WATCH; overwriting a destination clears its TTL.
        check("PFMERGE into itself noop", c.cmd("PFMERGE", "mdest", "mdest"), "OK")
        check("WATCH mdest", c.cmd("WATCH", "mdest"), "OK")
        check("PFMERGE mdest noop", c.cmd("PFMERGE", "mdest", "mhll1", "mhll2"), "OK")
        check("MULTI mdest noop", c.cmd("MULTI"), "OK")
        check("MULTI mdest noop SET queued", c.cmd("SET", "wtestkey3", "v1"), "QUEUED")
        check("EXEC mdest noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wtestkey3 after mdest noop EXEC", c.cmd("GET", "wtestkey3"), "v1")
        check("PFMERGE TTL setup", c.cmd("EXPIRE", "mdest", "100"), 1)
        check("PFMERGE overwrite clears TTL", c.cmd("PFMERGE", "mdest", "mhll2"), "OK")
        check("TTL cleared by PFMERGE", c.cmd("TTL", "mdest"), -1)
        check("PFCOUNT mdest after overwrite", c.cmd("PFCOUNT", "mdest"), 3)
        check("COMMAND lists PFMERGE", "PFMERGE" in c.cmd("COMMAND"), True)
        check("COMMAND lists PFADD", "PFADD" in c.cmd("COMMAND"), True)
        check("COMMAND lists PFCOUNT", "PFCOUNT" in c.cmd("COMMAND"), True)
        # A PFADD that moves no register does not invalidate a WATCH;
        # one that changes a register does.
        check("PFADD hllwatch", c.cmd("PFADD", "hllwatch", "a", "b"), 1)
        check("WATCH hllwatch", c.cmd("WATCH", "hllwatch"), "OK")
        check("PFADD hllwatch noop", c.cmd("PFADD", "hllwatch", "a", "b"), 0)
        check("MULTI hllwatch noop", c.cmd("MULTI"), "OK")
        check("MULTI hllwatch noop SET queued", c.cmd("SET", "wtestkey2", "v1"), "QUEUED")
        check("EXEC hllwatch noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wtestkey2 after hllwatch noop EXEC", c.cmd("GET", "wtestkey2"), "v1")
        check("WATCH hllwatch 2", c.cmd("WATCH", "hllwatch"), "OK")
        check("PFADD hllwatch change", c.cmd("PFADD", "hllwatch", "c"), 1)
        check("MULTI hllwatch change", c.cmd("MULTI"), "OK")
        check("MULTI hllwatch change SET queued", c.cmd("SET", "wtestkey2", "v2"), "QUEUED")
        check("EXEC hllwatch change watch aborts", c.cmd("EXEC"), None)
        check("wtestkey2 unchanged after hllwatch abort", c.cmd("GET", "wtestkey2"), "v1")
        # GEO: GEOADD / GEODIST. A geo set is a sorted set whose
        # scores are 52-bit geohashes, so ZSCORE sees the geohash;
        # GEODIST decodes the members back to coordinates (the
        # centres of their geohash cells, as in Redis) and measures
        # the haversine distance. Palermo/Catania are the example
        # from the Redis docs: real Redis answers 166274.1516 m.
        check("GEOADD new", c.cmd("GEOADD", "geo", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"), 2)
        check("GEODIST Palermo Catania m", c.cmd("GEODIST", "geo", "Palermo", "Catania"), "166274.1516")
        check("GEODIST Palermo Catania km", c.cmd("GEODIST", "geo", "Palermo", "Catania", "km"), "166.2742")
        check("GEODIST Palermo Catania ft", c.cmd("GEODIST", "geo", "Palermo", "Catania", "ft"), "545518.8700")
        check("GEODIST Palermo Catania mi", c.cmd("GEODIST", "geo", "Palermo", "Catania", "mi"), "103.3180")
        check("GEODIST same member", c.cmd("GEODIST", "geo", "Palermo", "Palermo"), "0.0000")
        check("GEODIST missing member", c.cmd("GEODIST", "geo", "Palermo", "Rome"), None)
        check("GEODIST missing key", c.cmd("GEODIST", "ghostgeo", "Palermo", "Catania"), None)
        check("GEODIST bad unit", c.cmd("GEODIST", "geo", "Palermo", "Catania", "parsecs"),
              ("error", "ERR unsupported unit provided. please use m, km, ft, mi"))
        check("GEODIST no members", c.cmd("GEODIST", "geo", "Palermo"),
              ("error", "ERR wrong number of arguments for 'geodist' command"))
        # The score is the 52-bit geohash itself, visible through
        # the sorted-set commands, as in Redis.
        check("GEOADD ZSCORE Palermo", c.cmd("ZSCORE", "geo", "Palermo"), "3479099956230698")
        check("GEOADD ZSCORE Catania", c.cmd("ZSCORE", "geo", "Catania"), "3479447370796909")
        check("GEOADD ZCARD", c.cmd("ZCARD", "geo"), 2)
        # GEOPOS answers the decoded cell centres, as in real Redis
        # (longitude first, then latitude, each a bulk string).
        check("GEOPOS Palermo Catania", c.cmd("GEOPOS", "geo", "Palermo", "Catania"),
              [["13.361389338970184", "38.115556395496299"],
               ["15.087267458438873", "37.502668423331613"]])
        check("GEOPOS single", c.cmd("GEOPOS", "geo", "Palermo"),
              [["13.361389338970184", "38.115556395496299"]])
        check("GEOPOS missing member", c.cmd("GEOPOS", "geo", "Palermo", "Rome"),
              [["13.361389338970184", "38.115556395496299"], None])
        check("GEOPOS missing key", c.cmd("GEOPOS", "ghostgeo", "Palermo", "Catania"),
              [None, None])
        check("GEOPOS no members", c.cmd("GEOPOS", "geo"),
              ("error", "ERR wrong number of arguments for 'geopos' command"))
        check("GEOPOS list wrongtype", c.cmd("GEOPOS", "plist", "x"), wrongtype)
        # GEOHASH answers the 11-character base32 geohash, as in
        # real Redis — Palermo/Catania are Redis's own example
        # values, and the last character is always '0' (52 bits
        # cannot fill the 55 an 11-character string holds).
        check("GEOHASH Palermo Catania", c.cmd("GEOHASH", "geo", "Palermo", "Catania"),
              ["sqc8b49rny0", "sqdtr74hyu0"])
        check("GEOHASH single", c.cmd("GEOHASH", "geo", "Palermo"), ["sqc8b49rny0"])
        check("GEOHASH missing member", c.cmd("GEOHASH", "geo", "Palermo", "Rome"),
              ["sqc8b49rny0", None])
        check("GEOHASH missing key", c.cmd("GEOHASH", "ghostgeo", "Palermo"), [None])
        check("GEOHASH no members", c.cmd("GEOHASH", "geo"),
              ("error", "ERR wrong number of arguments for 'geohash' command"))
        check("GEOHASH list wrongtype", c.cmd("GEOHASH", "plist", "x"), wrongtype)
        # A member of a plain sorted set decodes whatever score it
        # holds, as in Redis — but a score that is not a geohash
        # (0, or negative) has no position.
        check("GEOPOS plain zset setup", c.cmd("ZADD", "geoz", "3479099956230698", "Palermo", "0", "zero", "-5", "neg"), 3)
        check("GEOPOS plain zset", c.cmd("GEOPOS", "geoz", "Palermo"),
              [["13.361389338970184", "38.115556395496299"]])
        check("GEOHASH plain zset", c.cmd("GEOHASH", "geoz", "Palermo"), ["sqc8b49rny0"])
        check("GEOPOS zero score", c.cmd("GEOPOS", "geoz", "zero"), [None])
        check("GEOHASH zero score", c.cmd("GEOHASH", "geoz", "zero"), [None])
        check("GEOPOS negative score", c.cmd("GEOPOS", "geoz", "neg"), [None])
        check("GEOHASH negative score", c.cmd("GEOHASH", "geoz", "neg"), [None])
        check("GEOPOS plain zset cleanup", c.cmd("DEL", "geoz"), 1)
        # Re-adding a member at its current coordinates is a no-op
        # (0 added, no WATCH invalidation); moving it updates the
        # score and answers 0 too, since it was not newly added.
        check("GEOADD same coords", c.cmd("GEOADD", "geo", "13.361389", "38.115556", "Palermo"), 0)
        check("GEOADD move member", c.cmd("GEOADD", "geo", "13.5", "38.2", "Palermo"), 0)
        check("GEODIST after move differs", c.cmd("GEODIST", "geo", "Palermo", "Catania") != "166274.1516", True)
        check("GEOADD move back", c.cmd("GEOADD", "geo", "13.361389", "38.115556", "Palermo"), 0)
        check("GEODIST back to exact", c.cmd("GEODIST", "geo", "Palermo", "Catania"), "166274.1516")
        # Bad coordinates: out of range, not a number, and the wrong
        # argument count; a bad triple in a multi-member call adds
        # nothing, since every coordinate is parsed first.
        check("GEOADD bad lon", c.cmd("GEOADD", "geo", "200", "38.1", "Nowhere"),
              ("error", "ERR invalid longitude,latitude pair 200,38.1"))
        check("GEOADD bad lat", c.cmd("GEOADD", "geo", "13.3", "90", "Nowhere"),
              ("error", "ERR invalid longitude,latitude pair 13.3,90"))
        check("GEOADD bad number", c.cmd("GEOADD", "geo", "abc", "38.1", "Nowhere"),
              ("error", "ERR value is not a valid float"))
        check("GEOADD wrong argc", c.cmd("GEOADD", "geo", "13.3", "38.1"),
              ("error", "ERR wrong number of arguments for 'geoadd' command"))
        check("GEOADD partial apply", c.cmd("GEOADD", "geo2", "13.3", "38.1", "A", "200", "38.1", "B"),
              ("error", "ERR invalid longitude,latitude pair 200,38.1"))
        check("GEOADD partial apply adds nothing", c.cmd("ZCARD", "geo2"), 0)
        check("GEOADD list wrongtype", c.cmd("GEOADD", "plist", "13.3", "38.1", "x"), wrongtype)
        check("GEODIST list wrongtype", c.cmd("GEODIST", "plist", "x", "y"), wrongtype)
        # GEOADD preserves the TTL, as in Redis.
        check("GEOADD TTL setup", c.cmd("EXPIRE", "geo", "100"), 1)
        check("GEOADD TTL add", c.cmd("GEOADD", "geo", "12.5", "41.9", "Rome"), 1)
        check("GEOADD TTL member", c.cmd("GEODIST", "geo", "Palermo", "Rome") is not None, True)
        check("TTL survives GEOADD", 99 <= c.cmd("TTL", "geo") <= 100, True)
        # A no-op GEOADD does not invalidate a WATCH; a move does.
        check("GEOADD WATCH setup", c.cmd("WATCH", "geo"), "OK")
        check("GEOADD WATCH noop", c.cmd("GEOADD", "geo", "13.361389", "38.115556", "Palermo"), 0)
        check("MULTI geo noop", c.cmd("MULTI"), "OK")
        check("MULTI geo noop SET queued", c.cmd("SET", "wtestkey4", "v1"), "QUEUED")
        check("EXEC geo noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wtestkey4 after geo noop EXEC", c.cmd("GET", "wtestkey4"), "v1")
        check("GEOADD WATCH setup 2", c.cmd("WATCH", "geo"), "OK")
        check("GEOADD WATCH move", c.cmd("GEOADD", "geo", "13.5", "38.2", "Palermo"), 0)
        check("MULTI geo move", c.cmd("MULTI"), "OK")
        check("MULTI geo move SET queued", c.cmd("SET", "wtestkey4", "v2"), "QUEUED")
        check("EXEC geo move watch aborts", c.cmd("EXEC"), None)
        check("wtestkey4 unchanged after geo abort", c.cmd("GET", "wtestkey4"), "v1")
        check("COMMAND lists GEOADD", "GEOADD" in c.cmd("COMMAND"), True)
        check("COMMAND lists GEODIST", "GEODIST" in c.cmd("COMMAND"), True)
        check("COMMAND lists GEOPOS", "GEOPOS" in c.cmd("COMMAND"), True)
        check("COMMAND lists GEOHASH", "GEOHASH" in c.cmd("COMMAND"), True)
        # GEOSEARCH / GEORADIUS / GEORADIUSBYMEMBER. geos holds
        # Palermo, Catania and Rome; the centre (15, 37) is about
        # 56 km from Catania and 190 km from Palermo, and Rome is
        # far outside 200 km.
        check("GEOADD geos", c.cmd("GEOADD", "geos", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania", "12.5", "41.9", "Rome"), 3)
        check("GEOSEARCH byradius ASC", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC"), ["Catania", "Palermo"])
        check("GEOSEARCH byradius DESC", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "DESC"), ["Palermo", "Catania"])
        check("GEOSEARCH byradius default order", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), ["Catania", "Palermo"])
        check("GEOSEARCH byradius 100km", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "100", "km"), ["Catania"])
        check("GEOSEARCH byradius COUNT 1", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "COUNT", "1"), ["Catania"])
        # COUNT ... ANY takes the first hit in (score, member) order
        # without sorting: Palermo's geohash score is the smaller one.
        check("GEOSEARCH byradius COUNT 1 ANY", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "1", "ANY"), ["Palermo"])
        check("GEOSEARCH withdist", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "WITHDIST"), [["Catania", "56.4413"], ["Palermo", "190.4424"]])
        check("GEOSEARCH withdist withhash withcoord", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "WITHDIST", "WITHHASH", "WITHCOORD"),
              [["Catania", "56.4413", 3479447370796909, ["15.087267458438873", "37.502668423331613"]],
               ["Palermo", "190.4424", 3479099956230698, ["13.361389338970184", "38.115556395496299"]]])
        check("GEOSEARCH bybox 400km", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC"), ["Catania", "Palermo"])
        check("GEOSEARCH bybox 120km", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYBOX", "120", "120", "km", "ASC"), ["Catania"])
        check("GEOSEARCH bybox 100km empty", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYBOX", "100", "100", "km", "ASC"), [])
        check("GEOSEARCH frommember", c.cmd("GEOSEARCH", "geos", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC", "WITHDIST"), [["Palermo", "0.0000"], ["Catania", "166.2742"]])
        check("GEOSEARCH frommember missing", c.cmd("GEOSEARCH", "geos", "FROMMEMBER", "Ghost", "BYRADIUS", "200", "km"),
              ("error", "ERR could not decode requested zset member"))
        check("GEOSEARCH missing key", c.cmd("GEOSEARCH", "ghostgeo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), [])
        check("GEOSEARCH no shape", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37"),
              ("error", "ERR wrong number of arguments for 'geosearch' command"))
        check("GEOSEARCH no shape syntax", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "ASC"),
              ("error", "ERR syntax error"))
        check("GEOSEARCH bogus option", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "BOGUS"),
              ("error", "ERR syntax error"))
        check("GEOSEARCH both froms", c.cmd("GEOSEARCH", "geos", "FROMMEMBER", "Palermo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
              ("error", "ERR syntax error"))
        check("GEOSEARCH both sorts", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "DESC"),
              ("error", "ERR syntax error"))
        check("GEOSEARCH negative radius", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "-5", "km"),
              ("error", "ERR value is out of range, must be positive"))
        check("GEOSEARCH bad unit", c.cmd("GEOSEARCH", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "parsecs"),
              ("error", "ERR unsupported unit provided. please use m, km, ft, mi"))
        check("GEOSEARCH list wrongtype", c.cmd("GEOSEARCH", "plist", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), wrongtype)
        check("GEORADIUS", c.cmd("GEORADIUS", "geos", "15", "37", "200", "km", "ASC"), ["Catania", "Palermo"])
        check("GEORADIUSBYMEMBER", c.cmd("GEORADIUSBYMEMBER", "geos", "Palermo", "200", "km", "ASC"), ["Palermo", "Catania"])
        check("GEORADIUSBYMEMBER missing", c.cmd("GEORADIUSBYMEMBER", "geos", "Ghost", "200", "km"),
              ("error", "ERR could not decode requested zset member"))
        check("GEORADIUS STORE", c.cmd("GEORADIUS", "geos", "15", "37", "200", "km", "STORE", "geodest"), 2)
        check("GEORADIUS STORE zrange", c.cmd("ZRANGE", "geodest", "0", "-1", "WITHSCORES"), ["Palermo", "3479099956230698", "Catania", "3479447370796909"])
        check("GEORADIUS STOREDIST", c.cmd("GEORADIUS", "geos", "15", "37", "200", "km", "STOREDIST", "geodist"), 2)
        check("GEORADIUS STOREDIST zrange", c.cmd("ZRANGE", "geodist", "0", "-1", "WITHSCORES"), ["Catania", "56.441257870156797", "Palermo", "190.44242984775795"])
        # GEOSEARCHSTORE writes the same search to a destination
        # sorted set, as in real Redis: geohash scores by default,
        # distances in the shape's unit with STOREDIST.
        check("GEOSEARCHSTORE", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC"), 2)
        check("GEOSEARCHSTORE zrange", c.cmd("ZRANGE", "gsdest", "0", "-1", "WITHSCORES"), ["Palermo", "3479099956230698", "Catania", "3479447370796909"])
        check("GEOSEARCHSTORE STOREDIST", c.cmd("GEOSEARCHSTORE", "gsdist", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "STOREDIST"), 2)
        check("GEOSEARCHSTORE STOREDIST zrange", c.cmd("ZRANGE", "gsdist", "0", "-1", "WITHSCORES"), ["Catania", "56.441257870156797", "Palermo", "190.44242984775795"])
        check("GEOSEARCHSTORE bybox", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km"), 2)
        check("GEOSEARCHSTORE frommember", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km"), 2)
        check("GEOSEARCHSTORE count", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "1"), 1)
        check("GEOSEARCHSTORE count one", c.cmd("ZRANGE", "gsdest", "0", "-1"), ["Catania"])
        check("GEOSEARCHSTORE withdist error", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "WITHDIST"),
              ("error", "ERR syntax error"))
        check("GEOSEARCHSTORE TTL setup", c.cmd("EXPIRE", "gsdest", "100"), 1)
        check("GEOSEARCHSTORE overwrites TTL", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), 2)
        check("GEOSEARCHSTORE TTL cleared", c.cmd("TTL", "gsdest"), -1)
        check("GEOSEARCHSTORE empty", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "km"), 0)
        check("GEOSEARCHSTORE empty deletes dest", c.cmd("EXISTS", "gsdest"), 0)
        check("GEOSEARCHSTORE empty missing dest", c.cmd("GEOSEARCHSTORE", "gsnew", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "km"), 0)
        check("GEOSEARCHSTORE empty missing dest gone", c.cmd("EXISTS", "gsnew"), 0)
        check("GEOSEARCHSTORE refill", c.cmd("GEOSEARCHSTORE", "gsdest", "geos", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), 2)
        # The destination may be the source key itself.
        check("GEOSEARCHSTORE self setup", c.cmd("GEOADD", "gsself", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania", "12.5", "41.9", "Rome"), 3)
        check("GEOSEARCHSTORE self", c.cmd("GEOSEARCHSTORE", "gsself", "gsself", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), 2)
        check("GEOSEARCHSTORE self members", c.cmd("ZRANGE", "gsself", "0", "-1"), ["Palermo", "Catania"])
        check("GEOSEARCHSTORE list wrongtype", c.cmd("GEOSEARCHSTORE", "gsdest", "plist", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), wrongtype)
        check("COMMAND lists GEOSEARCHSTORE", "GEOSEARCHSTORE" in c.cmd("COMMAND"), True)
        check("GEORADIUS STORE withdist error", c.cmd("GEORADIUS", "geos", "15", "37", "200", "km", "STORE", "geodest", "WITHDIST"),
              ("error", "ERR STORE option is not compatible with WITHDIST, WITHCOORD and WITHHASH options"))
        check("GEORADIUS STORE empty", c.cmd("GEORADIUS", "geos", "15", "37", "1", "km", "STORE", "geoempty"), 0)
        check("GEORADIUS STORE empty creates nothing", c.cmd("EXISTS", "geoempty"), 0)
        check("GEORADIUS list wrongtype", c.cmd("GEORADIUS", "plist", "15", "37", "200", "km"), wrongtype)
        check("COMMAND lists GEOSEARCH", "GEOSEARCH" in c.cmd("COMMAND"), True)
        check("COMMAND lists GEORADIUS", "GEORADIUS" in c.cmd("COMMAND"), True)
        check("COMMAND lists GEORADIUSBYMEMBER", "GEORADIUSBYMEMBER" in c.cmd("COMMAND"), True)
        check("GEOSEARCH cleanup", c.cmd("DEL", "geos", "geodest", "geodist", "gsdest", "gsdist", "gsself"), 6)
        check("GEO cleanup", c.cmd("DEL", "geo", "geo2", "wtestkey4"), 2)
        # phll stays: the AOF restart below proves a HyperLogLog
        # survives a SHUTDOWN and reload — and phll is itself the
        # product of a PFMERGE, so the restart also proves a merged
        # HyperLogLog replays from the AOF byte-identically.
        # Everything else is cleaned up so the restart still sees
        # its expected key count.
        check("PFADD phll1", c.cmd("PFADD", "phll1", "a", "b"), 1)
        check("PFADD phll2", c.cmd("PFADD", "phll2", "b", "c"), 1)
        check("PFMERGE phll", c.cmd("PFMERGE", "phll", "phll1", "phll2"), "OK")
        check("PFCOUNT phll merged", c.cmd("PFCOUNT", "phll"), 3)
        check("HLL cleanup", c.cmd("DEL", "hll", "hlldup", "hll1", "hll2",
                                    "hllwatch", "wtestkey2", "mhll1", "mhll2",
                                    "mdest", "mdest2", "mdest3", "wtestkey3",
                                    "phll1", "phll2"), 14)
        check("ZREMRANGEBYLEX wrongtype", c.cmd("ZREMRANGEBYLEX", "after", "-", "+"),
              wrongtype)
        check("COMMAND lists ZREMRANGEBYLEX", "ZREMRANGEBYLEX" in c.cmd("COMMAND"), True)
        check("ZREM one", c.cmd("ZREM", "myzset", "b"), 1)
        check("ZCARD after ZREM", c.cmd("ZCARD", "myzset"), 3)
        check("ZRANK removed member", c.cmd("ZRANK", "myzset", "b"), None)
        check("ZREM missing member", c.cmd("ZREM", "myzset", "ghost"), 0)
        check("ZREM multi", c.cmd("ZREM", "myzset", "a", "c", "d"), 3)
        check("ZCARD emptied zset", c.cmd("ZCARD", "myzset"), 0)
        check("EXISTS emptied zset", c.cmd("EXISTS", "myzset"), 0)
        check("ZREM missing key", c.cmd("ZREM", "ghostzset", "a"), 0)
        check("ZRANGE wrong args", c.cmd("ZRANGE", "myzset", "0"),
              ("error", "ERR wrong number of arguments for 'zrange' command"))
        check("ZRANGE bad index", c.cmd("ZRANGE", "myzset", "x", "y"),
              ("error", "ERR value is not an integer or out of range"))
        check("ZRANGE bad option", c.cmd("ZRANGE", "myzset", "0", "-1", "BOGUS"),
              ("error", "ERR syntax error"))
        check("ZREVRANGE wrongtype", c.cmd("ZREVRANGE", "after", "0", "-1"), wrongtype)
        check("ZREM wrongtype", c.cmd("ZREM", "after", "x"), wrongtype)
        check("ZRANK wrongtype", c.cmd("ZRANK", "after", "x"), wrongtype)
        check("ZREVRANK wrongtype", c.cmd("ZREVRANK", "after", "x"), wrongtype)
        check("ZREVRANK wrong args", c.cmd("ZREVRANK", "myzset"),
              ("error", "ERR wrong number of arguments for 'zrevrank' command"))
        check("ZINCRBY wrongtype", c.cmd("ZINCRBY", "after", "1", "x"), wrongtype)
        check("COMMAND lists ZRANGE", "ZRANGE" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZREVRANGE", "ZREVRANGE" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZREM", "ZREM" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZRANK", "ZRANK" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZREVRANK", "ZREVRANK" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZINCRBY", "ZINCRBY" in c.cmd("COMMAND"), True)
        # ZPOPMIN / ZPOPMAX / ZRANDMEMBER. popzset holds a=1, b=2, c=3, d=4.
        check("ZADD popzset", c.cmd("ZADD", "popzset", "1", "a", "2", "b", "3", "c", "4", "d"), 4)
        check("ZPOPMIN one", c.cmd("ZPOPMIN", "popzset"), ["a", "1"])
        check("ZCARD after ZPOPMIN", c.cmd("ZCARD", "popzset"), 3)
        check("ZPOPMIN count", c.cmd("ZPOPMIN", "popzset", "2"), ["b", "2", "c", "3"])
        check("ZPOPMIN rest", c.cmd("ZPOPMIN", "popzset", "10"), ["d", "4"])
        check("EXISTS emptied by ZPOPMIN", c.cmd("EXISTS", "popzset"), 0)
        check("ZPOPMIN missing", c.cmd("ZPOPMIN", "ghostzset"), [])
        check("ZPOPMIN zero count", c.cmd("ZPOPMIN", "popzset", "0"),
              ("error", "ERR value is out of range, must be positive"))
        check("ZPOPMIN negative count", c.cmd("ZPOPMIN", "popzset", "-1"),
              ("error", "ERR value is out of range, must be positive"))
        check("ZPOPMIN bad count", c.cmd("ZPOPMIN", "popzset", "xx"),
              ("error", "ERR value is not an integer or out of range"))
        check("ZPOPMIN wrong args", c.cmd("ZPOPMIN", "popzset", "1", "2"),
              ("error", "ERR wrong number of arguments for 'zpopmin' command"))
        check("ZADD maxzset", c.cmd("ZADD", "maxzset", "1", "a", "2", "b", "3", "c"), 3)
        check("ZPOPMAX one", c.cmd("ZPOPMAX", "maxzset"), ["c", "3"])
        check("ZPOPMAX count", c.cmd("ZPOPMAX", "maxzset", "2"), ["b", "2", "a", "1"])
        check("EXISTS emptied by ZPOPMAX", c.cmd("EXISTS", "maxzset"), 0)
        check("ZPOPMAX missing", c.cmd("ZPOPMAX", "ghostzset"), [])
        check("ZPOPMAX wrongtype", c.cmd("ZPOPMAX", "after", "1"), wrongtype)
        check("ZPOPMIN wrongtype", c.cmd("ZPOPMIN", "after"), wrongtype)
        # ZRANDMEMBER. randzset holds x=1, y=2, z=3; the single-member
        # form is random, so the checks assert membership, not a
        # particular member, and the count forms sort before comparing.
        check("ZADD randzset", c.cmd("ZADD", "randzset", "1", "x", "2", "y", "3", "z"), 3)
        check("ZRANDMEMBER one in set", c.cmd("ZRANDMEMBER", "randzset") in ["x", "y", "z"], True)
        check("ZRANDMEMBER missing", c.cmd("ZRANDMEMBER", "ghostzset"), None)
        check("ZRANDMEMBER all", sorted(c.cmd("ZRANDMEMBER", "randzset", "3")), ["x", "y", "z"])
        check("ZRANDMEMBER over count", sorted(c.cmd("ZRANDMEMBER", "randzset", "10")),
              ["x", "y", "z"])
        check("ZRANDMEMBER zero", c.cmd("ZRANDMEMBER", "randzset", "0"), [])
        got = c.cmd("ZRANDMEMBER", "randzset", "-5")
        check("ZRANDMEMBER negative count len", len(got), 5)
        check("ZRANDMEMBER negative count members",
              all(m in ["x", "y", "z"] for m in got), True)
        got = c.cmd("ZRANDMEMBER", "randzset", "2", "WITHSCORES")
        check("ZRANDMEMBER withscores len", len(got), 4)
        want_scores = {"x": "1", "y": "2", "z": "3"}
        check("ZRANDMEMBER withscores scores match",
              all(want_scores[m] == s for m, s in zip(got[::2], got[1::2])), True)
        check("ZRANDMEMBER bad count", c.cmd("ZRANDMEMBER", "randzset", "xx"),
              ("error", "ERR value is not an integer or out of range"))
        check("ZRANDMEMBER bad option", c.cmd("ZRANDMEMBER", "randzset", "2", "BOGUS"),
              ("error", "ERR syntax error"))
        check("ZRANDMEMBER wrongtype", c.cmd("ZRANDMEMBER", "after"), wrongtype)
        check("ZRANDMEMBER wrongtype count", c.cmd("ZRANDMEMBER", "after", "2"), wrongtype)
        check("COMMAND lists ZPOPMIN", "ZPOPMIN" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZPOPMAX", "ZPOPMAX" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZRANDMEMBER", "ZRANDMEMBER" in c.cmd("COMMAND"), True)
        # ZSCAN: full iteration in score order, MATCH, COUNT
        # pagination, WITHSCORES, empty/missing key, errors.
        check("ZADD scanzset", c.cmd("ZADD", "scanzset", "1", "a1", "2", "a2", "3", "b1", "4", "b2", "5", "c1"), 5)
        check("ZSCAN all", c.cmd("ZSCAN", "scanzset", "0"),
              ["0", ["a1", "a2", "b1", "b2", "c1"]])
        check("ZSCAN WITHSCORES", c.cmd("ZSCAN", "scanzset", "0", "WITHSCORES"),
              ["0", ["a1", "1", "a2", "2", "b1", "3", "b2", "4", "c1", "5"]])
        check("ZSCAN MATCH a*", c.cmd("ZSCAN", "scanzset", "0", "MATCH", "a*"),
              ["0", ["a1", "a2"]])
        check("ZSCAN MATCH none", c.cmd("ZSCAN", "scanzset", "0", "MATCH", "zzz*"),
              ["0", []])
        check("ZSCAN COUNT 2 page 1", c.cmd("ZSCAN", "scanzset", "0", "COUNT", "2"),
              ["2", ["a1", "a2"]])
        check("ZSCAN COUNT 2 page 2", c.cmd("ZSCAN", "scanzset", "2", "COUNT", "2"),
              ["4", ["b1", "b2"]])
        check("ZSCAN COUNT 2 page 3", c.cmd("ZSCAN", "scanzset", "4", "COUNT", "2"),
              ["0", ["c1"]])
        check("ZSCAN COUNT 2 WITHSCORES page 1",
              c.cmd("ZSCAN", "scanzset", "0", "COUNT", "2", "WITHSCORES"),
              ["2", ["a1", "1", "a2", "2"]])
        check("ZSCAN COUNT 0", c.cmd("ZSCAN", "scanzset", "0", "COUNT", "0"),
              ["0", []])
        check("ZSCAN cursor past end", c.cmd("ZSCAN", "scanzset", "99"),
              ["0", []])
        check("ZSCAN missing key", c.cmd("ZSCAN", "ghostzset", "0"), ["0", []])
        check("ZSCAN wrongtype", c.cmd("ZSCAN", "after", "0"), wrongtype)
        check("ZSCAN bad cursor", c.cmd("ZSCAN", "scanzset", "abc"),
              ("error", "ERR invalid cursor"))
        check("ZSCAN wrong args", c.cmd("ZSCAN", "scanzset"),
              ("error", "ERR wrong number of arguments for 'zscan' command"))
        check("ZSCAN bad option", c.cmd("ZSCAN", "scanzset", "0", "NOPE"),
              ("error", "ERR syntax error"))
        check("ZSCAN duplicate WITHSCORES",
              c.cmd("ZSCAN", "scanzset", "0", "WITHSCORES", "WITHSCORES"),
              ("error", "ERR syntax error"))
        check("COMMAND lists ZSCAN", "ZSCAN" in c.cmd("COMMAND"), True)
        # Skip-list sorted sets at scale: 300 members added in
        # shuffled order, with repeated scores so member tiebreaks
        # matter, then the order, ranks, ranges and pops must all
        # agree with the same ordering computed here. The key is
        # deleted at the end so the AOF restart below still sees
        # its expected key count.
        rng = random.Random(42)
        pairs = [("m%03d" % i, rng.choice([1, 2, 3, 5, 8]))
                 for i in range(300)]
        rng.shuffle(pairs)
        args = []
        for member, score in pairs:
            args += [str(score), member]
        check("ZADD bigzset", c.cmd("ZADD", "bigzset", *args), 300)
        check("ZCARD bigzset", c.cmd("ZCARD", "bigzset"), 300)
        want = sorted(pairs, key=lambda p: (p[1], p[0]))
        check("ZRANGE bigzset all", c.cmd("ZRANGE", "bigzset", "0", "-1"),
              [m for m, _ in want])
        check("ZREVRANGE bigzset all", c.cmd("ZREVRANGE", "bigzset", "0", "-1"),
              [m for m, _ in reversed(want)])
        check("ZRANGE bigzset slice", c.cmd("ZRANGE", "bigzset", "100", "109"),
              [m for m, _ in want[100:110]])
        check("ZRANGE bigzset neg slice", c.cmd("ZRANGE", "bigzset", "-3", "-1"),
              [m for m, _ in want[-3:]])
        check("ZRANK bigzset first", c.cmd("ZRANK", "bigzset", want[0][0]), 0)
        check("ZRANK bigzset mid", c.cmd("ZRANK", "bigzset", want[150][0]), 150)
        check("ZRANK bigzset last", c.cmd("ZRANK", "bigzset", want[-1][0]), 299)
        check("ZSCORE bigzset mid", c.cmd("ZSCORE", "bigzset", want[150][0]),
              str(want[150][1]))
        check("ZRANGEBYSCORE bigzset 2", c.cmd("ZRANGEBYSCORE", "bigzset", "2", "2"),
              [m for m, s in want if s == 2])
        check("ZREVRANGEBYSCORE bigzset 2",
              c.cmd("ZREVRANGEBYSCORE", "bigzset", "2", "2"),
              [m for m, s in reversed([p for p in want if p[1] == 2])])
        check("ZRANGEBYSCORE bigzset limit",
              c.cmd("ZRANGEBYSCORE", "bigzset", "1", "3", "LIMIT", "5", "3"),
              [m for m, s in want if 1 <= s <= 3][5:8])
        # Rescore the first member across the set: it must move,
        # not duplicate, then an increment must bring it back.
        check("ZADD bigzset rescore", c.cmd("ZADD", "bigzset", "9", want[0][0]), 0)
        check("ZCARD after rescore", c.cmd("ZCARD", "bigzset"), 300)
        check("ZRANK after rescore", c.cmd("ZRANK", "bigzset", want[0][0]), 299)
        check("ZRANGE last after rescore",
              c.cmd("ZRANGE", "bigzset", "-1", "-1"), [want[0][0]])
        check("ZINCRBY bigzset", c.cmd("ZINCRBY", "bigzset", "-8", want[0][0]), "1")
        check("ZRANK after zincrby", c.cmd("ZRANK", "bigzset", want[0][0]), 0)
        check("ZPOPMIN bigzset", c.cmd("ZPOPMIN", "bigzset", "2"),
              [want[0][0], "1", want[1][0], str(want[1][1])])
        check("ZCARD after bigzset pops", c.cmd("ZCARD", "bigzset"), 298)
        check("ZRANGE first after pops", c.cmd("ZRANGE", "bigzset", "0", "0"),
              [want[2][0]])
        check("ZRANDMEMBER bigzset distinct",
              len(set(c.cmd("ZRANDMEMBER", "bigzset", "10"))), 10)
        check("ZREM bigzset mid", c.cmd("ZREM", "bigzset", want[150][0]), 1)
        check("ZRANK bigzset after rem", c.cmd("ZRANK", "bigzset", want[151][0]), 148)
        check("DEL bigzset", c.cmd("DEL", "bigzset"), 1)
        # Transactions: MULTI queues, EXEC runs the queue in order and
        # returns one reply per command, DISCARD drops the queue, and
        # the error cases. A second client proves the queued writes
        # are invisible until EXEC runs.
        c2 = Client(PORT)
        check("EXEC without MULTI", c.cmd("EXEC"), ("error", "ERR EXEC without MULTI"))
        check("DISCARD without MULTI", c.cmd("DISCARD"), ("error", "ERR DISCARD without MULTI"))
        check("MULTI", c.cmd("MULTI"), "OK")
        check("MULTI nested", c.cmd("MULTI"), ("error", "ERR MULTI calls can not be nested"))
        check("MULTI SET queued", c.cmd("SET", "txkey", "txval"), "QUEUED")
        check("MULTI INCR queued", c.cmd("INCR", "txctr"), "QUEUED")
        check("MULTI GET queued", c.cmd("GET", "txkey"), "QUEUED")
        check("txkey invisible before EXEC", c2.cmd("GET", "txkey"), None)
        check("EXEC", c.cmd("EXEC"), ["OK", 1, "txval"])
        check("txkey visible after EXEC", c2.cmd("GET", "txkey"), "txval")
        check("txctr after EXEC", c2.cmd("GET", "txctr"), "1")
        # An error inside a transaction does not abort it: the error
        # reply takes its place in the array and the rest still run.
        check("MULTI 2", c.cmd("MULTI"), "OK")
        check("MULTI bad INCR queued", c.cmd("INCR", "txkey"), "QUEUED")
        check("MULTI good SET queued", c.cmd("SET", "txkey2", "v2"), "QUEUED")
        check("EXEC with error", c.cmd("EXEC"),
              [("error", "ERR value is not an integer or out of range"), "OK"])
        check("txkey2 set despite error", c2.cmd("GET", "txkey2"), "v2")
        # DISCARD drops the queue: nothing queued ever executes.
        check("MULTI 3", c.cmd("MULTI"), "OK")
        check("MULTI discard SET queued", c.cmd("SET", "txgone", "x"), "QUEUED")
        check("DISCARD", c.cmd("DISCARD"), "OK")
        check("discarded key absent", c2.cmd("GET", "txgone"), None)
        # An empty transaction EXECs to an empty array, and a nested
        # array reply (LRANGE) comes back nested inside the EXEC array.
        check("MULTI empty", c.cmd("MULTI"), "OK")
        check("EXEC empty queue", c.cmd("EXEC"), [])
        check("MULTI 4", c.cmd("MULTI"), "OK")
        check("MULTI RPUSH queued", c.cmd("RPUSH", "txlist", "a", "b"), "QUEUED")
        check("MULTI LRANGE queued", c.cmd("LRANGE", "txlist", "0", "-1"), "QUEUED")
        check("EXEC nested array", c.cmd("EXEC"), [2, ["a", "b"]])
        check("COMMAND lists MULTI", "MULTI" in c.cmd("COMMAND"), True)
        check("COMMAND lists EXEC", "EXEC" in c.cmd("COMMAND"), True)
        check("COMMAND lists DISCARD", "DISCARD" in c.cmd("COMMAND"), True)
        # WATCH: optimistic locking for MULTI/EXEC. A watched key that
        # changes between WATCH and EXEC — by another client, by
        # expiry, or by FLUSHALL — aborts the transaction with a nil
        # array and the queued writes never run. UNWATCH, EXEC and
        # DISCARD all clear the watch set.
        check("WATCH no keys", c.cmd("WATCH"),
              ("error", "ERR wrong number of arguments for 'watch' command"))
        check("UNWATCH no watch", c.cmd("UNWATCH"), "OK")
        check("SET wkey", c.cmd("SET", "wkey", "v1"), "OK")
        check("WATCH wkey", c.cmd("WATCH", "wkey"), "OK")
        check("MULTI watch clean", c.cmd("MULTI"), "OK")
        check("MULTI watch SET queued", c.cmd("SET", "wkey", "v2"), "QUEUED")
        check("EXEC watch clean", c.cmd("EXEC"), ["OK"])
        check("wkey after clean EXEC", c.cmd("GET", "wkey"), "v2")
        # Dirty via SET by another client: EXEC aborts, queued write
        # never runs.
        check("WATCH wkey 2", c.cmd("WATCH", "wkey"), "OK")
        check("other client SET wkey", c2.cmd("SET", "wkey", "v3"), "OK")
        check("MULTI watch dirty", c.cmd("MULTI"), "OK")
        check("MULTI dirty SET queued", c.cmd("SET", "wkey", "v4"), "QUEUED")
        check("EXEC watch dirty aborts", c.cmd("EXEC"), None)
        check("wkey unchanged after abort", c.cmd("GET", "wkey"), "v3")
        # Dirty via creation of a watched-but-missing key.
        check("WATCH wmissing", c.cmd("WATCH", "wmissing"), "OK")
        check("other client creates wmissing", c2.cmd("SET", "wmissing", "x"), "OK")
        check("MULTI watch missing", c.cmd("MULTI"), "OK")
        check("MULTI missing SET queued", c.cmd("SET", "wkey", "v5"), "QUEUED")
        check("EXEC watch created aborts", c.cmd("EXEC"), None)
        check("wkey unchanged after create abort", c.cmd("GET", "wkey"), "v3")
        # Dirty via DEL of a watched key.
        check("WATCH wkey 3", c.cmd("WATCH", "wkey"), "OK")
        check("other client DEL wkey", c2.cmd("DEL", "wkey"), 1)
        check("MULTI watch deleted", c.cmd("MULTI"), "OK")
        check("MULTI deleted SET queued", c.cmd("SET", "wkey", "v6"), "QUEUED")
        check("EXEC watch deleted aborts", c.cmd("EXEC"), None)
        check("wkey still gone after abort", c.cmd("GET", "wkey"), None)
        # Dirty via expiry of a watched key.
        check("SET wexp PX", c.cmd("SET", "wexp", "v", "PX", "100"), "OK")
        check("WATCH wexp", c.cmd("WATCH", "wexp"), "OK")
        time.sleep(0.15)
        check("MULTI watch expired", c.cmd("MULTI"), "OK")
        check("MULTI expired SET queued", c.cmd("SET", "wexp", "v2"), "QUEUED")
        check("EXEC watch expired aborts", c.cmd("EXEC"), None)
        check("wexp gone after expiry", c.cmd("GET", "wexp"), None)
        # UNWATCH clears the set: a dirty key no longer aborts.
        check("SET wkey again", c.cmd("SET", "wkey", "v7"), "OK")
        check("WATCH wkey 4", c.cmd("WATCH", "wkey"), "OK")
        check("other client SET wkey 2", c2.cmd("SET", "wkey", "v8"), "OK")
        check("UNWATCH", c.cmd("UNWATCH"), "OK")
        check("MULTI after UNWATCH", c.cmd("MULTI"), "OK")
        check("MULTI unwatch SET queued", c.cmd("SET", "wkey", "v9"), "QUEUED")
        check("EXEC after UNWATCH runs", c.cmd("EXEC"), ["OK"])
        check("wkey after UNWATCH EXEC", c.cmd("GET", "wkey"), "v9")
        # WATCH inside MULTI is an error; UNWATCH inside MULTI queues
        # and runs at EXEC, as in real Redis.
        check("MULTI watch err", c.cmd("MULTI"), "OK")
        check("WATCH inside MULTI", c.cmd("WATCH", "wkey"),
              ("error", "ERR WATCH inside MULTI is not allowed"))
        check("UNWATCH inside MULTI queues", c.cmd("UNWATCH"), "QUEUED")
        check("EXEC with queued UNWATCH", c.cmd("EXEC"), ["OK"])
        # EXEC clears the watch set: a later change does not abort.
        check("WATCH wkey 5", c.cmd("WATCH", "wkey"), "OK")
        check("MULTI watch clear", c.cmd("MULTI"), "OK")
        check("MULTI clear SET queued", c.cmd("SET", "wkey", "v10"), "QUEUED")
        check("EXEC clears watch", c.cmd("EXEC"), ["OK"])
        check("other client SET wkey 3", c2.cmd("SET", "wkey", "v11"), "OK")
        check("MULTI after EXEC", c.cmd("MULTI"), "OK")
        check("MULTI after EXEC SET queued", c.cmd("SET", "wkey", "v12"), "QUEUED")
        check("EXEC after EXEC runs", c.cmd("EXEC"), ["OK"])
        # DISCARD clears the watch set too.
        check("WATCH wkey 6", c.cmd("WATCH", "wkey"), "OK")
        check("MULTI discard watch", c.cmd("MULTI"), "OK")
        check("DISCARD clears watch", c.cmd("DISCARD"), "OK")
        check("other client SET wkey 4", c2.cmd("SET", "wkey", "v13"), "OK")
        check("MULTI after DISCARD", c.cmd("MULTI"), "OK")
        check("MULTI after DISCARD SET queued", c.cmd("SET", "wkey", "v14"), "QUEUED")
        check("EXEC after DISCARD runs", c.cmd("EXEC"), ["OK"])
        # A no-op write (SADD of a member already present) does not
        # invalidate a watch.
        check("SADD wset m", c.cmd("SADD", "wset", "m"), 1)
        check("WATCH wset", c.cmd("WATCH", "wset"), "OK")
        check("other client SADD dup", c2.cmd("SADD", "wset", "m"), 0)
        check("MULTI noop watch", c.cmd("MULTI"), "OK")
        check("MULTI noop SET queued", c.cmd("SET", "wkey", "v15"), "QUEUED")
        check("EXEC noop watch runs", c.cmd("EXEC"), ["OK"])
        # A real HINCRBY invalidates a watch; a no-op one (delta 0 on
        # an existing field) does not.
        check("HSET whash", c.cmd("HSET", "whash", "n", "1"), 1)
        check("WATCH whash", c.cmd("WATCH", "whash"), "OK")
        check("other client HINCRBY whash", c2.cmd("HINCRBY", "whash", "n", "1"), 2)
        check("MULTI hincrby watch", c.cmd("MULTI"), "OK")
        check("MULTI hincrby SET queued", c.cmd("SET", "wkey", "v16"), "QUEUED")
        check("EXEC hincrby watch aborts", c.cmd("EXEC"), None)
        check("wkey unchanged after hincrby abort", c.cmd("GET", "wkey"), "v15")
        check("WATCH whash 2", c.cmd("WATCH", "whash"), "OK")
        check("other client HINCRBY noop", c2.cmd("HINCRBY", "whash", "n", "0"), 2)
        check("MULTI hincrby noop watch", c.cmd("MULTI"), "OK")
        check("MULTI hincrby noop SET queued", c.cmd("SET", "wkey", "v17"), "QUEUED")
        check("EXEC hincrby noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wkey after hincrby noop EXEC", c.cmd("GET", "wkey"), "v17")
        # A real HINCRBYFLOAT invalidates a watch; a no-op one
        # (delta 0 on an existing field) does not.
        check("WATCH whash 3", c.cmd("WATCH", "whash"), "OK")
        check("other client HINCRBYFLOAT whash", c2.cmd("HINCRBYFLOAT", "whash", "n", "0.5"), "2.5")
        check("MULTI hincrbyfloat watch", c.cmd("MULTI"), "OK")
        check("MULTI hincrbyfloat SET queued", c.cmd("SET", "wkey", "v18"), "QUEUED")
        check("EXEC hincrbyfloat watch aborts", c.cmd("EXEC"), None)
        check("wkey unchanged after hincrbyfloat abort", c.cmd("GET", "wkey"), "v17")
        check("WATCH whash 4", c.cmd("WATCH", "whash"), "OK")
        check("other client HINCRBYFLOAT noop", c2.cmd("HINCRBYFLOAT", "whash", "n", "0"), "2.5")
        check("MULTI hincrbyfloat noop watch", c.cmd("MULTI"), "OK")
        check("MULTI hincrbyfloat noop SET queued", c.cmd("SET", "wkey", "v19"), "QUEUED")
        check("EXEC hincrbyfloat noop watch runs", c.cmd("EXEC"), ["OK"])
        check("wkey after hincrbyfloat noop EXEC", c.cmd("GET", "wkey"), "v19")
        check("watch keys cleanup", c.cmd("DEL", "wkey", "wmissing", "wset", "whash"), 4)
        # FLUSHALL invalidates even a watch on a key that never
        # existed; it wipes the dataset, so rebuild the four keys the
        # AOF restart section below expects (after, plist, pset, phash),
        # plus the HyperLogLog phll the restart proves survives.
        check("WATCH wnever", c.cmd("WATCH", "wnever"), "OK")
        check("FLUSHALL watch", c.cmd("FLUSHALL"), "OK")
        check("MULTI flush watch", c.cmd("MULTI"), "OK")
        check("MULTI flush SET queued", c.cmd("SET", "wkey", "v16"), "QUEUED")
        check("EXEC flush watch aborts", c.cmd("EXEC"), None)
        # The flush also wiped the transaction keys, so rebuild them
        # for the tx cleanup below, alongside the four keys the AOF
        # restart section expects.
        check("rebuild txkey", c.cmd("SET", "txkey", "txval"), "OK")
        check("rebuild txctr", c.cmd("SET", "txctr", "1"), "OK")
        check("rebuild txkey2", c.cmd("SET", "txkey2", "v2"), "OK")
        check("rebuild txlist", c.cmd("RPUSH", "txlist", "a", "b"), 2)
        check("rebuild after", c.cmd("SET", "after", "flush"), "OK")
        check("rebuild plist", c.cmd("RPUSH", "plist", "x", "y", "z"), 3)
        check("rebuild pset", c.cmd("SADD", "pset", "x", "y", "z"), 3)
        check("rebuild phash", c.cmd("HSET", "phash", "a", "1", "b", "2"), 2)
        check("rebuild pzset", c.cmd("ZADD", "pzset", "1.5", "x", "2.25", "y", "10", "z"), 3)
        check("rebuild phll", c.cmd("PFADD", "phll", "a", "b", "c"), 1)
        # pgeo stays too: the AOF restart below proves a geo set
        # replays from the AOF with its geohash scores intact.
        check("rebuild pgeo", c.cmd("GEOADD", "pgeo", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"), 2)
        # A zero-padded string, so the restart proves SETRANGE padding
        # survives an AOF replay byte-for-byte.
        check("rebuild prange", c.cmd("SETRANGE", "prange", "5", "pad"), 8)
        check("COMMAND lists WATCH", "WATCH" in c.cmd("COMMAND"), True)
        check("COMMAND lists UNWATCH", "UNWATCH" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZADD", "ZADD" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZSCORE", "ZSCORE" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZCARD", "ZCARD" in c.cmd("COMMAND"), True)
        # Clean up the transaction keys: the AOF restart below counts
        # exact keys (11: after, plist, pset, phash, pzset, prange, pfield, punion, pdiff, phll, pgeo).
        # ZINCRBY and ZREM on pzset before the restart, so the AOF
        # replay section below can prove they were logged; the
        # ZREMRANGEBYSCORE removes y (2.25) from pzset the same way.
        check("AOF pre-restart ZREMRANGEBYSCORE", c.cmd("ZREMRANGEBYSCORE", "pzset", "2", "3"), 1)
        check("AOF pre-restart ZINCRBY", c.cmd("ZINCRBY", "pzset", "1", "x"), "2.5")
        check("AOF pre-restart ZREM", c.cmd("ZREM", "pzset", "z"), 1)
        check("AOF pre-restart HINCRBY", c.cmd("HINCRBY", "phash", "a", "5"), 6)
        check("AOF pre-restart ZUNIONSTORE", c.cmd("ZUNIONSTORE", "punion", "1", "pzset"), 1)
        check("AOF pre-restart ZDIFFSTORE", c.cmd("ZDIFFSTORE", "pdiff", "1", "pzset"), 1)
        check("AOF pre-restart BITFIELD", c.cmd("BITFIELD", "pfield", "SET", "u8", "0", "65", "SET", "u8", "8", "66"), [0, 0])
        check("tx cleanup", c.cmd("DEL", "txkey", "txctr", "txkey2", "txlist"), 4)
        c2.close()
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

    # Restart: AOF must rebuild the dataset ("after" and the "plist" list
    # survived the flush).
    proc = subprocess.Popen(
        [BIN, "--port", str(PORT), "--data-dir", DATA_DIR],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        wait_for_port(PORT, proc)
        c = Client(PORT)
        check("AOF replay GET after", c.cmd("GET", "after"), "flush")
        check("AOF replay list", c.cmd("LRANGE", "plist", "0", "-1"), ["x", "y", "z"])
        check("AOF replay list len", c.cmd("LLEN", "plist"), 3)
        check("AOF replay set", sorted(c.cmd("SMEMBERS", "pset")), ["x", "y", "z"])
        check("AOF replay set card", c.cmd("SCARD", "pset"), 3)
        check("AOF replay hash", c.cmd("HGETALL", "phash"), ["a", "6", "b", "2"])
        check("AOF replay hash len", c.cmd("HLEN", "phash"), 2)
        check("AOF replay zset card", c.cmd("ZCARD", "pzset"), 1)
        check("AOF replay zset range-removed gone", c.cmd("ZSCORE", "pzset", "y"), None)
        check("AOF replay zset incr score", c.cmd("ZSCORE", "pzset", "x"), "2.5")
        check("AOF replay zset range", c.cmd("ZRANGE", "pzset", "0", "-1"), ["x"])
        check("AOF replay zset removed gone", c.cmd("ZSCORE", "pzset", "z"), None)
        check("AOF replay zunionstore card", c.cmd("ZCARD", "punion"), 1)
        check("AOF replay zunionstore score", c.cmd("ZSCORE", "punion", "x"), "2.5")
        check("AOF replay zunionstore range", c.cmd("ZRANGE", "punion", "0", "-1"), ["x"])
        check("AOF replay zdiffstore card", c.cmd("ZCARD", "pdiff"), 1)
        check("AOF replay zdiffstore score", c.cmd("ZSCORE", "pdiff", "x"), "2.5")
        check("AOF replay zdiffstore range", c.cmd("ZRANGE", "pdiff", "0", "-1"), ["x"])
        check("AOF replay padded GET", c.cmd("GET", "prange"), "\x00" * 5 + "pad")
        check("AOF replay padded STRLEN", c.cmd("STRLEN", "prange"), 8)
        check("AOF replay padded GETRANGE", c.cmd("GETRANGE", "prange", "0", "4"), "\x00" * 5)
        check("AOF replay bitfield", c.cmd("BITFIELD", "pfield", "GET", "u16", "0"), [16706])
        check("AOF replay hll", c.cmd("PFCOUNT", "phll"), 3)
        check("AOF replay geo dist", c.cmd("GEODIST", "pgeo", "Palermo", "Catania"), "166274.1516")
        check("AOF replay geo card", c.cmd("ZCARD", "pgeo"), 2)
        # The replayed PEXPIRE re-arms blinklist's 50 ms TTL from replay
        # time, so it is briefly alive again. The checks above run only
        # a few ms after the restart, so whether the TTL has passed by
        # the time they run depends on startup timing — poll until the
        # three keys are gone instead of assuming enough time passed.
        deadline = time.time() + 2
        while time.time() < deadline:
            if (c.cmd("LLEN", "blinklist") == 0 and
                    c.cmd("SCARD", "blinkset") == 0 and
                    c.cmd("HLEN", "blinkhash") == 0 and
                    c.cmd("ZCARD", "blinkzset") == 0):
                break
            time.sleep(0.01)
        check("AOF replay expired list gone", c.cmd("LLEN", "blinklist"), 0)
        check("AOF replay expired set gone", c.cmd("SCARD", "blinkset"), 0)
        check("AOF replay expired hash gone", c.cmd("HLEN", "blinkhash"), 0)
        check("AOF replay expired zset gone", c.cmd("ZCARD", "blinkzset"), 0)
        check("AOF replay DBSIZE", c.cmd("DBSIZE"), 11)
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

    # everysec policy: writes reach the AOF immediately (only the fsync
    # is deferred), INFO reports the policy, and data survives a SIGKILL
    # + restart because the records are already in the file.
    es_dir = "/tmp/cinder-smoke-everysec"
    proc = start_server(["--aof-fsync", "everysec"], data_dir=es_dir)
    try:
        c = Client(PORT)
        check("everysec INFO policy", "aof_fsync_policy:everysec" in c.cmd("INFO"), True)
        check("everysec INFO flush thread", "aof_flush_thread:1" in c.cmd("INFO"), True)
        check("everysec SET", c.cmd("SET", "eskey", "esval"), "OK")
        check("everysec GET", c.cmd("GET", "eskey"), "esval")
        with open(os.path.join(es_dir, "appendonly.aof"), "rb") as f:
            aof = f.read()
        check("everysec AOF has record", b"eskey" in aof and b"esval" in aof, True)
        # The background flush thread syncs on its own: poll INFO until
        # it reports a completed flush, with no SAVE in between.
        def flushes(info):
            for line in info.split("\r\n"):
                if line.startswith("aof_flushes_completed:"):
                    return int(line.split(":", 1)[1])
            return -1
        deadline = time.time() + 5
        while time.time() < deadline:
            if flushes(c.cmd("INFO")) >= 1:
                break
            time.sleep(0.1)
        check("everysec background flush ran", flushes(c.cmd("INFO")) >= 1, True)
        check("everysec not dirty after flush", "aof_dirty:0" in c.cmd("INFO"), True)
        # SAVE still forces a sync: a key written right before SAVE,
        # with no time for the background flush, must survive SIGKILL.
        check("everysec SET 2", c.cmd("SET", "eskey2", "esval2"), "OK")
        check("everysec SAVE", c.cmd("SAVE"), "OK")
        c.close()
        proc.kill()  # unclean shutdown: no graceful flush
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
    proc = start_server(["--aof-fsync", "everysec"], data_dir=es_dir, fresh=False)
    try:
        c = Client(PORT)
        check("everysec replay after SIGKILL", c.cmd("GET", "eskey"), "esval")
        check("everysec replay SAVE key after SIGKILL", c.cmd("GET", "eskey2"), "esval2")
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

    # `always` policy: the fsync runs on the flush thread and the reply
    # waits for it (deferred replies), so the event loop never blocks
    # on the disk — but replies still come back in command order, even
    # when a read follows a write in the same pipeline, and a write
    # acknowledged before a SIGKILL is on disk after the restart.
    def raw_cmd(*args):
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
        return b"".join(parts)

    al_dir = "/tmp/cinder-smoke-always"
    proc = start_server(data_dir=al_dir)
    try:
        c = Client(PORT)
        info = c.cmd("INFO")
        check("always INFO policy", "aof_fsync_policy:always" in info, True)
        check("always INFO flush thread", "aof_flush_thread:1" in info, True)
        check("always INFO deferred field", "aof_deferred_replies:" in info, True)
        check("always INFO pending field", "aof_pending_appends:" in info, True)
        # A pipeline mixing writes and reads: the GETs must not
        # overtake the SETs' +OKs — the deferred replies keep
        # per-client order even though the reads need no sync.
        c.s.sendall(raw_cmd("SET", "pipe1", "v1") + raw_cmd("GET", "pipe1") +
                    raw_cmd("SET", "pipe2", "v2") + raw_cmd("GET", "pipe2") +
                    raw_cmd("INCR", "pipectr"))
        replies = [c._read_reply() for _ in range(5)]
        check("always pipeline order", replies, ["OK", "v1", "OK", "v2", 1])
        check("always queue drained", "aof_deferred_replies:0" in c.cmd("INFO"), True)
        # Two clients writing at once: each gets its own +OK, and the
        # values land.
        c2 = Client(PORT)
        check("always client2 SET", c2.cmd("SET", "other", "x"), "OK")
        check("always client1 GET other", c.cmd("GET", "other"), "x")
        check("always client2 GET pipe1", c2.cmd("GET", "pipe1"), "v1")
        c2.close()
        # QUIT sent in the same packet as a SET: the SET's reply is
        # still parked when QUIT runs, so the server must wait for
        # the sync and flush both +OKs before closing.
        cq = Client(PORT)
        cq.s.sendall(raw_cmd("SET", "quitkey", "qv") + raw_cmd("QUIT"))
        check("always QUIT SET reply", cq._read_reply(), "OK")
        check("always QUIT reply", cq._read_reply(), "OK")
        cq.close()
        # The acknowledged writes must be on disk: SIGKILL now, and
        # the restart must see them.
        check("always SET durable", c.cmd("SET", "killkey", "killval"), "OK")
        c.close()
        proc.kill()  # unclean shutdown: no graceful flush
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
    proc = start_server(data_dir=al_dir, fresh=False)
    try:
        c = Client(PORT)
        check("always replay after SIGKILL", c.cmd("GET", "killkey"), "killval")
        check("always replay pipeline key", c.cmd("GET", "pipe1"), "v1")
        check("always replay QUIT key", c.cmd("GET", "quitkey"), "qv")
        check("always replay counter", c.cmd("GET", "pipectr"), "1")
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

    # RDB snapshots: SAVE writes dump.rdb with every key type, and a
    # restart with the AOF deleted restores from the snapshot alone.
    # A key whose TTL passed before the save is not in the snapshot,
    # and a surviving TTL is preserved as an absolute expiry.
    rdb_dir = "/tmp/cinder-smoke-rdb"
    proc = start_server(data_dir=rdb_dir)
    try:
        c = Client(PORT)
        check("rdb SET", c.cmd("SET", "snapstr", "hello"), "OK")
        check("rdb RPUSH", c.cmd("RPUSH", "snaplist", "x", "y", "z"), 3)
        check("rdb SADD", c.cmd("SADD", "snapset", "a", "b"), 2)
        check("rdb HSET", c.cmd("HSET", "snaphash", "f1", "v1", "f2", "v2"), 2)
        check("rdb ZADD", c.cmd("ZADD", "snapzset", "1.5", "a", "2", "b"), 2)
        check("rdb PFADD", c.cmd("PFADD", "snaphll", "a", "b", "c", "d"), 1)
        check("rdb SET ttl", c.cmd("SET", "snapttl", "v", "EX", "100"), "OK")
        check("rdb SET expiring", c.cmd("SET", "snapgone", "v", "PX", "50"), "OK")
        check("rdb SETRANGE pad", c.cmd("SETRANGE", "snappad", "3", "pad"), 6)
        time.sleep(0.1)
        check("rdb SAVE", c.cmd("SAVE"), "OK")
        with open(os.path.join(rdb_dir, "dump.rdb"), "rb") as f:
            snap = f.read()
        check("rdb file magic", snap[:6], b"CINDER")
        check("rdb file has keys", b"snapstr" in snap and b"snaphash" in snap and b"snapzset" in snap, True)
        check("rdb file skips expired key", b"snapgone" not in snap, True)
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
    # Delete the AOF: the restart must restore from the snapshot alone.
    os.remove(os.path.join(rdb_dir, "appendonly.aof"))
    proc = start_server(data_dir=rdb_dir, fresh=False)
    try:
        c = Client(PORT)
        check("rdb reload GET", c.cmd("GET", "snapstr"), "hello")
        check("rdb reload list", c.cmd("LRANGE", "snaplist", "0", "-1"), ["x", "y", "z"])
        check("rdb reload set", sorted(c.cmd("SMEMBERS", "snapset")), ["a", "b"])
        check("rdb reload hash", c.cmd("HGETALL", "snaphash"), ["f1", "v1", "f2", "v2"])
        check("rdb reload zset card", c.cmd("ZCARD", "snapzset"), 2)
        check("rdb reload zset score", c.cmd("ZSCORE", "snapzset", "a"), "1.5")
        check("rdb reload hll", c.cmd("PFCOUNT", "snaphll"), 4)
        check("rdb reload TTL survives", 90000 <= c.cmd("PTTL", "snapttl") <= 100000, True)
        check("rdb reload expired gone", c.cmd("GET", "snapgone"), None)
        check("rdb reload padded GET", c.cmd("GET", "snappad"), "\x00" * 3 + "pad")
        check("rdb reload padded STRLEN", c.cmd("STRLEN", "snappad"), 6)
        check("rdb reload DBSIZE", c.cmd("DBSIZE"), 8)
        check("rdb reload SET still works", c.cmd("SET", "afterload", "1"), "OK")
        check("rdb SAVE again", c.cmd("SAVE"), "OK")
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

    # Background saves: BGSAVE serializes on the event loop and a
    # worker thread writes dump.rdb, so the server stays responsive;
    # INFO reports the pool size and the save status, and a restart
    # with the AOF deleted restores from the background snapshot.
    bg_dir = "/tmp/cinder-smoke-bgsave"
    proc = start_server(["--threads", "2"], data_dir=bg_dir)
    try:
        c = Client(PORT)

        def bgsave_ms(info):
            for line in info.split("\r\n"):
                if line.startswith("last_bgsave_time_ms:"):
                    return int(line.split(":", 1)[1])
            return 0

        info = c.cmd("INFO")
        check("bgsave INFO threads", "threads:2" in info, True)
        check("bgsave INFO no save yet", "last_bgsave_status:none" in info, True)
        check("bgsave SET", c.cmd("SET", "bgstr", "hello"), "OK")
        check("bgsave RPUSH", c.cmd("RPUSH", "bglist", "x", "y"), 2)
        check("bgsave SADD", c.cmd("SADD", "bgset", "a", "b"), 2)
        check("bgsave HSET", c.cmd("HSET", "bghash", "f1", "v1"), 1)
        check("bgsave SET ttl", c.cmd("SET", "bgttl", "v", "EX", "100"), "OK")
        check("bgsave SET expiring", c.cmd("SET", "bggone", "v", "PX", "50"), "OK")
        time.sleep(0.1)
        check("bgsave BGSAVE", c.cmd("BGSAVE"), "Background saving started")
        # The save runs on a worker thread; poll INFO until it lands.
        deadline = time.time() + 5
        saved = False
        while time.time() < deadline:
            if "last_bgsave_status:ok" in c.cmd("INFO"):
                saved = True
                break
            time.sleep(0.05)
        check("bgsave completes", saved, True)
        check("bgsave INFO not in progress", "bgsave_in_progress:0" in c.cmd("INFO"), True)
        check("bgsave INFO jobs completed",
              "threadpool_jobs_completed:1" in c.cmd("INFO"), True)
        with open(os.path.join(bg_dir, "dump.rdb"), "rb") as f:
            snap = f.read()
        check("bgsave file magic", snap[:6], b"CINDER")
        check("bgsave file has keys", b"bgstr" in snap and b"bghash" in snap, True)
        check("bgsave file skips expired key", b"bggone" not in snap, True)
        # The server stayed responsive while the save ran, and a
        # second BGSAVE starts cleanly once the first has finished:
        # its completion timestamp must advance past the first save's.
        t_before = bgsave_ms(c.cmd("INFO"))
        check("bgsave again", c.cmd("BGSAVE"), "Background saving started")
        deadline = time.time() + 5
        saved = False
        while time.time() < deadline:
            if bgsave_ms(c.cmd("INFO")) > t_before:
                saved = True
                break
            time.sleep(0.05)
        check("bgsave second completes", saved, True)
        check("bgsave GET while saving", c.cmd("GET", "bgstr"), "hello")
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
    # Delete the AOF: the restart must restore from the background
    # snapshot alone.
    os.remove(os.path.join(bg_dir, "appendonly.aof"))
    proc = start_server(["--threads", "2"], data_dir=bg_dir, fresh=False)
    try:
        c = Client(PORT)
        check("bgsave reload GET", c.cmd("GET", "bgstr"), "hello")
        check("bgsave reload list", c.cmd("LRANGE", "bglist", "0", "-1"), ["x", "y"])
        check("bgsave reload set", sorted(c.cmd("SMEMBERS", "bgset")), ["a", "b"])
        check("bgsave reload hash", c.cmd("HGETALL", "bghash"), ["f1", "v1"])
        check("bgsave reload TTL survives", 90000 <= c.cmd("PTTL", "bgttl") <= 100000, True)
        check("bgsave reload expired gone", c.cmd("GET", "bggone"), None)
        check("bgsave reload DBSIZE", c.cmd("DBSIZE"), 5)
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

    # Benchmark tool: tools/bench.py runs against a live server and
    # exits 0 with a Markdown table (one row per operation). A small
    # run here just proves the tool works end to end; the real
    # numbers vs Redis live in the README Benchmarks section.
    bench_dir = "/tmp/cinder-smoke-bench"
    proc = start_server(data_dir=bench_dir)
    try:
        bench = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(
                os.path.abspath(__file__)), "..", "tools", "bench.py"),
             "--port", str(PORT), "--requests", "200", "--pipeline", "10"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            timeout=60,
        )
        check("bench exits 0", bench.returncode, 0)
        check("bench reports SET", "| `SET` |" in bench.stdout, True)
        check("bench reports GET", "| `GET` |" in bench.stdout, True)
        check("bench reports pipelined", "| `SET-pipelined` |" in bench.stdout, True)
        c = Client(PORT)
        check("bench INFO version", "0.60.0" in c.cmd("INFO"), True)
        c.cmd("SHUTDOWN")
        c.close()
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

    # Replication: REPLICAOF streams a full sync, then live writes.
    # A key whose TTL passed before the sync is not in the dump.
    repl_primary_dir = "/tmp/cinder-smoke-repl-primary"
    repl_replica_dir = "/tmp/cinder-smoke-repl-replica"
    primary = start_server(data_dir=repl_primary_dir)
    replica = None
    try:
        p = Client(PORT)
        check("repl SET", p.cmd("SET", "repstr", "hello"), "OK")
        check("repl RPUSH", p.cmd("RPUSH", "replist", "x", "y", "z"), 3)
        check("repl SADD", p.cmd("SADD", "repset", "a", "b"), 2)
        check("repl HSET", p.cmd("HSET", "rephash", "f1", "v1", "f2", "v2"), 2)
        check("repl ZADD", p.cmd("ZADD", "repzset", "1.5", "a", "2", "b"), 2)
        check("repl PFADD", p.cmd("PFADD", "rephll", "a", "b", "c"), 1)
        check("repl SET ttl", p.cmd("SET", "repttl", "v", "EX", "100"), "OK")
        check("repl SET gone soon", p.cmd("SET", "repgone", "v", "PX", "50"), "OK")
        # Bit keys, including values ending in a zero byte, which the
        # full sync rebuilds with a trailing SETBIT.
        check("repl SETBIT trailing NUL", p.cmd("SETBIT", "repbits", "7", "0"), 0)
        check("repl SETBIT mixed", p.cmd("SETBIT", "repbits2", "7", "1"), 0)
        check("repl SETBIT mixed tail", p.cmd("SETBIT", "repbits2", "15", "0"), 0)
        time.sleep(0.1)  # repgone expires before the sync: it must not appear
        replica = start_server(port=7778, data_dir=repl_replica_dir)
        r = Client(7778)
        check("repl INFO master role", "role:master" in p.cmd("INFO"), True)
        check("repl REPLICAOF", r.cmd("REPLICAOF", "127.0.0.1", "7777"), "OK")
        check("repl REPLICAOF bad port", r.cmd("REPLICAOF", "127.0.0.1", "99999"),
              ("error", "ERR invalid port"))
        # The full sync arrives asynchronously; poll for it.
        deadline = time.time() + 5
        synced = False
        while time.time() < deadline:
            try:
                if (r.cmd("GET", "repstr") == "hello" and
                        r.cmd("LRANGE", "replist", "0", "-1") == ["x", "y", "z"]):
                    synced = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("repl full sync arrived", synced, True)
        check("repl sync set", sorted(r.cmd("SMEMBERS", "repset")), ["a", "b"])
        check("repl sync hash", r.cmd("HGETALL", "rephash"), ["f1", "v1", "f2", "v2"])
        check("repl sync zset", r.cmd("ZCARD", "repzset"), 2)
        check("repl sync zset score", r.cmd("ZSCORE", "repzset", "a"), "1.5")
        check("repl sync hll", r.cmd("PFCOUNT", "rephll"), 3)
        check("repl sync TTL survives", 90000 <= r.cmd("PTTL", "repttl") <= 100000, True)
        check("repl sync skips expired key", r.cmd("GET", "repgone"), None)
        check("repl sync bit key trailing NUL", r.cmd("STRLEN", "repbits"), 1)
        check("repl sync bit key bit", r.cmd("GETBIT", "repbits", "7"), 0)
        check("repl sync bit key mixed len", r.cmd("STRLEN", "repbits2"), 2)
        check("repl sync bit key mixed bit", r.cmd("GETBIT", "repbits2", "7"), 1)
        check("repl sync bit key mixed count", r.cmd("BITCOUNT", "repbits2"), 1)
        check("repl INFO replica role", "role:replica" in r.cmd("INFO"), True)
        check("repl INFO replicas", "connected_replicas:1" in p.cmd("INFO"), True)
        # Live writes stream after the sync.
        check("repl live SET", p.cmd("SET", "newkey", "live"), "OK")
        check("repl live INCR", p.cmd("INCR", "repcounter"), 1)
        check("repl live DEL", p.cmd("DEL", "repstr"), 1)
        check("repl live LPUSH", p.cmd("LPUSH", "replist", "front"), 4)
        check("repl live ZADD zset2", p.cmd("ZADD", "repz2", "10", "a", "1", "c"), 2)
        check("repl live PFADD", p.cmd("PFADD", "rephll", "d"), 1)
        check("repl live ZUNIONSTORE", p.cmd("ZUNIONSTORE", "repunion", "2", "repzset", "repz2"), 3)
        check("repl live ZDIFFSTORE", p.cmd("ZDIFFSTORE", "repdiff", "2", "repzset", "repz2"), 1)
        check("repl live ZREMRANGEBYSCORE", p.cmd("ZREMRANGEBYSCORE", "repzset", "1", "2"), 2)
        deadline = time.time() + 5
        streamed = False
        while time.time() < deadline:
            try:
                if (r.cmd("GET", "newkey") == "live" and
                        r.cmd("GET", "repstr") is None and
                        r.cmd("GET", "repcounter") == "1" and
                        r.cmd("ZCARD", "repzset") == 0 and
                        r.cmd("ZCARD", "repunion") == 3):
                    streamed = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("repl streamed writes arrived", streamed, True)
        check("repl streamed hll", r.cmd("PFCOUNT", "rephll"), 4)
        check("repl streamed zunionstore", r.cmd("ZSCORE", "repunion", "a"), "11.5")
        check("repl streamed zdiffstore", r.cmd("ZSCORE", "repdiff", "b"), "2")
        check("repl streamed list", r.cmd("LRANGE", "replist", "0", "-1"),
              ["front", "x", "y", "z"])
        # Replication offsets: the primary counts forwarded stream bytes,
        # the replica tracks the same position, and PSYNC answers
        # FULLRESYNC (no backlog yet) then a full sync.
        def info_field(info, name):
            for line in info.split("\r\n"):
                if line.startswith(name + ":"):
                    return line[len(name) + 1:]
            return None

        def resp_len(args):
            n = len(f"*{len(args)}\r\n".encode())
            for a in args:
                b = a if isinstance(a, bytes) else str(a).encode()
                n += len(f"${len(b)}\r\n".encode()) + len(b) + 2
            return n

        pinfo = p.cmd("INFO")
        replid = info_field(pinfo, "master_replid")
        check("repl INFO master_replid 40 hex",
              len(replid or "") == 40 and
              all(ch in "0123456789abcdef" for ch in replid), True)
        check("repl INFO backlog active", "repl_backlog_active:1" in pinfo, True)
        check("repl INFO backlog size", "repl_backlog_size:1048576" in pinfo, True)
        check("repl INFO backlog histlen",
              int(info_field(pinfo, "repl_backlog_histlen")) > 0, True)
        off_before = int(info_field(pinfo, "master_repl_offset"))
        check("repl offset SET", p.cmd("SET", "offkey", "offval"), "OK")
        off_after = int(info_field(p.cmd("INFO"), "master_repl_offset"))
        check("repl offset grows by command bytes", off_after - off_before,
              resp_len(["SET", "offkey", "offval"]))
        # The replica's slave offset catches up to the primary's.
        deadline = time.time() + 5
        caught_up = False
        while time.time() < deadline:
            rinfo = r.cmd("INFO")
            if (info_field(rinfo, "slave_repl_offset") == str(off_after) and
                    info_field(rinfo, "master_replid") == replid):
                caught_up = True
                break
            time.sleep(0.1)
        check("repl slave offset catches up", caught_up, True)
        check("repl INFO replica line",
              "replica0:ip=127.0.0.1,port=" in p.cmd("INFO") and
              f",offset={off_after}" in p.cmd("INFO"), True)
        # PSYNC with an unknown run id ("?") answers +FULLRESYNC with
        # the primary's run id and offset, then streams a full sync.
        ps = Client(PORT)
        ps.s.sendall(b"PSYNC ? -1\r\n")
        line = ps._read_line()
        check("psync fullresync line",
              line.startswith(b"+FULLRESYNC ") and replid.encode() in line, True)
        check("psync fullresync offset",
              line.decode().rsplit(" ", 1)[1] == str(off_after), True)
        dump = []
        while True:
            item = ps._read_reply()
            if item == "FULLSYNC OK":
                break
            dump.append(item)
        check("psync dump starts with FLUSHALL", dump[0], ["FLUSHALL"])
        check("psync dump has live key", ["SET", "newkey", "live"] in dump, True)
        check("psync dump has offset key", ["SET", "offkey", "offval"] in dump, True)
        ps.close()
        deadline = time.time() + 5
        back_to_one = False
        while time.time() < deadline:
            if "connected_replicas:1" in p.cmd("INFO"):
                back_to_one = True
                break
            time.sleep(0.1)
        check("repl INFO replicas back to 1", back_to_one, True)
        # Partial resync: a replica that drops and reconnects while its
        # offset is still in the primary's backlog resumes with
        # +CONTINUE and replays only the missed writes. No FLUSHALL
        # runs, so a key written directly to the replica survives —
        # that is what distinguishes a partial resync from a full one.
        # The drop is a clean shutdown: the shutdown persists the
        # replica's stream position, and the restart resumes from it.
        # (REPLICAOF NO ONE is promotion now, not a transient detach:
        # it forgets the stream position, so reattaching after one
        # full-syncs.)
        check("partial local write", r.cmd("SET", "localonly", "keepme"), "OK")
        check("partial local read", r.cmd("GET", "localonly"), "keepme")
        check("partial shutdown", r.cmd("SHUTDOWN"), "OK")
        r.close()
        replica.wait(timeout=5)
        replica = None
        check("partial missed SET 1", p.cmd("SET", "missed1", "one"), "OK")
        check("partial missed SET 2", p.cmd("SET", "missed2", "two"), "OK")
        off_missed = int(info_field(p.cmd("INFO"), "master_repl_offset"))
        replica = start_server(port=7778, data_dir=repl_replica_dir, fresh=False)
        r = Client(7778)
        check("partial reattach", r.cmd("REPLICAOF", "127.0.0.1", "7777"), "OK")
        deadline = time.time() + 5
        resumed = False
        while time.time() < deadline:
            try:
                if (r.cmd("GET", "missed1") == "one" and
                        r.cmd("GET", "missed2") == "two" and
                        r.cmd("GET", "localonly") == "keepme"):
                    resumed = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("partial resync replays missed writes", resumed, True)
        check("partial resync keeps local-only key",
              r.cmd("GET", "localonly"), "keepme")
        deadline = time.time() + 5
        caught_up = False
        while time.time() < deadline:
            if info_field(r.cmd("INFO"), "slave_repl_offset") == str(off_missed):
                caught_up = True
                break
            time.sleep(0.1)
        check("partial resync slave offset catches up", caught_up, True)
        # Detach: writes stop flowing, and the replica keeps its data.
        check("repl REPLICAOF NO ONE", r.cmd("REPLICAOF", "NO", "ONE"), "OK")
        check("repl INFO back to master", "role:master" in r.cmd("INFO"), True)
        check("repl after detach SET", p.cmd("SET", "afterdetach", "no"), "OK")
        time.sleep(0.3)
        check("repl after detach not streamed", r.cmd("GET", "afterdetach"), None)
        check("repl after detach keeps data", r.cmd("GET", "newkey"), "live")
        check("repl INFO replicas gone", "connected_replicas:0" in p.cmd("INFO"), True)
        r.cmd("SHUTDOWN")
        r.close()
        replica.wait(timeout=5)
        replica = None
        p.cmd("SHUTDOWN")
        p.close()
        primary.wait(timeout=5)
        primary = None
    finally:
        if replica is not None and replica.poll() is None:
            replica.terminate()
            replica.wait(timeout=5)
        if primary is not None and primary.poll() is None:
            primary.terminate()
            primary.wait(timeout=5)
    # Restart the replica: its AOF must rebuild the replicated data,
    # and the key written after the detach must stay absent.
    replica = start_server(port=7778, data_dir=repl_replica_dir, fresh=False)
    try:
        r = Client(7778)
        check("repl AOF reload GET", r.cmd("GET", "newkey"), "live")
        check("repl AOF reload list", r.cmd("LRANGE", "replist", "0", "-1"),
              ["front", "x", "y", "z"])
        check("repl AOF reload no detached key", r.cmd("GET", "afterdetach"), None)
        check("repl AOF reload hash", r.cmd("HGETALL", "rephash"), ["f1", "v1", "f2", "v2"])
        r.cmd("SHUTDOWN")
        r.close()
        replica.wait(timeout=5)
    finally:
        if replica.poll() is None:
            replica.terminate()
            replica.wait(timeout=5)

    # Backlog fallback: with a tiny backlog, a replica whose offset has
    # aged out of it gets a full resync instead — the FLUSHALL wipes a
    # key written directly to the replica, and the dump rebuilds the
    # whole dataset.
    fb_primary_dir = "/tmp/cinder-smoke-fallback-primary"
    fb_replica_dir = "/tmp/cinder-smoke-fallback-replica"
    fb_primary = start_server(["--repl-backlog-size", "128"],
                              port=7779, data_dir=fb_primary_dir)
    fb_replica = None
    try:
        fp = Client(7779)
        check("fallback INFO backlog size",
              "repl_backlog_size:128" in fp.cmd("INFO"), True)
        check("fallback SET base", fp.cmd("SET", "fbkey", "base"), "OK")
        fb_replica = start_server(port=7780, data_dir=fb_replica_dir)
        fr = Client(7780)
        check("fallback REPLICAOF", fr.cmd("REPLICAOF", "127.0.0.1", "7779"), "OK")
        deadline = time.time() + 5
        fb_synced = False
        while time.time() < deadline:
            try:
                if fr.cmd("GET", "fbkey") == "base":
                    fb_synced = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("fallback full sync arrived", fb_synced, True)
        check("fallback local write", fr.cmd("SET", "fblocal", "keepme"), "OK")
        check("fallback detach", fr.cmd("REPLICAOF", "NO", "ONE"), "OK")
        # Ten 70-byte SETs push the replica's offset out of the
        # 128-byte backlog.
        for i in range(10):
            fp.cmd("SET", f"pad{i}", "x" * 40)
        check("fallback reattach", fr.cmd("REPLICAOF", "127.0.0.1", "7779"), "OK")
        deadline = time.time() + 5
        fb_full = False
        while time.time() < deadline:
            try:
                if (fr.cmd("GET", "pad9") == "x" * 40 and
                        fr.cmd("GET", "fblocal") is None):
                    fb_full = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("fallback full resync replays all writes", fb_full, True)
        check("fallback local key wiped by full sync",
              fr.cmd("GET", "fblocal"), None)
        check("fallback base key present", fr.cmd("GET", "fbkey"), "base")
        fr.cmd("SHUTDOWN")
        fr.close()
        fb_replica.wait(timeout=5)
        fb_replica = None
        fp.cmd("SHUTDOWN")
        fp.close()
        fb_primary.wait(timeout=5)
        fb_primary = None
    finally:
        if fb_replica is not None and fb_replica.poll() is None:
            fb_replica.terminate()
            fb_replica.wait(timeout=5)
        if fb_primary is not None and fb_primary.poll() is None:
            fb_primary.terminate()
            fb_primary.wait(timeout=5)

    # Failover: REPLICAOF NO ONE promotes a replica to primary. The
    # promoted node keeps the data it replicated, accepts writes, and
    # reports role:master; the old primary's later writes stop flowing.
    # Promotion forgets the stream position, so reattaching to the old
    # primary full-syncs and wipes writes made while promoted.
    fo_primary_dir = "/tmp/cinder-smoke-failover-primary"
    fo_replica_dir = "/tmp/cinder-smoke-failover-replica"
    fo_primary = start_server(port=7781, data_dir=fo_primary_dir)
    fo_replica = None
    try:
        fp = Client(7781)
        check("failover SET", fp.cmd("SET", "fokey", "before"), "OK")
        check("failover HSET", fp.cmd("HSET", "fohash", "f1", "v1"), 1)
        fo_replica = start_server(port=7782, data_dir=fo_replica_dir)
        fr = Client(7782)
        check("failover REPLICAOF", fr.cmd("REPLICAOF", "127.0.0.1", "7781"), "OK")
        deadline = time.time() + 5
        fo_synced = False
        while time.time() < deadline:
            try:
                if fr.cmd("GET", "fokey") == "before":
                    fo_synced = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("failover full sync arrived", fo_synced, True)
        check("failover replica role", "role:replica" in fr.cmd("INFO"), True)
        check("failover promote", fr.cmd("REPLICAOF", "NO", "ONE"), "OK")
        check("failover INFO role master", "role:master" in fr.cmd("INFO"), True)
        check("failover INFO no master host",
              "master_host:" not in fr.cmd("INFO"), True)
        fo_replid = info_field(fr.cmd("INFO"), "master_replid")
        check("failover INFO master_replid 40 hex",
              len(fo_replid or "") == 40 and
              all(ch in "0123456789abcdef" for ch in fo_replid), True)
        check("failover keeps replicated data", fr.cmd("GET", "fokey"), "before")
        check("failover keeps replicated hash",
              fr.cmd("HGETALL", "fohash"), ["f1", "v1"])
        check("failover promoted accepts writes",
              fr.cmd("SET", "fowrite", "after"), "OK")
        check("failover promoted write readable",
              fr.cmd("GET", "fowrite"), "after")
        check("failover old primary write", fp.cmd("SET", "foafter", "no"), "OK")
        time.sleep(0.3)
        check("failover old writes stop flowing", fr.cmd("GET", "foafter"), None)
        check("failover promote again is OK", fr.cmd("REPLICAOF", "NO", "ONE"), "OK")
        check("failover still master", "role:master" in fr.cmd("INFO"), True)
        check("failover still has promoted write",
              fr.cmd("GET", "fowrite"), "after")
        # Reattaching after promotion full-syncs: the write made while
        # promoted is wiped and the old primary's writes come back.
        check("failover reattach", fr.cmd("REPLICAOF", "127.0.0.1", "7781"), "OK")
        deadline = time.time() + 5
        fo_resynced = False
        while time.time() < deadline:
            try:
                if (fr.cmd("GET", "foafter") == "no" and
                        fr.cmd("GET", "fowrite") is None):
                    fo_resynced = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("failover reattach full-syncs", fo_resynced, True)
        fr.cmd("SHUTDOWN")
        fr.close()
        fo_replica.wait(timeout=5)
        fo_replica = None
        fp.cmd("SHUTDOWN")
        fp.close()
        fo_primary.wait(timeout=5)
        fo_primary = None
    finally:
        if fo_replica is not None and fo_replica.poll() is None:
            fo_replica.terminate()
            fo_replica.wait(timeout=5)
        if fo_primary is not None and fo_primary.poll() is None:
            fo_primary.terminate()
            fo_primary.wait(timeout=5)

    # Unknown policy is a startup error, not a silent default.
    bad = subprocess.run(
        [BIN, "--port", str(PORT), "--data-dir", "/tmp/cinder-smoke-bad",
         "--aof-fsync", "bogus"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=10,
    )
    check("bad policy exits nonzero", bad.returncode != 0, True)
    check("bad policy message", "unknown AOF fsync policy" in bad.stdout, True)

    # A thread count outside 1..32 is a startup error too.
    bad_threads = subprocess.run(
        [BIN, "--port", str(PORT), "--data-dir", "/tmp/cinder-smoke-bad",
         "--threads", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=10,
    )
    check("bad threads exits nonzero", bad_threads.returncode != 0, True)
    check("bad threads message", "invalid thread count" in bad_threads.stdout, True)

    if FAILURES:
        print(f"\n{len(FAILURES)} failure(s)")
        sys.exit(1)
    print("\nall smoke tests passed")


if __name__ == "__main__":
    main()
