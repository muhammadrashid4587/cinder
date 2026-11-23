"""Smoke tests for cinder: speaks raw RESP over a socket, no redis-py needed.

Starts the server on a scratch port with a scratch data dir, runs through
the core commands, checks AOF durability across a restart, and shuts it down.
"""
import os
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
        check("ZINCRBY wrongtype", c.cmd("ZINCRBY", "after", "1", "x"), wrongtype)
        check("COMMAND lists ZRANGE", "ZRANGE" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZREVRANGE", "ZREVRANGE" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZREM", "ZREM" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZRANK", "ZRANK" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZINCRBY", "ZINCRBY" in c.cmd("COMMAND"), True)
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
        check("watch keys cleanup", c.cmd("DEL", "wkey", "wmissing", "wset"), 3)
        # FLUSHALL invalidates even a watch on a key that never
        # existed; it wipes the dataset, so rebuild the four keys the
        # AOF restart section below expects (after, plist, pset, phash).
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
        check("COMMAND lists WATCH", "WATCH" in c.cmd("COMMAND"), True)
        check("COMMAND lists UNWATCH", "UNWATCH" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZADD", "ZADD" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZSCORE", "ZSCORE" in c.cmd("COMMAND"), True)
        check("COMMAND lists ZCARD", "ZCARD" in c.cmd("COMMAND"), True)
        # Clean up the transaction keys: the AOF restart below counts
        # exact keys (5: after, plist, pset, phash, pzset).
        # ZINCRBY and ZREM on pzset before the restart, so the AOF
        # replay section below can prove they were logged.
        check("AOF pre-restart ZINCRBY", c.cmd("ZINCRBY", "pzset", "1", "x"), "2.5")
        check("AOF pre-restart ZREM", c.cmd("ZREM", "pzset", "z"), 1)
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
        check("AOF replay hash", c.cmd("HGETALL", "phash"), ["a", "1", "b", "2"])
        check("AOF replay hash len", c.cmd("HLEN", "phash"), 2)
        check("AOF replay zset card", c.cmd("ZCARD", "pzset"), 2)
        check("AOF replay zset score", c.cmd("ZSCORE", "pzset", "y"), "2.25")
        check("AOF replay zset incr score", c.cmd("ZSCORE", "pzset", "x"), "2.5")
        check("AOF replay zset range", c.cmd("ZRANGE", "pzset", "0", "-1"), ["y", "x"])
        check("AOF replay zset removed gone", c.cmd("ZSCORE", "pzset", "z"), None)
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
        check("AOF replay DBSIZE", c.cmd("DBSIZE"), 5)
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
        check("rdb SET ttl", c.cmd("SET", "snapttl", "v", "EX", "100"), "OK")
        check("rdb SET expiring", c.cmd("SET", "snapgone", "v", "PX", "50"), "OK")
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
        check("rdb reload TTL survives", 90000 <= c.cmd("PTTL", "snapttl") <= 100000, True)
        check("rdb reload expired gone", c.cmd("GET", "snapgone"), None)
        check("rdb reload DBSIZE", c.cmd("DBSIZE"), 6)
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
        check("bench INFO version", "0.24.0" in c.cmd("INFO"), True)
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
        check("repl SET ttl", p.cmd("SET", "repttl", "v", "EX", "100"), "OK")
        check("repl SET gone soon", p.cmd("SET", "repgone", "v", "PX", "50"), "OK")
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
        check("repl sync TTL survives", 90000 <= r.cmd("PTTL", "repttl") <= 100000, True)
        check("repl sync skips expired key", r.cmd("GET", "repgone"), None)
        check("repl INFO replica role", "role:replica" in r.cmd("INFO"), True)
        check("repl INFO replicas", "connected_replicas:1" in p.cmd("INFO"), True)
        # Live writes stream after the sync.
        check("repl live SET", p.cmd("SET", "newkey", "live"), "OK")
        check("repl live INCR", p.cmd("INCR", "repcounter"), 1)
        check("repl live DEL", p.cmd("DEL", "repstr"), 1)
        check("repl live LPUSH", p.cmd("LPUSH", "replist", "front"), 4)
        deadline = time.time() + 5
        streamed = False
        while time.time() < deadline:
            try:
                if (r.cmd("GET", "newkey") == "live" and
                        r.cmd("GET", "repstr") is None and
                        r.cmd("GET", "repcounter") == "1"):
                    streamed = True
                    break
            except (EOFError, OSError):
                pass
            time.sleep(0.1)
        check("repl streamed writes arrived", streamed, True)
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
