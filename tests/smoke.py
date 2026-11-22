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
        check("KEYS *", sorted(c.cmd("KEYS", "*")), ["counter", "name", "temp"])
        check("KEYS c*", c.cmd("KEYS", "c*"), ["counter"])
        check("DBSIZE", c.cmd("DBSIZE"), 3)
        check("SET persist", c.cmd("SET", "durable", "yes"), "OK")
        info = c.cmd("INFO")
        check("INFO has keys", "keys:4" in info, True)
        check("INFO default fsync policy", "aof_fsync_policy:always" in info, True)
        check("unknown cmd", c.cmd("NOPE"), ("error", "ERR unknown command"))
        check("FLUSHALL", c.cmd("FLUSHALL"), "OK")
        check("DBSIZE after flush", c.cmd("DBSIZE"), 0)
        check("SET after flush", c.cmd("SET", "after", "flush"), "OK")
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
        # The replayed PEXPIRE re-arms blinklist's 50 ms TTL from replay
        # time, so it is briefly alive again; touching it expires it.
        check("AOF replay expired list gone", c.cmd("LLEN", "blinklist"), 0)
        check("AOF replay expired set gone", c.cmd("SCARD", "blinkset"), 0)
        check("AOF replay expired hash gone", c.cmd("HLEN", "blinkhash"), 0)
        check("AOF replay DBSIZE", c.cmd("DBSIZE"), 4)
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
        check("everysec SET", c.cmd("SET", "eskey", "esval"), "OK")
        check("everysec GET", c.cmd("GET", "eskey"), "esval")
        with open(os.path.join(es_dir, "appendonly.aof"), "rb") as f:
            aof = f.read()
        check("everysec AOF has record", b"eskey" in aof and b"esval" in aof, True)
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
        check("rdb SET ttl", c.cmd("SET", "snapttl", "v", "EX", "100"), "OK")
        check("rdb SET expiring", c.cmd("SET", "snapgone", "v", "PX", "50"), "OK")
        time.sleep(0.1)
        check("rdb SAVE", c.cmd("SAVE"), "OK")
        with open(os.path.join(rdb_dir, "dump.rdb"), "rb") as f:
            snap = f.read()
        check("rdb file magic", snap[:6], b"CINDER")
        check("rdb file has keys", b"snapstr" in snap and b"snaphash" in snap, True)
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
        check("rdb reload TTL survives", 90000 <= c.cmd("PTTL", "snapttl") <= 100000, True)
        check("rdb reload expired gone", c.cmd("GET", "snapgone"), None)
        check("rdb reload DBSIZE", c.cmd("DBSIZE"), 5)
        check("rdb reload SET still works", c.cmd("SET", "afterload", "1"), "OK")
        check("rdb SAVE again", c.cmd("SAVE"), "OK")
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
        check("repl SET ttl", p.cmd("SET", "repttl", "v", "EX", "100"), "OK")
        check("repl SET gone soon", p.cmd("SET", "repgone", "v", "PX", "50"), "OK")
        time.sleep(0.1)  # repgone expires before the sync: it must not appear
        replica = start_server(port=7778, data_dir=repl_replica_dir)
        r = Client(7778)
        check("repl INFO primary role", "role:primary" in p.cmd("INFO"), True)
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
        # PSYNC: the primary answers +FULLRESYNC with its run id and
        # offset, then streams a full sync; even a resume offset gets
        # a full sync, because there is no backlog yet.
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
        # Detach: writes stop flowing, and the replica keeps its data.
        check("repl REPLICAOF NO ONE", r.cmd("REPLICAOF", "NO", "ONE"), "OK")
        check("repl INFO back to primary", "role:primary" in r.cmd("INFO"), True)
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

    # Unknown policy is a startup error, not a silent default.
    bad = subprocess.run(
        [BIN, "--port", str(PORT), "--data-dir", "/tmp/cinder-smoke-bad",
         "--aof-fsync", "bogus"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=10,
    )
    check("bad policy exits nonzero", bad.returncode != 0, True)
    check("bad policy message", "unknown AOF fsync policy" in bad.stdout, True)

    if FAILURES:
        print(f"\n{len(FAILURES)} failure(s)")
        sys.exit(1)
    print("\nall smoke tests passed")


if __name__ == "__main__":
    main()
