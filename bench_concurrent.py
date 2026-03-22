"""Concurrent benchmark — N clients each running M queries simultaneously."""
import socket
import struct
import hashlib
import threading
import time
import sys

class PgConn:
    def __init__(self, host, port, user, db, password=""):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock.settimeout(30)
        params = f"user\x00{user}\x00database\x00{db}\x00\x00".encode()
        msg = struct.pack("!I", 4+4+len(params)) + struct.pack("!HH", 3, 0) + params
        self.sock.sendall(msg)
        self._handshake(user, password)

    def _handshake(self, user, password):
        while True:
            tag, payload = self._read_msg()
            if tag == ord('R') and len(payload) >= 4:
                auth = struct.unpack("!I", payload[:4])[0]
                if auth == 5 and password:  # MD5
                    salt = payload[4:8]
                    inner = hashlib.md5((password + user).encode()).hexdigest()
                    outer = "md5" + hashlib.md5((inner + salt.hex()).encode()).hexdigest()
                    # Actually need bytes for salt
                    inner_h = hashlib.md5((password + user).encode()).hexdigest().encode()
                    outer_h = hashlib.md5(inner_h + salt).hexdigest()
                    pw = f"md5{outer_h}\x00".encode()
                    self.sock.sendall(b'p' + struct.pack("!I", 4+len(pw)) + pw)
                elif auth == 3 and password:  # Cleartext
                    pw = password.encode() + b'\x00'
                    self.sock.sendall(b'p' + struct.pack("!I", 4+len(pw)) + pw)
            elif tag == ord('Z'):
                return
            elif tag == ord('E'):
                raise Exception("Auth failed")

    def query(self, sql):
        q = sql.encode() + b'\x00'
        self.sock.sendall(b'Q' + struct.pack("!I", 4+len(q)) + q)
        while True:
            tag, _ = self._read_msg()
            if tag == ord('Z'):
                return

    def close(self):
        try:
            self.sock.sendall(b'X\x00\x00\x00\x04')
            self.sock.close()
        except:
            pass

    def _read_msg(self):
        hdr = self._recv_exact(5)
        tag = hdr[0]
        length = struct.unpack("!I", hdr[1:5])[0]
        payload = self._recv_exact(length - 4) if length > 4 else b''
        return tag, payload

    def _recv_exact(self, n):
        buf = b''
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise Exception("connection closed")
            buf += chunk
        return buf


def worker(host, port, user, db, password, sql, n_queries, results, idx):
    try:
        conn = PgConn(host, port, user, db, password)
        times = []
        for _ in range(n_queries):
            t0 = time.perf_counter()
            conn.query(sql)
            t1 = time.perf_counter()
            times.append((t1 - t0) * 1000)
        conn.close()
        results[idx] = times
    except Exception as e:
        results[idx] = str(e)


def run_bench(label, host, port, user, db, password, sql, n_clients, queries_per_client):
    print(f"\n  {label}")
    print(f"  {n_clients} concurrent clients × {queries_per_client} queries = {n_clients * queries_per_client} total")

    results = [None] * n_clients
    threads = []

    t_start = time.perf_counter()
    for i in range(n_clients):
        t = threading.Thread(target=worker, args=(host, port, user, db, password, sql, queries_per_client, results, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    t_total = (time.perf_counter() - t_start) * 1000

    # Collect all times
    all_times = []
    errors = 0
    for r in results:
        if isinstance(r, list):
            all_times.extend(r)
        else:
            errors += 1

    if not all_times:
        print(f"  ALL FAILED ({errors} errors)")
        return

    all_times.sort()
    total_queries = len(all_times)
    avg = sum(all_times) / len(all_times)
    p50 = all_times[len(all_times) // 2]
    p99 = all_times[int(len(all_times) * 0.99)]
    qps = total_queries / (t_total / 1000)

    print(f"  Wall time:  {t_total:.0f} ms")
    print(f"  Total QPS:  {qps:.0f}")
    print(f"  Avg latency: {avg:.2f} ms")
    print(f"  P50 latency: {p50:.2f} ms")
    print(f"  P99 latency: {p99:.2f} ms")
    print(f"  Min: {all_times[0]:.2f} ms  Max: {all_times[-1]:.2f} ms")
    if errors:
        print(f"  Errors: {errors}/{n_clients} clients failed")


print("=" * 60)
print("  CONCURRENT BENCHMARK")
print("=" * 60)

for n_clients in [10, 50, 100, 200]:
    queries_per = 100

    print(f"\n{'─' * 60}")
    print(f"  {n_clients} CONCURRENT CLIENTS × {queries_per} QUERIES")
    print(f"{'─' * 60}")

    run_bench(
        "PostgreSQL 17.8",
        "127.0.0.1", 5432, "postgres", "postgres", "postgres",
        "SELECT 1;", n_clients, queries_per
    )

    run_bench(
        "pgrx 0.1.0 (BEAM + Rust)",
        "127.0.0.1", 5433, "test", "pgrx", "",
        "SELECT 1;", n_clients, queries_per
    )

print(f"\n{'=' * 60}")
print("  DONE")
print("=" * 60)
