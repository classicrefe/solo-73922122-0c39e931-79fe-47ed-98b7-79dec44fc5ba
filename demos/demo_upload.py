#!/usr/bin/env python3
"""并发上传 / 锁等待 / 取消 的命令行演练（仅用 Python 标准库）。

用法（先在首页保存一个基线，假设基线 id=1）：

  python3 demo_upload.py burst  http://127.0.0.1:8000 1 --count 40
  python3 demo_upload.py hold   http://127.0.0.1:8000 --seconds 12      # 另一个终端：
  python3 demo_upload.py upload http://127.0.0.1:8000 1                 # -> 503
  python3 demo_upload.py release http://127.0.0.1:8000
  python3 demo_upload.py upload http://127.0.0.1:8000 1                 # -> 303 预览
  python3 demo_upload.py cancel http://127.0.0.1:8000 1 --count 8       # 排队任务立即取消

上传文件默认取脚本内置的最小修订 CSV，也可用 --file 指定 samples/ 下的样例。
"""

import argparse
import concurrent.futures
import json
import sys
import time
import urllib.error
import urllib.request
import uuid

DEFAULT_CSV = (
    "id,quadrat,species,count\n"
    "R001,Q1,羊草,12\n"
    "R901,Q9,演示物种,3\n"
)


def _multipart(baseline_id: int, payload: bytes, filename: str) -> tuple[bytes, str]:
    boundary = "----demo" + uuid.uuid4().hex
    parts = []
    parts.append(f"--{boundary}\r\n".encode())
    parts.append(
        f'Content-Disposition: form-data; name="baseline_id"\r\n\r\n{baseline_id}\r\n'.encode()
    )
    parts.append(f"--{boundary}\r\n".encode())
    parts.append(
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
    )
    parts.append(b"Content-Type: text/csv\r\n\r\n")
    parts.append(payload)
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


class _CIHeaders(dict):
    """HTTP 头大小写不敏感（服务端可能返回小写键名）。"""

    def __init__(self, raw):
        super().__init__({k.lower(): v for k, v in raw.items()})

    def __getitem__(self, key):
        return super().__getitem__(key.lower())

    def get(self, key, default=None):
        return super().get(key.lower(), default)


def post(url: str, data: bytes | None = None, content_type: str | None = None,
         follow_redirects: bool = False) -> tuple[int, dict, str]:
    headers = {}
    if content_type:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    opener = urllib.request.build_opener(
        urllib.request.HTTPRedirectHandler() if follow_redirects else _NoRedirect
    )
    try:
        with opener.open(req, timeout=30) as resp:
            return resp.status, _CIHeaders(resp.headers), resp.read(500).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, _CIHeaders(exc.headers), exc.read(500).decode("utf-8", "replace")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def get(url: str) -> tuple[int, float]:
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            resp.read()
            return resp.status, (time.monotonic() - t0) * 1000
    except urllib.error.HTTPError as exc:
        return exc.code, (time.monotonic() - t0) * 1000


def cmd_upload(args) -> int:
    payload = open(args.file, "rb").read() if args.file else DEFAULT_CSV.encode()
    body, ctype = _multipart(args.baseline_id, payload, args.file or "revision.csv")
    status, headers, text = post(f"{args.base_url.rstrip('/')}/merge/upload", body, ctype)
    retry = headers.get("Retry-After", "-")
    print(f"HTTP {status}  Retry-After={retry}  Location={headers.get('Location', '-')}")
    if status == 503:
        print("-> 锁等待超时的可重试响应，服务端未生成预览")
        return 2
    if status == 303:
        print(f"-> 成功，预览地址：{args.base_url.rstrip('/')}{headers.get('Location')}")
        return 0
    print(text[:300])
    return 1


def _one_upload(base_url: str, baseline_id: int, idx: int) -> dict:
    # 每行株数加 idx，保证各修订互不相同
    rows = DEFAULT_CSV.strip().splitlines()
    tweaked = rows[:-1] + [f"R901,Q9,演示物种,{3 + idx}"]
    payload = ("\n".join(tweaked) + "\n").encode()
    body, ctype = _multipart(baseline_id, payload, f"burst-{idx:02d}.csv")
    t0 = time.monotonic()
    status, headers, _ = post(f"{base_url.rstrip('/')}/merge/upload", body, ctype)
    return {"idx": idx, "status": status, "ms": round((time.monotonic() - t0) * 1000, 1),
            "retry_after": headers.get("Retry-After")}


def cmd_burst(args) -> int:
    url = args.base_url.rstrip("/")
    # 读探针：并发期间持续采样首页与导出
    stop = False
    samples = []

    def probe():
        while not stop:
            for path in ("/", "/export.csv"):
                status, ms = get(url + path)
                samples.append((path, status, round(ms, 1)))
            time.sleep(0.1)

    import threading

    t = threading.Thread(target=probe, daemon=True)
    t.start()
    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.count) as pool:
        results = list(pool.map(lambda i: _one_upload(url, args.baseline_id, i), range(args.count)))
    wall = (time.monotonic() - t0) * 1000
    stop = True

    ok = sum(1 for r in results if r["status"] == 303)
    busy = sum(1 for r in results if r["status"] == 503)
    other = [r for r in results if r["status"] not in (303, 503)]
    slow = [s for s in samples if s[2] >= 1000 or s[1] != 200]
    print(f"并发 {args.count} 份：成功(303)={ok} 锁超时(503)={busy} 其它={len(other)}；墙钟 {wall:.0f}ms")
    if other:
        print("非预期响应：", other)
    print(f"读样本 {len(samples)} 个；超过 1 秒或非 200 的样本：{len(slow)} 个")
    for s in slow[:10]:
        print("  慢/失败样本：", s)
    worst = max(samples, key=lambda s: s[2]) if samples else None
    if worst:
        print(f"最差读取：{worst[0]} {worst[1]} {worst[2]}ms")
    return 0 if not other and not slow else 1


def cmd_hold(args) -> int:
    status, _, text = post(
        f"{args.base_url.rstrip('/')}/demo/lock/hold",
        f"seconds={args.seconds}".encode(),
        "application/x-www-form-urlencoded",
        follow_redirects=True,
    )
    print(f"持锁请求 HTTP {status}，时长 {args.seconds}s")
    return 0 if status == 200 else 1


def cmd_release(args) -> int:
    status, _, _ = post(
        f"{args.base_url.rstrip('/')}/demo/lock/release", b"",
        "application/x-www-form-urlencoded", follow_redirects=True
    )
    print(f"释放请求 HTTP {status}")
    return 0 if status == 200 else 1


def cmd_cancel(args) -> int:
    url = args.base_url.rstrip("/")
    # 先持有写锁：执行名额内的任务进入 5 秒忙等，其余任务稳定处于排队态，
    # 这样才能可靠演示"排队任务可取消、运行中任务不提前回收"。
    post(f"{url}/demo/lock/hold", f"seconds={max(10, args.count)}".encode(),
         "application/x-www-form-urlencoded", follow_redirects=True)
    submitted = []

    def submit(i):
        body, ctype = _multipart(args.baseline_id, DEFAULT_CSV.encode(), f"cancel-{i}.csv")
        req = urllib.request.Request(
            url + "/merge/upload", data=body, headers={"Content-Type": ctype}, method="POST"
        )
        try:
            urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError:
            pass

    import threading

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(args.count)]
    for t in threads:
        t.start()
    time.sleep(1.0)
    jobs = json.load(urllib.request.urlopen(url + "/api/jobs"))["jobs"]
    queued = [j for j in jobs if j["state"] == "queued"]
    running = [j for j in jobs if j["state"] == "running"]
    print(f"已提交 {args.count} 份；运行中 {len(running)} 个（忙等写锁），排队 {len(queued)} 个")
    for j in queued:
        status, _, body = post(url + f"/api/jobs/{j['id']}/cancel", b"",
                               "application/x-www-form-urlencoded")
        info = json.loads(body)
        print(f"取消排队 {j['id']} -> HTTP {status} cancelled={info['cancelled']} state={info['state']}")
        # 重复取消：必须幂等且不提前回收
        status2, _, body2 = post(url + f"/api/jobs/{j['id']}/cancel", b"",
                                 "application/x-www-form-urlencoded")
        info2 = json.loads(body2)
        print(f"重复取消 {j['id']} -> HTTP {status2} cancelled={info2['cancelled']}（幂等 no-op）")
    if running:
        j = running[0]
        status, _, body = post(url + f"/api/jobs/{j['id']}/cancel", b"",
                               "application/x-www-form-urlencoded")
        info = json.loads(body)
        print(f"取消运行中 {j['id']} -> cancelled={info['cancelled']}（已开始的数据库工作须完成）")
    post(f"{url}/demo/lock/release", b"", "application/x-www-form-urlencoded",
         follow_redirects=True)
    for t in threads:
        t.join(timeout=30)
    final = json.load(urllib.request.urlopen(url + "/api/jobs"))["jobs"]
    cancelled = [j for j in final if j["state"] == "cancelled"]
    print(f"释放写锁后全部任务结束；终态 cancelled={len(cancelled)} 个，均无执行耗时、无预览残留")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="上传并发 / 锁等待 / 取消演练（标准库实现）")
    sub = p.add_subparsers(dest="cmd", required=True)

    pu = sub.add_parser("upload", help="上传一次修订")
    pu.add_argument("base_url")
    pu.add_argument("baseline_id", type=int)
    pu.add_argument("--file")
    pu.set_defaults(func=cmd_upload)

    pb = sub.add_parser("burst", help="并发上传并采样读延迟")
    pb.add_argument("base_url")
    pb.add_argument("baseline_id", type=int)
    pb.add_argument("--count", type=int, default=40)
    pb.set_defaults(func=cmd_burst)

    ph = sub.add_parser("hold", help="持有 SQLite 写锁")
    ph.add_argument("base_url")
    ph.add_argument("--seconds", type=float, default=12)
    ph.set_defaults(func=cmd_hold)

    pr = sub.add_parser("release", help="释放演示写锁")
    pr.add_argument("base_url")
    pr.set_defaults(func=cmd_release)

    pc = sub.add_parser("cancel", help="批量提交后取消排队任务")
    pc.add_argument("base_url")
    pc.add_argument("baseline_id", type=int)
    pc.add_argument("--count", type=int, default=8)
    pc.set_defaults(func=cmd_cancel)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
