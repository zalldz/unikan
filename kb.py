#!/usr/bin/env python3
"""kb — standalone kanban board for the agent office. SQLite + HTTP + CLI, stdlib only, no LLM.

Server (`kb serve`) enforces the rules; identity comes from the bearer token, never from the body.
Columns: backlog -> todo -> doing -> done | blocked.  See README.md.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import queue
import re
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

HOME = Path.home()
CONF_DIR = Path(os.environ.get("KB_CONF", HOME / ".config/kb"))
DATA_DIR = Path(os.environ.get("KB_DATA", HOME / ".local/share/kb"))
DB_PATH = DATA_DIR / "kb.db"
TOKENS = CONF_DIR / "tokens.json"
SECRETS = CONF_DIR / "secrets"

COLS = ("backlog", "todo", "doing", "blocked", "done")
ID_RE = re.compile(r"^[0-9a-f]{6}$")
MAX_BODY = 65536
MAX_REWORK = 2

DEFAULT_CFG = {
    "owner": "owner",               # the human; may do anything, creates cards
    "bosses": [],                   # extra names allowed to create cards and requeue
    "agents": ["worker-a", "worker-b"],  # workers: only move their own cards
    "bind": ["127.0.0.1"],          # extra tailnet address goes here
    "port": 8787,
    "url": "http://127.0.0.1:8787",
    "ttl": 1800,                    # seconds a card may sit in doing/ without touch
    "min_ttl": 60,
    "max_wakes": 3,
    "retry_delay": 30,
    "wake_timeout": 1800,
    "reap_interval": 30,
    "notify": {},                   # agent -> argv list, "{id}" substituted
    "serialize": [],                # agents whose wake commands never overlap (RAM)
}


def roles(cfg: dict) -> tuple[list, list, list]:
    """(workers, creators, all names). The owner is always a creator; "system" is internal only."""
    workers = list(cfg["agents"])
    creators = list(cfg["bosses"]) + [cfg["owner"]]
    return workers, creators, workers + creators


def load_cfg() -> dict:
    cfg = dict(DEFAULT_CFG)
    try:
        cfg.update(json.loads((CONF_DIR / "config.json").read_text()))
    except FileNotFoundError:
        pass
    for k in ("ttl", "min_ttl", "max_wakes", "retry_delay", "wake_timeout", "reap_interval"):
        if os.environ.get("KB_" + k.upper()):
            cfg[k] = int(os.environ["KB_" + k.upper()])
    return cfg


class KBError(Exception):
    def __init__(self, code: int, msg: str):
        super().__init__(msg)
        self.code, self.msg = code, msg


# ---------------------------------------------------------------- storage
SCHEMA = """
CREATE TABLE IF NOT EXISTS cards(
  id TEXT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '',
  assignee TEXT NOT NULL, created_by TEXT NOT NULL, col TEXT NOT NULL,
  chk TEXT NOT NULL DEFAULT '', result TEXT NOT NULL DEFAULT '', question TEXT NOT NULL DEFAULT '',
  rework INTEGER NOT NULL DEFAULT 0, ttl INTEGER NOT NULL, wakes INTEGER NOT NULL DEFAULT 0,
  created INTEGER NOT NULL, updated INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS events(
  n INTEGER PRIMARY KEY AUTOINCREMENT, card TEXT NOT NULL, ts INTEGER NOT NULL,
  actor TEXT NOT NULL, frm TEXT, "to" TEXT NOT NULL, note TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS ev_card ON events(card);
CREATE INDEX IF NOT EXISTS cards_col ON cards(col);
"""


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=15000")
    return c


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    c = db()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.executescript(SCHEMA)
    c.close()


def card_json(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "title": r["title"], "body": r["body"], "assignee": r["assignee"],
            "created_by": r["created_by"], "column": r["col"], "check": r["chk"],
            "result": r["result"], "question": r["question"], "rework": r["rework"],
            "ttl": r["ttl"], "wakes": r["wakes"], "created": r["created"], "updated": r["updated"]}


def _text(d: dict, key: str, limit: int, required: bool = False) -> str:
    v = d.get(key, "")
    if v is None:
        v = ""
    if not isinstance(v, str):
        raise KBError(400, f"{key} must be a string")
    v = v.strip()
    if required and not v:
        raise KBError(400, f"{key} is required")
    if len(v) > limit:
        raise KBError(400, f"{key} too long (max {limit})")
    return v


class Board:
    def __init__(self, cfg: dict, wake=None):
        self.cfg, self.wake = cfg, wake
        self.workers, self.creators, _ = roles(cfg)
        self.owner = cfg["owner"]

    # -- reads
    def list(self, col: str | None, assignee: str | None) -> list[dict]:
        q, a = "SELECT * FROM cards WHERE 1=1", []
        if col:
            if col not in COLS:
                raise KBError(400, "bad column")
            q += " AND col=?"; a.append(col)
        if assignee:
            if assignee not in self.workers:
                raise KBError(400, "bad assignee")
            q += " AND assignee=?"; a.append(assignee)
        c = db()
        try:
            return [card_json(r) for r in c.execute(q + " ORDER BY created, id", a)]
        finally:
            c.close()

    def get(self, cid: str) -> dict:
        c = db()
        try:
            r = c.execute("SELECT * FROM cards WHERE id=?", (cid,)).fetchone()
            if not r:
                raise KBError(404, "card not found")
            out = card_json(r)
            out["history"] = [dict(h) for h in c.execute(
                'SELECT ts, actor, frm, "to", note FROM events WHERE card=? ORDER BY n', (cid,))]
            return out
        finally:
            c.close()

    # -- writes
    def create(self, actor: str, d: dict) -> dict:
        if actor not in self.creators:
            raise KBError(403, "only the owner or a boss may create cards")
        title = _text(d, "title", 200, True)
        body = _text(d, "body", 8000)
        chk = _text(d, "check", 1000)
        assignee = d.get("assignee")
        if assignee not in self.workers:
            raise KBError(400, "assignee must be one of " + "|".join(self.workers))
        col = d.get("column", "todo")
        if col not in ("backlog", "todo"):
            raise KBError(400, "column must be backlog or todo")
        ttl = d.get("ttl", self.cfg["ttl"])
        if not isinstance(ttl, int) or isinstance(ttl, bool) or not (self.cfg["min_ttl"] <= ttl <= 86400):
            raise KBError(400, f"ttl must be an integer {self.cfg['min_ttl']}..86400")
        now = int(time.time())
        c = db()
        try:
            for _ in range(5):
                cid = secrets.token_hex(3)
                try:
                    c.execute("BEGIN IMMEDIATE")
                    c.execute("INSERT INTO cards(id,title,body,assignee,created_by,col,chk,ttl,created,updated)"
                              " VALUES(?,?,?,?,?,?,?,?,?,?)",
                              (cid, title, body, assignee, actor, col, chk, ttl, now, now))
                    c.execute('INSERT INTO events(card,ts,actor,frm,"to",note) VALUES(?,?,?,?,?,?)',
                              (cid, now, actor, None, col, "created"))
                    c.execute("COMMIT")
                    break
                except sqlite3.IntegrityError:
                    c.execute("ROLLBACK")
            else:
                raise KBError(500, "could not allocate id")
        finally:
            c.close()
        if col == "todo" and self.wake:
            self.wake(assignee, cid)
        return self.get(cid)

    def move(self, actor: str, cid: str, to: str, d: dict | None = None) -> dict:
        d = d or {}
        if to not in COLS:
            raise KBError(400, "bad column")
        result = _text(d, "result", 20000)
        question = _text(d, "question", 2000)
        reason = _text(d, "reason", 1000)
        now = int(time.time())
        c = db()
        try:
            c.execute("BEGIN IMMEDIATE")
            try:
                r = c.execute("SELECT * FROM cards WHERE id=?", (cid,)).fetchone()
                if not r:
                    raise KBError(404, "card not found")
                frm, assignee = r["col"], r["assignee"]
                me = actor == assignee
                boss = actor in self.creators
                priv = actor in (self.owner, "system")
                upd: dict = {"col": to, "updated": now}
                note = reason
                key = (frm, to)
                if frm == "done":
                    raise KBError(409, "done is terminal")
                if frm == to:
                    raise KBError(409, f"card already in {to}")
                if key in (("backlog", "todo"), ("todo", "backlog"), ("blocked", "backlog")):
                    if not boss:
                        raise KBError(403, "only the owner or a boss may do this")
                    if to == "todo":
                        upd["wakes"] = 0
                elif key == ("blocked", "todo"):
                    if not boss:
                        raise KBError(403, "only the owner or a boss may requeue")
                    if not reason:
                        raise KBError(400, "reason is required for rework")
                    if r["rework"] >= MAX_REWORK and actor != self.owner:
                        raise KBError(409, f"rework limit ({MAX_REWORK}) reached; ask the owner")
                    upd.update(rework=r["rework"] + 1, wakes=0, question="")
                elif key == ("todo", "doing"):
                    if not (me or priv):
                        raise KBError(403, "only the assignee may claim")
                elif key == ("doing", "done"):
                    if actor == "system" or not (me or priv):
                        raise KBError(403, "only the assignee may finish")
                    if not result:
                        raise KBError(400, "result is required")
                    upd["result"] = result
                    note = "done"
                elif key in (("doing", "blocked"), ("todo", "blocked")):
                    if not (me or priv):
                        raise KBError(403, "only the assignee may block")
                    if not question:
                        raise KBError(400, "question is required")
                    upd["question"] = question
                    note = question
                else:
                    raise KBError(409, f"invalid transition {frm} -> {to}")
                sets = ",".join(f"{k}=?" for k in upd)
                c.execute(f"UPDATE cards SET {sets} WHERE id=?", [*upd.values(), cid])
                c.execute('INSERT INTO events(card,ts,actor,frm,"to",note) VALUES(?,?,?,?,?,?)',
                          (cid, now, actor, frm, to, note[:500]))
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        finally:
            c.close()
        if to == "todo" and self.wake:
            self.wake(assignee, cid)
        return self.get(cid)

    def touch(self, actor: str, cid: str) -> dict:
        c = db()
        try:
            c.execute("BEGIN IMMEDIATE")
            r = c.execute("SELECT * FROM cards WHERE id=?", (cid,)).fetchone()
            if not r:
                c.execute("ROLLBACK"); raise KBError(404, "card not found")
            if r["col"] != "doing" or not (actor == r["assignee"] or actor == self.owner):
                c.execute("ROLLBACK"); raise KBError(409, "only the assignee can touch a card in doing")
            c.execute("UPDATE cards SET updated=? WHERE id=?", (int(time.time()), cid))
            c.execute("COMMIT")
        finally:
            c.close()
        return self.get(cid)

    def stats(self) -> dict:
        c = db()
        try:
            counts = {r["col"]: r["n"] for r in c.execute("SELECT col, COUNT(*) n FROM cards GROUP BY col")}
            first_claim = {r["card"]: r["ts"] for r in c.execute(
                'SELECT card, MIN(ts) ts FROM events WHERE "to"=\'doing\' GROUP BY card')}
            created = {r["id"]: r["created"] for r in c.execute("SELECT id, created FROM cards")}
            done = {r["card"]: r["ts"] for r in c.execute(
                'SELECT card, MAX(ts) ts FROM events WHERE "to"=\'done\' GROUP BY card')}
            lat = [first_claim[k] - created[k] for k in first_claim if k in created]
            dur = [done[k] - first_claim[k] for k in done if k in first_claim]
            avg = lambda xs: round(sum(xs) / len(xs), 1) if xs else None
            return {"counts": {k: counts.get(k, 0) for k in COLS},
                    "avg_claim_seconds": avg(lat), "avg_work_seconds": avg(dur),
                    "blocked_by_system": c.execute(
                        "SELECT COUNT(*) FROM events WHERE actor='system'").fetchone()[0]}
        finally:
            c.close()

    def reap(self) -> int:
        """doing/ cards past their ttl -> blocked. Returns number moved."""
        now, n = int(time.time()), 0
        c = db()
        try:
            rows = c.execute("SELECT id, ttl FROM cards WHERE col='doing' AND updated + ttl < ?", (now,)).fetchall()
        finally:
            c.close()
        for r in rows:
            try:
                self.move("system", r["id"], "blocked",
                          {"question": f"timeout: no progress for {r['ttl'] // 60} min"})
                n += 1
            except KBError:
                pass
        return n


def backup(keep: int = 7) -> Path:
    d = DATA_DIR / "backups"
    d.mkdir(parents=True, exist_ok=True)
    out = d / f"kb-{time.strftime('%Y%m%d-%H%M%S')}.db"
    src, dst = db(), sqlite3.connect(out)
    try:
        src.backup(dst)
    finally:
        dst.close(); src.close()
    for old in sorted(d.glob("kb-*.db"))[:-keep]:
        old.unlink()
    return out


# ---------------------------------------------------------------- wake router
class Router:
    """Wakes an agent (external command) when a card enters its todo/. No LLM here."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.board = Board(cfg, wake=self.enqueue)
        self.queues = {a: queue.Queue() for a in cfg["notify"]}
        self.heavy = threading.Lock()
        for a in self.queues:
            threading.Thread(target=self._worker, args=(a,), daemon=True, name=f"wake-{a}").start()

    def enqueue(self, agent: str, cid: str) -> None:
        if agent in self.queues:
            self.queues[agent].put(cid)

    def start_sweep(self) -> None:
        for card in self.board.list("todo", None):
            self.enqueue(card["assignee"], card["id"])

    def _worker(self, agent: str) -> None:
        q = self.queues[agent]
        while True:
            cid = q.get()
            try:
                self._wake(agent, cid)
            except Exception:  # never let the worker die
                traceback.print_exc()

    def _wake(self, agent: str, cid: str) -> None:
        c = db()
        try:
            c.execute("BEGIN IMMEDIATE")
            r = c.execute("SELECT col, wakes FROM cards WHERE id=?", (cid,)).fetchone()
            if not r or r["col"] != "todo":
                c.execute("ROLLBACK"); return
            if r["wakes"] >= self.cfg["max_wakes"]:
                c.execute("ROLLBACK")
                self.board.move("system", cid, "blocked",
                                {"question": f"{agent} did not claim after {r['wakes']} wake(s)"})
                return
            c.execute("UPDATE cards SET wakes=wakes+1 WHERE id=?", (cid,))
            c.execute("COMMIT")
        finally:
            c.close()
        argv = [a.replace("{id}", cid) for a in self.cfg["notify"][agent]]
        env = dict(os.environ, KB_AS=agent, KB_CARD=cid)
        lock = self.heavy if agent in self.cfg["serialize"] else None
        if lock:
            lock.acquire()
        try:
            p = subprocess.run(argv, env=env, stdin=subprocess.DEVNULL, capture_output=True,
                               timeout=self.cfg["wake_timeout"])
            print(f"wake {agent} {cid} rc={p.returncode}", file=sys.stderr, flush=True)
        except subprocess.TimeoutExpired:
            print(f"wake {agent} {cid} TIMEOUT", file=sys.stderr, flush=True)
        except OSError as e:
            print(f"wake {agent} {cid} failed: {e}", file=sys.stderr, flush=True)
        finally:
            if lock:
                lock.release()
        try:
            still = self.board.get(cid)["column"] == "todo"
        except KBError:
            still = False
        if still:  # not claimed: retry later (max_wakes caps it)
            t = threading.Timer(self.cfg["retry_delay"], self.enqueue, (agent, cid))
            t.daemon = True
            t.start()


# ---------------------------------------------------------------- auth + http
def _hash(tok: str) -> str:
    return hashlib.sha256(tok.encode()).hexdigest()


def who(token: str, cfg: dict) -> str | None:
    try:
        table = json.loads(TOKENS.read_text())
    except (OSError, ValueError):
        return None
    h = _hash(token)
    found = None
    names = roles(cfg)[2]
    for name, hv in table.items():          # constant-time over all entries
        if hmac.compare_digest(hv, h) and name in names:
            found = name
    return found


def make_handler(board: Board):
    class H(BaseHTTPRequestHandler):
        server_version = "kb"

        def log_message(self, fmt, *a):
            sys.stderr.write("%s %s\n" % (self.address_string(), fmt % a))

        def _send(self, code: int, obj) -> None:
            data = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length") or 0)
            if n > MAX_BODY:
                raise KBError(413, "body too large")
            if n == 0:
                return {}
            try:
                d = json.loads(self.rfile.read(n))
            except ValueError:
                raise KBError(400, "invalid JSON")
            if not isinstance(d, dict):
                raise KBError(400, "JSON object expected")
            return d

        def _route(self, method: str) -> None:
            try:
                u = urlsplit(self.path)
                parts = [p for p in u.path.split("/") if p]
                if method == "GET" and parts == ["health"]:
                    return self._send(200, {"ok": True})
                auth = self.headers.get("Authorization", "")
                actor = who(auth[7:].strip(), board.cfg) if auth.startswith("Bearer ") else None
                if not actor:
                    return self._send(401, {"error": "unauthorized"})
                qs = parse_qs(u.query)
                one = lambda k: (qs.get(k) or [None])[0]
                if parts == ["cards"] and method == "GET":
                    return self._send(200, board.list(one("column"), one("assignee")))
                if parts == ["cards"] and method == "POST":
                    return self._send(201, board.create(actor, self._body()))
                if parts == ["stats"] and method == "GET":
                    return self._send(200, board.stats())
                if len(parts) >= 2 and parts[0] == "cards":
                    cid = parts[1]
                    if not ID_RE.match(cid):
                        raise KBError(404, "card not found")
                    if len(parts) == 2 and method == "GET":
                        return self._send(200, board.get(cid))
                    if len(parts) == 3 and method == "POST" and parts[2] == "move":
                        d = self._body()
                        return self._send(200, board.move(actor, cid, d.get("to"), d))
                    if len(parts) == 3 and method == "POST" and parts[2] == "touch":
                        return self._send(200, board.touch(actor, cid))
                raise KBError(404, "not found")
            except KBError as e:
                self._send(e.code, {"error": e.msg})
            except Exception:
                traceback.print_exc()
                self._send(500, {"error": "internal error"})

        def do_GET(self): self._route("GET")
        def do_POST(self): self._route("POST")

    return H


def serve() -> None:
    cfg = load_cfg()
    init_db()
    router = Router(cfg)
    board = router.board
    handler = make_handler(board)
    port = cfg["port"]
    first = ThreadingHTTPServer((cfg["bind"][0], port), handler)   # must succeed
    first.daemon_threads = True

    def extra(host: str) -> None:
        while True:
            try:
                s = ThreadingHTTPServer((host, port), handler)
                s.daemon_threads = True
                print(f"kb listening on {host}:{port}", file=sys.stderr, flush=True)
                s.serve_forever()
            except OSError as e:
                print(f"bind {host}:{port} failed ({e}); retry in 10s", file=sys.stderr, flush=True)
                time.sleep(10)

    for h in cfg["bind"][1:]:
        threading.Thread(target=extra, args=(h,), daemon=True).start()

    def reaper() -> None:
        while True:
            time.sleep(cfg["reap_interval"])
            try:
                n = board.reap()
                if n:
                    print(f"reaped {n} stale card(s)", file=sys.stderr, flush=True)
            except Exception:
                traceback.print_exc()

    threading.Thread(target=reaper, daemon=True).start()
    router.start_sweep()
    print(f"kb listening on {cfg['bind'][0]}:{port}", file=sys.stderr, flush=True)
    first.serve_forever()


# ---------------------------------------------------------------- client CLI
def _token(name: str) -> str:
    if os.environ.get("KB_TOKEN"):
        return os.environ["KB_TOKEN"]
    try:
        return (SECRETS / f"{name}.token").read_text().strip()
    except OSError:
        sys.exit(f"kb: no token for '{name}' (set KB_TOKEN or create {SECRETS}/{name}.token)")


def call(args, method: str, path: str, body: dict | None = None):
    name = args.who or os.environ.get("KB_AS")
    if not name:
        sys.exit("kb: say who you are: --as NAME or KB_AS=NAME")
    url = (args.url or os.environ.get("KB_URL") or load_cfg()["url"]).rstrip("/") + path
    req = urllib.request.Request(url, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Bearer " + _token(name),
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        try:
            msg = json.load(e).get("error", "")
        except Exception:
            msg = ""
        sys.exit(f"kb: {e.code} {msg}")
    except urllib.error.URLError as e:
        sys.exit(f"kb: server unreachable ({e.reason})")


def line(c: dict) -> str:
    return f"{c['id']}  {c['column']:<8} {c['assignee']:<7} {c['title']}"


def create_token(name: str, cfg: dict) -> Path:
    if name not in roles(cfg)[2]:
        sys.exit("kb: unknown name " + name + "; add it to config.json first")
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    try:
        table = json.loads(TOKENS.read_text())
    except (OSError, ValueError):
        table = {}
    tok = secrets.token_urlsafe(32)
    table[name] = _hash(tok)
    tmp = TOKENS.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(table, f)
    tmp.replace(TOKENS)
    SECRETS.mkdir(mode=0o700, exist_ok=True)
    sp = SECRETS / f"{name}.token"
    fd = os.open(sp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(tok + "\n")
    return sp


def token_admin(args) -> None:
    if args.tcmd == "list":
        try:
            print("\n".join(sorted(json.loads(TOKENS.read_text()))))
        except OSError:
            pass
        return
    sp = create_token(args.name, load_cfg())
    print(f"token for {args.name} written to {sp} (not shown)")


def init_cmd(args) -> None:
    cfile = CONF_DIR / "config.json"
    if cfile.exists() and not args.force:
        sys.exit(f"kb: already set up ({cfile}). Use --force to rewrite the config and rotate all tokens.")
    split = lambda v: [x.strip() for x in v.split(",") if x.strip()]
    cfg = dict(DEFAULT_CFG)
    cfg.update(owner=args.owner, agents=split(args.agents), bosses=split(args.bosses),
               port=args.port, url=f"http://127.0.0.1:{args.port}")
    if not args.owner or " " in args.owner:
        sys.exit("kb: --owner must be one word")
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    cfile.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n")
    for name in roles(cfg)[2]:
        create_token(name, cfg)
    print(f"kb set up in {CONF_DIR}")
    print(f"owner: {args.owner}  bosses: {','.join(cfg['bosses']) or '-'}  workers: {','.join(cfg['agents'])}")
    print(f"tokens: {SECRETS}/<name>.token (never share them)")
    print("next: kb serve   (or install the systemd service, see README)")


def main() -> None:
    p = argparse.ArgumentParser(prog="kb", description="standalone kanban board")
    p.add_argument("--as", dest="who", help="identity (or KB_AS)")
    p.add_argument("--url", help="server URL (or KB_URL)")
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("serve")
    ini = sp.add_parser("init", help="first-time setup: config + tokens")
    ini.add_argument("--owner", default="owner", help="your name (the human who creates cards)")
    ini.add_argument("--agents", default="worker-a,worker-b", help="comma-separated worker names")
    ini.add_argument("--bosses", default="", help="optional extra card creators, comma-separated")
    ini.add_argument("--port", type=int, default=8787)
    ini.add_argument("--force", action="store_true")
    sp.add_parser("backup")
    sp.add_parser("stats")
    t = sp.add_parser("token"); ts = t.add_subparsers(dest="tcmd", required=True)
    tn = ts.add_parser("new"); tn.add_argument("name")
    ts.add_parser("list")
    a = sp.add_parser("add"); a.add_argument("title"); a.add_argument("--to", required=True)
    a.add_argument("--body", default=""); a.add_argument("--check", default="")
    a.add_argument("--backlog", action="store_true"); a.add_argument("--ttl", type=int)
    l = sp.add_parser("ls"); l.add_argument("--col"); l.add_argument("--to"); l.add_argument("--all", action="store_true")
    sp.add_parser("next")
    for n in ("show", "claim", "touch", "promote"):
        x = sp.add_parser(n); x.add_argument("id")
    d = sp.add_parser("done"); d.add_argument("id"); d.add_argument("--result", required=True)
    b = sp.add_parser("block"); b.add_argument("id"); b.add_argument("--question", required=True)
    r = sp.add_parser("requeue"); r.add_argument("id"); r.add_argument("--reason", required=True)
    s = sp.add_parser("shelve"); s.add_argument("id")  # blocked->backlog / todo->backlog
    args = p.parse_args()

    if args.cmd == "init":
        return init_cmd(args)
    if args.cmd == "serve":
        return serve()
    if args.cmd == "backup":
        init_db(); print(backup()); return
    if args.cmd == "token":
        return token_admin(args)
    if args.cmd == "stats":
        print(json.dumps(call(args, "GET", "/stats"), indent=1)); return
    if args.cmd == "add":
        body = {"title": args.title, "body": args.body, "assignee": args.to, "check": args.check,
                "column": "backlog" if args.backlog else "todo"}
        if args.ttl:
            body["ttl"] = args.ttl
        c = call(args, "POST", "/cards", body); print(line(c)); return
    if args.cmd == "ls":
        qs = []
        if args.col: qs.append("column=" + args.col)
        if args.to: qs.append("assignee=" + args.to)
        cards = call(args, "GET", "/cards" + ("?" + "&".join(qs) if qs else ""))
        for c in cards:
            if args.all or args.col or c["column"] != "done":
                print(line(c))
        return
    if args.cmd == "next":
        me = args.who or os.environ.get("KB_AS")
        for c in call(args, "GET", f"/cards?column=todo&assignee={me}"):
            print(line(c))
        return
    if args.cmd == "show":
        print(json.dumps(call(args, "GET", f"/cards/{args.id}"), ensure_ascii=False, indent=1)); return
    moves = {"claim": ("doing", {}), "promote": ("todo", {}), "shelve": ("backlog", {})}
    if args.cmd in moves:
        to, body = moves[args.cmd]
    elif args.cmd == "done":
        to, body = "done", {"result": args.result}
    elif args.cmd == "block":
        to, body = "blocked", {"question": args.question}
    elif args.cmd == "requeue":
        to, body = "todo", {"reason": args.reason}
    elif args.cmd == "touch":
        print(line(call(args, "POST", f"/cards/{args.id}/touch", {}))); return
    body["to"] = to
    print(line(call(args, "POST", f"/cards/{args.id}/move", body)))


if __name__ == "__main__":
    main()
