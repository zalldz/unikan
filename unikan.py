#!/usr/bin/env python3
"""unikan — standalone kanban board for the agent office. SQLite + HTTP + CLI, stdlib only, no LLM.

Server (`unikan serve`) enforces the rules; identity comes from the bearer token, never from the body.
Columns: backlog -> todo -> doing -> done | blocked.  See README.md.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import socket
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
CONF_DIR = Path(os.environ.get("UNIKAN_CONF", HOME / ".config/unikan"))
DATA_DIR = Path(os.environ.get("UNIKAN_DATA", HOME / ".local/share/unikan"))
DB_PATH = DATA_DIR / "unikan.db"
TOKENS = CONF_DIR / "tokens.json"
SECRETS = CONF_DIR / "secrets"

COLS = ("backlog", "todo", "doing", "blocked", "done")
ID_RE = re.compile(r"^[0-9a-f]{6}$")
VERSION = "0.4.0"
RELEASE_API = os.environ.get("UNIKAN_RELEASE_API", "https://api.github.com/repos/zalldz/unikan/releases/latest")
RAW_BASE = os.environ.get("UNIKAN_RAW_BASE", "https://raw.githubusercontent.com/zalldz/unikan")
MAX_UPDATE_BYTES = 512 * 1024
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
        if os.environ.get("UNIKAN_" + k.upper()):
            cfg[k] = int(os.environ["UNIKAN_" + k.upper()])
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

    def wait_list(self, col: str | None, assignee: str | None, wait: int) -> list[dict]:
        """Long-poll: return as soon as matching cards exist, else after `wait` seconds (empty)."""
        deadline = time.time() + min(max(wait, 0), 30)
        while True:
            rows = self.list(col, assignee)
            if rows or time.time() >= deadline:
                return rows
            time.sleep(0.5)

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

    def delete(self, actor: str, cid: str) -> dict:
        if actor != self.owner:
            raise KBError(403, "only the owner may delete cards")
        c = db()
        try:
            c.execute("BEGIN IMMEDIATE")
            if not c.execute("SELECT 1 FROM cards WHERE id=?", (cid,)).fetchone():
                c.execute("ROLLBACK"); raise KBError(404, "card not found")
            c.execute("DELETE FROM events WHERE card=?", (cid,))
            c.execute("DELETE FROM cards WHERE id=?", (cid,))
            c.execute("COMMIT")
        finally:
            c.close()
        return {"deleted": cid}

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
    out = d / f"unikan-{time.strftime('%Y%m%d-%H%M%S')}.db"
    src, dst = db(), sqlite3.connect(out)
    try:
        src.backup(dst)
    finally:
        dst.close(); src.close()
    for old in sorted(d.glob("unikan-*.db"))[:-keep]:
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
        env = dict(os.environ, UNIKAN_AS=agent, UNIKAN_CARD=cid)
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
        server_version = "unikan"

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
                    try:
                        wait = int(one("wait") or 0)
                    except ValueError:
                        raise KBError(400, "wait must be a number of seconds")
                    return self._send(200, board.wait_list(one("column"), one("assignee"), wait))
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
                    if len(parts) == 2 and method == "DELETE":
                        return self._send(200, board.delete(actor, cid))
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
        def do_DELETE(self): self._route("DELETE")

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
                print(f"unikan listening on {host}:{port}", file=sys.stderr, flush=True)
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
    print(f"unikan listening on {cfg['bind'][0]}:{port}", file=sys.stderr, flush=True)
    first.serve_forever()


# ---------------------------------------------------------------- client CLI
def _token(name: str) -> str:
    if os.environ.get("UNIKAN_TOKEN"):
        return os.environ["UNIKAN_TOKEN"]
    try:
        return (SECRETS / f"{name}.token").read_text().strip()
    except OSError:
        sys.exit(f"unikan: no token for '{name}' (set UNIKAN_TOKEN or create {SECRETS}/{name}.token)")


def call(args, method: str, path: str, body: dict | None = None, timeout: int = 20):
    name = args.who or os.environ.get("UNIKAN_AS")
    if not name:
        sys.exit("unikan: say who you are: --as NAME or UNIKAN_AS=NAME")
    url = (args.url or os.environ.get("UNIKAN_URL") or load_cfg()["url"]).rstrip("/") + path
    req = urllib.request.Request(url, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Bearer " + _token(name),
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        try:
            msg = json.load(e).get("error", "")
        except Exception:
            msg = ""
        sys.exit(f"unikan: {e.code} {msg}")
    except urllib.error.URLError as e:
        sys.exit(f"unikan: server unreachable ({e.reason})")


def line(c: dict) -> str:
    return f"{c['id']}  {c['column']:<8} {c['assignee']:<7} {c['title']}"


def create_token(name: str, cfg: dict) -> Path:
    if name not in roles(cfg)[2]:
        sys.exit("unikan: unknown name " + name + "; add it to config.json first")
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
        sys.exit(f"unikan: already set up ({cfile}). Use --force to rewrite the config and rotate all tokens.")
    split = lambda v: [x.strip() for x in v.split(",") if x.strip()]
    cfg = dict(DEFAULT_CFG)
    cfg.update(owner=args.owner, agents=split(args.agents), bosses=split(args.bosses),
               port=args.port, url=f"http://127.0.0.1:{args.port}")
    if not args.owner or " " in args.owner:
        sys.exit("unikan: --owner must be one word")
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    cfile.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n")
    for name in roles(cfg)[2]:
        create_token(name, cfg)
    print(f"unikan set up in {CONF_DIR}")
    print(f"owner: {args.owner}  bosses: {','.join(cfg['bosses']) or '-'}  workers: {','.join(cfg['agents'])}")
    print(f"tokens: {SECRETS}/<name>.token (never share them)")
    print("next: unikan serve   (or install the systemd service, see README)")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def demo_cmd(args) -> None:
    """Throwaway board in a temp dir, real server, nothing of yours is touched."""
    import shutil, tempfile
    tmp = Path(tempfile.mkdtemp(prefix="unikan-demo-"))
    port = free_port()
    env = dict(os.environ, UNIKAN_CONF=str(tmp / "conf"), UNIKAN_DATA=str(tmp / "data"))
    env.pop("UNIKAN_TOKEN", None); env.pop("UNIKAN_AS", None)
    base = [sys.executable, str(Path(__file__).resolve())]
    me = base + ["--url", f"http://127.0.0.1:{port}"]

    def run(who, *a):
        r = subprocess.run(me + ["--as", who, *a], env=env, capture_output=True, text=True)
        print(f"$ unikan --as {who} {' '.join(a)}")
        print("  " + ((r.stdout or r.stderr).strip() or "(ok)").replace("\n", "\n  "))
        return r

    try:
        subprocess.run(base + ["init", "--owner", "kamu", "--agents", "rina,dodi", "--port", str(port)],
                       env=env, check=True, capture_output=True)
        srv = subprocess.Popen(base + ["serve"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(100):
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1)
                    break
                except Exception:
                    time.sleep(0.1)
            r = run("kamu", "add", "Tulis ringkasan minggu ini", "--to", "rina", "--body", "maksimal 5 poin")
            cid = r.stdout.split()[0]
            run("rina", "next")
            run("dodi", "claim", cid)
            print("  ^ dodi ditolak: kartu itu milik rina. Aturan dijaga server, bukan kesepakatan.")
            run("rina", "claim", cid)
            run("rina", "done", cid, "--result", "ringkasan sudah ditulis")
            run("kamu", "ls", "--all")
        finally:
            srv.terminate(); srv.wait(10)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _fetch(url: str, limit: int) -> bytes:
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = r.read(limit + 1)
    except (urllib.error.URLError, OSError) as e:
        sys.exit(f"unikan: cannot reach {url} ({e})")
    if len(data) > limit:
        sys.exit("unikan: download too large, refused")
    return data


def update_cmd(args) -> None:
    """Install the latest published release (a git tag), never an unreleased commit.

    The file must carry VERSION equal to its tag, so a tag cannot silently ship other code.
    The previous copy is kept as unikan.py.prev.
    """
    me = Path(__file__).resolve()
    tag = json.loads(_fetch(RELEASE_API, 64 * 1024))["tag_name"]
    if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
        sys.exit(f"unikan: unexpected release tag {tag!r}, refused")
    text = _fetch(f"{RAW_BASE}/{tag}/unikan.py", MAX_UPDATE_BYTES).decode("utf-8", "replace")
    m = re.search(r'^VERSION = "([^"]+)"', text, re.M)
    if not m:
        sys.exit("unikan: release file has no VERSION, refused")
    new = m.group(1)
    if new != tag[1:]:
        sys.exit(f"unikan: tag {tag} contains VERSION {new}, refused")
    if new == VERSION:
        return print(f"unikan {VERSION}: already up to date (latest release {tag})")
    if args.check:
        return print(f"unikan {VERSION}: update available -> {new} (run: unikan update)")
    try:
        compile(text, "unikan.py", "exec")
    except SyntaxError as e:
        sys.exit(f"unikan: update does not compile ({e}); keeping {VERSION}")
    tmp = me.with_name(me.name + ".new")
    tmp.write_text(text, encoding="utf-8")
    os.chmod(tmp, 0o755)
    os.replace(me, me.with_name(me.name + ".prev"))
    os.replace(tmp, me)
    print(f"unikan {VERSION} -> {new} updated. Old copy: {me.name}.prev")
    print("If this machine runs `unikan serve`, restart that service to load the new version.")


def main() -> None:
    p = argparse.ArgumentParser(prog="unikan", description="standalone kanban board")
    p.add_argument("--as", dest="who", help="identity (or UNIKAN_AS)")
    p.add_argument("--url", help="server URL (or UNIKAN_URL)")
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("serve")
    dl = sp.add_parser("delete", help="owner only: remove a card and its history"); dl.add_argument("id")
    up = sp.add_parser("update", help="fetch the latest unikan.py from GitHub")
    up.add_argument("--check", action="store_true", help="only check, do not replace")
    sp.add_parser("version")
    sp.add_parser("demo", help="try it out: runs a throwaway board, touches nothing of yours")
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
    nx = sp.add_parser("next"); nx.add_argument("--wait", type=int, default=0, help="wait up to N seconds (max 30) for a card")
    for n in ("show", "claim", "touch", "promote"):
        x = sp.add_parser(n); x.add_argument("id")
    d = sp.add_parser("done"); d.add_argument("id"); d.add_argument("--result", required=True)
    b = sp.add_parser("block"); b.add_argument("id"); b.add_argument("--question", required=True)
    r = sp.add_parser("requeue"); r.add_argument("id"); r.add_argument("--reason", required=True)
    s = sp.add_parser("shelve"); s.add_argument("id")  # blocked->backlog / todo->backlog
    args = p.parse_args()

    if args.cmd == "version":
        return print(VERSION)
    if args.cmd == "update":
        return update_cmd(args)
    if args.cmd == "init":
        return init_cmd(args)
    if args.cmd == "demo":
        return demo_cmd(args)
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
        me = args.who or os.environ.get("UNIKAN_AS")
        w = max(0, min(args.wait, 30))
        for c in call(args, "GET", f"/cards?column=todo&assignee={me}&wait={w}", timeout=20 + w):
            print(line(c))
        return
    if args.cmd == "delete":
        print(json.dumps(call(args, "DELETE", f"/cards/{args.id}"))); return
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
