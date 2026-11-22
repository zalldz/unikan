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
import unicodedata
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
VERSION = "0.9.0"
RELEASE_API = os.environ.get("UNIKAN_RELEASE_API", "https://api.github.com/repos/zalldz/unikan/releases/latest")
RAW_BASE = os.environ.get("UNIKAN_RAW_BASE", "https://raw.githubusercontent.com/zalldz/unikan")
MAX_UPDATE_BYTES = 512 * 1024
MAX_BODY = 65536
MAX_REWORK = 2
STALL = "timeout:"  # question prefix the reaper writes; marks a card as stalled (not a worker's own question)

EVENT_TEXT = {
    "done": "Selesai {id} oleh {actor}: {result}",
    "blocked": "Terhambat {id} ({actor}): {question}",
    "todo": "Diulang {id} ({actor}): {reason}",
}

DEFAULT_CFG = {
    "owner": "owner",               # the human; may do anything, creates cards
    "viewers": ["viewer"],          # read-only names: may GET the board, cannot create or move cards
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
    "on_event": [],                 # e.g. ["python3", "/path/send.py", "{text}"]; empty = off
}


def mins_text(sec: int) -> str:
    return f"{sec} detik" if sec < 60 else f"{sec // 60} menit"


NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$")  # names become token file names: no "/" etc.


def roles(cfg: dict) -> tuple[list, list, list]:
    """(workers, creators, all names). The owner is always a creator; "system" is internal only."""
    workers = list(cfg["agents"])
    creators = list(cfg["bosses"]) + [cfg["owner"]]
    names = workers + creators + list(cfg.get("viewers") or [])
    bad = [n for n in names if not isinstance(n, str) or n == "system" or not NAME_RE.match(n)]
    if bad:  # "system" is the internal actor with owner-level move rights
        raise ValueError(f"bad name(s) in config: {bad}; use letters, digits, _ . - (and not 'system')")
    return workers, creators, names


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
    os.chmod(DATA_DIR, 0o700)  # cards and backups are private to this OS user
    c = db()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.executescript(SCHEMA)
    c.close()


def card_json(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "title": r["title"], "body": r["body"], "assignee": r["assignee"],
            "created_by": r["created_by"], "column": r["col"], "check": r["chk"],
            "result": r["result"], "question": r["question"], "rework": r["rework"],
            "ttl": r["ttl"], "wakes": r["wakes"], "created": r["created"], "updated": r["updated"],
            "stalled": r["col"] == "blocked" and r["question"].startswith(STALL)}


def _text(d: dict, key: str, limit: int, required: bool = False) -> str:
    v = d.get(key, "")
    if v is None:
        v = ""
    if not isinstance(v, str):
        raise KBError(400, f"{key} must be a string")
    # drop control chars (ANSI/C1 escapes would reach terminals via `ls`/`show`); keep newline and tab
    v = "".join(ch for ch in v if ch in "\n\t" or unicodedata.category(ch) != "Cc").strip()
    if required and not v:
        raise KBError(400, f"{key} is required")
    if len(v) > limit:
        raise KBError(400, f"{key} too long (max {limit})")
    return v


_CHANGE = threading.Condition()
_VER = [0]
_LIVE = threading.BoundedSemaphore(20)  # max open live streams


def _changed() -> None:
    with _CHANGE:
        _VER[0] += 1
        _CHANGE.notify_all()


class Board:
    def __init__(self, cfg: dict, wake=None, event=None):
        self.cfg, self.wake, self.event = cfg, wake, event
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
        if self.event:
            self.event(f"Kartu baru {cid} untuk {assignee}: {title}")
        if col == "todo" and self.wake:
            self.wake(assignee, cid)
        _changed()
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
                elif key in (("blocked", "doing"), ("blocked", "done")) and r["question"].startswith(STALL):
                    # a stalled card can be resumed or finished by its worker (work may have finished late)
                    if not (me or priv):
                        raise KBError(403, "only the assignee may resume a stalled card")
                    if to == "done" and not result:
                        raise KBError(400, "result is required")
                    upd["question"] = ""
                    if to == "done":
                        upd["result"] = result
                    note = "resumed" if to == "doing" else "done (after stall)"
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
                    if actor == "system" and frm == "doing" and r["updated"] + r["ttl"] >= now:
                        raise KBError(409, "touched since the reaper looked; not stale")  # reap() race
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
        if self.event and (to in ("done", "blocked") or (to == "todo" and frm == "blocked")):
            self.event(EVENT_TEXT[to].format(
                id=cid, actor=actor, to=to, result=result[:300], question=question[:300], reason=reason[:300]))
        if to == "todo" and self.wake:
            self.wake(assignee, cid)
        _changed()
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
        _changed()
        return {"deleted": cid}

    def activity(self, limit: int) -> list[dict]:
        c = db()
        try:
            rows = c.execute('''SELECT e.ts, e.actor, e.frm, e."to", e.note, e.card, c.title
                FROM events e LEFT JOIN cards c ON c.id = e.card ORDER BY e.n DESC LIMIT ?''', (limit,))
            return [dict(r) for r in rows]
        finally:
            c.close()

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
        _changed()
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
                          {"question": f"{STALL} tidak ada kabar selama {mins_text(r['ttl'])}"})
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
        self.board = Board(cfg, wake=self.enqueue, event=self.emit)
        self.queues = {a: queue.Queue() for a in cfg["notify"]}
        self.heavy = threading.Lock()
        for a in self.queues:
            threading.Thread(target=self._worker, args=(a,), daemon=True, name=f"wake-{a}").start()

    def emit(self, text: str) -> None:
        """Send a short event line through the optional on_event command (e.g. a Telegram bot). Never blocks."""
        argv_t = self.cfg.get("on_event")
        if not argv_t:
            return
        argv = [a.replace("{text}", text) for a in argv_t]
        threading.Thread(target=self._emit, args=(argv,), daemon=True).start()

    def _emit(self, argv: list) -> None:
        try:
            subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as e:
            print(f"on_event failed: {e}", file=sys.stderr, flush=True)

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


VIEW_HTML = r"""<!doctype html>
<html lang="id"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="light dark">
<title>unikan</title>
<style>
:root{--bg:#f3f5f8;--fg:#18202b;--card:#ffffff;--line:#dbe1e8;--mute:#5d6977;--accent:#2563eb;--red:#c2410c;--green:#15803d;--amber:#a16207;--chip:#e8edf3;--shadow:0 1px 2px rgba(15,23,42,.06)}
@media (prefers-color-scheme:dark){:root{--bg:#0f1318;--fg:#e7ecf2;--card:#181e26;--line:#2a3340;--mute:#94a0ae;--accent:#60a5fa;--red:#fb923c;--green:#4ade80;--amber:#fbbf24;--chip:#222a35;--shadow:none}}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;font:16px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--fg);padding-bottom:env(safe-area-inset-bottom)}
header{position:sticky;top:0;z-index:5;background:var(--bg);border-bottom:1px solid var(--line);padding:10px max(14px,env(safe-area-inset-left)) 10px}
.bar{display:flex;align-items:center;gap:10px;justify-content:space-between;max-width:1400px;margin:0 auto}
h1{font-size:19px;margin:0;line-height:1.2}
.sub{font-size:13px;color:var(--mute)}
.live{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:var(--mute);white-space:nowrap}
.dot{width:9px;height:9px;border-radius:50%;background:var(--mute);display:inline-block}
.live.on .dot{background:var(--green);box-shadow:0 0 0 3px rgba(21,128,61,.2)}
main{max-width:1400px;margin:0 auto;padding:12px max(14px,env(safe-area-inset-left)) 40px}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin:4px 0 12px}
.chip{background:var(--chip);border-radius:999px;padding:6px 12px;font-size:14px;white-space:nowrap}
.chip b{font-size:16px;margin-right:4px}
.chip.red{color:var(--red)}.chip.amber{color:var(--amber)}.chip.green{color:var(--green)}
.people{font-size:14px;color:var(--mute);margin:0 0 12px}
.people b{color:var(--fg)}
.help{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 12px;margin-bottom:12px;font-size:15px;box-shadow:var(--shadow)}
.help b{display:block}
.grid{display:grid;gap:14px;grid-template-columns:1fr}
@media (min-width:820px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media (min-width:1180px){.grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
.col{min-width:0}
.col h2{font-size:16px;margin:4px 2px 2px;display:flex;justify-content:space-between;align-items:baseline}
.col h2 span{font-size:14px;color:var(--mute);font-weight:500}
.col p{margin:0 2px 8px;color:var(--mute);font-size:13px}
.empty{color:var(--mute);font-size:14px;padding:10px 4px;border:1px dashed var(--line);border-radius:10px;text-align:center}
.card{background:var(--card);border:1px solid var(--line);border-left:5px solid var(--accent);border-radius:12px;margin-bottom:10px;box-shadow:var(--shadow)}
.card.blocked{border-left-color:var(--red)}.card.doing{border-left-color:var(--amber)}.card.done{border-left-color:var(--green)}.card.backlog{border-left-style:dashed}
.head{all:unset;box-sizing:border-box;cursor:pointer;display:flex;gap:10px;align-items:center;width:100%;padding:12px 12px;min-height:52px}
.head:focus-visible{outline:2px solid var(--accent);outline-offset:-2px;border-radius:12px}
.title{flex:1;font-size:16px;font-weight:600;overflow-wrap:anywhere}
.chev{color:var(--mute);transition:transform .15s;font-size:14px}
.card.folded .chev{transform:rotate(-90deg)}
.body{padding:0 12px 12px}
.card.folded .body{display:none}
.line{font-size:14px;color:var(--mute);margin:2px 0}
.note{margin:8px 0 0;padding:9px 11px;border-radius:9px;background:var(--chip);font-size:15px;white-space:pre-wrap;overflow-wrap:anywhere}
.actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}
button{font:inherit;min-height:44px;padding:8px 14px;border-radius:10px;border:1px solid var(--line);background:var(--card);color:var(--fg);cursor:pointer}
button.primary{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}
button.danger{color:var(--red)}
button:disabled{opacity:.5}
input,textarea,select{font:inherit;width:100%;min-height:44px;padding:8px 10px;border-radius:10px;border:1px solid var(--line);background:var(--card);color:var(--fg)}
textarea{min-height:84px}
label{display:block;font-size:14px;color:var(--mute);margin:12px 0 4px}
.check{display:flex;gap:8px;align-items:center;margin-top:10px;font-size:14px;color:var(--mute)}
.check input{width:auto;min-height:0}
.row{display:flex;gap:8px;justify-content:flex-end;margin-top:16px;flex-wrap:wrap}
details.feed{margin-top:22px;background:var(--card);border:1px solid var(--line);border-radius:12px;box-shadow:var(--shadow)}
details.feed summary{cursor:pointer;padding:12px;min-height:48px;font-weight:600;list-style:none}
details.feed ul{list-style:none;margin:0;padding:0 12px 12px}
details.feed li{padding:8px 0;border-top:1px solid var(--line);font-size:14px}
details.feed li small{color:var(--mute);display:block}
dialog{width:min(480px,94vw);border:1px solid var(--line);border-radius:14px;background:var(--card);color:var(--fg);padding:18px}
dialog::backdrop{background:rgba(0,0,0,.45)}
.toast{position:fixed;left:50%;transform:translateX(-50%);bottom:calc(16px + env(safe-area-inset-bottom));background:#1f2937;color:#fff;padding:10px 16px;border-radius:10px;font-size:15px;max-width:92vw;z-index:9;box-shadow:0 4px 16px rgba(0,0,0,.25)}
.tools{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}

.stat{display:flex;flex-wrap:wrap;gap:6px 14px;font-size:14px;color:var(--mute);margin:-4px 0 12px}
.stat b{color:var(--fg)}
.alert{background:rgba(194,65,12,.1);border:1px solid var(--red);color:var(--fg);border-radius:12px;padding:10px 12px;margin-bottom:12px;font-size:15px}
.alert b{color:var(--red)}
.pill{display:inline-block;font-size:12px;font-weight:600;padding:2px 8px;border-radius:999px;background:var(--chip);color:var(--mute);white-space:nowrap}
.pill.red{color:var(--red)}.pill.amber{color:var(--amber)}.pill.green{color:var(--green)}.pill.blue{color:var(--accent)}
.ava{width:26px;height:26px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#fff;flex:none}
.people button{all:unset;cursor:pointer;display:inline-flex;align-items:center;gap:6px;margin:3px 8px 3px 0;padding:4px 8px 4px 4px;border-radius:999px;background:var(--chip)}
.people button[aria-pressed=true]{outline:2px solid var(--accent)}
.bar{height:6px;border-radius:4px;background:var(--line);overflow:hidden;margin:8px 0 2px}
.bar i{display:block;height:100%;background:var(--green)}
.bar i.amber{background:var(--amber)}.bar i.red{background:var(--red)}
.card.stalled{border-left-color:var(--red);background:rgba(194,65,12,.06)}
.card.stalled .title::before{content:"⚠ ";color:var(--red)}
.filterbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:0 0 10px;font-size:14px;color:var(--mute)}
</style></head>
<body>
<header><div class="bar">
  <div><h1>unikan</h1><div class="sub" id="who">papan tim</div></div>
  <div class="tools">
    <span class="live" id="live"><span class="dot"></span><span id="livetxt">menyambung…</span></span>
    <button class="primary" id="new" hidden>+ Tugas</button>
    <button id="logout" hidden>Keluar</button>
  </div>
</div></header>
<main>
  <div id="login" hidden>
    <div class="help"><b>Masuk</b>Masukkan token yang diberikan kepadamu. Token hanya tersimpan di perangkat ini jika kamu mencentangnya.</div>
    <label for="tok">Token</label>
    <input id="tok" type="password" autocomplete="off" placeholder="tempel token di sini">
    <div class="check"><input id="remember" type="checkbox"><label for="remember" style="margin:0">Ingat di perangkat ini</label></div>
    <div class="row"><button class="primary" id="go">Masuk</button></div>
  </div>
  <div id="app" hidden>
    <div class="alert" id="alert" hidden></div>
    <div class="help" id="guide"></div>
    <div class="stat" id="stat"></div>
    <div class="chips" id="chips"></div>
    <div class="people" id="people"></div>
    <div class="grid" id="board"></div>
    <details class="feed" id="feed"><summary>Aktivitas terbaru</summary><ul id="acts"></ul></details>
  </div>
</main>

<dialog id="dlg"><form method="dialog" id="form">
  <h2 style="margin:0 0 6px;font-size:19px">Buat tugas baru</h2>
  <label for="f-title">Judul tugas</label>
  <input id="f-title" required maxlength="200" placeholder="contoh: Rangkum berita hari ini">
  <label for="f-to">Untuk siapa?</label>
  <select id="f-to"></select>
  <label for="f-body">Penjelasan (opsional)</label>
  <textarea id="f-body" maxlength="8000" placeholder="Apa yang harus dikerjakan dan batasannya"></textarea>
  <label for="f-check">Cara mengecek selesai (opsional)</label>
  <input id="f-check" maxlength="1000" placeholder="contoh: hasil berisi 5 poin">
  <div class="check"><input id="f-backlog" type="checkbox"><label for="f-backlog" style="margin:0">Simpan dulu sebagai ide</label></div>
  <div class="row"><button value="cancel" formnovalidate>Batal</button><button class="primary" value="ok">Simpan</button></div>
</form></dialog>
<div class="toast" id="toast" hidden></div>


<script>
const KEY="unikan-token", FOLD="unikan-folded";
const COLS=[
  ["blocked","⚠ Butuh perhatian","Macet atau menunggu jawaban owner. Paling penting."],
  ["doing","⚙ Sedang dikerjakan","Sedang ditangani sekarang. Bar menunjukkan sisa waktu sebelum dianggap macet."],
  ["todo","⏳ Menunggu dikerjakan","Sudah siap, menunggu giliran."],
  ["backlog","💡 Ide","Belum dijadwalkan. Owner bisa menjadikannya tugas."],
  ["done","✅ Selesai","Sudah beres. Dilipat supaya ringkas; ketuk judul untuk membuka."]];
const PILL={blocked:["Terhambat","red"],doing:["Dikerjakan","amber"],todo:["Menunggu","blue"],backlog:["Ide",""],done:["Selesai","green"]};
const $=id=>document.getElementById(id);
let ME=null, token=null, lastSig="", live=false, folded={}, filter=null, lastData=null, stats=null;
try{folded=JSON.parse(localStorage.getItem(FOLD)||"{}")}catch(e){folded={}}

function tok(){return sessionStorage.getItem(KEY)||localStorage.getItem(KEY)}
function el(tag,cls,text){const e=document.createElement(tag);if(cls)e.className=cls;if(text!=null)e.textContent=text;return e}
let tt;function toast(m){const t=$("toast");t.textContent=m;t.hidden=false;clearTimeout(tt);tt=setTimeout(()=>t.hidden=true,3500)}
function mins(sec){sec=Math.max(0,Math.round(sec));if(sec<60)return sec+" dtk";if(sec<3600)return Math.round(sec/60)+" menit";if(sec<86400)return (sec/3600).toFixed(1).replace(".0","")+" jam";return Math.round(sec/86400)+" hari"}
function ago(ts){const d=Math.max(0,Date.now()/1000-ts);return d<45?"baru saja":mins(d)+" lalu"}
function clock(ts){return new Date(ts*1000).toLocaleString("id-ID",{day:"numeric",month:"short",hour:"2-digit",minute:"2-digit"})}
function hue(name){let h=0;for(const ch of name)h=(h*31+ch.charCodeAt(0))%360;return h}
function ava(name){const a=el("span","ava",name.slice(0,2).toUpperCase());a.style.background="hsl("+hue(name)+" 55% 42%)";return a}

async function api(path,opts={}){
  const r=await fetch(path,{...opts,headers:{Authorization:"Bearer "+token,"Content-Type":"application/json"}});
  let data=null;try{data=await r.json()}catch(e){}
  if(!r.ok){const err=new Error((data&&data.error)||("gagal ("+r.status+")"));err.status=r.status;throw err}
  return data;
}
const isOwner=()=>ME&&ME.you===ME.owner;
const isFolded=c=>c.id in folded?folded[c.id]:c.column==="done";
function toggle(c){folded[c.id]=!isFolded(c);localStorage.setItem(FOLD,JSON.stringify(folded));render(lastData)}

function sentence(c){
  switch(c.column){
    case"done":return c.created_by+" memberi tugas ke "+c.assignee+". Selesai.";
    case"doing":return c.assignee+" sedang mengerjakan tugas dari "+c.created_by+".";
    case"blocked":return c.stalled?c.assignee+" tidak memberi kabar. Pekerjaannya bisa dilanjutkan atau diselesaikan olehnya.":c.assignee+" berhenti dan butuh jawaban:";
    case"todo":return"Dari "+c.created_by+" untuk "+c.assignee+". Belum dimulai.";
    default:return"Ide dari "+c.created_by+(c.assignee?" (untuk "+c.assignee+")":"")+".";
  }
}
function button(label,cls,fn){const b=el("button",cls,label);b.onclick=async()=>{b.disabled=true;try{await fn()}catch(e){toast(e.message)}b.disabled=false};return b}
async function move(id,to,body){await api("/cards/"+id+"/move",{method:"POST",body:JSON.stringify({to,...body})});toast("Berhasil");load()}

function cardEl(c){
  const now=Date.now()/1000;
  const folded_=isFolded(c);
  const box=el("div","card "+c.column+(c.stalled?" stalled":"")+(folded_?" folded":""));
  const head=el("button","head");
  head.setAttribute("aria-expanded",String(!folded_));
  const top=el("span","title",c.title);
  head.appendChild(top);
  const p=PILL[c.column];
  head.appendChild(el("span","pill "+(c.stalled?"red":p[1]),c.stalled?"Macet":p[0]));
  head.appendChild(el("span","chev","▼"));
  head.onclick=()=>toggle(c);
  box.appendChild(head);
  const body=el("div","body");
  body.appendChild(el("div","line",sentence(c)));
  if(c.column==="doing"){
    const used=(now-c.updated)/c.ttl, pct=Math.min(100,Math.round(used*100));
    const bar=el("div","bar"); const i=el("i",used>0.8?"red":used>0.5?"amber":"",""); i.style.width=pct+"%"; bar.appendChild(i);
    body.appendChild(bar);
    body.appendChild(el("div","line","Terakhir ada kabar "+ago(c.updated)+". Dianggap macet setelah "+mins(c.ttl)+" tanpa kabar."));
  }
  if(c.stalled)body.appendChild(el("div","note","Tidak ada kabar sejak "+ago(c.updated)+"."));
  else if(c.column==="blocked"&&c.question)body.appendChild(el("div","note",c.question));
  else if(c.column==="done"&&c.result)body.appendChild(el("div","note",c.result));
  if(c.check)body.appendChild(el("div","line","Cara cek: "+c.check));
  const t=el("div","line","");
  t.appendChild(document.createTextNode(c.id+" · dibuat "+clock(c.created)+" · "+(c.column==="done"?"selesai ":"diubah ")+ago(c.updated)+(c.rework?" · diulang "+c.rework+"x":"")));
  t.title=new Date(c.updated*1000).toLocaleString("id-ID");
  body.appendChild(t);
  if(isOwner()){
    const a=el("div","actions");
    if(c.column==="backlog")a.appendChild(button("Jadikan tugas","primary",()=>move(c.id,"todo",{})));
    if(c.column==="blocked")a.appendChild(button("Jawab & ulangi","primary",async()=>{
      const reason=prompt("Tulis jawabanmu. Akan dikirim ke "+c.assignee+" untuk dikerjakan ulang:");
      if(reason&&reason.trim())await move(c.id,"todo",{reason:reason.trim()});
    }));
    a.appendChild(button("Hapus","danger",async()=>{
      if(!confirm("Hapus tugas ini beserta riwayatnya? Tidak bisa dibatalkan."))return;
      await api("/cards/"+c.id,{method:"DELETE"});toast("Tugas dihapus");load();
    }));
    body.appendChild(a);
  }
  box.appendChild(body);
  return box;
}

function describe(e){
  const what=e.title?("«"+e.title+"»"):"tugas (sudah dihapus)";
  if(e.frm==null)return e.actor+" membuat "+what;
  if(e.to==="doing"&&e.frm==="blocked")return e.actor+" melanjutkan "+what;
  if(e.to==="doing")return e.actor+" mulai mengerjakan "+what;
  if(e.to==="done")return e.actor+" menyelesaikan "+what;
  if(e.to==="blocked"&&(e.note||"").startsWith("timeout:"))return "Sistem: "+what+" macet, tidak ada kabar";
  if(e.to==="blocked")return e.actor+" berhenti di "+what+": "+(e.note||"");
  if(e.to==="todo"&&e.frm==="blocked")return e.actor+" mengulang "+what+(e.note?" — "+e.note:"");
  if(e.to==="todo")return e.actor+" menjadikan "+what+" tugas";
  if(e.to==="backlog")return e.actor+" menyimpan "+what+" sebagai ide";
  return e.actor+" memindahkan "+what+" ke "+e.to;
}

function render(data){
  lastData=data; const [cards,acts]=data;
  const ids=new Set(cards.map(c=>c.id)), gone=Object.keys(folded).filter(k=>!ids.has(k));  // forget deleted cards
  if(gone.length){gone.forEach(k=>delete folded[k]);try{localStorage.setItem(FOLD,JSON.stringify(folded))}catch(e){}}
  const owner=isOwner();
  const stalled=cards.filter(c=>c.stalled).length, blocked=cards.filter(c=>c.column==="blocked").length;
  document.title=(stalled||blocked?"("+(stalled||blocked)+") ":"")+"unikan";
  $("who").textContent=(owner?"Owner":"Lihat saja")+" · "+ME.you;
  $("new").hidden=!owner;
  $("guide").innerHTML="";
  $("guide").appendChild(el("b",null,owner?"Kamu bisa mengatur semua tugas":"Mode lihat saja"));
  $("guide").appendChild(el("div",null,owner
    ?"Kolom paling atas adalah yang perlu perhatian. Ketuk judul kartu untuk membuka atau melipat. \"+ Tugas\" untuk tugas baru."
    :"Ketuk judul kartu untuk membuka detail. Ketuk nama orang di bawah untuk melihat tugasnya saja."));
  const alert=$("alert"); alert.textContent=""; alert.hidden=!(stalled||blocked);
  if(stalled||blocked){const b=el("b",null,(stalled?stalled+" tugas macet":"")+(stalled&&blocked-stalled?" · ":"")+(blocked-stalled?(blocked-stalled)+" menunggu jawaban":""));
    alert.appendChild(b); alert.appendChild(document.createTextNode(" — lihat kolom paling atas."));}
  const count=k=>cards.filter(c=>c.column===k).length;
  const chips=$("chips"); chips.textContent="";
  const today=new Date().toDateString();
  const doneToday=cards.filter(c=>c.column==="done"&&new Date(c.updated*1000).toDateString()===today).length;
  [["blocked","Butuh perhatian","red"],["doing","Dikerjakan","amber"],["todo","Menunggu",""],["backlog","Ide",""],["done","Selesai","green"]].forEach(([k,l,cl])=>{
    const c=el("span","chip "+cl); c.appendChild(el("b",null,String(count(k)))); c.appendChild(document.createTextNode(l)); chips.appendChild(c);
  });
  const st=$("stat"); st.textContent="";
  const add=(label,val)=>{const s=el("span",null,label+" ");s.appendChild(el("b",null,val));st.appendChild(s)};
  add("Selesai hari ini",String(doneToday));
  if(stats&&stats.avg_work_seconds!=null)add("Rata-rata waktu kerja",mins(stats.avg_work_seconds));
  if(stats&&stats.avg_claim_seconds!=null)add("Rata-rata sampai diambil",mins(stats.avg_claim_seconds));
  const people={}; cards.forEach(c=>{if(c.column==="done"||c.column==="backlog")return;
    const p=people[c.assignee]||(people[c.assignee]={todo:0,doing:0,blocked:0});p[c.column]++});
  const pe=$("people"); pe.textContent="";
  const names=Object.keys(people).sort();
  names.forEach(n=>{const p=people[n];const b=el("button");b.setAttribute("aria-pressed",String(filter===n));
    b.appendChild(ava(n)); b.appendChild(el("b",null,n));
    const parts=[]; if(p.doing)parts.push(p.doing+" jalan"); if(p.todo)parts.push(p.todo+" antre"); if(p.blocked)parts.push(p.blocked+" macet");
    b.appendChild(document.createTextNode(parts.join(" · ")));
    b.onclick=()=>{filter=filter===n?null:n;render(lastData)}; pe.appendChild(b)});
  if(!names.length)pe.textContent="Tidak ada yang sedang mengerjakan tugas.";
  const board=$("board"); board.textContent="";
  for(const [key,label,desc] of COLS){
    const list=cards.filter(c=>c.column===key&&(!filter||c.assignee===filter));
    if(key==="backlog"&&!list.length)continue;
    const col=el("section","col");
    const h=el("h2",null,label); h.appendChild(el("span",null,String(list.length)));
    col.appendChild(h); col.appendChild(el("p",null,desc));
    if(!list.length)col.appendChild(el("div","empty",filter?"Tidak ada untuk "+filter+".":"Kosong"));
    list.forEach(c=>col.appendChild(cardEl(c)));
    board.appendChild(col);
  }
  const ul=$("acts"); ul.textContent="";
  if(!acts.length)ul.appendChild(el("li",null,"Belum ada aktivitas."));
  acts.forEach(e=>{const li=el("li",null,describe(e));li.appendChild(el("small",null,clock(e.ts)+" · "+ago(e.ts)));ul.appendChild(li)});
}

async function load(){
  if(!token){showLogin();return}
  try{
    if(!ME)ME=await api("/meta");
    const [cards,acts,st]=await Promise.all([api("/cards"),api("/activity?limit=30"),api("/stats")]);
    stats=st;
    $("login").hidden=true;$("app").hidden=false;$("logout").hidden=false;
    const sig=JSON.stringify([cards,acts,st.avg_work_seconds,st.avg_claim_seconds]);
    if(sig!==lastSig){lastSig=sig;render([cards,acts])}
  }catch(e){
    if(e.status===401){logout();toast("Token tidak dikenal. Masukkan lagi.");return}
    toast("Tidak bisa memuat: "+e.message);
  }
}
function showLogin(){$("login").hidden=false;$("app").hidden=true;$("logout").hidden=true;$("new").hidden=true;$("tok").focus()}
function logout(){gen++;if(ctl)ctl.abort();sessionStorage.removeItem(KEY);localStorage.removeItem(KEY);token=null;ME=null;lastSig="";showLogin()}

function setLive(on,txt){live=on;$("live").classList.toggle("on",on);$("livetxt").textContent=txt}
let gen=0, ctl=null;
async function stream(){
  const my=++gen; if(ctl)ctl.abort(); ctl=new AbortController(); const sig=ctl.signal;  // one stream loop only
  while(token&&my===gen){
    try{
      const r=await fetch("/stream",{headers:{Authorization:"Bearer "+token},signal:sig});
      if(r.status===401){logout();toast("Token tidak dikenal. Masukkan lagi.");return}
      if(!r.ok)throw new Error("status "+r.status);
      setLive(true,"langsung"); load();
      const rd=r.body.getReader(), dec=new TextDecoder(); let buf="";
      for(;;){
        const {value,done}=await rd.read(); if(done)break;
        buf+=dec.decode(value,{stream:true});
        let i; while((i=buf.indexOf("\n\n"))>=0){const block=buf.slice(0,i);buf=buf.slice(i+2);if(block.includes("event: change"))load()}
      }
    }catch(e){}
    if(my!==gen)return;
    setLive(false,"menyambung ulang…");
    await new Promise(r=>setTimeout(r,3000));
  }
}
setInterval(()=>{if(token&&ME)load()},20000);   // refreshes "time ago" labels and the stall bar

$("new").onclick=()=>{
  if(!isOwner())return;
  const sel=$("f-to"); sel.textContent="";
  ME.workers.forEach(w=>{const o=el("option",null,w);o.value=w;sel.appendChild(o)});
  $("f-title").value="";$("f-body").value="";$("f-check").value="";$("f-backlog").checked=false;
  $("dlg").showModal();
};
$("dlg").addEventListener("close",async()=>{
  if($("dlg").returnValue!=="ok")return;
  const title=$("f-title").value.trim();
  if(!title){toast("Judul tugas wajib diisi");return}
  try{
    const body={title,assignee:$("f-to").value,body:$("f-body").value.trim(),check:$("f-check").value.trim()};
    if($("f-backlog").checked)body.column="backlog";
    await api("/cards",{method:"POST",body:JSON.stringify(body)});
    toast("Tugas dibuat");load();
  }catch(e){toast(e.message)}
});
$("go").onclick=()=>{
  const v=$("tok").value.trim(); if(!v)return;
  sessionStorage.removeItem(KEY);localStorage.removeItem(KEY);
  (($("remember").checked)?localStorage:sessionStorage).setItem(KEY,v);
  token=v;ME=null;$("tok").value="";lastSig="";load();stream();
};
$("tok").addEventListener("keydown",e=>{if(e.key==="Enter")$("go").click()});
$("logout").onclick=logout;
token=tok(); if(token){load();stream()} else showLogin();
</script>
</body></html>
"""


def make_handler(board: Board):
    class H(BaseHTTPRequestHandler):
        server_version = "unikan"
        timeout = 60  # idle/slow sockets (no headers, trickled body) free their thread; streams ping every 15s

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
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                raise KBError(400, "bad Content-Length")
            if n < 0:  # rfile.read(-1) would block until the client hangs up
                raise KBError(400, "bad Content-Length")
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
                if method == "GET" and parts == ["view"]:
                    data = VIEW_HTML.encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Security-Policy", "frame-ancestors 'none'")  # no clickjacking of owner buttons
                    self.send_header("X-Frame-Options", "DENY")
                    self.end_headers()
                    return self.wfile.write(data)
                if method == "GET" and parts == ["health"]:
                    return self._send(200, {"ok": True})
                auth = self.headers.get("Authorization", "")
                actor = who(auth[7:].strip(), board.cfg) if auth.startswith("Bearer ") else None
                if not actor:
                    return self._send(401, {"error": "unauthorized"})
                if parts == ["stream"] and method == "GET":
                    return self._stream()
                qs = parse_qs(u.query)
                one = lambda k: (qs.get(k) or [None])[0]
                if parts == ["activity"] and method == "GET":
                    try:
                        limit = int(one("limit") or 25)
                    except ValueError:
                        raise KBError(400, "limit must be a number")
                    return self._send(200, board.activity(max(1, min(limit, 100))))
                if parts == ["cards"] and method == "GET":
                    try:
                        wait = int(one("wait") or 0)
                    except ValueError:
                        raise KBError(400, "wait must be a number of seconds")
                    return self._send(200, board.wait_list(one("column"), one("assignee"), wait))
                if parts == ["cards"] and method == "POST":
                    return self._send(201, board.create(actor, self._body()))
                if parts == ["meta"] and method == "GET":
                    return self._send(200, {"you": actor, "owner": board.owner, "workers": board.workers})
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

        def _stream(self) -> None:
            """Server-Sent Events: one 'change' event per board write; pings keep the link alive."""
            if not _LIVE.acquire(blocking=False):
                return self._send(503, {"error": "too many live viewers, try again later"})
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()
                self.wfile.write(b"retry: 3000\n\n"); self.wfile.flush()
                seen, end = _VER[0], time.time() + 1800
                while time.time() < end:
                    with _CHANGE:
                        if _VER[0] == seen:
                            _CHANGE.wait(15)
                        moved = _VER[0] != seen
                        seen = _VER[0]
                    msg = b"event: change\ndata: %d\n\n" % seen if moved else b": ping\n\n"
                    self.wfile.write(msg); self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                _LIVE.release()

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
    try:
        roles(cfg)
    except ValueError as e:
        sys.exit(f"unikan: {e}")
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
