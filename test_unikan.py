#!/usr/bin/env python3
"""Tests for unikan: spins a real server on a temp dir + random port. Run: python3 test_unikan.py"""
import json, os, socket, subprocess, sys, tempfile, threading, time, unittest, urllib.request, urllib.error
from pathlib import Path

UNIKAN = str(Path(__file__).with_name("unikan.py"))


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


class Env:
    def __init__(self, notify=None, serialize=(), **extra):
        self.tmp = tempfile.TemporaryDirectory()
        t = Path(self.tmp.name)
        self.conf, self.data = t / "conf", t / "data"
        self.conf.mkdir()
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        cfg = {"port": self.port, "url": self.url, "notify": notify or {}, "serialize": list(serialize),
               "owner": "owner", "bosses": ["lead"], "agents": ["alice", "bob", "carol"],
               "retry_delay": 1, "reap_interval": 1, "min_ttl": 1, "max_wakes": 3, "wake_timeout": 20}
        cfg.update(extra)
        (self.conf / "config.json").write_text(json.dumps(cfg))
        self.env = dict(os.environ, UNIKAN_CONF=str(self.conf), UNIKAN_DATA=str(self.data))
        self.env.pop("UNIKAN_TOKEN", None); self.env.pop("UNIKAN_AS", None)
        for n in ("lead", "alice", "bob", "carol", "owner"):  # token names must match config
            subprocess.run([sys.executable, UNIKAN, "token", "new", n], env=self.env, check=True, capture_output=True)
        self.proc = None
        self.start()

    def start(self):
        self.proc = subprocess.Popen([sys.executable, UNIKAN, "serve"], env=self.env,
                                     stdout=subprocess.DEVNULL, stderr=open(self.conf / "server.log", "ab"))
        for _ in range(100):
            try:
                urllib.request.urlopen(self.url + "/health", timeout=1); return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def stop(self):
        self.proc.terminate(); self.proc.wait(10)

    def close(self):
        if self.proc and self.proc.poll() is None:
            self.stop()
        self.tmp.cleanup()

    def tok(self, name):
        return (self.conf / "secrets" / f"{name}.token").read_text().strip()

    def req(self, who, method, path, body=None, token=None):
        r = urllib.request.Request(self.url + path, method=method,
                                   data=json.dumps(body).encode() if body is not None else None,
                                   headers={"Authorization": "Bearer " + (token or self.tok(who)) if who or token else ""})
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)

    def cli(self, who, *args):
        return subprocess.run([sys.executable, UNIKAN, "--as", who, *args], env=self.env, capture_output=True, text=True)

    def add(self, to="bob", **kw):
        s, c = self.req("lead", "POST", "/cards", {"title": "t", "assignee": to, **kw})
        assert s == 201, (s, c)
        return c["id"]

    def mv(self, who, cid, to, **kw):
        return self.req(who, "POST", f"/cards/{cid}/move", {"to": to, **kw})


class Rules(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.e = Env()
    @classmethod
    def tearDownClass(cls): cls.e.close()

    def test_auth(self):
        e = self.e
        self.assertEqual(e.req(None, "GET", "/cards", token="nope")[0], 401)
        r = urllib.request.Request(e.url + "/cards")
        with self.assertRaises(urllib.error.HTTPError) as cm: urllib.request.urlopen(r)
        self.assertEqual(cm.exception.code, 401)
        self.assertEqual(e.req("bob", "GET", "/cards")[0], 200)

    def test_create_rights_and_identity(self):
        e = self.e
        for who in ("carol", "alice", "bob"):
            self.assertEqual(e.req(who, "POST", "/cards", {"title": "x", "assignee": "bob"})[0], 403, who)
        s, c = e.req("owner", "POST", "/cards", {"title": "x", "assignee": "bob", "created_by": "lead"})
        self.assertEqual((s, c["created_by"]), (201, "owner"))
        self.assertEqual(e.req("lead", "POST", "/cards", {"title": "", "assignee": "bob"})[0], 400)
        self.assertEqual(e.req("lead", "POST", "/cards", {"title": "x", "assignee": "nobody"})[0], 400)
        self.assertEqual(e.req("lead", "POST", "/cards", {"title": "x", "assignee": "bob", "ttl": "5"})[0], 400)
        self.assertEqual(e.req("lead", "POST", "/cards", {"title": "x", "assignee": "bob", "column": "done"})[0], 400)

    def test_flow_and_permissions(self):
        e = self.e
        cid = e.add("alice", title="résumé ✓ <script>'\"; DROP TABLE cards;--")
        self.assertEqual(e.mv("bob", cid, "doing")[0], 403)       # not the assignee
        self.assertEqual(e.mv("carol", cid, "doing")[0], 403)
        self.assertEqual(e.mv("alice", cid, "done", result="x")[0], 409)  # todo->done invalid
        self.assertEqual(e.mv("alice", cid, "doing")[0], 200)
        self.assertEqual(e.mv("alice", cid, "doing")[0], 409)        # already doing
        self.assertEqual(e.mv("alice", cid, "done")[0], 400)         # result required
        self.assertEqual(e.mv("bob", cid, "done", result="x")[0], 403)
        s, c = e.mv("alice", cid, "done", result="beres")
        self.assertEqual((s, c["column"], c["result"]), (200, "done", "beres"))
        for to in ("todo", "doing", "blocked", "backlog"):
            self.assertEqual(e.mv("owner", cid, to, reason="r", question="q")[0], 409, to)  # terminal
        self.assertEqual(e.req("bob", "GET", "/cards?column=todo")[0], 200)
        self.assertTrue(any(h["to"] == "done" for h in e.req("bob", "GET", f"/cards/{cid}")[1]["history"]))

    def test_block_and_rework_cap(self):
        e = self.e
        cid = e.add("bob")
        e.mv("bob", cid, "doing")
        self.assertEqual(e.mv("bob", cid, "blocked")[0], 400)     # question required
        self.assertEqual(e.mv("bob", cid, "blocked", question="?")[0], 200)
        self.assertEqual(e.mv("bob", cid, "todo", reason="x")[0], 403)   # assignee cannot requeue
        self.assertEqual(e.mv("lead", cid, "todo")[0], 400)               # reason required
        for i in range(2):
            self.assertEqual(e.mv("lead", cid, "todo", reason=f"r{i}")[0], 200)
            e.mv("bob", cid, "doing"); e.mv("bob", cid, "blocked", question="?")
        s, c = e.mv("lead", cid, "todo", reason="third")
        self.assertEqual(s, 409); self.assertIn("rework limit", c["error"])
        self.assertEqual(e.mv("owner", cid, "todo", reason="owner override")[0], 200)

    def test_backlog_promote(self):
        e = self.e
        cid = e.add("bob", column="backlog")
        self.assertEqual(e.mv("bob", cid, "todo")[0], 403)
        self.assertEqual(e.mv("lead", cid, "todo")[0], 200)

    def test_concurrent_claim_single_winner(self):
        e = self.e
        cid = e.add("bob")
        res = []
        def go(): res.append(e.mv("bob", cid, "doing")[0])
        ts = [threading.Thread(target=go) for _ in range(12)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(sorted(res), [200] + [409] * 11)

    def test_bad_input(self):
        e = self.e
        self.assertEqual(e.req("bob", "GET", "/cards/zzzzzz")[0], 404)
        self.assertEqual(e.req("bob", "GET", "/cards/../../etc/passwd")[0], 404)
        self.assertEqual(e.req("bob", "GET", "/cards/abcdef")[0], 404)
        self.assertEqual(e.req("bob", "GET", "/cards?column=bogus")[0], 400)
        self.assertEqual(e.req("bob", "GET", "/cards?assignee=bogus")[0], 400)
        self.assertEqual(e.req("bob", "POST", "/cards/abcdef/move", {"to": "done"})[0], 404)
        cid = e.add("bob")
        self.assertEqual(e.mv("bob", cid, "bogus")[0], 400)
        self.assertEqual(e.req("bob", "GET", "/nothing")[0], 404)
        r = urllib.request.Request(e.url + "/cards", method="POST", data=b"{not json",
                                   headers={"Authorization": "Bearer " + e.tok("lead")})
        with self.assertRaises(urllib.error.HTTPError) as cm: urllib.request.urlopen(r)
        self.assertEqual(cm.exception.code, 400)
        r = urllib.request.Request(e.url + "/cards", method="POST", data=b"x" * 70000,
                                   headers={"Authorization": "Bearer " + e.tok("lead")})
        with self.assertRaises((urllib.error.HTTPError, urllib.error.URLError, ConnectionError)): urllib.request.urlopen(r)
        self.assertEqual(e.req("bob", "GET", "/health")[0], 200)   # still alive

    def test_cli_roundtrip(self):
        e = self.e
        r = e.cli("lead", "add", "cli card", "--to", "bob", "--check", "echo ok"); self.assertEqual(r.returncode, 0, r.stderr)
        cid = r.stdout.split()[0]
        self.assertIn(cid, e.cli("bob", "next").stdout)
        self.assertEqual(e.cli("bob", "claim", cid).returncode, 0)
        self.assertEqual(e.cli("bob", "touch", cid).returncode, 0)
        r = e.cli("bob", "done", cid, "--result", "ok"); self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotIn(cid, e.cli("bob", "ls").stdout)
        self.assertIn(cid, e.cli("bob", "ls", "--all").stdout)
        self.assertNotEqual(e.cli("carol", "add", "nope", "--to", "bob").returncode, 0)
        self.assertIn('"counts"', e.cli("bob", "stats").stdout)


class Reaper(unittest.TestCase):
    def test_timeout_and_touch(self):
        e = Env()
        try:
            slow, alive = e.add("bob", ttl=1), e.add("bob", ttl=3)
            e.mv("bob", slow, "doing"); e.mv("bob", alive, "doing")
            for _ in range(5):
                time.sleep(1); e.req("bob", "POST", f"/cards/{alive}/touch", {})
            s, c = e.req("lead", "GET", f"/cards/{slow}")
            self.assertEqual(c["column"], "blocked"); self.assertIn("timeout", c["question"])
            self.assertEqual(e.req("lead", "GET", f"/cards/{alive}")[1]["column"], "doing")
        finally:
            e.close()


class Persistence(unittest.TestCase):
    def test_restart_keeps_cards(self):
        e = Env()
        try:
            cid = e.add("bob"); e.mv("bob", cid, "doing")
            e.stop(); e.start()
            s, c = e.req("bob", "GET", f"/cards/{cid}")
            self.assertEqual((s, c["column"]), (200, "doing"))
            out = subprocess.run([sys.executable, UNIKAN, "backup"], env=e.env, capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr); self.assertTrue(Path(out.stdout.strip()).exists())
        finally:
            e.close()


class Wake(unittest.TestCase):
    def test_unclaimed_retries_then_blocks(self):
        tmp = tempfile.mkdtemp(); log = Path(tmp) / "woke.txt"
        e = Env(notify={"alice": ["sh", "-c", f"echo {{id}} >> {log}"]})
        try:
            cid = e.add("alice")
            for _ in range(80):
                time.sleep(0.25)
                if e.req("lead", "GET", f"/cards/{cid}")[1]["column"] == "blocked": break
            c = e.req("lead", "GET", f"/cards/{cid}")[1]
            self.assertEqual(c["column"], "blocked"); self.assertIn("did not claim", c["question"])
            self.assertEqual(log.read_text().split(), [cid] * 3)
        finally:
            e.close()

    def test_wake_that_claims_runs_once(self):
        tmp = tempfile.mkdtemp(); log = Path(tmp) / "woke.txt"
        # the wake command claims the card itself via the CLI, like a real agent
        e = EnvWithWake(log)
        try:
            cid = e.add("bob")
            for _ in range(40):
                time.sleep(0.25)
                if e.req("lead", "GET", f"/cards/{cid}")[1]["column"] == "doing": break
            time.sleep(2)  # no retry should happen
            self.assertEqual(e.req("lead", "GET", f"/cards/{cid}")[1]["column"], "doing")
            self.assertEqual(log.read_text().split(), [cid])
        finally:
            e.close()

    def test_serialize_heavy(self):
        tmp = tempfile.mkdtemp(); log = Path(tmp) / "t.txt"
        cmd = ["sh", "-c", f"echo start $(date +%s.%N) >> {log}; sleep 1; echo end $(date +%s.%N) >> {log}"]
        e = Env(notify={"bob": cmd, "alice": cmd}, serialize=["bob", "alice"], max_wakes=1, retry_delay=60)
        try:
            e.add("bob"); e.add("alice")
            for _ in range(40):
                time.sleep(0.25)
                if log.exists() and len(log.read_text().splitlines()) >= 4: break
            kinds = [l.split()[0] for l in log.read_text().splitlines()]
            self.assertEqual(kinds, ["start", "end", "start", "end"])   # never overlapped
        finally:
            e.close()

    def test_startup_sweep_wakes_existing_todo(self):
        tmp = tempfile.mkdtemp(); log = Path(tmp) / "w.txt"
        e = Env()
        try:
            cid = e.add("alice")
            e.stop()
            cfg = json.loads((e.conf / "config.json").read_text())
            cfg["notify"] = {"alice": ["sh", "-c", f"echo {{id}} >> {log}"]}
            (e.conf / "config.json").write_text(json.dumps(cfg))
            e.start()
            for _ in range(40):
                time.sleep(0.25)
                if log.exists(): break
            self.assertIn(cid, log.read_text())
        finally:
            e.close()


class EnvWithWake(Env):
    """Env whose wake command claims via the CLI (needs UNIKAN_CONF/UNIKAN_DATA/UNIKAN_URL in the child env)."""
    def __init__(self, log):
        cmd = ["sh", "-c", f"echo {{id}} >> {log}; {sys.executable} {UNIKAN} --as bob claim {{id}}"]
        super().__init__(notify={"bob": cmd})


class FreshInstall(unittest.TestCase):
    """What a newcomer does: unikan init, unikan serve, then use the board with the names they chose."""
    def test_init_then_use(self):
        tmp = tempfile.TemporaryDirectory(); t = Path(tmp.name)
        port = free_port()
        env = dict(os.environ, UNIKAN_CONF=str(t / "conf"), UNIKAN_DATA=str(t / "data"))
        env.pop("UNIKAN_TOKEN", None); env.pop("UNIKAN_AS", None)
        r = subprocess.run([sys.executable, UNIKAN, "init", "--owner", "ani", "--agents", "rina,dodi", "--port", str(port)],
                           env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        r = subprocess.run([sys.executable, UNIKAN, "init", "--owner", "ani"], env=env, capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)  # refuses to overwrite without --force
        self.assertFalse(subprocess.run([sys.executable, UNIKAN, "init", "--owner", "a b"], env=dict(env, UNIKAN_CONF=str(t / "c2")),
                                        capture_output=True).returncode == 0)
        srv = subprocess.Popen([sys.executable, UNIKAN, "serve"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            url = f"http://127.0.0.1:{port}"
            for _ in range(100):
                try: urllib.request.urlopen(url + "/health", timeout=1); break
                except Exception: time.sleep(0.1)
            def cli(who, *a):
                return subprocess.run([sys.executable, UNIKAN, "--as", who, "--url", url, *a], env=env, capture_output=True, text=True)
            r = cli("ani", "add", "first card", "--to", "rina", "--body", "hello")
            self.assertEqual(r.returncode, 0, r.stderr)
            cid = r.stdout.split()[0]
            self.assertIn(cid, cli("rina", "next").stdout)
            self.assertNotEqual(cli("dodi", "claim", cid).returncode, 0)   # not dodi's card
            self.assertEqual(cli("rina", "claim", cid).returncode, 0)
            self.assertEqual(cli("rina", "done", cid, "--result", "ok").returncode, 0)
            self.assertNotEqual(cli("ani", "add", "bad", "--to", "nobody").returncode, 0)
            self.assertNotEqual(cli("rina", "add", "x", "--to", "dodi").returncode, 0)  # workers cannot create
        finally:
            srv.terminate(); srv.wait(10); tmp.cleanup()


class Demo(unittest.TestCase):
    def test_demo_runs_and_leaves_no_temp_dir(self):
        import glob, tempfile
        before = set(glob.glob(os.path.join(tempfile.gettempdir(), "unikan-demo-*")))
        r = subprocess.run([sys.executable, UNIKAN, "demo"], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("403 only the assignee may claim", r.stdout)
        self.assertIn("done", r.stdout)
        self.assertEqual(set(glob.glob(os.path.join(tempfile.gettempdir(), "unikan-demo-*"))), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
