"""
Agentic Top-Up API — scheduled & agent-initiated payments for travel cards
(e.g. Leap Card).

Demo: `uvicorn Stripehackathon:app --reload` then open http://127.0.0.1:8000/docs

Set STRIPE_SECRET_KEY=sk_test_... to charge real (test-mode) cards via Stripe.
Without a key, payments run on a mock rail so the whole flow still works.
"""

import json
import os
import sqlite3
import threading
import urllib.request
import uuid
from datetime import datetime, timedelta
from typing import Optional

import stripe
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

_BASE = os.path.dirname(os.path.abspath(__file__))

# Load .env written by `stripe projects env --pull` (no dependency needed)
_env = os.path.join(_BASE, ".env")
if os.path.exists(_env):
    for _line in open(_env):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

DB_PATH = os.path.join(_BASE, "topup.db")
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
stripe.api_key = STRIPE_SECRET_KEY or None

TURSO_URL = os.environ.get("TURSO_DATABASE_URL", "").strip()
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "").strip()
USE_TURSO = bool(TURSO_URL and TURSO_TOKEN)

INTERVALS = {"demo": 60, "daily": 86400, "weekly": 604800, "monthly": 2592000}
WINDOWS = {"hourly": 3600, "daily": 86400, "weekly": 604800}
LOW_BALANCE_COOLDOWN = 300  # seconds between low-balance top-ups

app = FastAPI(title="Agentic Top-Up API", version="0.1.0")
_lock = threading.Lock()


# ---------------------------------------------------------------- db helpers
# Turso (libSQL) via the HTTP pipeline API — no native client needed.

def _enc(v):
    if v is None:
        return {"type": "null", "value": None}
    if isinstance(v, bool):
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    return {"type": "text", "value": str(v)}


def _dec(v):
    t, val = v.get("type"), v.get("value")
    if val is None or t == "null":
        return None
    if t == "integer":
        return int(val)
    if t == "float":
        return float(val)
    return val


class _Cursor:
    def __init__(self, result):
        self.description = [(c["name"],) for c in result.get("cols", [])]
        self._rows = [[_dec(v) for v in r] for r in result.get("rows", [])]
        self.rowcount = result.get("affected_row_count", 0)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        out, self._rows = self._rows, []
        return out


class TursoDB:
    """Minimal sqlite3-like wrapper over Turso's /v2/pipeline endpoint."""

    def __init__(self):
        base = (TURSO_URL.replace("libsql://", "https://")
                .replace("wss://", "https://").replace("ws://", "http://")
                .rstrip("/"))
        self._endpoint = base + "/v2/pipeline"

    def execute(self, sql, args=()):
        if sql.strip().upper().startswith("PRAGMA"):
            return _Cursor({})
        body = {"requests": [
            {"type": "execute",
             "stmt": {"sql": sql, "args": [_enc(a) for a in args]}},
            {"type": "close"}]}
        req = urllib.request.Request(
            self._endpoint, data=json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + TURSO_TOKEN,
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            out = json.loads(r.read())
        res = out["results"][0]
        if res.get("type") != "ok":
            raise RuntimeError("Turso error: %s" % res.get("error"))
        return _Cursor(res["response"]["result"])

    def executescript(self, script):
        for stmt in script.split(";"):
            if stmt.strip():
                self.execute(stmt)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def db():
    if USE_TURSO:
        return TursoDB()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS customers(
                id TEXT PRIMARY KEY, name TEXT, email TEXT,
                stripe_customer_id TEXT, payment_method TEXT,
                created_at TEXT);
            CREATE TABLE IF NOT EXISTS cards(
                leap_card_number TEXT PRIMARY KEY, customer_id TEXT,
                balance_cents INTEGER DEFAULT 0, created_at TEXT);
            CREATE TABLE IF NOT EXISTS schedules(
                id TEXT PRIMARY KEY, customer_id TEXT, leap_card_number TEXT,
                amount_cents INTEGER, currency TEXT,
                schedule_type TEXT, interval_seconds INTEGER,
                threshold_cents INTEGER, status TEXT,
                next_run_at TEXT, last_run_at TEXT, created_at TEXT);
            CREATE TABLE IF NOT EXISTS mandates(
                id TEXT PRIMARY KEY, agent_id TEXT, customer_id TEXT,
                leap_card_number TEXT, max_amount_cents INTEGER,
                frequency TEXT, status TEXT, description TEXT,
                last_used_at TEXT, uses_in_window INTEGER DEFAULT 0,
                created_at TEXT);
            CREATE TABLE IF NOT EXISTS transactions(
                id TEXT PRIMARY KEY, kind TEXT, leap_card_number TEXT,
                amount_cents INTEGER, currency TEXT, status TEXT,
                stripe_payment_intent TEXT, source_id TEXT,
                description TEXT, created_at TEXT);
            """
        )


def utcnow():
    return datetime.utcnow()


def iso(dt):
    return dt.isoformat() + "Z"


def _dict(cur, r):
    if r is None:
        return None
    if isinstance(r, sqlite3.Row):
        return dict(r)
    return dict(zip([d[0] for d in cur.description], r))


def row(conn, q, args=()):
    cur = conn.execute(q, args)
    return _dict(cur, cur.fetchone())


def rows(conn, q, args=()):
    cur = conn.execute(q, args)
    return [_dict(cur, r) for r in cur.fetchall()]


def record_tx(conn, kind, card_no, amount, currency, status, pi=None,
              source_id=None, description=None):
    tx_id = "tx_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO transactions VALUES(?,?,?,?,?,?,?,?,?,?)",
        (tx_id, kind, card_no, amount, currency, status, pi, source_id,
         description, iso(utcnow())))
    return tx_id


# ------------------------------------------------------------- payment rails

def ensure_stripe_customer(name, email, payment_method):
    """Create a Stripe customer with a default saved payment method.
    Returns (customer_id, attached_payment_method_id)."""
    cust = stripe.Customer.create(name=name, email=email)
    pm = stripe.PaymentMethod.attach(payment_method, customer=cust.id)
    stripe.Customer.modify(
        cust.id,
        invoice_settings={"default_payment_method": pm.id})
    return cust.id, pm.id


def charge(customer, amount_cents, currency, description):
    """Charge the customer's saved card off-session. Returns (pi_id, status)."""
    if not STRIPE_SECRET_KEY:
        return "pi_mock_" + uuid.uuid4().hex[:16], "succeeded"
    pi = stripe.PaymentIntent.create(
        amount=amount_cents,
        currency=currency,
        customer=customer["stripe_customer_id"],
        payment_method=customer["payment_method"],
        off_session=True,
        confirm=True,
        description=description)
    return pi.id, pi.status


def perform_topup(conn, customer, card_no, amount_cents, currency,
                  kind, source_id=None, description="Leap card top-up"):
    """Charge via Stripe then credit the card balance. Returns tx id."""
    pi_id, status = charge(customer, amount_cents, currency, description)
    if status == "succeeded":
        conn.execute(
            "UPDATE cards SET balance_cents = balance_cents + ? "
            "WHERE leap_card_number = ?", (amount_cents, card_no))
    else:
        description += f" (stripe status: {status})"
    return record_tx(conn, kind, card_no, amount_cents, currency, status,
                     pi_id, source_id, description)


# ---------------------------------------------------------------- scheduler

def run_due_schedules():
    with _lock, db() as conn:
        now = utcnow()
        due = rows(
            conn,
            "SELECT * FROM schedules WHERE status='active' "
            "AND schedule_type='interval' AND next_run_at <= ?",
            (iso(now),))
        for s in due:
            cust = row(conn, "SELECT * FROM customers WHERE id=?",
                       (s["customer_id"],))
            if not cust:
                continue
            perform_topup(conn, cust, s["leap_card_number"],
                          s["amount_cents"], s["currency"], "scheduled",
                          s["id"], "Scheduled Leap card top-up")
            conn.execute(
                "UPDATE schedules SET next_run_at=?, last_run_at=? WHERE id=?",
                (iso(now + timedelta(seconds=s["interval_seconds"])),
                 iso(now), s["id"]))

        low = rows(
            conn,
            "SELECT s.*, c.balance_cents FROM schedules s "
            "JOIN cards c ON c.leap_card_number = s.leap_card_number "
            "WHERE s.status='active' AND s.schedule_type='low_balance'")
        for s in low:
            if s["balance_cents"] >= s["threshold_cents"]:
                continue
            if s["last_run_at"]:
                last = datetime.fromisoformat(s["last_run_at"].rstrip("Z"))
                if (now - last).total_seconds() < LOW_BALANCE_COOLDOWN:
                    continue
            cust = row(conn, "SELECT * FROM customers WHERE id=?",
                       (s["customer_id"],))
            if not cust:
                continue
            perform_topup(conn, cust, s["leap_card_number"],
                          s["amount_cents"], s["currency"], "scheduled",
                          s["id"], "Auto top-up: balance below threshold")
            conn.execute(
                "UPDATE schedules SET last_run_at=? WHERE id=?",
                (iso(now), s["id"]))


scheduler = BackgroundScheduler()
scheduler.add_job(run_due_schedules, "interval", seconds=15,
                  id="due_schedules")


@app.on_event("startup")
def startup():
    init_db()
    if not scheduler.running:
        scheduler.start()


@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown(wait=False)


@app.middleware("http")
async def check_due_schedules(request, call_next):
    """Serverless safety net: evaluate due schedules on each request so
    top-ups still fire where no background scheduler can run."""
    try:
        run_due_schedules()
    except Exception:
        pass
    return await call_next(request)


# ------------------------------------------------------------------- models

class CustomerIn(BaseModel):
    name: str
    email: str
    leap_card_number: str = Field(..., example="LEAP-1234-5678")
    opening_balance_cents: int = 0
    payment_method: str = "pm_card_visa"  # Stripe test token


class ScheduleIn(BaseModel):
    customer_id: str
    amount_cents: int = Field(..., gt=0, example=2000)
    currency: str = "eur"
    schedule_type: str = Field("interval", pattern="^(interval|low_balance)$")
    interval: Optional[str] = None        # demo | daily | weekly | monthly
    threshold_cents: Optional[int] = None  # required for low_balance


class MandateIn(BaseModel):
    agent_id: str = Field(..., example="travel-agent-v1")
    customer_id: str
    max_amount_cents: int = Field(..., gt=0)
    frequency: str = Field("daily", pattern="^(hourly|daily|weekly)$")
    description: Optional[str] = None


class AgentTopupIn(BaseModel):
    mandate_id: str
    amount_cents: int = Field(..., gt=0)
    reason: Optional[str] = None


class TravelIn(BaseModel):
    fare_cents: int = Field(..., gt=0)


# ----------------------------------------------------------------- endpoints

@app.get("/", response_class=FileResponse)
def root():
    return os.path.join(_BASE, "public", "index.html")


@app.get("/status")
def status():
    return {
        "service": "Agentic Top-Up API",
        "payments": "stripe" if STRIPE_SECRET_KEY else "mock",
        "database": "turso" if USE_TURSO else "sqlite",
        "docs": "/docs",
    }


@app.post("/customers", status_code=201)
def create_customer(body: CustomerIn):
    cust_id = "cus_" + uuid.uuid4().hex[:12]
    scid = None
    payment_method = body.payment_method
    if STRIPE_SECRET_KEY:
        try:
            scid, payment_method = ensure_stripe_customer(
                body.name, body.email, body.payment_method)
        except stripe.error.StripeError as e:
            raise HTTPException(502, f"Stripe: {e.user_message or e}")
    with _lock, db() as conn:
        conn.execute("INSERT INTO customers VALUES(?,?,?,?,?,?)",
                     (cust_id, body.name, body.email, scid,
                      payment_method, iso(utcnow())))
        conn.execute(
            "INSERT OR REPLACE INTO cards VALUES(?,?,?,?)",
            (body.leap_card_number, cust_id, body.opening_balance_cents,
             iso(utcnow())))
    return {"customer_id": cust_id, "stripe_customer_id": scid,
            "leap_card_number": body.leap_card_number,
            "balance_cents": body.opening_balance_cents}


@app.get("/customers/{customer_id}")
def get_customer(customer_id: str):
    with db() as conn:
        cust = row(conn, "SELECT * FROM customers WHERE id=?", (customer_id,))
        if not cust:
            raise HTTPException(404, "customer not found")
        cust["cards"] = rows(
            conn, "SELECT * FROM cards WHERE customer_id=?", (customer_id,))
    return cust


@app.post("/schedules", status_code=201)
def create_schedule(body: ScheduleIn):
    if body.schedule_type == "interval":
        if body.interval not in INTERVALS:
            raise HTTPException(
                422, "interval must be one of: " + ", ".join(INTERVALS))
        interval_seconds = INTERVALS[body.interval]
        threshold = None
        next_run = iso(utcnow() + timedelta(seconds=interval_seconds))
    else:
        if body.threshold_cents is None:
            raise HTTPException(422, "threshold_cents required for "
                                     "low_balance schedules")
        interval_seconds = None
        threshold = body.threshold_cents
        next_run = None

    with _lock, db() as conn:
        card = row(conn, "SELECT * FROM cards WHERE customer_id=?",
                   (body.customer_id,))
        if not card:
            raise HTTPException(404, "customer or card not found")
        sid = "sch_" + uuid.uuid4().hex[:12]
        conn.execute(
            "INSERT INTO schedules VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, body.customer_id, card["leap_card_number"],
             body.amount_cents, body.currency, body.schedule_type,
             interval_seconds, threshold, "active", next_run, None,
             iso(utcnow())))
        return row(conn, "SELECT * FROM schedules WHERE id=?", (sid,))


@app.get("/schedules")
def list_schedules():
    with db() as conn:
        return rows(conn, "SELECT * FROM schedules ORDER BY created_at DESC")


@app.post("/schedules/{schedule_id}/trigger")
def trigger_schedule(schedule_id: str):
    """Fire a schedule immediately — the demo button."""
    with _lock, db() as conn:
        s = row(conn, "SELECT * FROM schedules WHERE id=?", (schedule_id,))
        if not s:
            raise HTTPException(404, "schedule not found")
        cust = row(conn, "SELECT * FROM customers WHERE id=?",
                   (s["customer_id"],))
        tx_id = perform_topup(
            conn, cust, s["leap_card_number"], s["amount_cents"],
            s["currency"], "scheduled", s["id"],
            "Manually triggered top-up")
        if s["schedule_type"] == "interval":
            conn.execute(
                "UPDATE schedules SET next_run_at=?, last_run_at=? "
                "WHERE id=?",
                (iso(utcnow() + timedelta(seconds=s["interval_seconds"])),
                 iso(utcnow()), schedule_id))
        return row(conn, "SELECT * FROM transactions WHERE id=?", (tx_id,))


@app.patch("/schedules/{schedule_id}")
def update_schedule(schedule_id: str, status: str):
    if status not in ("active", "paused", "cancelled"):
        raise HTTPException(422, "status must be active|paused|cancelled")
    with _lock, db() as conn:
        cur = conn.execute("UPDATE schedules SET status=? WHERE id=?",
                           (status, schedule_id))
        if cur.rowcount == 0:
            raise HTTPException(404, "schedule not found")
        return row(conn, "SELECT * FROM schedules WHERE id=?",
                   (schedule_id,))


# ------------------------------------------------------------- agentic API

@app.post("/agent/mandates", status_code=201)
def create_mandate(body: MandateIn):
    """Give an AI agent a scoped spending mandate over a customer's card."""
    with _lock, db() as conn:
        card = row(conn, "SELECT * FROM cards WHERE customer_id=?",
                   (body.customer_id,))
        if not card:
            raise HTTPException(404, "customer or card not found")
        mid = "man_" + uuid.uuid4().hex[:12]
        conn.execute(
            "INSERT INTO mandates VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (mid, body.agent_id, body.customer_id,
             card["leap_card_number"], body.max_amount_cents,
             body.frequency, "active", body.description, None, 0,
             iso(utcnow())))
        return row(conn, "SELECT * FROM mandates WHERE id=?", (mid,))


@app.get("/agent/mandates/{mandate_id}")
def get_mandate(mandate_id: str):
    with db() as conn:
        m = row(conn, "SELECT * FROM mandates WHERE id=?", (mandate_id,))
        if not m:
            raise HTTPException(404, "mandate not found")
    return m


@app.post("/agent/topup")
def agent_topup(body: AgentTopupIn):
    """Agent-initiated top-up. Enforces the mandate's spending limits."""
    with _lock, db() as conn:
        m = row(conn, "SELECT * FROM mandates WHERE id=?",
                (body.mandate_id,))
        if not m:
            raise HTTPException(404, "mandate not found")
        if m["status"] != "active":
            raise HTTPException(403, f"mandate is {m['status']}")
        if body.amount_cents > m["max_amount_cents"]:
            raise HTTPException(
                403, f"exceeds mandate cap of {m['max_amount_cents']}c")

        now = utcnow()
        window = timedelta(seconds=WINDOWS[m["frequency"]])
        uses = m["uses_in_window"]
        if m["last_used_at"]:
            last = datetime.fromisoformat(m["last_used_at"].rstrip("Z"))
            if now - last > window:
                uses = 0
        if uses >= 1:
            raise HTTPException(
                429, f"mandate allows 1 top-up per {m['frequency']} window")

        cust = row(conn, "SELECT * FROM customers WHERE id=?",
                   (m["customer_id"],))
        tx_id = perform_topup(
            conn, cust, m["leap_card_number"], body.amount_cents, "eur",
            "agent", m["id"], f"Agent top-up ({body.reason or m['agent_id']})")
        conn.execute(
            "UPDATE mandates SET last_used_at=?, uses_in_window=? WHERE id=?",
            (iso(now), uses + 1, m["id"]))
        return row(conn, "SELECT * FROM transactions WHERE id=?", (tx_id,))


# ------------------------------------------------------------------ cards

@app.get("/cards/{leap_card_number}")
def get_card(leap_card_number: str):
    with db() as conn:
        card = row(conn, "SELECT * FROM cards WHERE leap_card_number=?",
                   (leap_card_number,))
        if not card:
            raise HTTPException(404, "card not found")
        card["transactions"] = rows(
            conn,
            "SELECT * FROM transactions WHERE leap_card_number=? "
            "ORDER BY created_at DESC LIMIT 20", (leap_card_number,))
    return card


@app.post("/cards/{leap_card_number}/travel")
def simulate_travel(leap_card_number: str, body: TravelIn):
    """Simulate a journey — deducts fare so low-balance top-ups can fire."""
    with _lock, db() as conn:
        card = row(conn, "SELECT * FROM cards WHERE leap_card_number=?",
                   (leap_card_number,))
        if not card:
            raise HTTPException(404, "card not found")
        if card["balance_cents"] < body.fare_cents:
            raise HTTPException(402, "insufficient balance on card")
        conn.execute(
            "UPDATE cards SET balance_cents = balance_cents - ? "
            "WHERE leap_card_number=?",
            (body.fare_cents, leap_card_number))
        tx_id = record_tx(conn, "fare", leap_card_number,
                          -body.fare_cents, "eur", "succeeded",
                          description="Simulated journey fare")
        return row(conn, "SELECT * FROM transactions WHERE id=?", (tx_id,))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
