#!/usr/bin/env python3
"""
cvd_zero_cross.py
CVD ZERO-LINE TOUCH / CROSS + RANGE  —  only 1h and 4h, 50 symbols.

Logic mirrors the Pine Script "CVD - Cumulative Volume Delta Candles":
  * delta per intrabar from candle direction (close>open / close<open / fallbacks)
  * CVD is CUMULATIVE and resets on a higher timeframe anchor
        1h leg -> resets daily   (Pine htfStep for 1h chart = 1D)
        4h leg -> resets weekly  (Pine htfStep for 4h chart = 1W)
  * alert when BOTH legs are at/through the zero line AND price is ranging

No volume filters. No OBI. No footprint. No CHoCH. No session gating.
Binance Spot only. Alerts only. No trading. No links in alert body.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

import aiohttp
import websockets
from dotenv import load_dotenv


CONFIG: dict[str, Any] = {
    "max_symbols": 50,
    "base_interval": "5m",          # intrabar timeframe used to build delta
    "base_seconds": 300,
    "history_days": 9,              # enough for weekly reset + range lookback
    "eval_interval_sec": 10,
    "per_symbol_min_gap_sec": 5,
    "delta_unit": "quote",          # "quote" = USDT, "base" = coin units

    "legs": {
        "1h": {
            "seconds": 3600,
            "reset": "1D",
            "range_lookback": 8,        # 8 closed 1h bars
            "range_max_pct": 2.0,       # (high-low) / price <= 2%
            "net_move_max_pct": 1.0,    # |close-open| over lookback <= 1%
            "zero_tol_mult": 0.30,      # tol = avg|delta| * 0.30
            "zero_avg_len": 20,
        },
        "4h": {
            "seconds": 14400,
            "reset": "1W",
            "range_lookback": 6,        # 6 closed 4h bars = 24h
            "range_max_pct": 4.5,
            "net_move_max_pct": 2.5,
            "zero_tol_mult": 0.30,
            "zero_avg_len": 20,
        },
    },

    "require_range_on": "both",     # both | any | 1h | 4h
    "refire_on_cross": True,        # new cross while both active -> allow again
    "cooldown_min": 90,
    "max_alerts_per_day": 120,

    "bootstrap_concurrency": 5,
    "http_timeout_sec": 30,
    "http_retries": 3,
    "ws_recv_timeout_sec": 60,
    "reconnect_backoff_max_sec": 60,
    "rebootstrap_gap_sec": 180,
    "diag_interval_min": 10,
    "state_save_interval_sec": 60,
    "ntfy_timeout_sec": 10,
    "ntfy_min_gap_sec": 1.1,
}

REST_BASE = os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision").rstrip("/")
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision/stream")
TICKER_URL = REST_BASE + "/api/v3/ticker/24hr"
INFO_URL = REST_BASE + "/api/v3/exchangeInfo"
STATE_FILE = "cvd_zero_state.json"
LOG_FILE = "cvd_zero.log"

STABLE_BASES = {
    "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "EUR", "GBP", "AEUR", "EURI", "USDP", "USDE",
    "USDS", "USDG", "USD1", "PYUSD", "RLUSD", "RWUSD", "BFUSD", "XUSD", "U", "TRY", "BIDR",
    "UST", "USTC", "USDD", "FRAX", "LUSD", "GUSD", "USDB", "CRVUSD", "GHO", "USDJ",
}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

log = logging.getLogger("cvd_zero")


def setup_logging() -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fmt.converter = time.gmtime
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    for h in (fh, ch):
        h.setFormatter(fmt)
        log.addHandler(h)


def utc_day(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), timezone.utc).strftime("%Y-%m-%d")


def anchor_key(open_ms: int, mode: str) -> str:
    dt = datetime.fromtimestamp(open_ms / 1000.0, timezone.utc)
    if mode == "1D":
        return dt.strftime("%Y-%m-%d")
    if mode == "1W":
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if mode == "1M":
        return dt.strftime("%Y-%m")
    return "none"


# ---------------------------------------------------------------- symbols

class SymbolSelector:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg

    @staticmethod
    def excluded(base: str, trading_bases: set[str]) -> bool:
        if base in STABLE_BASES or base.startswith("USD"):
            return True
        if base.endswith("USD") and len(base) > 3:
            return True
        for suf in LEVERAGED_SUFFIXES:
            if base.endswith(suf) and base[:-len(suf)] in trading_bases:
                return True
        return False

    async def _get(self, session: aiohttp.ClientSession, url: str) -> Any:
        last: Exception | None = None
        for attempt in range(1, int(self.cfg["http_retries"]) + 1):
            try:
                async with session.get(url) as resp:
                    if resp.status in (418, 429):
                        try:
                            wait = min(float(resp.headers.get("Retry-After", "5")), 60.0)
                        except ValueError:
                            wait = 5.0
                        log.warning("[REST] rate limited %s, waiting %.0fs", resp.status, wait)
                        await asyncio.sleep(wait)
                        raise RuntimeError(f"rate limited {resp.status}")
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as exc:
                last = exc
                log.warning("[REST] %s failed (%d): %s", url, attempt, exc)
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"REST failed: {url}: {last}")

    async def select(self, session: aiohttp.ClientSession) -> list[str]:
        tickers, info = await asyncio.gather(self._get(session, TICKER_URL), self._get(session, INFO_URL))
        symbols = [s for s in info.get("symbols", []) if s.get("status") == "TRADING"]
        trading = {s["symbol"] for s in symbols}
        bases = {str(s.get("baseAsset", "")) for s in symbols}
        rows: list[tuple[str, float]] = []
        for t in tickers:
            sym = str(t.get("symbol", ""))
            if not sym.endswith("USDT") or sym not in trading:
                continue
            base = sym[:-4]
            if not base or self.excluded(base, bases):
                continue
            try:
                qv = float(t["quoteVolume"])
            except (KeyError, TypeError, ValueError):
                continue
            rows.append((sym, qv))
        rows.sort(key=lambda kv: kv[1], reverse=True)
        picked = [s for s, _ in rows[: int(self.cfg["max_symbols"])]]
        log.info("[SYMBOLS] picked %d of %d USDT pairs (top by 24h volume, no thresholds)", len(picked), len(rows))
        log.info("[SYMBOLS] %s", ", ".join(picked))
        return picked


# ---------------------------------------------------------------- CVD build

def classify_deltas(bars: list[dict[str, Any]]) -> None:
    """Pine upDnIntrabarVolumes(): direction of the intrabar candle decides sign."""
    prev_close: float | None = None
    prev_sign = 0
    for b in bars:
        o, c, vol = b["o"], b["c"], b["vol"]
        if c > o:
            sign = 1
        elif c < o:
            sign = -1
        elif prev_close is not None and c > prev_close:
            sign = 1
        elif prev_close is not None and c < prev_close:
            sign = -1
        elif prev_sign > 0:
            sign = 1
        elif prev_sign < 0:
            sign = -1
        else:
            sign = 1
        b["delta"] = sign * vol
        prev_close = c
        prev_sign = sign


def aggregate(bars: list[dict[str, Any]], tf_sec: int) -> list[dict[str, Any]]:
    tf_ms = tf_sec * 1000
    out: dict[int, dict[str, Any]] = {}
    for b in bars:
        key = (b["t"] // tf_ms) * tf_ms
        agg = out.get(key)
        if agg is None:
            out[key] = {
                "t": key, "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"],
                "delta": b["delta"], "closed": b["closed"],
            }
        else:
            agg["h"] = max(agg["h"], b["h"])
            agg["l"] = min(agg["l"], b["l"])
            agg["c"] = b["c"]
            agg["delta"] += b["delta"]
            agg["closed"] = agg["closed"] and b["closed"]
    return [out[k] for k in sorted(out)]


def cvd_series(bars: list[dict[str, Any]], reset_mode: str) -> list[dict[str, Any]]:
    cvd = 0.0
    prev_anchor: str | None = None
    series: list[dict[str, Any]] = []
    for b in bars:
        a = anchor_key(b["t"], reset_mode)
        if prev_anchor is None or a != prev_anchor:
            cvd = 0.0
        before = cvd
        cvd += b["delta"]
        series.append({**b, "cvd_before": before, "cvd_after": cvd, "anchor": a})
        prev_anchor = a
    return series


class SymbolStore:
    __slots__ = ("symbol", "bars", "last_eval", "prev_both", "dirty")

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bars: dict[int, dict[str, Any]] = {}
        self.last_eval = 0.0
        self.prev_both = False
        self.dirty = True

    def put(self, t: int, o: float, h: float, l: float, c: float,
            vol: float, closed: bool) -> None:
        self.bars[t] = {"t": t, "o": o, "h": h, "l": l, "c": c, "vol": vol, "closed": closed}
        self.dirty = True

    def prune(self, cutoff_ms: int) -> None:
        old = [t for t in self.bars if t < cutoff_ms]
        for t in old:
            del self.bars[t]

    def sorted_bars(self) -> list[dict[str, Any]]:
        return [self.bars[t] for t in sorted(self.bars)]


# ---------------------------------------------------------------- alerts

class Ntfy:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.url = os.getenv("NTFY_URL_ZERO", os.getenv("NTFY_URL", "")).strip()
        self.token = os.getenv("NTFY_TOKEN", "").strip()
        self.timeout = float(cfg["ntfy_timeout_sec"])
        self.min_gap = float(cfg["ntfy_min_gap_sec"])
        self.session: aiohttp.ClientSession | None = None
        self._lock: asyncio.Lock | None = None
        self._next = 0.0
        if not self.url:
            log.warning("[NTFY] NTFY_URL not set; alerts logged only")

    async def start(self) -> None:
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    async def send(self, title: str, body: str, priority: str = "high", tags: str = "") -> None:
        if not self.url or self.session is None:
            return
        headers = {"Title": title.encode("ascii", "ignore").decode(), "Priority": priority}
        if tags:
            headers["Tags"] = tags
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        for attempt in (1, 2):
            if self._lock is not None:
                async with self._lock:
                    wait = self._next - time.monotonic()
                    if wait > 0:
                        await asyncio.sleep(wait)
                    self._next = time.monotonic() + self.min_gap
            try:
                async with self.session.post(self.url, data=body.encode("utf-8"), headers=headers) as resp:
                    if resp.status == 429 and attempt == 1:
                        try:
                            wait = min(float(resp.headers.get("Retry-After", "5")), 30.0)
                        except ValueError:
                            wait = 5.0
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 300:
                        log.warning("[NTFY] HTTP %s for %s", resp.status, title)
                    return
            except Exception as exc:
                log.warning("[NTFY] send failed: %s", exc)
                if attempt == 1:
                    await asyncio.sleep(2.0)


class Book:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.day = utc_day()
        self.sent_today = 0
        self.last_alert: dict[str, float] = {}

    def load(self) -> None:
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.day = str(d.get("day", self.day))
            self.sent_today = int(d.get("sent_today", 0))
            self.last_alert = {k: float(v) for k, v in d.get("last_alert", {}).items()}
            log.info("[STATE] loaded day=%s sent_today=%d", self.day, self.sent_today)
        except Exception as exc:
            log.error("[STATE] corrupt (%s); fresh start", exc)

    def rollover(self) -> None:
        today = utc_day()
        if today != self.day:
            self.day = today
            self.sent_today = 0
        cutoff = time.time() - 86400
        self.last_alert = {k: v for k, v in self.last_alert.items() if v > cutoff}

    async def save(self) -> None:
        data = {"day": self.day, "sent_today": self.sent_today,
                "last_alert": self.last_alert, "saved_at": datetime.now(timezone.utc).isoformat()}

        def write() -> None:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, STATE_FILE)

        try:
            await asyncio.to_thread(write)
        except Exception as exc:
            log.error("[STATE] save failed: %s", exc)

    def cooldown(self, key: str, now: float) -> bool:
        return now - self.last_alert.get(key, 0.0) < float(self.cfg["cooldown_min"]) * 60

    def cap(self) -> bool:
        return self.sent_today >= int(self.cfg["max_alerts_per_day"])


# ---------------------------------------------------------------- engine

class Engine:
    def __init__(self) -> None:
        self.cfg = CONFIG
        self.book = Book(self.cfg)
        self.ntfy = Ntfy(self.cfg)
        self.stores: dict[str, SymbolStore] = {}
        self.stop = asyncio.Event()
        self.connected = False
        self.last_msg_ts = 0.0
        self.counts: dict[str, int] = {}
        self.selector = SymbolSelector(self.cfg)
        self._bg: set[asyncio.Task[Any]] = set()
        self.start_ts = time.time()
        self.max_runtime = int(os.getenv("MAX_RUNTIME_SEC", "0"))

    def _spawn(self, coro: Any) -> None:
        t = asyncio.create_task(coro)
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    def _count(self, key: str) -> None:
        self.counts[key] = self.counts.get(key, 0) + 1

    def request_stop(self, why: str) -> None:
        if not self.stop.is_set():
            log.info("[ENGINE] stop requested (%s)", why)
            self.stop.set()

    async def _sleep_or_stop(self, sec: float) -> bool:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=max(sec, 0.0))
            return True
        except asyncio.TimeoutError:
            return False

    # ---- bootstrap

    async def _bootstrap_symbol(self, session: aiohttp.ClientSession,
                                sem: asyncio.Semaphore, sym: str) -> None:
        cfg = self.cfg
        base_ms = int(cfg["base_seconds"]) * 1000
        now_ms = int(time.time() * 1000)
        start = now_ms - int(cfg["history_days"]) * 86_400_000
        store = self.stores[sym]
        got = 0
        async with sem:
            while not self.stop.is_set():
                url = (f"{REST_BASE}/api/v3/klines?symbol={sym}"
                       f"&interval={cfg['base_interval']}&startTime={start}&limit=1000")
                try:
                    rows = await self.selector._get(session, url)
                except Exception as exc:
                    log.warning("[%s] bootstrap failed: %s", sym, exc)
                    return
                if not rows:
                    break
                for r in rows:
                    if int(r[6]) >= now_ms:
                        continue
                    vol = float(r[7]) if cfg["delta_unit"] == "quote" else float(r[5])
                    store.put(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), vol, True)
                    got += 1
                if len(rows) < 1000:
                    break
                start = int(rows[-1][0]) + base_ms
        log.info("[%s] bootstrap %d intrabars", sym, got)

    async def _bootstrap_all(self) -> None:
        sem = asyncio.Semaphore(int(self.cfg["bootstrap_concurrency"]))
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await asyncio.gather(*[self._bootstrap_symbol(session, sem, s) for s in self.stores])
        log.info("[ENGINE] bootstrap complete for %d symbols", len(self.stores))

    # ---- websocket

    def _handle(self, raw: str | bytes) -> None:
        msg = json.loads(raw)
        data = msg.get("data", msg)
        if data.get("e") != "kline":
            return
        k = data["k"]
        sym = str(data["s"]).upper()
        store = self.stores.get(sym)
        if store is None:
            return
        self.last_msg_ts = time.time()
        vol = float(k["q"]) if self.cfg["delta_unit"] == "quote" else float(k["v"])
        store.put(int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]),
                  float(k["c"]), vol, bool(k["x"]))

    async def _ws_loop(self, url: str) -> None:
        fails = 0
        while not self.stop.is_set():
            gap = time.time() - self.last_msg_ts if self.last_msg_ts else float("inf")
            if gap > float(self.cfg["rebootstrap_gap_sec"]):
                log.info("[ENGINE] gap %.0fs -> re-bootstrap", gap if gap != float("inf") else -1)
                await self._bootstrap_all()
            got = False
            try:
                log.info("[ENGINE] connecting WS (%d chars)", len(url))
                async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                              max_size=None, close_timeout=5) as ws:
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg["ws_recv_timeout_sec"])
                        if not got:
                            got, fails, self.connected = True, 0, True
                            log.info("[ENGINE] WS connected, receiving klines")
                        try:
                            self._handle(raw)
                        except Exception as exc:
                            log.warning("[ENGINE] bad message: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                log.warning("[ENGINE] WS error: %s: %s", type(exc).__name__, exc)
                if self.stop.is_set():
                    break
                if got:
                    delay = 1.0
                else:
                    fails += 1
                    delay = min(2.0 ** (fails - 1), float(self.cfg["reconnect_backoff_max_sec"]))
                if await self._sleep_or_stop(delay):
                    break

    # ---- evaluation

    def _leg_state(self, bars: list[dict[str, Any]], leg: dict[str, Any],
                   price: float) -> dict[str, Any] | None:
        if len(bars) < 3:
            return None
        series = cvd_series(bars, str(leg["reset"]))
        last = series[-1]
        closed = [b for b in series if b["closed"]]
        n = int(leg["range_lookback"])
        if len(closed) < n:
            return None

        avg_len = int(leg["zero_avg_len"])
        sample = [abs(b["delta"]) for b in closed[-avg_len:]]
        avg_abs = sum(sample) / len(sample) if sample else 0.0
        tol = max(avg_abs * float(leg["zero_tol_mult"]), 1e-9)

        cur = float(last["cvd_after"])
        prev = float(last["cvd_before"])
        cross_up = prev <= 0.0 < cur
        cross_down = prev >= 0.0 > cur
        near = abs(cur) <= tol
        if cross_up:
            event = "CROSS_UP"
        elif cross_down:
            event = "CROSS_DOWN"
        elif near:
            event = "TOUCH"
        else:
            event = "AWAY"

        window = closed[-n:]
        hi = max(b["h"] for b in window)
        lo = min(b["l"] for b in window)
        first_open = float(window[0]["o"])
        last_close = float(window[-1]["c"])
        rng_pct = (hi - lo) / price * 100.0 if price > 0 else 1e9
        net_pct = abs(last_close - first_open) / first_open * 100.0 if first_open > 0 else 1e9
        in_range = rng_pct <= float(leg["range_max_pct"]) and net_pct <= float(leg["net_move_max_pct"])

        return {
            "cvd": cur, "cvd_prev": prev, "tol": tol, "event": event,
            "zero": near or cross_up or cross_down,
            "cross": cross_up or cross_down,
            "range_pct": rng_pct, "net_pct": net_pct, "in_range": in_range,
            "anchor": last["anchor"], "bars": len(closed),
            "forming": not last["closed"],
        }

    def _evaluate_symbol(self, sym: str, store: SymbolStore, now: float) -> None:
        if now - store.last_eval < float(self.cfg["per_symbol_min_gap_sec"]):
            return
        if not store.dirty:
            return
        store.last_eval = now
        store.dirty = False

        bars = store.sorted_bars()
        if len(bars) < 20:
            self._count("no_data")
            return
        cutoff = int(now * 1000) - int(self.cfg["history_days"]) * 86_400_000
        store.prune(cutoff)
        classify_deltas(bars)
        price = float(bars[-1]["c"])
        if price <= 0:
            self._count("no_price")
            return

        legs: dict[str, dict[str, Any]] = {}
        for name, leg in self.cfg["legs"].items():
            agg = aggregate(bars, int(leg["seconds"]))
            st = self._leg_state(agg, leg, price)
            if st is None:
                self._count(f"{name}:not_ready")
                return
            legs[name] = st

        for name, st in legs.items():
            if not st["zero"]:
                self._count(f"{name}:away_from_zero")
            if not st["in_range"]:
                self._count(f"{name}:not_range")

        if not all(st["zero"] for st in legs.values()):
            store.prev_both = False
            return

        mode = str(self.cfg["require_range_on"])
        if mode == "both":
            range_ok = all(st["in_range"] for st in legs.values())
        elif mode == "any":
            range_ok = any(st["in_range"] for st in legs.values())
        else:
            range_ok = bool(legs.get(mode, {}).get("in_range", False))
        if not range_ok:
            store.prev_both = False
            self._count("range_gate")
            return

        fresh = not store.prev_both
        new_cross = any(st["cross"] for st in legs.values())
        store.prev_both = True
        if not fresh and not (self.cfg["refire_on_cross"] and new_cross):
            self._count("already_active")
            return

        key = f"{sym}|zero_range"
        if self.book.cooldown(key, now):
            self._count("cooldown")
            return
        if self.book.cap():
            self._count("daily_cap")
            return

        self.book.last_alert[key] = now
        self.book.sent_today += 1
        self._count("signal")
        self._send(sym, price, legs, now)

    def _send(self, sym: str, price: float, legs: dict[str, dict[str, Any]], now: float) -> None:
        events = "+".join(st["event"] for st in legs.values())
        lines = [
            f"Symbol: {sym}",
            f"Price: {price:.8g}",
            f"Event: {events}",
            "",
        ]
        for name, st in legs.items():
            lines.append(f"[{name}] CVD: {st['cvd_prev']:+,.0f} -> {st['cvd']:+,.0f}")
            lines.append(f"[{name}] zero tolerance: +/-{st['tol']:,.0f}")
            lines.append(f"[{name}] reset anchor: {st['anchor']}")
            lines.append(f"[{name}] range: {st['range_pct']:.2f}% | net move: {st['net_pct']:.2f}%")
            lines.append(f"[{name}] closed bars: {st['bars']} | forming: {'yes' if st['forming'] else 'no'}")
            lines.append("")
        lines.append("Both 1h and 4h CVD at zero line, price in range.")
        lines.append(f"Time: {datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        body = "\n".join(lines)
        title = f"{sym} CVD ZERO + RANGE [1h+4h]"
        log.info("[%s] %s | %s", sym, title, body.replace("\n", " | "))
        self._spawn(self.ntfy.send(title, body, "high", "warning"))

    async def _eval_loop(self) -> None:
        while not await self._sleep_or_stop(float(self.cfg["eval_interval_sec"])):
            if not self.connected:
                continue
            now = time.time()
            for sym, store in list(self.stores.items()):
                try:
                    self._evaluate_symbol(sym, store, now)
                except Exception as exc:
                    log.exception("[%s] eval error: %s", sym, exc)

    async def _diag_loop(self) -> None:
        while not await self._sleep_or_stop(float(self.cfg["diag_interval_min"]) * 60):
            counts, self.counts = self.counts, {}
            total = sum(counts.values()) or 1
            parts = " ".join(f"{k}={v * 100 // total}%" for k, v in sorted(counts.items(), key=lambda kv: -kv[1])[:10])
            log.info("[DIAG] uptime=%.1fh sent_today=%d | %s",
                     (time.time() - self.start_ts) / 3600.0, self.book.sent_today, parts or "idle")

    async def _save_loop(self) -> None:
        while not await self._sleep_or_stop(float(self.cfg["state_save_interval_sec"])):
            self.book.rollover()
            await self.book.save()

    async def _runtime_guard(self) -> None:
        if self.max_runtime <= 0:
            return
        wait = max(self.max_runtime - 120, 60)
        if await self._sleep_or_stop(wait):
            return
        self.request_stop("max_runtime")

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda s, f, n=sig.name: loop.call_soon_threadsafe(self.request_stop, n))

        await self.ntfy.start()
        await asyncio.to_thread(self.book.load)
        self.book.rollover()

        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                symbols = await self.selector.select(session)
        except Exception as exc:
            log.error("[ENGINE] symbol selection failed: %s", exc)
            await self.ntfy.close()
            return 1
        if not symbols:
            log.error("[ENGINE] no symbols")
            await self.ntfy.close()
            return 1

        for s in symbols:
            self.stores[s] = SymbolStore(s)

        await self._bootstrap_all()

        url = WS_BASE + "?streams=" + "/".join(
            f"{s.lower()}@kline_{self.cfg['base_interval']}" for s in symbols
        )
        log.info("[ENGINE] CVD zero+range | %d symbols | legs=%s | resets=%s | range_on=%s",
                 len(symbols), ",".join(self.cfg["legs"]),
                 ",".join(f"{k}:{v['reset']}" for k, v in self.cfg["legs"].items()),
                 self.cfg["require_range_on"])

        coros = [self._ws_loop(url), self._eval_loop(), self._diag_loop(),
                 self._save_loop(), self._runtime_guard()]
        tasks = [asyncio.create_task(c) for c in coros]
        await self.stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.book.save()
        if self._bg:
            await asyncio.wait(self._bg, timeout=15)
        await self.ntfy.close()
        log.info("[ENGINE] shutdown complete | sent_today=%d", self.book.sent_today)
        return 0


def main() -> None:
    load_dotenv()
    setup_logging()
    code = 0
    try:
        code = asyncio.run(Engine().run())
    except KeyboardInterrupt:
        log.info("[ENGINE] interrupted")
    sys.exit(code)


if __name__ == "__main__":
    main()
