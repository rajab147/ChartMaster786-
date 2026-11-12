#!/usr/bin/env python3
"""
cvd_zero_cross.py
SEPARATE ALERT SYSTEM: CVD Zero Cross / Zero Proximity Alerts

Philosophy:
- CVD near zero = market equilibrium = potential breakout
- Price often ranges when CVD is near zero
- When BOTH 1h AND 4h CVD are near zero = high probability squeeze
- This is SEPARATE from the main order-flow system

Alerts only. No trading. Binance Spot WebSocket only.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

import aiohttp
import websockets
from dotenv import load_dotenv


CONFIG: dict[str, Any] = {
    "max_symbols": 100,
    "min_quote_volume_24h": 10_000_000,
    "min_trades_24h": 10_000,
    "max_price_change_24h_pct": 30.0,
    "eval_interval_sec": 3,
    "bucket_seconds": 5,
    "diag_interval_min": 30,
    "max_alerts_per_day": 100,
    "depth_levels": 20,
    "depth_update_ms": 100,
    "stale_seconds": 30,
    "ws_recv_timeout_sec": 30,
    "reconnect_wipe_after_sec": 120,
    "reconnect_backoff_max_sec": 60,
    "reconnect_fail_alert_after": 3,
    "state_save_interval_sec": 60,
    "http_timeout_sec": 60,
    "http_retries": 3,
    "ntfy_timeout_sec": 10,
    "ntfy_min_gap_sec": 1.1,
    "warmup_seconds": 900,

    # ===== ZERO CROSS CONFIG =====
    "timeframes": {
        "1h": {"seconds": 3600},
        "4h": {"seconds": 14400},
    },
    "zero_proximity_pct": 0.5,  # CVD within 0.5% of total volume = near zero
    "cooldown_min": 120,  # 2 hours between alerts per symbol
    "min_volume_usdt": 100_000,
    "min_trades": 200,

    "sessions": {
        "enabled": True,
        "definitions": {
            "Asia": [0, 8],
            "London": [7, 16],
            "New York": [12, 21],
        },
    },

    "health": {
        "silent_limit_sec": 120,
        "check_interval_sec": 60,
        "alert_on_silent": True,
    },
}


REST_BASE = os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision").rstrip("/")
TICKER_URL = REST_BASE + "/api/v3/ticker/24hr"
INFO_URL = REST_BASE + "/api/v3/exchangeInfo"
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision/stream")
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
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    for h in (fh, ch):
        h.setFormatter(fmt)
        log.addHandler(h)


def utc_day(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), timezone.utc).strftime("%Y-%m-%d")


class SymbolSelector:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg

    @staticmethod
    def is_excluded_base(base: str, trading_bases: set[str]) -> bool:
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
                        ra = resp.headers.get("Retry-After", "5")
                        try:
                            wait = min(float(ra), 60.0)
                        except ValueError:
                            wait = 5.0
                        log.warning("[CVD_ZERO] REST rate limited (%s), waiting %.0fs", resp.status, wait)
                        await asyncio.sleep(wait)
                        raise RuntimeError(f"rate limited {resp.status}")
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as exc:
                last = exc
                log.warning("[CVD_ZERO] REST %s failed (attempt %d): %s", url, attempt, exc)
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"REST request failed: {url}: {last}")

    def _check(self, t: dict[str, Any], trading: set[str], trading_bases: set[str]) -> dict[str, Any] | None:
        sym = str(t["symbol"])
        if not sym.endswith("USDT") or sym not in trading:
            return None
        base = sym[:-4]
        if not base or self.is_excluded_base(base, trading_bases):
            return None
        qv = float(t["quoteVolume"])
        cnt = int(t["count"])
        chg = float(t["priceChangePercent"])
        if qv < self.cfg["min_quote_volume_24h"] or cnt < self.cfg["min_trades_24h"]:
            return None
        if abs(chg) > self.cfg["max_price_change_24h_pct"]:
            return None
        return {"symbol": sym, "quote_volume": qv, "count": cnt, "change_pct": chg}

    async def select(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        tickers, info = await asyncio.gather(self._get(session, TICKER_URL), self._get(session, INFO_URL))
        symbols = [s for s in info.get("symbols", []) if s.get("status") == "TRADING"]
        ticks: dict[str, float] = {}
        for s in symbols:
            for flt in s.get("filters", []):
                if flt.get("filterType") == "PRICE_FILTER":
                    try:
                        ticks[s["symbol"]] = float(flt.get("tickSize", 0.0))
                    except (TypeError, ValueError):
                        pass
        trading = {s["symbol"] for s in symbols}
        trading_bases = {str(s.get("baseAsset", "")) for s in symbols}
        usdt = [t for t in tickers if str(t.get("symbol", "")).endswith("USDT")]
        picked: list[dict[str, Any]] = []
        for t in usdt:
            try:
                item = self._check(t, trading, trading_bases)
            except (KeyError, ValueError, TypeError):
                continue
            if item:
                item["tick"] = ticks.get(item["symbol"], 0.0)
                picked.append(item)
        picked.sort(key=lambda x: x["quote_volume"], reverse=True)
        picked = picked[: int(self.cfg["max_symbols"])]
        log.info("[CVD_ZERO] Selected %d symbols after filtering %d total USDT pairs", len(picked), len(usdt))
        return picked


class CVDTracker:
    """Track absolute CVD (not percentage) for zero-cross detection."""
    __slots__ = ("window", "bucket", "buckets", "buy_vol", "sell_vol", "count", "cvd", "start_ts")

    def __init__(self, seconds: float, bucket_sec: float) -> None:
        self.window = float(seconds)
        self.bucket = float(bucket_sec)
        self.reset()

    def reset(self) -> None:
        self.buckets: deque[list[float]] = deque()
        self.buy_vol = self.sell_vol = 0.0
        self.count = 0
        self.cvd = 0.0
        self.start_ts = 0.0

    def _current(self, now: float) -> list[float]:
        bid = int(now // self.bucket)
        if not self.buckets or self.buckets[-1][0] != bid:
            self.buckets.append([bid, 0.0, 0.0, 0, 0.0])  # id, buy, sell, count, bucket_cvd
        return self.buckets[-1]

    def add_trade(self, price: float, quote: float, is_buy: bool, now: float) -> None:
        if self.start_ts == 0.0:
            self.start_ts = now
        b = self._current(now)
        if is_buy:
            b[1] += quote
            self.buy_vol += quote
            self.cvd += quote
            b[4] += quote
        else:
            b[2] += quote
            self.sell_vol += quote
            self.cvd -= quote
            b[4] -= quote
        b[3] += 1
        self.count += 1
        self.evict(now)

    def evict(self, now: float) -> None:
        limit = now - self.window
        while self.buckets and (self.buckets[0][0] + 1) * self.bucket <= limit:
            _, buy, sell, cnt, bucket_cvd = self.buckets.popleft()
            self.buy_vol -= buy
            self.sell_vol -= sell
            self.count -= int(cnt)
            self.cvd -= bucket_cvd
        if self.count <= 0:
            self.count, self.buy_vol, self.sell_vol, self.cvd = 0, 0.0, 0.0, 0.0
        if not self.buckets:
            self.start_ts = 0.0

    def fill(self, now: float) -> float:
        if self.start_ts == 0.0:
            return 0.0
        return min(1.0, max(0.0, (now - self.start_ts) / self.window))

    @property
    def total_volume(self) -> float:
        return max(self.buy_vol + self.sell_vol, 0.0)

    @property
    def trade_count(self) -> int:
        return self.count

    def proximity_to_zero_pct(self) -> float:
        """How close is CVD to zero, as % of total volume?"""
        if self.total_volume <= 0:
            return 100.0
        return abs(self.cvd) / self.total_volume * 100.0

    def is_near_zero(self, threshold_pct: float) -> bool:
        """Is CVD within threshold% of total volume from zero?"""
        return self.proximity_to_zero_pct() <= threshold_pct


class CVDState:
    def __init__(self, symbol: str, timeframes: dict[str, dict[str, Any]], bucket_sec: float) -> None:
        self.symbol = symbol
        self.windows = {name: CVDTracker(tf["seconds"], bucket_sec) for name, tf in timeframes.items()}
        self.last_price = 0.0
        self.last_update = 0.0

    def reset(self) -> None:
        for w in self.windows.values():
            w.reset()
        self.last_price = 0.0
        self.last_update = 0.0

    def add_trade(self, price: float, qty: float, buyer_is_maker: bool, now: float) -> None:
        quote = price * qty
        is_buy = not buyer_is_maker
        for w in self.windows.values():
            w.add_trade(price, quote, is_buy, now)
        self.last_price = price
        self.last_update = now

    def is_stale(self, now: float, limit: float) -> bool:
        return self.last_update == 0.0 or now - self.last_update > limit


def current_sessions(now: float, cfg: dict[str, Any]) -> list[str]:
    defs = cfg.get("definitions", {})
    hour = datetime.fromtimestamp(now, timezone.utc).hour
    out = []
    for name, rng in defs.items():
        try:
            s, e = int(rng[0]), int(rng[1])
        except Exception:
            continue
        if s <= e:
            if s <= hour < e:
                out.append(name)
        else:
            if hour >= s or hour < e:
                out.append(name)
    return sorted(out)


class NtfyAlerter:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.url = os.getenv("NTFY_URL", "").strip()
        self.token = os.getenv("NTFY_TOKEN", "").strip()
        self.session: aiohttp.ClientSession | None = None
        self.min_gap = float(cfg.get("ntfy_min_gap_sec", 1.1))
        self._lock: asyncio.Lock | None = None
        self._next_slot = 0.0
        if not self.url:
            log.warning("[CVD_ZERO] ntfy disabled (NTFY_URL not set); alerts are logged only")

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.cfg["ntfy_timeout_sec"])
        self.session = aiohttp.ClientSession(timeout=timeout)
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    async def _throttle(self) -> None:
        if self._lock is None:
            return
        async with self._lock:
            wait = self._next_slot - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._next_slot = time.monotonic() + self.min_gap

    async def send(self, title: str, body: str, priority: str = "high", tags: str = "") -> bool:
        if not self.url or self.session is None:
            return False
        headers = {"Title": title.encode("ascii", "ignore").decode(), "Priority": priority}
        if tags:
            headers["Tags"] = tags
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        for attempt in (1, 2):
            await self._throttle()
            try:
                async with self.session.post(self.url, data=body.encode("utf-8"), headers=headers) as resp:
                    if resp.status == 429 and attempt == 1:
                        ra = resp.headers.get("Retry-After", "5")
                        try:
                            wait = min(float(ra), 30.0)
                        except ValueError:
                            wait = 5.0
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 300:
                        log.warning("[CVD_ZERO] ntfy HTTP %s for '%s'", resp.status, title)
                        return False
                    return True
            except Exception as exc:
                log.warning("[CVD_ZERO] ntfy send failed for '%s': %s", title, exc)
                if attempt == 1:
                    await asyncio.sleep(2.0)
        return False


class HealthMonitor:
    def __init__(self, cfg: dict[str, Any], alerter: NtfyAlerter) -> None:
        h = cfg.get("health", {})
        self.silent_limit = float(h.get("silent_limit_sec", 120))
        self.check_interval = float(h.get("check_interval_sec", 60))
        self.alert_on_silent = bool(h.get("alert_on_silent", True))
        self.alerter = alerter
        self.spawn: Any = None
        self.start_ts = time.time()
        self.last_msg_ts = 0.0
        self.window_msgs = 0
        self.total_signals = 0
        self.eval_cycles = 0
        self.eval_errors = 0
        self.last_check_ts = 0.0
        self.last_rate = 0.0
        self.silent_alerted = False

    def note_message(self, kind: str, now: float) -> None:
        self.window_msgs += 1
        self.last_msg_ts = now
        self.silent_alerted = False

    def note_signal(self) -> None:
        self.total_signals += 1

    def note_eval(self, error: bool = False) -> None:
        self.eval_cycles += 1
        if error:
            self.eval_errors += 1

    def check(self, now: float, connected: bool) -> None:
        if self.last_check_ts == 0.0:
            self.last_check_ts = now
            return
        elapsed = now - self.last_check_ts
        if elapsed < self.check_interval:
            return
        self.last_rate = self.window_msgs / max(elapsed, 1.0)
        self.window_msgs = 0
        self.last_check_ts = now
        if connected and self.last_msg_ts > 0 and (now - self.last_msg_ts) > self.silent_limit:
            if not self.silent_alerted and self.alert_on_silent and self.spawn is not None:
                self.silent_alerted = True
                gap = now - self.last_msg_ts
                self.spawn(self.alerter.send(
                    "CVD Zero bot: data stream silent",
                    f"No WebSocket message for {gap:.0f}s while connected. Bot may be stuck.",
                    "high",
                    "warning",
                ))

    def summary(self, now: float) -> str:
        up = (now - self.start_ts) / 3600.0
        return (
            f"uptime={up:.1f}h msg_rate={self.last_rate:.1f}/s signals={self.total_signals} "
            f"eval_cycles={self.eval_cycles} eval_errors={self.eval_errors}"
        )


class AlertBook:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.day = utc_day()
        self.alerts_today = 0
        self.last_alert: dict[str, float] = {}
        self.today_symbols: dict[str, int] = {}
        self.today_counts: dict[str, int] = {}

    def load(self) -> None:
        if not os.path.exists(STATE_FILE):
            log.info("[CVD_ZERO] No saved state; starting fresh")
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.day = str(d.get("day", self.day))
            self.alerts_today = int(d.get("alerts_today", 0))
            self.last_alert = {k: float(v) for k, v in d.get("last_alert", {}).items()}
            self.today_symbols = {k: int(v) for k, v in d.get("today_symbols", {}).items()}
            self.today_counts = {k: int(v) for k, v in d.get("today_counts", {}).items()}
            log.info("[CVD_ZERO] Loaded state: day=%s alerts_today=%d", self.day, self.alerts_today)
        except Exception as exc:
            log.error("[CVD_ZERO] Corrupt state file (%s); starting fresh", exc)

    def snapshot(self) -> dict[str, Any]:
        return {
            "day": self.day,
            "alerts_today": self.alerts_today,
            "last_alert": self.last_alert,
            "today_symbols": self.today_symbols,
            "today_counts": self.today_counts,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _write_atomic(data: dict[str, Any]) -> None:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)

    async def save(self) -> None:
        try:
            await asyncio.to_thread(self._write_atomic, self.snapshot())
        except Exception as exc:
            log.error("[CVD_ZERO] State save failed: %s", exc)

    def on_cooldown(self, key: str, now: float, minutes: float) -> bool:
        return now - self.last_alert.get(key, 0.0) < minutes * 60

    def cap_reached(self) -> bool:
        return self.alerts_today >= self.cfg["max_alerts_per_day"]

    def register(self, key: str, symbol: str, now: float) -> None:
        self.alerts_today += 1
        self.last_alert[key] = now
        self.today_symbols[symbol] = self.today_symbols.get(symbol, 0) + 1
        self.today_counts["ZERO_SQUEEZE"] = self.today_counts.get("ZERO_SQUEEZE", 0) + 1

    def build_summary(self, label: str) -> str:
        ranked = sorted(self.today_symbols.items(), key=lambda kv: kv[1], reverse=True)
        top = ", ".join(f"{s} x{n}" for s, n in ranked[:5]) or "n/a"
        lines = [f"CVD Zero Summary {label}", f"Alerts sent: {self.alerts_today}"]
        lines += [f"{k}: {v}" for k, v in sorted(self.today_counts.items())]
        lines.append(f"Most active: {top}")
        return "\n".join(lines)

    def reset_day(self, new_day: str) -> None:
        self.day, self.alerts_today = new_day, 0
        self.today_symbols, self.today_counts = {}, {}
        cutoff = time.time() - 86400
        self.last_alert = {k: v for k, v in self.last_alert.items() if v > cutoff}


class CVDEngine:
    def __init__(self) -> None:
        self.cfg = CONFIG
        self.book = AlertBook(self.cfg)
        self.alerter = NtfyAlerter(self.cfg)
        self.health = HealthMonitor(self.cfg, self.alerter)
        self.health.spawn = self._spawn
        self.states: dict[str, CVDState] = {}
        self.stop = asyncio.Event()
        self.connected = False
        self.stream_start = time.time()
        self.last_msg_ts = 0.0
        self._wiped_since_msg = False
        self.block_counts: dict[str, int] = {}
        self._bg: set[asyncio.Task[Any]] = set()

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    def request_stop(self, why: str) -> None:
        if not self.stop.is_set():
            log.info("[CVD_ZERO] Stop requested (%s)", why)
            self.stop.set()

    async def _sleep_or_stop(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=max(seconds, 0.0))
            return True
        except asyncio.TimeoutError:
            return False

    def _count(self, key: str) -> None:
        self.block_counts[key] = self.block_counts.get(key, 0) + 1

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda s, f, n=sig.name: loop.call_soon_threadsafe(self.request_stop, n))

        lvl = int(self.cfg["depth_levels"])
        dms = int(self.cfg["depth_update_ms"])
        await self.alerter.start()
        await asyncio.to_thread(self.book.load)

        try:
            timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
            async with aiohttp.ClientSession(timeout=timeout) as session:
                picked = await SymbolSelector(self.cfg).select(session)
        except Exception as exc:
            log.error("[CVD_ZERO] Symbol selection failed: %s", exc)
            if "451" in str(exc):
                log.error(
                    "[CVD_ZERO] HTTP 451 = Binance blocks this server's region. Run from a "
                    "non-US/non-restricted host (VPS), or set BINANCE_REST_BASE / BINANCE_WS_BASE."
                )
            await self.alerter.close()
            return 1

        symbols = [p["symbol"] for p in picked]
        if not symbols:
            log.error("[CVD_ZERO] No symbols passed the filters; exiting")
            await self.alerter.close()
            return 1

        tf_cfg = self.cfg["timeframes"]
        for s in symbols:
            self.states[s] = CVDState(s, tf_cfg, self.cfg["bucket_seconds"])

        if self.book.day != utc_day():
            await self._rollover()

        url = WS_BASE + "?streams=" + "/".join(
            f"{s.lower()}@aggTrade/{s.lower()}@depth{lvl}@{dms}ms" for s in symbols
        )
        log.info(
            "[CVD_ZERO] Starting CVD zero-cross alerts: %d symbols | "
            "timeframes=1h+4h | proximity=%.2f%% | cooldown=%dm",
            len(symbols),
            float(self.cfg["zero_proximity_pct"]),
            int(self.cfg["cooldown_min"]),
        )

        coros = [
            self._ws_loop(url),
            self._eval_loop(),
            self._save_loop(),
            self._daily_loop(),
            self._diag_loop(),
        ]
        tasks = [asyncio.create_task(c) for c in coros]
        await self.stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._shutdown()
        return 0

    async def _shutdown(self) -> None:
        await self.book.save()
        if self._bg:
            await asyncio.wait(self._bg, timeout=self.cfg["ntfy_timeout_sec"])
        await self.alerter.close()
        log.info("[CVD_ZERO] Shutdown complete | alerts_today=%d", self.book.alerts_today)

    def _handle_message(self, raw: str | bytes) -> None:
        msg = json.loads(raw)
        stream, data = msg["stream"], msg["data"]
        now = time.time()
        self.last_msg_ts = now
        self._wiped_since_msg = False
        self.health.note_message("trade" if "@aggTrade" in stream else "depth", now)
        sym = stream.split("@", 1)[0].upper()
        st = self.states.get(sym)
        if st is None:
            return
        try:
            if "@aggTrade" in stream:
                st.add_trade(float(data["p"]), float(data["q"]), bool(data["m"]), now)
        except (KeyError, ValueError, TypeError, IndexError) as exc:
            log.warning("[%s] Bad %s message skipped: %s", sym, stream.split("@", 1)[1], exc)

    def _on_connect(self) -> None:
        self.stream_start = time.time()

    async def _prepare_state(self) -> None:
        gap = time.time() - self.last_msg_ts if self.last_msg_ts else float("inf")
        if gap <= self.cfg["reconnect_wipe_after_sec"] or self._wiped_since_msg:
            return
        for st in self.states.values():
            st.reset()
        self._wiped_since_msg = True
        log.info(
            "[CVD_ZERO] CVD trackers wiped (%s)",
            "start" if not self.last_msg_ts else f"gap {gap:.0f}s",
        )

    async def _ws_loop(self, url: str) -> None:
        fails, alerted = 0, False
        while not self.stop.is_set():
            got_data = False
            await self._prepare_state()
            try:
                log.info("[CVD_ZERO] Connecting Spot WebSocket (%d chars URL)", len(url))
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20, max_size=None, close_timeout=5
                ) as ws:
                    self._on_connect()
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg["ws_recv_timeout_sec"])
                        if not got_data:
                            got_data, fails, alerted, self.connected = True, 0, False, True
                            log.info("[CVD_ZERO] Spot WebSocket connected, receiving data")
                        try:
                            self._handle_message(raw)
                        except Exception as exc:
                            log.warning("[CVD_ZERO] Malformed message skipped: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("[CVD_ZERO] Spot WebSocket closed/error: %s: %s", type(exc).__name__, exc)
                self.connected = False
                if self.stop.is_set():
                    break
                if got_data:
                    delay = 1.0
                    log.info("[CVD_ZERO] Reconnecting Spot WS after session end in %.0fs", delay)
                else:
                    fails += 1
                    delay = min(2.0 ** (fails - 1), self.cfg["reconnect_backoff_max_sec"])
                    log.warning("[CVD_ZERO] Spot WS reconnect failure #%d, retry in %.0fs", fails, delay)
                    if fails >= self.cfg["reconnect_fail_alert_after"] and not alerted:
                        alerted = True
                        self._spawn(self.alerter.send(
                            "CVD Zero: Spot WebSocket reconnect failing",
                            f"{fails} consecutive reconnect failures.",
                            "high",
                            "warning",
                        ))
                if await self._sleep_or_stop(delay):
                    break

    async def _eval_loop(self) -> None:
        while not await self._sleep_or_stop(self.cfg["eval_interval_sec"]):
            if not self.connected:
                continue
            now = time.time()
            self.health.note_eval()
            self.health.check(now, self.connected)
            for sym, st in list(self.states.items()):
                try:
                    self._evaluate(sym, st, now)
                except Exception as exc:
                    self.health.note_eval(error=True)
                    log.exception("[%s] Evaluation error: %s", sym, exc)

    def _evaluate(self, sym: str, st: CVDState, now: float) -> None:
        for w in st.windows.values():
            w.evict(now)

        # Global blocks
        if st.is_stale(now, self.cfg["stale_seconds"]) or st.last_price <= 0:
            self._count("all:stale")
            return
        if now - self.stream_start < self.cfg["warmup_seconds"]:
            self._count("all:warmup")
            return

        # Check all timeframes
        checks = {}
        for name, w in st.windows.items():
            if w.fill(now) < 0.50:
                self._count(f"{name}:underfill")
                return
            if self.cfg["min_volume_usdt"] > 0 and w.total_volume < self.cfg["min_volume_usdt"]:
                self._count(f"{name}:low_volume")
                return
            if self.cfg["min_trades"] > 0 and w.trade_count < self.cfg["min_trades"]:
                self._count(f"{name}:low_trades")
                return

            prox = w.proximity_to_zero_pct()
            near_zero = w.is_near_zero(self.cfg["zero_proximity_pct"])
            checks[name] = {
                "cvd": w.cvd,
                "total_vol": w.total_volume,
                "proximity_pct": prox,
                "near_zero": near_zero,
                "trades": w.trade_count,
            }

        # ALL timeframes must be near zero
        if not all(c["near_zero"] for c in checks.values()):
            self._count("flow:not_near_zero")
            return

        # Cooldown check
        key = f"{sym}|zero_squeeze"
        if self.book.on_cooldown(key, now, float(self.cfg["cooldown_min"])):
            self._count("zero:cooldown")
            return

        if self.book.cap_reached():
            self._count("zero:daily_cap")
            return

        self._count("zero:signal")
        self._send_alert(sym, st, checks, now, key)

    def _send_alert(
        self,
        sym: str,
        st: CVDState,
        checks: dict[str, dict[str, Any]],
        now: float,
        key: str,
    ) -> None:
        sessions = current_sessions(now, self.cfg.get("sessions", {}))
        lines = [
            f"Symbol: {sym}",
            f"Alert Type: CVD ZERO SQUEEZE",
            f"Current Price: {st.last_price:.8g}",
            "",
            "--- CVD Analysis ---",
        ]
        for name, c in checks.items():
            lines.append(f"{name} CVD: {c['cvd']:+,.0f} USDT")
            lines.append(f"{name} Proximity to Zero: {c['proximity_pct']:.2f}%")
            lines.append(f"{name} Total Volume: {c['total_vol']:,.0f} USDT")
            lines.append(f"{name} Trades: {c['trades']}")
            lines.append("")

        lines.append("--- Interpretation ---")
        lines.append("Both 1h AND 4h CVD are near zero")
        lines.append("Market is in equilibrium (potential breakout setup)")
        lines.append("Price is likely ranging - watch for direction")
        lines.append("")
        lines.append(f"Sessions: {', '.join(sessions) or 'none'}")
        lines.append(
            f"Alert Timestamp: {datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )

        body = "\n".join(lines)
        title = f"{sym} CVD ZERO SQUEEZE [1h+4h]"
        tags = "warning"

        self.book.register(key, sym, now)
        self.health.note_signal()
        log.info("[%s] %s | %s", sym, title, body.replace("\n", " | "))
        self._spawn(self.alerter.send(title, body, "high", tags))

    async def _diag_loop(self) -> None:
        minutes = self.cfg["diag_interval_min"]
        while not await self._sleep_or_stop(minutes * 60):
            if not self.connected:
                continue
            now = time.time()
            log.info("[DIAG] health: %s", self.health.summary(now))
            counts, self.block_counts = self.block_counts, {}
            groups: dict[str, dict[str, int]] = {}
            for k, n in counts.items():
                grp, reason = k.split(":", 1)
                groups.setdefault(grp, {})[reason] = n
            for grp, d in sorted(groups.items()):
                tot = sum(d.values())
                parts = " ".join(f"{r}={n / tot * 100:.0f}%" for r, n in sorted(d.items(), key=lambda kv: -kv[1]))
                log.info("[DIAG] %s gates over last %dmin: %s", grp, minutes, parts)

    async def _save_loop(self) -> None:
        while not await self._sleep_or_stop(self.cfg["state_save_interval_sec"]):
            await self.book.save()
            log.info("[CVD_ZERO] State saved | alerts_today=%d connected=%s", self.book.alerts_today, self.connected)

    async def _rollover(self) -> None:
        label = self.book.day
        summary = self.book.build_summary(label)
        log.info("[CVD_ZERO] Daily rollover:\n%s", summary)
        self._spawn(self.alerter.send(f"CVD Zero Summary {label}", summary, "default", "chart_with_upwards_trend"))
        self.book.reset_day(utc_day())
        await self.book.save()

    async def _daily_loop(self) -> None:
        while True:
            now = datetime.now(timezone.utc)
            nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=1, microsecond=0)
            if await self._sleep_or_stop((nxt - now).total_seconds()):
                return
            await self._rollover()


def main() -> None:
    load_dotenv()
    setup_logging()
    code = 0
    try:
        code = asyncio.run(CVDEngine().run())
    except KeyboardInterrupt:
        log.info("[CVD_ZERO] Interrupted")
    sys.exit(code)


if __name__ == "__main__":
    main()