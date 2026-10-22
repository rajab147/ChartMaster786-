#!/usr/bin/env python3
"""Hyperliquid perp monitor: top-N coins by 24h volume -> ntfy alerts.

Stdlib only. Compares each run with the previous one (state.json).
"""
import json
import os
import urllib.request

NTFY_TOPIC = os.environ["NTFY_TOPIC"]
NTFY_SERVER = os.getenv("NTFY_SERVER", "https://ntfy.sh")
STATE_FILE = "state.json"

TOP_N = int(os.getenv("TOP_N", "30"))
# Optional fixed list, e.g. "BTC,ETH,SOL". If set, it replaces the top-N ranking.
COINS = [c.strip().upper() for c in os.getenv("COINS", "").split(",") if c.strip()]

# Thresholds (override via env)
OI_CHANGE_PCT = float(os.getenv("OI_CHANGE_PCT", "2.0"))        # OI move since last run
PRICE_CHANGE_PCT = float(os.getenv("PRICE_CHANGE_PCT", "1.0"))  # price move since last run
FUNDING_HOURLY_PCT = float(os.getenv("FUNDING_HOURLY_PCT", "0.004"))  # abs hourly funding %
BASIS_PCT = float(os.getenv("BASIS_PCT", "0.15"))               # mark vs oracle %
MAX_ALERT_LINES = int(os.getenv("MAX_ALERT_LINES", "25"))


def post_json(url, payload):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def get_market():
    """Return {coin: {...}} for the selected coins."""
    meta, ctxs = post_json("https://api.hyperliquid.xyz/info",
                           {"type": "metaAndAssetCtxs"})
    rows = {}
    for asset, ctx in zip(meta["universe"], ctxs):
        if asset.get("isDelisted"):
            continue
        try:
            mark = float(ctx["markPx"])
            oracle = float(ctx["oraclePx"])
            rows[asset["name"]] = {
                "price": mark,
                "oracle": oracle,
                "oi": float(ctx["openInterest"]),
                "oi_usd": float(ctx["openInterest"]) * mark,
                "funding": float(ctx["funding"]) * 100,  # hourly, in %
                "vol24": float(ctx.get("dayNtlVlm") or 0),
            }
        except (KeyError, TypeError, ValueError):
            continue

    if COINS:
        names = [c for c in COINS if c in rows]
    else:
        names = sorted(rows, key=lambda c: rows[c]["vol24"], reverse=True)[:TOP_N]
    return {c: rows[c] for c in names}


def fmt_price(p):
    return f"{p:,.2f}" if p >= 1 else f"{p:.6g}"


def notify(title, body, priority="high", tags="warning"):
    req = urllib.request.Request(
        f"{NTFY_SERVER}/{NTFY_TOPIC}",
        data=body.encode("utf-8"),
        headers={"Title": title, "Priority": priority, "Tags": tags},
    )
    urllib.request.urlopen(req, timeout=20).read()


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def check_coin(coin, d, prev):
    """Return list of (severity, text) alerts for one coin."""
    out = []

    if prev.get("oi"):
        oi_chg = (d["oi"] - prev["oi"]) / prev["oi"] * 100
        if abs(oi_chg) >= OI_CHANGE_PCT:
            kind = "اضافہ (نئی پوزیشنیں)" if oi_chg > 0 else "کمی (پوزیشنیں بند/لیکویڈیشن)"
            out.append((abs(oi_chg) / OI_CHANGE_PCT, f"OI {kind}: {oi_chg:+.2f}%"))

    if prev.get("price"):
        p_chg = (d["price"] - prev["price"]) / prev["price"] * 100
        if abs(p_chg) >= PRICE_CHANGE_PCT:
            out.append((abs(p_chg) / PRICE_CHANGE_PCT, f"قیمت: {p_chg:+.2f}%"))

    if abs(d["funding"]) >= FUNDING_HOURLY_PCT:
        side = "longs ادا کر رہے ہیں" if d["funding"] > 0 else "shorts ادا کر رہے ہیں"
        out.append((abs(d["funding"]) / FUNDING_HOURLY_PCT,
                    f"Funding {d['funding']:.4f}%/h - {side}"))

    basis = (d["price"] - d["oracle"]) / d["oracle"] * 100 if d["oracle"] else 0
    if abs(basis) >= BASIS_PCT:
        out.append((abs(basis) / BASIS_PCT, f"Basis (mark-oracle): {basis:+.3f}%"))

    return out


def main():
    market = get_market()
    state = load_state()
    new_state = {}
    blocks = []  # (severity, text)

    for coin, d in market.items():
        prev = state.get(coin, {})
        alerts = check_coin(coin, d, prev)
        if alerts:
            sev = max(s for s, _ in alerts)
            lines = "\n".join(f"  - {t}" for _, t in alerts)
            blocks.append((sev, f"{coin} ${fmt_price(d['price'])}\n{lines}"))
        new_state[coin] = {"price": d["price"], "oi": d["oi"]}

    print(f"checked {len(market)} coins | coins with alerts: {len(blocks)}")

    if blocks:
        blocks.sort(key=lambda x: x[0], reverse=True)
        shown = blocks[:MAX_ALERT_LINES]
        body = "\n\n".join(t for _, t in shown)
        if len(blocks) > len(shown):
            body += f"\n\n... اور {len(blocks) - len(shown)} کوائنز"
        notify(f"Crypto Alert ({len(blocks)} coins)", body[:3800])

    with open(STATE_FILE, "w") as f:
        json.dump(new_state, f)


if __name__ == "__main__":
    main()
