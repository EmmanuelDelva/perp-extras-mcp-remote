#!/usr/bin/env python3
"""
delva-perp-extras — MCP a medida para day trading de perpetuos (Binance USDT-M).

18 tools en 6 grupos: análisis (multi_tf_snapshot, resample_ohlcv), posicionamiento
(long_short_and_oi, positioning), ahora (realtime_pulse), plan (trade_plan), gatillos de la
skill (wldlive/wldlivenow/wldlivefull + genéricos live/livenow/livefull + watchlist_scan),
ciclo de trade (trade_open, actualizame, trade_close, journal) y diagnóstico (venue_health).

Resiliencia (v4): cadena de venues binanceusdm→bybit→okx con DEMOCIÓN EN CALIENTE — si el
venue activo empieza a fallar a media sesión (p.ej. ban 418 de Binance sobre la IP compartida
de Render), se marca su ventana de ban, se re-sondea la cadena y el builder se reintenta UNA
vez sobre el siguiente venue vivo. Símbolos laxos ('BTC', 'BTCUSDT') y resolución por venue
(Binance 1000SHIB ↔ Bybit SHIB1000 ↔ OKX SHIB). Sin inventos: dato faltante -> null,
degradación siempre rotulada (venue/degraded en cada respuesta).
"""
from __future__ import annotations
import concurrent.futures as cf
import json
import os
import re
import time
from pathlib import Path
import ccxt
import pandas as pd
from mcp.server.fastmcp import FastMCP

STATE_DIR = Path(__file__).parent / "state"
STATE_DIR.mkdir(exist_ok=True)
ACTIVE_FILE = STATE_DIR / "active_trade.json"
JOURNAL_FILE = STATE_DIR / "journal.jsonl"

mcp = FastMCP("delva-perp-extras")

NATIVE_MIN = {
    '1m': 1, '3m': 3, '5m': 5, '15m': 15, '30m': 30, '1h': 60, '2h': 120,
    '4h': 240, '6h': 360, '8h': 480, '12h': 720, '1d': 1440, '3d': 4320, '1w': 10080,
}
ALL_NATIVE = list(NATIVE_MIN.keys())


# Cadena de venues: Binance geobloquea IPs de datacenter US (p.ej. Render Oregon) ->
# sondear en orden y quedarse con el primer venue VIVO (1 sondeo por proceso).
VENUES = [v.strip() for v in os.environ.get("PERP_VENUES", "binanceusdm,bybit,okx").split(",") if v.strip()]
_venue_cache: dict = {}
_active_venue: list = []
_venue_errors: dict = {}       # venue -> último error de probe (depurar geo-block sin Shell)
_last_probe_ts: list = [0.0]   # time.monotonic() del último sondeo completo
_REPROBE_SECS = 300            # si el activo NO es el preferido, reintenta la cadena cada 5 min
_venue_ban_until: dict = {}    # venue -> epoch_s hasta el que Binance lo baneó (418/-1003)


def _parse_ban_until(msg: str):
    """Epoch (s) de un ban 418 de Binance: '...banned until 1783714675643.' (ms) -> segundos."""
    m = re.search(r"banned until (\d+)", msg)
    return int(m.group(1)) / 1000.0 if m else None


SERVER_BUILD = "2026-07-10 v4.0 (failover en caliente + multi-moneda)"

# Fallo A NIVEL VENUE (ban/geo/caída/red) vs error de símbolo o parámetro. Solo el
# primero justifica demover el venue activo y reintentar en el siguiente de la cadena.
_VENUE_FAIL_RE = re.compile(
    r"418|-1003|banned until|DDoSProtection|RateLimitExceeded|ExchangeNotAvailable"
    r"|NetworkError|RequestTimeout|restricted location|451|403|502|503",
    re.I,
)


def _is_venue_failure(e: Exception) -> bool:
    return bool(_VENUE_FAIL_RE.search(f"{type(e).__name__}: {e}"))


def _demote_active(e: Exception) -> bool:
    """El venue activo empezó a fallar A MEDIA SESIÓN (p.ej. Binance banea la IP con el
    proceso ya clavado en binanceusdm, que por ser el preferido nunca se re-sondea):
    registra el error, marca su ventana de ban, suelta el venue y fuerza re-sondeo.
    Devuelve True si había venue activo que soltar (=> vale reintentar una vez)."""
    if not _active_venue:
        return False
    v = _active_venue[0]
    _venue_errors[v] = f"{type(e).__name__}: {str(e)[:180]}"
    bu = _parse_ban_until(str(e))
    _venue_ban_until[v] = bu or (time.time() + 120)  # sin 'banned until' explícito: castigo corto
    _active_venue[:] = []
    _last_probe_ts[0] = 0.0
    return True


def _with_failover(fn):
    """Ejecuta fn(); si el venue activo falla A NIVEL VENUE, demueve y reintenta UNA vez
    (el reintento ya corre sobre el siguiente venue vivo de la cadena)."""
    try:
        return fn()
    except Exception as e:
        if _is_venue_failure(e) and _demote_active(e):
            return fn()
        raise


def _mk(venue: str):
    ex = getattr(ccxt, venue)({'enableRateLimit': True})
    # pool amplio: el snapshot dispara ~15 requests concurrentes (fix del TF caído por pool=10)
    try:
        import requests.adapters as _ra
        ex.session.mount('https://', _ra.HTTPAdapter(pool_connections=20, pool_maxsize=20))
    except Exception:
        pass
    return ex


def _probe(v):
    ex = _venue_cache.get(v) or _mk(v)
    _venue_cache[v] = ex
    ex.fetch_time()  # geobloqueo/venue caído -> excepción
    return ex


def _ex():
    # Reutiliza el venue activo, salvo que NO sea el preferido y ya toque re-sondear:
    # así un bloqueo TRANSITORIO de binanceusdm no deja el proceso pegado a Bybit para siempre.
    now = time.monotonic()
    if _active_venue and (_active_venue[0] == VENUES[0] or now - _last_probe_ts[0] < _REPROBE_SECS):
        return _venue_cache[_active_venue[0]]
    _last_probe_ts[0] = now
    last = None
    wall = time.time()
    for v in VENUES:
        if _venue_ban_until.get(v, 0) > wall:  # aún baneado -> NO sondear (evita EXTENDER el ban)
            _venue_errors[v] = f"baneado hasta epoch {int(_venue_ban_until[v])} — no se sondea"
            continue
        try:
            ex = _probe(v)
            _venue_errors.pop(v, None)
            _active_venue[:] = [v]
            return ex
        except Exception as e:
            _venue_errors[v] = f"{type(e).__name__}: {str(e)[:180]}"
            bu = _parse_ban_until(str(e))
            if bu:
                _venue_ban_until[v] = bu  # respeta la ventana de ban en próximos sondeos
            last = e
    if _active_venue:  # el re-sondeo falló pero ya teníamos un venue vivo -> úsalo
        return _venue_cache[_active_venue[0]]
    raise RuntimeError(f"ningún venue vivo en {VENUES}: {str(last)[:120]}")


# Rótulo honesto del venue activo (anti-inventos): cada respuesta declara de dónde
# salieron los datos y marca la degradación cuando Binance no está accesible (geo-block).
_VENUE_LABEL = {"binanceusdm": "Binance USDⓈ-M", "bybit": "Bybit perp", "okx": "OKX swap"}

def _norm_symbol(symbol: str) -> str:
    """Acepta formato laxo ('WLD', 'btc', 'BTCUSDT', 'BTC/USDT') y devuelve el unificado
    ccxt del perp lineal: 'BTC/USDT:USDT'. Los ya-unificados pasan intactos."""
    s = (symbol or "").strip().upper()
    if not s:
        return "WLD/USDT:USDT"
    if ":" in s:
        return s
    if "/" in s:
        return f"{s}:USDT"
    if s.endswith("USDT") and len(s) > 4:
        s = s[:-4]
    return f"{s}/USDT:USDT"


def _resolve_symbol(ex, symbol: str) -> str:
    """Símbolo tal como existe en el venue ACTIVO. Cubre los renombres de memes entre
    venues (Binance '1000SHIB' ↔ Bybit 'SHIB1000'). SEGURIDAD DE ESCALA (comité 2026-07-10):
    solo se cruzan variantes de la MISMA familia de contrato 1000-unidades; NUNCA se mapea
    en silencio '1000SHIB' -> 'SHIB' (el precio cambia 1000x y contamina stops/sizing).
    Si ninguna variante segura lista en este venue -> ValueError honesto (no demueve venue)."""
    symbol = _norm_symbol(symbol)
    try:
        if not getattr(ex, "markets", None):  # por INSTANCIA (una fresca post-democión trae markets=None)
            ex.load_markets()
    except Exception:
        return symbol  # sin markets no se puede validar: dejar pasar al error real del fetch
    if symbol in ex.markets:
        return symbol
    base = symbol.split("/")[0]
    if base.startswith("1000"):
        cands = [f"{base[4:]}1000"]                # 1000SHIB -> SHIB1000 (misma escala)
    elif base.endswith("1000"):
        cands = [f"1000{base[:-4]}"]               # SHIB1000 -> 1000SHIB (misma escala)
    else:
        cands = [f"1000{base}", f"{base}1000"]     # atajo 'SHIB' -> contrato canónico (escala visible en symbol y price)
    for b in cands:
        alt = f"{b}/USDT:USDT"
        if alt in ex.markets:
            return alt
    raise ValueError(f"{symbol} no listado en {ex.id} (variantes de misma escala probadas: {', '.join(cands)})")


def _venue_meta() -> dict:
    """Metadatos del venue activo para que cada gatillo NUNCA se rotule 'Binance' si sirve otro."""
    vid = _ex().id
    is_binance = vid == "binanceusdm"
    meta = {"venue": vid, "venue_label": _VENUE_LABEL.get(vid, vid), "binance": is_binance}
    if not is_binance:
        meta["degraded"] = (
            f"⚠️ Binance no accesible desde este host (geo-block o ban de rate-limit) — "
            f"datos de {_VENUE_LABEL.get(vid, vid)}. "
            "Funding/OI/L-S son de ESTE venue, no Binance; "
            "top-trader smart-money (Binance-only) no disponible."
        )
        err = _venue_errors.get("binanceusdm")
        if err:
            meta["binance_error"] = err
    return meta


def build_venue_health() -> dict:
    active = _ex().id
    now = time.time()
    return {
        "active_venue": active,
        "binance": active == "binanceusdm",
        "build": SERVER_BUILD,
        "chain": VENUES,
        "reprobe_secs": _REPROBE_SECS,
        "errors": dict(_venue_errors),
        "banned_until": {k: int(v) for k, v in _venue_ban_until.items() if v > now},
    }


@mcp.tool()
def venue_health() -> dict:
    """Diagnóstico de venue: cuál está activo, la cadena de fallback y el último error de probe de
    cada venue (para depurar el geo-block de Binance sin Shell). Fuerza un re-sondeo si toca."""
    return build_venue_health()


def _tf_to_min(tf: str) -> int:
    tf = tf.strip().lower()
    if tf in NATIVE_MIN:
        return NATIVE_MIN[tf]
    unit = tf[-1]; num = int(tf[:-1])
    return num * {'m': 1, 'h': 60, 'd': 1440, 'w': 10080}[unit]


def _fetch_any(ex, symbol: str, tf: str, need: int) -> pd.DataFrame:
    """OHLCV de cualquier TF: nativo del VENUE ACTUAL directo, o resampleado desde el mayor nativo que lo divide."""
    tf = tf.strip()
    if not tf.endswith('M'):
        tf = tf.lower()
    avail = {k: m for k, m in NATIVE_MIN.items() if k in (ex.timeframes or {})} or NATIVE_MIN
    if tf in avail:
        o = ex.fetch_ohlcv(symbol, timeframe=tf, limit=need)
    else:
        tgt = _tf_to_min(tf)
        base = max((m for m in avail.values() if tgt % m == 0 and m < tgt), default=1)
        base_tf = [k for k, v in avail.items() if v == base][0]
        factor = tgt // base
        raw = ex.fetch_ohlcv(symbol, timeframe=base_tf, limit=need * factor + factor)
        df = pd.DataFrame(raw, columns=['t', 'open', 'high', 'low', 'close', 'vol'])
        bucket = (df['t'] // (tgt * 60_000))  # anclado a época -> 00:00 UTC
        g = df.groupby(bucket)
        df = pd.DataFrame({
            't': g['t'].first(), 'open': g['open'].first(), 'high': g['high'].max(),
            'low': g['low'].min(), 'close': g['close'].last(), 'vol': g['vol'].sum(),
        }).reset_index(drop=True)
        # descarta la última vela si está incompleta (menos velas base de las esperadas)
        if g.size().iloc[-1] < factor:
            df = df.iloc[:-1]
        return df.tail(need).reset_index(drop=True)
    return pd.DataFrame(o, columns=['t', 'open', 'high', 'low', 'close', 'vol'])


def _ema(s, n): return s.ewm(span=n, adjust=False).mean()

def _rsi(s, n=14):
    d = s.diff()
    up = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.replace(0, 1e-12))

def _atr(h, l, c, n=14):
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean()


def _analyze(df: pd.DataFrame) -> dict:
    c, h, l, v = df['close'], df['high'], df['low'], df['vol']
    n = len(df); px = float(c.iloc[-1])
    e20 = float(_ema(c, 20).iloc[-1])
    e50 = float(_ema(c, 50).iloc[-1]) if n >= 50 else None
    e200 = float(_ema(c, 200).iloc[-1]) if n >= 200 else None
    r = float(_rsi(c).iloc[-1])
    macd = _ema(c, 12) - _ema(c, 26); hist = float((macd - _ema(macd, 9)).iloc[-1])
    a = float(_atr(h, l, c).iloc[-1])
    sma20 = c.rolling(20).mean(); sd = c.rolling(20).std()
    bb_u = float((sma20 + 2 * sd).iloc[-1]); bb_l = float((sma20 - 2 * sd).iloc[-1])
    bb = "above_upper" if px > bb_u else ("below_lower" if px < bb_l else "inside")
    vavg = float(v.tail(20).mean()); vx = round(float(v.iloc[-1]) / vavg, 2) if vavg else None
    sc = (1 if px > e20 else -1) + (1 if hist > 0 else -1)
    if e50: sc += 1 if px > e50 else -1
    if e200: sc += 1 if px > e200 else -1
    if r >= 70: sc -= 1
    if r <= 30: sc += 1
    sig = 1 if sc >= 2 else (-1 if sc <= -2 else 0)
    return {
        "price": round(px, 6), "rsi": round(r, 1), "macd_hist": round(hist, 6),
        "macd_dir": "up" if hist > 0 else "down", "atr": round(a, 6),
        "atr_pct": round(a / px * 100, 2),
        "ema20": round(e20, 6), "ema50": round(e50, 6) if e50 else None,
        "ema200": round(e200, 6) if e200 else None,
        "vs_ema20": "above" if px > e20 else "below",
        "bollinger": bb, "vol_x": vx, "candles": n,
        "signal": sig, "bias": "bullish" if sig > 0 else ("bearish" if sig < 0 else "neutral"),
    }


def build_snapshot(symbol: str = "WLD/USDT:USDT", timeframes: list[str] | None = None,
                   light: bool = False) -> dict:
    """light=True: omite el fetch de OI (presupuesto de requests — p.ej. watchlist_scan)."""
    tfs = timeframes or ALL_NATIVE
    ex = _ex()
    symbol = _resolve_symbol(ex, symbol)

    def work(tf):
        try:
            df = _fetch_any(ex, symbol, tf, 210)
            if len(df) < 30:
                return tf, {"error": f"solo {len(df)} velas"}
            d = _analyze(df); d["native"] = tf.lower() in NATIVE_MIN
            return tf, d
        except Exception as e:
            return tf, {"error": str(e)[:80]}

    with cf.ThreadPoolExecutor(max_workers=min(4, len(tfs))) as pool:  # cap: evita el burst que dispara el 418 de Binance
        res = dict(pool.map(work, tfs))

    # Si el 100% de los TFs falló con patrón de fallo-de-venue, el venue murió A MEDIA
    # SESIÓN (p.ej. ban 418 con el proceso clavado en Binance): raise para que
    # _with_failover demueva y reintente el snapshot completo en el siguiente venue.
    errs = [d.get("error", "") for d in res.values() if isinstance(d, dict) and "error" in d]
    if errs and len(errs) == len(res) and any(_VENUE_FAIL_RE.search(x) for x in errs):
        raise ccxt.ExchangeNotAvailable(f"{ex.id} caído para {symbol}: {errs[0][:120]}")

    ctx = {}
    try:
        ctx["funding_pct"] = round(ex.fetch_funding_rate(symbol).get("fundingRate", 0) * 100, 4)
    except Exception as e:
        ctx["funding_pct"] = None; ctx["funding_err"] = str(e)[:60]
    if light:
        ctx["open_interest"] = None  # omitido a propósito (modo light), no medido
    else:
        try:
            oi = ex.fetch_open_interest(symbol)
            ctx["open_interest"] = oi.get("openInterestAmount") or oi.get("openInterestValue")
        except Exception as e:
            ctx["open_interest"] = None; ctx["oi_err"] = str(e)[:60]

    sigs = [d["signal"] for d in res.values() if "signal" in d]
    net = sum(sigs)
    read = "bullish_bias" if net >= 4 else ("bearish_bias" if net <= -4 else "mixed_no_alignment")
    return {
        "symbol": symbol, "exchange": ex.id,
        "note": "Datos reales ccxt. Non-native TFs (p.ej. 45m/3h) resampleados desde nativos, anclados a 00:00 UTC.",
        "timeframes": res, "context": ctx,
        "confluence": {"net_score": net, "tfs_counted": len(sigs), "read": read},
    }


@mcp.tool()
def multi_tf_snapshot(symbol: str = "WLD/USDT:USDT", timeframes: list[str] | None = None) -> dict:
    """Snapshot multi-TF en paralelo de un perpetuo Binance USDT-M.

    symbol: par ccxt unificado, p.ej. 'WLD/USDT:USDT', 'BTC/USDT:USDT'.
    timeframes: lista de TFs. Default = todos los nativos de Binance futures.
      Acepta no-nativos ('45m','3h') que se resamplean automáticamente.
    Devuelve indicadores por TF (RSI, MACD, ATR, EMA20/50/200, Bollinger, vol),
    señal -1/0/+1, funding rate, open interest y score de confluencia.
    """
    return _with_failover(lambda: build_snapshot(symbol, timeframes))


@mcp.tool()
def resample_ohlcv(symbol: str, target_tf: str, limit: int = 100) -> dict:
    """Velas OHLCV de un TF NO nativo (45m, 3h, etc.) resampleadas desde velas nativas de Binance futures."""
    def _run():
        ex = _ex()
        sym = _resolve_symbol(ex, symbol)
        df = _fetch_any(ex, sym, target_tf, limit)
        return {"symbol": sym, "target_tf": target_tf, "count": len(df),
                "candles": df.tail(limit).values.tolist(),
                "columns": ["timestamp", "open", "high", "low", "close", "volume"]}
    return _with_failover(_run)


# ============================ ITEM 1: Long/Short + Open Interest ============================

def _raw_symbol(symbol: str) -> str:
    return symbol.replace("/", "").split(":")[0]  # 'WLD/USDT:USDT' -> 'WLDUSDT'


def build_ls_oi(symbol: str, timeframe: str = "15m", lookback: int = 24) -> dict:
    ex = _ex()
    symbol = _resolve_symbol(ex, symbol)
    out = {"symbol": symbol, "timeframe": timeframe}
    try:
        ls = []
        for tf_try in [timeframe, "1h", "4h", "1d"]:  # algunos venues (bybit) no soportan todos los periodos
            try:
                ls = ex.fetch_long_short_ratio_history(symbol, tf_try, limit=3)
            except Exception:
                ls = []
            if ls:
                out["ls_period"] = tf_try
                break
        if not ls:
            raise ValueError("sin datos L/S en este venue")
        info = ls[-1].get("info", {})
        out["ls_ratio"] = round(float(ls[-1]["longShortRatio"]), 4)
        long_acc = info.get("longAccount") or info.get("buyRatio")
        short_acc = info.get("shortAccount") or info.get("sellRatio")
        if long_acc:
            out["long_pct"] = round(float(long_acc) * 100, 2)
        if short_acc:
            out["short_pct"] = round(float(short_acc) * 100, 2)
        out["ls_prev"] = round(float(ls[0]["longShortRatio"]), 4)
    except Exception as e:
        out["ls_error"] = str(e)[:70]
    try:
        oi = ex.fetch_open_interest_history(symbol, timeframe, limit=lookback)
        _v = lambda r: r.get("openInterestValue") or r.get("openInterestAmount")
        now, then = _v(oi[-1]), _v(oi[0])
        if oi[-1].get("openInterestValue"):
            out["oi_usd"] = round(now, 0)
        else:
            out["oi_base"] = round(now, 0)  # venue sin valor USD: OI en moneda base
        out["oi_change_pct"] = round((now - then) / then * 100, 2) if then else None
        out["oi_periods"] = len(oi)
    except Exception as e:
        out["oi_error"] = str(e)[:70]
    return out


@mcp.tool()
def long_short_and_oi(symbol: str = "WLD/USDT:USDT", timeframe: str = "15m", lookback: int = 24) -> dict:
    """Sentiment OBJETIVO de posicionamiento en Binance Futures: long/short account ratio
    (crowding de retail) + open interest y su cambio en `lookback` periodos.
    Lectura: OI subiendo + precio bajando = shorts tomando control; L/S ratio alto = largos amontonados (riesgo de squeeze)."""
    return _with_failover(lambda: build_ls_oi(symbol, timeframe, lookback))


# ============================ ITEM 2: Real-time pulse (REST order-flow) ============================

def build_pulse(symbol: str = "WLD/USDT:USDT", trades_n: int = 100, depth: int = 20) -> dict:
    """Order-flow de los últimos trades (REST) + presión del libro. Fiable y portable (sin websocket)."""
    ex = _ex()
    symbol = _resolve_symbol(ex, symbol)
    try:
        t = ex.fetch_trades(symbol, limit=min(max(trades_n, 20), 500))
        buy = sum(x["amount"] for x in t if x.get("side") == "buy")
        sell = sum(x["amount"] for x in t if x.get("side") == "sell")
        total = buy + sell
        pv = sum(x["price"] * x["amount"] for x in t)
        qv = sum(x["amount"] for x in t)
        last = float(t[-1]["price"])
        span = (t[-1]["timestamp"] - t[0]["timestamp"]) / 1000.0 if len(t) > 1 else None
        tps = round(len(t) / span, 1) if span else None
        rr = 4 if last < 1 else (2 if last < 1000 else 1)
    except Exception as e:
        if _is_venue_failure(e):
            raise  # que _with_failover demueva el venue y reintente, en vez de error suave
        return {"symbol": symbol, "error": f"trades: {str(e)[:70]}"}
    out = {
        "symbol": symbol, "trades": len(t), "window_s": round(span, 1) if span else None,
        "trades_per_s": tps, "last": round(last, rr),
        "vwap": round(pv / qv, rr) if qv else None,
        "buy_vol": round(buy, 3), "sell_vol": round(sell, 3),
        "delta": round(buy - sell, 3),
        "buy_pressure_pct": round(buy / total * 100, 1) if total else None,
        "flow": "buyers" if buy > sell else ("sellers" if sell > buy else "flat"),
    }
    try:
        ob = ex.fetch_order_book(symbol, limit=depth)
        bid, ask = ob["bids"][0][0], ob["asks"][0][0]
        bidv = sum(v for _, v in ob["bids"]); askv = sum(v for _, v in ob["asks"])
        imb = (bidv - askv) / (bidv + askv) if (bidv + askv) else None
        out.update({
            "bid": bid, "ask": ask, "spread": round(ask - bid, 8),
            "spread_bps": round((ask - bid) / last * 1e4, 2) if last else None,
            "book_imbalance": round(imb, 3) if imb is not None else None,
            "book_pressure": "bids" if (imb or 0) > 0.05 else ("asks" if (imb or 0) < -0.05 else "balanced"),
        })
    except Exception as e:
        out["ob_error"] = str(e)[:60]
    return out


@mcp.tool()
def realtime_pulse(symbol: str = "WLD/USDT:USDT", trades_n: int = 100, depth: int = 20) -> dict:
    """Pulso 'ahora' de un perpetuo Binance: order-flow de los últimos `trades_n` trades
    (presión compradora/vendedora, delta, VWAP, trades/seg) + presión del order book
    (imbalance bid/ask, spread). Para timing de entrada/scalp. Datos REST reales."""
    return _with_failover(lambda: build_pulse(symbol, trades_n, depth))


# ============================ ITEM 3 (motor): Trade plan con 3 TP + ETA ============================

def _fmt_eta(mins):
    if mins is None:
        return "n/d"
    mins = round(mins)
    if mins < 60:
        return f"~{mins}m"
    return f"~{mins // 60}h {mins % 60:02d}m"


def build_trade_plan(symbol: str = "WLD/USDT:USDT", direction: str | None = None,
                     risk_pct: float = 1.0, account_usd: float = 1000.0,
                     entry_tf: str = "15m") -> dict:
    ex = _ex()
    symbol = _resolve_symbol(ex, symbol)
    snap = build_snapshot(symbol, [entry_tf, "1h", "4h", "1d"])
    tfd = snap["timeframes"]
    entry_d = tfd.get(entry_tf) or {}
    if "price" not in entry_d:
        # El TF de entrada vino sin datos (venue degradado, símbolo sin velas, etc.):
        # error ESTRUCTURADO y honesto — nunca más el KeyError 'price' que tumbaba el gatillo.
        return {"error": "sin_datos_tf_entrada", "symbol": symbol, "entry_tf": entry_tf,
                "detail": entry_d.get("error", "sin velas suficientes"), **_venue_meta(),
                "hint": "reintenta en ~1 min (failover de venue) o usa el fallback TradingView"}
    price = entry_d["price"]
    atr_e = entry_d["atr"]
    net = snap["confluence"]["net_score"]
    if direction is None:
        direction = "short" if net < 0 else ("long" if net > 0 else "none")
    rr = 4 if price < 1 else (2 if price < 1000 else 1)
    R = lambda x: round(float(x), rr)

    df = _fetch_any(ex, symbol, entry_tf, 40)
    swing_hi = float(df["high"].tail(20).max())
    swing_lo = float(df["low"].tail(20).min())

    sign = -1 if direction == "short" else 1
    entry = price
    atr_stop = 1.5 * atr_e
    if direction == "short":
        stop = max(swing_hi, entry + atr_stop)
    elif direction == "long":
        stop = min(swing_lo, entry - atr_stop)
    else:
        stop = entry
    risk = abs(entry - stop)

    now_s = time.time()
    tf_min = _tf_to_min(entry_tf)
    vel = 0.6 * atr_e  # velocidad direccional estimada por vela (60% del ATR)

    def _clock(mins):
        return time.strftime("%H:%M", time.localtime(now_s + (mins or 0) * 60))

    tps = []
    for i, mult in enumerate([1.0, 2.0, 3.0], 1):
        tp = entry + sign * mult * risk
        dist = abs(tp - entry)
        mins = (dist / vel) * tf_min if vel else None
        tps.append({"tag": f"TP{i}", "price": R(tp), "rr": f"{mult:.0f}R",
                    "move_pct": round(dist / entry * 100, 2), "eta": _fmt_eta(mins),
                    "hora_est": _clock(mins)})

    # --- timing de ENTRADA: mercado vs pullback a EMA20 del TF de entrada ---
    e20 = tfd[entry_tf].get("ema20")
    timing = {"ahora": _clock(0), "entrada_mercado": "inmediata"}
    pullback_ok = e20 and ((direction == "short" and e20 > entry) or (direction == "long" and e20 < entry))
    if pullback_ok:
        d_pb = abs(e20 - entry)
        eta_pb = (d_pb / vel) * tf_min if vel else None
        timing["entrada_pullback"] = {
            "zona": R(e20), "mejora_entrada_pct": round(d_pb / entry * 100, 2),
            "eta": _fmt_eta(eta_pb), "hora_est": _clock(eta_pb),
        }
    expiry_min = 8 * tf_min  # regla: si el setup no activa en ~8 velas, se descarta
    timing["caducidad_setup"] = f"si no activa en {_fmt_eta(expiry_min)} (hacia las {_clock(expiry_min)}), descartar"
    timing["duracion_estimada_trade"] = tps[1]["eta"] + " (a TP2, mediana)" if tps else None

    risk_usd = account_usd * risk_pct / 100.0
    units = risk_usd / risk if risk > 0 else None
    notional = units * entry if units else None

    lsoi = build_ls_oi(symbol, entry_tf)
    funding = snap["context"].get("funding_pct")

    checks = []
    if direction in ("short", "long"):
        want_short = direction == "short"
        if funding is not None:
            ok = (funding > 0) if want_short else (funding < 0)
            checks.append({"factor": "funding", "value": f"{funding}%", "confirms": ok,
                           "note": "largos pagan (combustible bajista)" if funding > 0 else "cortos pagan"})
        ls = lsoi.get("ls_ratio")
        if ls is not None:
            ok = (ls > 1) if want_short else (ls < 1)
            checks.append({"factor": "long/short", "value": ls, "confirms": ok,
                           "note": "retail amontonado en largos" if ls > 1 else "retail amontonado en cortos"})
        oic = lsoi.get("oi_change_pct")
        if oic is not None:
            ok = oic > 0  # OI subiendo = convicción/posiciones nuevas en la dirección dominante
            checks.append({"factor": "open interest", "value": f"{oic:+.2f}%", "confirms": ok,
                           "note": "OI subiendo (posiciones nuevas)" if oic > 0 else "OI bajando (cierre)"})

    tf_dots = {tf: tfd[tf].get("signal") for tf in [entry_tf, "1h", "4h", "1d"] if "signal" in tfd.get(tf, {})}

    return {
        "symbol": symbol, "direction": direction, "entry_tf": entry_tf,
        "confluence": snap["confluence"], "tf_signals": tf_dots,
        "plan": {
            "entry": R(entry),
            "stop": R(stop),
            "stop_pct": round(risk / entry * 100, 2),
            "tps": tps,
            "rr_max": f"{abs((tps[-1]['price'] - entry) / risk):.1f}R" if risk else None,
        },
        "timing": timing,
        "sizing": {
            "risk_pct": risk_pct, "account_usd": account_usd,
            "risk_usd": round(risk_usd, 2),
            "units": round(units, 2) if units else None,
            "notional_usd": round(notional, 2) if notional else None,
        },
        "context": {"funding_pct": funding, **lsoi},
        "confirmations": checks,
        "note": f"Datos reales {_ex().id}. ETA = estimación por velocidad ATR (0.6·ATR/vela), no garantía temporal.",
    }


@mcp.tool()
def trade_plan(symbol: str = "WLD/USDT:USDT", direction: str | None = None,
               risk_pct: float = 1.0, account_usd: float = 1000.0, entry_tf: str = "15m") -> dict:
    """Plan de trade accionable para un perpetuo: entrada, stop estructural, 3 take-profits
    (1R/2R/3R) con % de movimiento y ETA por objetivo, sizing por riesgo, y confirmaciones
    de contexto (funding, long/short ratio, open interest) + señales multi-TF.
    direction: 'long'/'short'/None (auto por confluencia)."""
    return _with_failover(lambda: build_trade_plan(symbol, direction, risk_pct, account_usd, entry_tf))


# ============================ Posicionamiento PRO (smart money / taker / basis / funding countdown) ============================

def build_positioning(symbol: str = "WLD/USDT:USDT", timeframe: str = "15m") -> dict:
    """Posicionamiento avanzado Binance Futures: retail vs TOP TRADERS, taker buy/sell, basis y countdown a funding."""
    ex = _ex()
    symbol = _resolve_symbol(ex, symbol)
    raw = _raw_symbol(symbol)
    out = {"symbol": symbol, "timeframe": timeframe}
    base = build_ls_oi(symbol, timeframe)
    out.update({k: base.get(k) for k in ("ls_ratio", "long_pct", "short_pct", "oi_usd", "oi_change_pct") if k in base})
    out["venue"] = ex.id
    if ex.id == "binanceusdm":
        try:  # smart money: top traders por POSICIONES (endpoint exclusivo Binance)
            r = ex.fapiDataGetTopLongShortPositionRatio({"symbol": raw, "period": timeframe, "limit": 2})[-1]
            out["top_traders_ls"] = round(float(r["longShortRatio"]), 3)
            out["top_traders_long_pct"] = round(float(r["longAccount"]) * 100, 1)
            rt = out.get("ls_ratio")
            if rt:
                d = out["top_traders_ls"] - rt
                out["smart_vs_retail"] = ("smart_money_long_retail_short" if d > 0.15
                                          else ("smart_money_short_retail_long" if d < -0.15 else "aligned"))
        except Exception as e:
            out["top_traders_err"] = str(e)[:60]
        try:  # flujo agresor agregado (ventana del periodo, no 120 trades)
            r = ex.fapiDataGetTakerlongshortRatio({"symbol": raw, "period": timeframe, "limit": 2})[-1]
            out["taker_buy_sell_ratio"] = round(float(r["buySellRatio"]), 3)
            out["taker_read"] = "buyers" if float(r["buySellRatio"]) > 1 else "sellers"
        except Exception as e:
            out["taker_err"] = str(e)[:60]
    else:
        out["top_traders_note"] = f"solo disponible vía Binance (venue actual: {ex.id})"
    try:  # basis + countdown a funding — UNIFICADO ccxt (funciona en cualquier venue)
        fr = ex.fetch_funding_rate(symbol)
        mark, idx = fr.get("markPrice"), fr.get("indexPrice")
        if mark and idx:
            out["basis_bps"] = round((mark - idx) / idx * 1e4, 2)
            out["basis_read"] = "premium (perp>spot, presión larga)" if mark > idx else "descuento (perp<spot, presión corta)"
        if fr.get("fundingRate") is not None:
            out["funding_next_pct"] = round(fr["fundingRate"] * 100, 4)
        ts = fr.get("nextFundingTimestamp") or fr.get("fundingTimestamp")
        if ts:
            out["funding_countdown_min"] = max(0, round((ts - time.time() * 1000) / 60000))
    except Exception as e:
        out["premium_err"] = str(e)[:60]
    return out


@mcp.tool()
def positioning(symbol: str = "WLD/USDT:USDT", timeframe: str = "15m") -> dict:
    """Posicionamiento PRO de un perp Binance: retail vs TOP TRADERS (smart money), taker buy/sell
    ratio agregado, basis perp-spot y countdown al próximo funding. Complementa long_short_and_oi."""
    return _with_failover(lambda: build_positioning(symbol, timeframe))


# ============================ Ciclo de vida del trade (entrada -> actualízame -> salida -> journal) ============================

def _load_active() -> dict | None:
    if ACTIVE_FILE.exists():
        try:
            return json.loads(ACTIVE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _save_active(d: dict | None):
    if d is None:
        ACTIVE_FILE.unlink(missing_ok=True)
    else:
        ACTIVE_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")


def build_trade_open(symbol: str, side: str, entry: float, size_units: float | None = None,
                     stop: float | None = None, entry_tf: str = "5m", note: str = "") -> dict:
    """Registra el trade activo (desde captura o precio dicho por el usuario)."""
    ex = _ex()
    symbol = _resolve_symbol(ex, symbol)
    df = _fetch_any(ex, symbol, entry_tf, 40)
    a = float(_atr(df["high"], df["low"], df["close"]).iloc[-1])
    sign = 1 if side == "long" else -1
    if stop is None:
        swing = float(df["low"].tail(20).min()) if side == "long" else float(df["high"].tail(20).max())
        stop = min(swing, entry - 1.5 * a) if side == "long" else max(swing, entry + 1.5 * a)
    risk = abs(entry - stop)
    rr = 4 if entry < 1 else (2 if entry < 1000 else 1)
    R = lambda x: round(float(x), rr)
    trade = {
        "symbol": symbol, "side": side, "entry": entry, "stop": R(stop),
        "initial_stop": R(stop), "risk": risk, "atr_at_open": a, "entry_tf": entry_tf,
        "venue_at_open": ex.id,  # barrera anti-drift: la gestión valida contra este venue
        "size_units": size_units, "opened_at": int(time.time() * 1000),
        "tps": [R(entry + sign * m * risk) for m in (1, 2, 3)],
        "mfe_r": 0.0, "mae_r": 0.0, "updates": 0, "be_moved": False, "note": note,
    }
    _save_active(trade)
    return {"status": "trade_registrado", **trade,
            "plan": f"{side.upper()} {symbol} @ {entry} | stop {trade['stop']} | TP1 {trade['tps'][0]} TP2 {trade['tps'][1]} TP3 {trade['tps'][2]}"}


def _detect_reversal(side: str, tfs: dict, pulse: dict, pos: dict) -> tuple[int, list[str]]:
    """Score objetivo de giro de tendencia EN CONTRA del trade (0-6). >=4 = cambio real."""
    against = 1 if side == "short" else -1
    score, reasons = 0, []
    sigs = [tfs[tf].get("signal") for tf in ("5m", "15m", "1h") if tf in tfs and "signal" in tfs[tf]]
    flipped = sum(1 for s in sigs if s == against)
    if flipped >= 2:
        score += 2; reasons.append(f"{flipped}/3 TFs rápidos girados en contra")
    elif flipped == 1:
        score += 1; reasons.append("1 TF rápido girado en contra")
    m15 = tfs.get("15m", {})
    if m15.get("macd_dir") == ("up" if side == "short" else "down"):
        score += 1; reasons.append("MACD 15m girado en contra")
    if m15.get("vs_ema20") == ("above" if side == "short" else "below"):
        score += 1; reasons.append("precio cruzó EMA20 15m en contra")
    bp = pulse.get("buy_pressure_pct")
    if bp is not None and ((side == "short" and bp >= 65) or (side == "long" and bp <= 35)):
        score += 1; reasons.append(f"order-flow {bp}% en contra")
    sv = pos.get("smart_vs_retail")
    if (side == "short" and sv == "smart_money_long_retail_short") or \
       (side == "long" and sv == "smart_money_short_retail_long"):
        score += 1; reasons.append("smart money posicionado en contra")
    return score, reasons


def build_trade_update(note: str = "") -> dict:
    """El 'actualízame' con trade abierto: PnL vivo, R alcanzado, TPs, trailing y recomendación para maximizar."""
    t = _load_active()
    if not t:
        return {"error": "no_hay_trade_activo", "hint": "usa trade_open o el gatillo wldlivenow para buscar entrada"}
    ex = _ex()
    # Barrera venue_at_open (comité 2026-07-10): si el trade se abrió con precio de un venue
    # que ya no es el activo, TODO número de este update viene de otro exchange (drift bps
    # que a 50-75x sí importa) -> se avisa arriba y NO se mueven stops automáticamente.
    v_open = t.get("venue_at_open")
    cross_venue = bool(v_open and ex.id != v_open)
    symbol, side, entry, risk = t["symbol"], t["side"], t["entry"], t["risk"]
    sign = 1 if side == "long" else -1
    rr = 4 if entry < 1 else (2 if entry < 1000 else 1)
    R = lambda x: round(float(x), rr)

    pulse = build_pulse(symbol, 100, 20)
    px = pulse.get("last") or pulse.get("vwap")
    if px is None:
        return {"error": "sin_precio_vivo", "symbol": symbol, "detail": pulse.get("error"),
                **_venue_meta(), "hint": "reintenta en ~1 min; el trade sigue registrado intacto"}
    snap = build_snapshot(symbol, ["5m", "15m", "1h"])
    pos = build_positioning(symbol, "15m")

    r_now = sign * (px - entry) / risk if risk else 0
    t["mfe_r"] = max(t["mfe_r"], r_now)
    t["mae_r"] = min(t["mae_r"], r_now)
    t["updates"] += 1

    df = _fetch_any(ex, symbol, t["entry_tf"], 30)
    a = float(_atr(df["high"], df["low"], df["close"]).iloc[-1])
    if side == "long":
        trail = float(df["high"].tail(12).max()) - 2 * a
        new_stop = max(t["stop"], R(trail))
    else:
        trail = float(df["low"].tail(12).min()) + 2 * a
        new_stop = min(t["stop"], R(trail))

    tps_hit = sum(1 for tp in t["tps"] if (px >= tp if side == "long" else px <= tp))
    recs = []
    if r_now <= -0.8:
        recs.append("⛔ cerca del stop: respétalo, no lo muevas en contra")
    if r_now >= 1 and not t["be_moved"]:
        if cross_venue:
            recs.append(f"✅ +1R visto (precio de {ex.id}) → mueve TÚ el stop a BE {R(entry)} "
                        "en tu exchange; no lo muevo automáticamente con precio cruzado")
        else:
            t["stop"] = R(entry); t["be_moved"] = True
            recs.append("✅ +1R alcanzado → stop movido a BREAK-EVEN (riesgo cero)")
    if tps_hit >= 1 and r_now >= 1:
        recs.append(f"💰 TP{tps_hit} tocado → toma parcial si no lo hiciste")
    if r_now >= 2:
        better = new_stop != t["stop"] and ((side == "short" and new_stop < t["stop"]) or (side == "long" and new_stop > t["stop"]))
        if better and not cross_venue:
            t["stop"] = new_stop
            recs.append(f"🏃 modo runner: trailing ATR ajustado a {new_stop} — deja correr hacia TP3+")
        elif better and cross_venue:
            recs.append(f"🏃 modo runner: trailing sugerido {new_stop} (calculado con {ex.id}) — ajústalo TÚ en tu exchange")
        else:
            recs.append("🏃 modo runner: mantén trailing, deja correr")
    flow_against = (pulse.get("flow") == "buyers" and side == "short") or (pulse.get("flow") == "sellers" and side == "long")
    if flow_against and pulse.get("buy_pressure_pct") and abs(pulse["buy_pressure_pct"] - 50) > 20:
        recs.append("⚠️ order-flow fuerte EN CONTRA: considera asegurar parcial/salida")
    fc = pos.get("funding_countdown_min")
    fp = pos.get("funding_next_pct")
    if fc is not None and fc < 30 and fp is not None:
        paga = (fp > 0 and side == "long") or (fp < 0 and side == "short")
        if paga:
            recs.append(f"⏳ funding en {fc}m y TU LADO PAGA ({fp}%): si vas a cerrar, hazlo antes")
    # --- detector de CAMBIO DE TENDENCIA REAL (honesto: si giró, se dice y punto) ---
    rev_score, rev_reasons = _detect_reversal(side, snap["timeframes"], pulse, pos)
    trend_change = None
    if rev_score >= 4:
        flip_dir = "long" if side == "short" else "short"
        flip = build_trade_plan(symbol, flip_dir, 1.0, 1000.0, "5m")
        recs.insert(0, f"🔄 CAMBIO DE TENDENCIA REAL ({rev_score}/6): CIERRA EL {side.upper()} AHORA en {px}")
        trend_change = {
            "detected": True, "score": f"{rev_score}/6", "razones": rev_reasons,
            "cerrar_en": px,
        }
        fp = flip.get("plan")
        if fp:  # si el plan del flip degradó (venue sin datos), el aviso de giro sale igual
            trend_change["setup_contrario_5m"] = {
                "direccion": flip_dir, "entry": fp["entry"], "stop": fp["stop"],
                "tps": [(tp["tag"], tp["price"], tp["eta"]) for tp in fp["tps"]],
                "timing": flip.get("timing"),
            }
        else:
            trend_change["setup_contrario_5m"] = {"error": flip.get("error", "sin datos"),
                                                  "hint": "pide wldlivenow para el setup del flip"}
    elif rev_score >= 2:
        recs.append(f"⚠️ señales de giro ({rev_score}/6): {'; '.join(rev_reasons[:2])} — vigila de cerca")

    if not recs:
        recs.append("🕒 en rango: mantén el plan, ni codicia ni pánico")

    if cross_venue:
        recs.insert(0, f"⚠️ VENUE CRUZADO: tu trade abrió con precio de {v_open}, pero ahora el "
                       f"precio de referencia es de {ex.id}. Verifica todo número contra TU "
                       "pantalla antes de actuar; stops NO se mueven automáticamente.")

    _save_active(t)
    return {
        "trigger": "actualizame(trade)", "symbol": symbol, "side": side,
        "venue_now": ex.id, "venue_at_open": v_open, "cross_venue": cross_venue,
        "trend_change": trend_change,
        "entry": entry, "price_now": px, "stop_now": t["stop"], "be_moved": t["be_moved"],
        "pnl_pct": round(sign * (px - entry) / entry * 100, 2),
        "r_multiple": round(r_now, 2), "mfe_r": round(t["mfe_r"], 2), "mae_r": round(t["mae_r"], 2),
        "tps": t["tps"], "tps_hit": tps_hit, "trailing_suggested": new_stop,
        "minutes_in_trade": round((time.time() * 1000 - t["opened_at"]) / 60000),
        "recommendations": recs,
        "flow_now": {k: pulse.get(k) for k in ("flow", "buy_pressure_pct", "delta", "spread_bps")},
        "fast_tf_signals": {tf: snap["timeframes"][tf].get("signal") for tf in ("5m", "15m", "1h") if tf in snap["timeframes"]},
        "positioning": {k: pos.get(k) for k in ("top_traders_ls", "smart_vs_retail", "taker_buy_sell_ratio",
                                                 "basis_bps", "funding_next_pct", "funding_countdown_min")},
        "note": note or None,
    }


def build_trade_close(exit_price: float | None = None, note: str = "") -> dict:
    """Cierra el trade activo, calcula resultado y lo registra en el journal (W4)."""
    t = _load_active()
    if not t:
        return {"error": "no_hay_trade_activo"}
    v_open = t.get("venue_at_open")
    exit_source = "manual"
    if exit_price is None:
        cur = _ex().id
        if v_open and cur != v_open:
            # Cerrar escribe el journal PERMANENTE: con venue cruzado se exige el fill real
            # del exchange de Emmanuel — nunca journalear con precio de otro venue (comité).
            return {"error": "venue_cruzado_sin_precio", **_venue_meta(),
                    "hint": f"tu trade abrió con precio de {v_open} y ahora solo hay {cur}: "
                            "pásame tu precio REAL de salida (exit_price, de tu captura/exchange) "
                            "para journalear sin drift; el trade sigue abierto"}
        exit_price = build_pulse(t["symbol"], 30, 5).get("last")
        exit_source = cur
    if exit_price is None:
        return {"error": "sin_precio_de_cierre", **_venue_meta(),
                "hint": "no hay precio vivo del venue ahora — pásame exit_price explícito; el trade sigue abierto"}
    sign = 1 if t["side"] == "long" else -1
    r_final = sign * (exit_price - t["entry"]) / t["risk"] if t["risk"] else 0
    entry_dec = 4 if t["entry"] < 1 else (2 if t["entry"] < 1000 else 1)
    rec = {
        "closed_at": int(time.time() * 1000), "opened_at": t["opened_at"],
        "duration_min": round((time.time() * 1000 - t["opened_at"]) / 60000),
        "symbol": t["symbol"], "side": t["side"],
        "entry": t["entry"], "exit": round(float(exit_price), entry_dec),
        "initial_stop": t["initial_stop"], "final_stop": t["stop"],
        "pnl_pct": round(sign * (exit_price - t["entry"]) / t["entry"] * 100, 2),
        "r_result": round(r_final, 2), "mfe_r": round(t["mfe_r"], 2), "mae_r": round(t["mae_r"], 2),
        "updates": t["updates"], "size_units": t.get("size_units"),
        "venue_at_open": v_open, "exit_source": exit_source,
        "note": note or t.get("note", ""),
    }
    with JOURNAL_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    _save_active(None)
    verdict = "GANADOR" if r_final > 0 else ("PERDEDOR" if r_final < 0 else "BREAK-EVEN")
    eff = f"capturaste {rec['r_result']}R de un máximo visto de {rec['mfe_r']}R" if t["mfe_r"] > 0 else "el trade nunca fue a favor"
    return {"status": "cerrado_y_journaleado", "verdict": verdict, "eficiencia": eff, **rec}


@mcp.tool()
def trade_open(symbol: str = "WLD/USDT:USDT", side: str = "short", entry: float = 0.0,
               size_units: float | None = None, stop: float | None = None,
               entry_tf: str = "5m", note: str = "") -> dict:
    """Registra un trade ABIERTO (el usuario da su precio de entrada, p.ej. desde una captura).
    Calcula stop estructural (si no se da), 3 TPs (1R/2R/3R) y guarda estado para 'actualízame'."""
    if entry <= 0:
        return {"error": "entry_requerido", "hint": "pasa el precio real de entrada"}
    return _with_failover(lambda: build_trade_open(symbol, side, entry, size_units, stop, entry_tf, note))


@mcp.tool()
def actualizame(symbol: str = "WLD/USDT:USDT") -> dict:
    """Gatillo ACTUALÍZAME (context-aware): si hay trade abierto → gestión en vivo (PnL, R, BE,
    trailing ATR, TPs, order-flow, funding countdown, recomendaciones para maximizar). Si NO hay
    trade → corre wldlivenow para buscar la mejor entrada."""
    if _load_active():
        return _with_failover(build_trade_update)
    return {"trigger": "actualizame(sin_trade)", **_with_failover(lambda: build_wldlivenow(symbol))}


@mcp.tool()
def trade_close(exit_price: float | None = None, note: str = "") -> dict:
    """Cierra el trade activo (a precio dado o al precio de mercado actual), calcula resultado
    (R, %PnL, eficiencia vs MFE) y lo registra en el journal automáticamente."""
    return _with_failover(lambda: build_trade_close(exit_price, note))


@mcp.tool()
def journal(limit: int = 10) -> dict:
    """Últimos trades del journal (W4): resultado en R, %PnL, duración, MFE/MAE y notas."""
    if not JOURNAL_FILE.exists():
        return {"trades": [], "note": "journal vacío"}
    lines = JOURNAL_FILE.read_text(encoding="utf-8").strip().splitlines()
    trades = [json.loads(l) for l in lines[-limit:]]
    wins = [t for t in trades if t["r_result"] > 0]
    return {"trades": trades, "count": len(trades),
            "win_rate_pct": round(len(wins) / len(trades) * 100, 1) if trades else None,
            "total_r": round(sum(t["r_result"] for t in trades), 2)}


# ============================ Watchlist scan — radar multi-moneda T1/T2/T3 ============================

# Formato: "SIMBOLO:TIER,..." — override por env var sin redeploy de código.
WATCHLIST = [w.strip() for w in os.environ.get(
    "PERP_WATCHLIST",
    "WLD:T1,BTC:T2,ETH:T2,SOL:T2,FET:T2,GMT:T3,1000SHIB:T3,1000FLOKI:T3,GALA:T3",
).split(",") if w.strip()]


def build_watchlist_scan(symbols: list[str] | None = None, timeframes: list[str] | None = None) -> dict:
    # LEAN a propósito (comité 2026-07-10): 3 TFs + funding, SIN OI por moneda — un barrido de
    # 9 símbolos es acelerante de rate-limit sobre la IP compartida; cada request cuenta.
    tfs = timeframes or ["15m", "1h", "4h"]
    if symbols:
        pairs = [(s, "") for s in symbols]
    else:
        pairs = [tuple(w.split(":", 1)) if ":" in w else (w, "") for w in WATCHLIST]

    def scan_one(pair):
        sym, tier = pair
        row = {"tier": tier or None}
        try:
            snap = _with_failover(lambda: build_snapshot(sym, tfs, light=True))
            t0 = snap["timeframes"].get(tfs[0]) or {}
            n = snap["confluence"]["net_score"]
            k = max(snap["confluence"]["tfs_counted"], 1)
            row.update({
                "symbol": snap["symbol"], "price": t0.get("price"),
                "net_score": n, "tfs_counted": k, "read": snap["confluence"]["read"],
                "alineacion_pct": round(abs(n) / k * 100),
                "lado": "long" if n > 0 else ("short" if n < 0 else "neutro"),
                "tf_signals": {tf: (snap["timeframes"].get(tf) or {}).get("signal") for tf in tfs},
                "funding_pct": snap["context"].get("funding_pct"),
            })
        except Exception as e:
            row.update({"symbol": _norm_symbol(sym), "error": str(e)[:100]})
        return row

    with cf.ThreadPoolExecutor(max_workers=2) as pool:  # bajo a propósito: 9 símbolos sin disparar rate-limits
        rows = list(pool.map(scan_one, pairs))

    ok = [r for r in rows if "net_score" in r]
    ok.sort(key=lambda r: r["alineacion_pct"], reverse=True)
    err = [r for r in rows if "net_score" not in r]
    top = ok[0] if ok else None
    return {
        "trigger": "watchlist_scan", **_venue_meta(), "timeframes": tfs,
        "ranking": ok + err,
        "mejor_candidato": (
            {"symbol": top["symbol"], "lado": top["lado"], "alineacion_pct": top["alineacion_pct"],
             "hint": f"pide livenow {top['symbol'].split('/')[0]} para el plan de entrada"}
            if top and top["alineacion_pct"] >= 75 else None
        ),
        "note": ("Radar de confluencia por símbolo (señal -1/0/+1 por TF sobre "
                 f"{'/'.join(tfs)}). La política de riesgo/sizing por tier vive en la skill; "
                 "esto es datos, no recomendación."),
    }


@mcp.tool()
def watchlist_scan(symbols: list[str] | None = None, timeframes: list[str] | None = None) -> dict:
    """Radar multi-moneda de la watchlist T1/T2/T3 en UNA llamada: por símbolo trae precio,
    señales por TF (default lean: 15m/1h/4h), confluencia neta y funding; devuelve ranking por
    fuerza de alineación + mejor candidato. symbols opcional y laxo ('BTC','SOL','1000SHIB');
    default = watchlist completa WLD·BTC·ETH·SOL·FET·GMT·1000SHIB·1000FLOKI·GALA (env PERP_WATCHLIST).
    Para profundizar en un candidato: livenow/livefull {moneda}."""
    return build_watchlist_scan(symbols, timeframes)


# ============================ TRIGGERS de la skill (wldlive / wldlivenow / wldlivefull) ============================

def build_wldlive(symbol: str = "WLD/USDT:USDT") -> dict:
    """Vistazo rápido: precio, confluencia 4-TF, order-flow y funding."""
    symbol = _norm_symbol(symbol)
    pulse = build_pulse(symbol, 80, 20)
    snap = build_snapshot(symbol, ["15m", "1h", "4h", "1d"])
    return {
        "trigger": "wldlive", "symbol": symbol, **_venue_meta(), "price": pulse.get("last"),
        "confluence": snap["confluence"],
        "tf_signals": {tf: snap["timeframes"][tf].get("signal") for tf in ["15m", "1h", "4h", "1d"]},
        "flow": pulse.get("flow"), "buy_pressure_pct": pulse.get("buy_pressure_pct"),
        "funding_pct": snap["context"].get("funding_pct"),
    }


def build_wldlivenow(symbol: str = "WLD/USDT:USDT", entry_tf: str = "5m") -> dict:
    """Timing 'ahora': order-flow + libro + plan de scalp en TF corto."""
    symbol = _norm_symbol(symbol)
    return {"trigger": "wldlivenow", "symbol": symbol, **_venue_meta(),
            "pulse": build_pulse(symbol, 120, 20),
            "plan": build_trade_plan(symbol, None, 1.0, 1000.0, entry_tf)}


def build_wldlivefull(symbol: str = "WLD/USDT:USDT", risk_pct: float = 1.0,
                      account_usd: float = 1000.0, entry_tf: str = "15m") -> dict:
    """Análisis completo: snapshot de TODOS los TFs nativos + plan (3 TP/ETA) + order-flow + sentiment objetivo."""
    symbol = _norm_symbol(symbol)
    return {"trigger": "wldlivefull", "symbol": symbol, **_venue_meta(),
            "snapshot": build_snapshot(symbol),
            "plan": build_trade_plan(symbol, None, risk_pct, account_usd, entry_tf),
            "pulse": build_pulse(symbol, 120, 20),
            "positioning": build_positioning(symbol, entry_tf)}


@mcp.tool()
def wldlive(symbol: str = "WLD/USDT:USDT") -> dict:
    """Gatillo wldlive — vistazo rápido de un perp: precio, confluencia multi-TF, order-flow y funding."""
    return _with_failover(lambda: build_wldlive(symbol))


@mcp.tool()
def wldlivenow(symbol: str = "WLD/USDT:USDT", entry_tf: str = "5m") -> dict:
    """Gatillo wldlivenow — timing de entrada AHORA: order-flow REST + presión de libro + plan de scalp en TF corto."""
    return _with_failover(lambda: build_wldlivenow(symbol, entry_tf))


@mcp.tool()
def wldlivefull(symbol: str = "WLD/USDT:USDT", risk_pct: float = 1.0,
                account_usd: float = 1000.0, entry_tf: str = "15m") -> dict:
    """Gatillo wldlivefull — análisis completo de un perp: todos los TFs nativos, plan de trade
    (entrada/stop/3 TP/ETA), sizing por riesgo, sentiment objetivo (funding, L/S, OI) y order-flow."""
    return _with_failover(lambda: build_wldlivefull(symbol, risk_pct, account_usd, entry_tf))


# ============================ Gatillos GENÉRICOS multi-moneda (v4) ============================
# Los mismos motores que wldlive/wldlivenow/wldlivefull, con nombre agnóstico para operar
# cualquier perp de la watchlist ("live BTC", "livenow SOL", "livefull 1000SHIB"...).
# Los wld* se conservan por compatibilidad con la skill v3.4.1 y la memoria muscular.

@mcp.tool()
def live(symbol: str = "WLD/USDT:USDT") -> dict:
    """Gatillo live {moneda} — vistazo rápido de CUALQUIER perp de la watchlist: precio,
    confluencia multi-TF, order-flow y funding. Símbolo laxo OK: 'BTC', 'btcusdt', '1000SHIB'."""
    r = _with_failover(lambda: build_wldlive(symbol))
    r["trigger"] = "live"
    return r


@mcp.tool()
def livenow(symbol: str = "WLD/USDT:USDT", entry_tf: str = "5m") -> dict:
    """Gatillo livenow {moneda} — timing de entrada AHORA para CUALQUIER perp: order-flow +
    presión de libro + plan de scalp en TF corto. Símbolo laxo OK ('BTC', 'FET', 'GALA')."""
    r = _with_failover(lambda: build_wldlivenow(symbol, entry_tf))
    r["trigger"] = "livenow"
    return r


@mcp.tool()
def livefull(symbol: str = "WLD/USDT:USDT", risk_pct: float = 1.0,
             account_usd: float = 1000.0, entry_tf: str = "15m") -> dict:
    """Gatillo livefull {moneda} — análisis completo de CUALQUIER perp: todos los TFs nativos,
    plan (entrada/stop/3 TP/ETA), sizing por riesgo, positioning y order-flow. Símbolo laxo OK."""
    r = _with_failover(lambda: build_wldlivefull(symbol, risk_pct, account_usd, entry_tf))
    r["trigger"] = "livefull"
    return r


def main():
    mcp.run()


if __name__ == "__main__":
    main()
