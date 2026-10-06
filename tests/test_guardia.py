"""Bateria de la guardia de rate-limit (v5.2). Sin red: se falsea session.request de ccxt,
asi el camino real (ccxt -> ex.fetch guardado -> manejo de errores de ccxt) queda cubierto.

    pip install "mcp[cli]" ccxt pandas pytest
    python -m pytest tests/ -q
"""
import json
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import ccxt
import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import main as srv  # noqa: E402

BASES = ["WLD", "BTC", "ETH", "SOL", "FET", "GMT", "1000SHIB", "1000FLOKI", "GALA"]


def _simbolo_info(base):
    return {
        "symbol": f"{base}USDT", "pair": f"{base}USDT", "contractType": "PERPETUAL",
        "deliveryDate": 4133404800000, "onboardDate": 1569398400000, "status": "TRADING",
        "maintMarginPercent": "2.5000", "requiredMarginPercent": "5.0000",
        "baseAsset": base, "quoteAsset": "USDT", "marginAsset": "USDT",
        "pricePrecision": 4, "quantityPrecision": 0, "baseAssetPrecision": 8,
        "quotePrecision": 8, "underlyingType": "COIN", "underlyingSubType": [],
        "triggerProtect": "0.0500", "liquidationFee": "0.0125", "marketTakeBound": "0.05",
        "filters": [
            {"filterType": "PRICE_FILTER", "minPrice": "0.0001", "maxPrice": "100000", "tickSize": "0.0001"},
            {"filterType": "LOT_SIZE", "minQty": "1", "maxQty": "10000000", "stepSize": "1"},
            {"filterType": "MARKET_LOT_SIZE", "minQty": "1", "maxQty": "1000000", "stepSize": "1"},
            {"filterType": "MAX_NUM_ORDERS", "limit": 200},
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
            {"filterType": "PERCENT_PRICE", "multiplierUp": "1.05", "multiplierDown": "0.95",
             "multiplierDecimal": "4"},
        ],
        "orderTypes": ["LIMIT", "MARKET", "STOP"], "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
    }


class Resp:
    def __init__(self, status, cuerpo, headers):
        self.status_code = status
        self.reason = {200: "OK", 400: "Bad Request", 418: "I'm a teapot",
                       429: "Too Many Requests"}.get(status, "")
        self.text = json.dumps(cuerpo)
        self.headers = headers
        self.encoding = "utf-8"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code}", response=self)


class Binance:
    """Binance de mentira. Cuenta requests y deja programar el fallo."""

    def __init__(self):
        self.reqs = []                 # rutas pedidas, en orden
        self.peso = 10                 # lo que dira X-MBX-USED-WEIGHT-1M
        self.fallar_desde = None       # indice de request (0-based) a partir del cual falla
        self.fallo = None              # (status, cuerpo, headers extra)
        self.tendencia = {}            # base o (base, tf) -> +1 / -1 (default +1)
        self.velas_rotas = set()       # (base, tf) -> 400 en klines
        self.timeout_si = None         # f(path, query, i) -> True = ese request da timeout

    def request(self, method, url, **kw):
        u = urlparse(url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        i = len(self.reqs)
        self.reqs.append(u.path)
        h = {"X-MBX-USED-WEIGHT-1M": str(self.peso), "Content-Type": "application/json"}
        if self.timeout_si and self.timeout_si(u.path, q, i):
            raise requests.exceptions.Timeout("read timed out")
        if self.fallar_desde is not None and i >= self.fallar_desde:
            st, cuerpo, extra = self.fallo
            return Resp(st, cuerpo, {**h, **extra})
        if u.path.endswith("/time"):
            return Resp(200, {"serverTime": int(time.time() * 1000)}, h)
        if u.path.endswith("/exchangeInfo"):
            return Resp(200, {"timezone": "UTC", "serverTime": int(time.time() * 1000),
                              "rateLimits": [], "exchangeFilters": [], "assets": [],
                              "symbols": [_simbolo_info(b) for b in BASES]}, h)
        if u.path.endswith("/klines"):
            base = q["symbol"][:-4]
            if (base, q["interval"]) in self.velas_rotas:
                return Resp(400, {"code": -1121, "msg": "Invalid symbol."}, h)
            n = int(q.get("limit", 500))
            paso = self.tendencia.get((base, q["interval"]), self.tendencia.get(base, 1))
            ahora = int(time.time() * 1000)
            filas = []
            for j in range(n):
                c = 100 + paso * j * 0.5
                filas.append([ahora - (n - j) * 60000, str(c - 0.2), str(c + 0.6), str(c - 0.6),
                              str(c), "1000", ahora, "100000", 10, "500", "50000", "0"])
            return Resp(200, filas, h)
        if u.path.endswith("/premiumIndex"):
            return Resp(200, {"symbol": q.get("symbol", "X"), "markPrice": "100", "indexPrice": "100",
                              "estimatedSettlePrice": "100", "lastFundingRate": "0.00010000",
                              "interestRate": "0.0001", "nextFundingTime": int(time.time() * 1000) + 3600000,
                              "time": int(time.time() * 1000)}, h)
        ahora = int(time.time() * 1000)
        if u.path.endswith("/aggTrades"):
            return Resp(200, [{"a": j, "p": "204.5", "q": "5", "f": j, "l": j, "T": ahora - (60 - j) * 100,
                               "m": bool(j % 3)} for j in range(60)], h)
        if u.path.endswith("/depth"):
            return Resp(200, {"lastUpdateId": 1, "E": ahora, "T": ahora,
                              "bids": [[str(204.4 - j * 0.1), "50"] for j in range(20)],
                              "asks": [[str(204.6 + j * 0.1), "40"] for j in range(20)]}, h)
        if "/futures/data/" in u.path:
            fila = {"symbol": q.get("symbol"), "longShortRatio": "1.05", "longAccount": "0.51",
                    "shortAccount": "0.49", "buySellRatio": "1.0", "buyVol": "10", "sellVol": "10",
                    "sumOpenInterest": "1000", "sumOpenInterestValue": "100000", "timestamp": ahora}
            return Resp(200, [fila, fila, fila], h)
        return Resp(200, {}, h)

    def n(self, sufijo):
        return sum(1 for p in self.reqs if p.endswith(sufijo))


class _SinUrllib:
    class request:
        @staticmethod
        def Request(*a, **k):
            return None

        @staticmethod
        def urlopen(*a, **k):
            raise OSError("sin red en pruebas")


class Reloj:
    """Sustituto de `time` dentro de main: reloj de pared controlado."""

    def __init__(self, t):
        self.t = t
        self.dormido = []

    def time(self):
        return self.t

    def sleep(self, s):
        self.dormido.append(s)
        self.t += s

    def monotonic(self):
        return self.t

    def strftime(self, *a):
        return time.strftime(*a)

    def gmtime(self, *a):
        return time.gmtime(*a)

    def localtime(self, *a):
        return time.localtime(*a)


class Bybit:
    """Venue de failover de mentira (lo minimo para build_trade_open)."""
    id = "bybit"
    timeframes = {"5m": "5", "15m": "15", "1h": "60"}
    markets = {"WLD/USDT:USDT": {"precision": {"price": 0.0001}}}

    def fetch_time(self):
        return 1

    def load_markets(self):
        return self.markets

    def fetch_ohlcv(self, symbol, timeframe=None, limit=None):
        return [[j * 60000, 200.0, 201.0, 199.0, 200.0, 1.0] for j in range(limit)]

    def price_to_precision(self, s, px):
        return str(round(px, 4))


def _con_bybit(monkeypatch, tmp_path):
    mk0 = srv._mk
    monkeypatch.setattr(srv, "_mk", lambda v: Bybit() if v == "bybit" else mk0(v))
    monkeypatch.setattr(srv, "ACTIVE_FILE", tmp_path / "active.json")


@pytest.fixture
def b(monkeypatch):
    """Servidor limpio + Binance falso ya enchufado como venue activo."""
    falso = Binance()
    for d in (srv._venue_cache, srv._venue_errors, srv._venue_ban_until, srv._ohlcv_cache):
        d.clear()
    srv._active_venue[:] = []
    srv._last_probe_ts[0] = 0.0
    srv._strict_probe_ts[0] = 0.0
    srv._guardia.update({"peso": None, "peso_ts": 0.0, "ultimo_req": 0.0, "requests": 0,
                         "req_min": 0, "req_min_id": 0, "fallos_red": 0,
                         "cortes_429": 0, "cortes_418": 0, "cortes_red": 0, "frenos_tope": 0,
                         "cache_hits": 0, "cache_miss": 0})
    srv._corte.update({"motivo": None, "hasta": 0.0})
    srv._modo_trade[0] = 0
    monkeypatch.setattr(srv, "_GAP_S", 0.0)
    monkeypatch.setattr(srv, "_PESO_TOPE", 1600)
    monkeypatch.setattr(srv, "_PESO_TOPE_TRADE", 2200)
    monkeypatch.setattr(srv, "urllib", _SinUrllib())    # fundingInfo/brackets: sin red en pruebas
    monkeypatch.setattr(srv, "_ESPERA_MAX_S", 6.0)
    monkeypatch.setattr(srv, "_TTL_VELAS_MAX_S", 45.0)

    def mk(venue):
        ex = getattr(ccxt, venue)({"enableRateLimit": False})
        ex.session.request = falso.request
        return srv._instalar_guardia(ex)

    monkeypatch.setattr(srv, "_mk", mk)
    return falso


def test_barrido_completo_y_segundo_barrido_sale_del_cache(b):
    r = srv.watchlist_scan()
    assert r["binance"] is True and r["parcial"] is False
    assert r["cobertura"] == {"simbolos": 9, "completos": 9, "parciales": 0, "sin_datos": 0}
    assert all(f["completo"] and f["cobertura"] == "3/3" for f in r["ranking"])
    assert r["mejor_candidato"]["symbol"] == "WLD/USDT:USDT"      # empate 3/3: gana el tier T1
    assert b.n("/klines") == 27
    r2 = srv.watchlist_scan()
    assert b.n("/klines") == 27, "el segundo barrido no debe pedir velas otra vez"
    assert r2["rate_limit"]["cache_velas"]["hits"] == 27
    assert all(f["edad_max_s"] == 0.0 for f in r["ranking"]), "primer barrido: velas recien pedidas"
    for k in list(srv._ohlcv_cache):                    # envejecer el cache 20 s
        ts, lim, filas = srv._ohlcv_cache[k]
        srv._ohlcv_cache[k] = (ts - 20, lim, filas)
    r3 = srv.watchlist_scan(symbols=["BTC"], timeframes=["15m", "1h", "4h"])
    assert 19.5 <= r3["ranking"][0]["edad_max_s"] <= 23, "la edad del dato cacheado debe declararse"


def test_429_corta_el_barrido_y_no_se_envia_nada_mas(b):
    b.fallar_desde = 12        # time + exchangeInfo + 10 requests buenos, luego 429
    b.fallo = (429, {"code": -1003, "msg": "Too many requests; current limit is 2400 request weight per 1 MINUTE."},
               {"Retry-After": "37"})
    r = srv.watchlist_scan()
    enviados = len(b.reqs)
    assert enviados <= 14, f"siguio pidiendo tras el 429: {enviados} requests"   # 2 hilos: a lo mas 1 en vuelo
    assert r["parcial"] is True and "INTERRUMPIDO" in r["aviso"]
    assert r["rate_limit"]["corte"]["motivo"] == "rate_limit_429"
    assert 30 <= r["rate_limit"]["corte"]["reintentar_en_s"] <= 38
    assert any(f.get("omitido") for f in r["ranking"])
    assert srv._guardia["cortes_429"] == 1, "un solo 429 recibido: los demas requests no salieron"
    # repetir durante el corte: error estructurado y CERO requests nuevos
    r2 = srv.watchlist_scan()
    assert r2["error"] == "binance_no_disponible" and r2["motivo"] == "rate_limit_429"
    assert r2["reintentar_en_s"] <= 38 and "no_reintentar_antes_de_utc" in r2
    r3 = srv.livefull("ETH")
    assert r3["error"] == "binance_no_disponible"
    assert len(b.reqs) == enviados, "con corte activo no debe salir ni un request"


def test_418_respeta_banned_until_y_se_rotula_ban(b):
    hasta_ms = int((time.time() + 1800) * 1000)
    b.fallar_desde = 5
    b.fallo = (418, {"code": -1003, "msg": f"Way too many requests; IP(1.2.3.4) banned until {hasta_ms}. "
                                           "Please use the websocket for live updates to avoid bans."}, {})
    r = srv.watchlist_scan()
    corte = r["rate_limit"]["corte"] if "rate_limit" in r and r["rate_limit"]["corte"] else None
    assert corte and corte["motivo"] == "ban_418"
    assert abs(srv._venue_ban_until["binanceusdm"] - hasta_ms / 1000) < 1
    assert abs(srv._corte["hasta"] - hasta_ms / 1000) < 1
    antes = len(b.reqs)
    r2 = srv.wldlive()
    assert r2["motivo"] == "ban_418" and "IP baneada por Binance" in r2["detalle"]
    assert len(b.reqs) == antes


def test_tope_preventivo_no_dispara_con_la_ip_saturada(b, monkeypatch):
    monkeypatch.setattr(srv, "_ESPERA_MAX_S", 0.0)     # sin espera: debe frenar, no dormir
    b.peso = 2100                                       # otro inquilino tiene la IP al 87%
    r = srv.watchlist_scan()
    assert r["error"] == "binance_no_disponible" and r["motivo"] == "tope_preventivo"
    assert r["reintentar_en_s"] <= 62
    assert len(b.reqs) == 1, "solo el probe barato que revelo el peso; nada mas"
    assert srv._guardia["cortes_429"] == 0 and srv._guardia["cortes_418"] == 0


def test_tope_espera_al_cambio_de_minuto_si_falta_poco(b, monkeypatch):
    dormidas = []
    monkeypatch.setattr(srv, "_ESPERA_MAX_S", 100.0)
    monkeypatch.setattr(srv.time, "sleep", lambda s: dormidas.append(s))
    b.peso = 2100
    ex = srv._mk("binanceusdm")
    ex.fetch_time()                     # revela el peso
    b.peso = 20                         # minuto nuevo en Binance
    ex.fetch_time()                     # debe esperar y luego pedir
    assert dormidas and 1.0 <= dormidas[0] <= 61.0
    assert len(b.reqs) == 2 and srv._guardia["peso"] == 20
    assert not srv._cortado()


def test_mejor_candidato_exige_cobertura_completa(b):
    # BTC solo trae 1 de 3 timeframes (y a favor); ETH trae 3 de 3. Antes ganaba BTC con "100%".
    b.velas_rotas = {("BTC", "15m"), ("BTC", "4h")}
    r = srv.watchlist_scan(symbols=["BTC", "ETH"])
    filas = {f["symbol"]: f for f in r["ranking"]}
    btc, eth = filas["BTC/USDT:USDT"], filas["ETH/USDT:USDT"]
    assert btc["cobertura"] == "1/3" and btc["completo"] is False and btc["alineacion_pct"] == 33
    assert btc["read"] == "incompleto" and set(btc["tf_errores"]) == {"15m", "4h"}
    assert btc["price"] is not None, "el precio sale del TF que si llego"
    assert eth["cobertura"] == "3/3" and eth["alineacion_pct"] == 100 and eth["read"] == "alineado_long"
    assert r["ranking"][0]["symbol"] == "ETH/USDT:USDT"
    assert r["mejor_candidato"]["symbol"] == "ETH/USDT:USDT"
    assert r["parcial"] is True and "sin rate-limit" in r["aviso"]
    assert not srv._cortado(), "un 400 de simbolo/TF no es rate-limit: no corta nada"


def test_sin_ningun_completo_no_se_nombra_candidato(b):
    b.velas_rotas = {("BTC", "4h"), ("ETH", "15m")}
    r = srv.watchlist_scan(symbols=["BTC", "ETH"])
    assert r["mejor_candidato"] is None and "todos los timeframes" in r["mejor_candidato_motivo"]


def test_alineado_short_y_mixto(b):
    b.tendencia = {"SOL": -1, ("ETH", "15m"): -1}
    r = srv.watchlist_scan(symbols=["SOL", "ETH"])
    filas = {f["symbol"]: f for f in r["ranking"]}
    sol, eth = filas["SOL/USDT:USDT"], filas["ETH/USDT:USDT"]
    assert sol["lado"] == "short" and sol["read"] == "alineado_short" and sol["net_score"] == -3
    assert eth["tf_signals"] == {"15m": -1, "1h": 1, "4h": 1} and eth["net_score"] == 1
    assert eth["read"] == "mixed_no_alignment" and eth["alineacion_pct"] == 33 and eth["completo"]
    assert r["mejor_candidato"]["symbol"] == "SOL/USDT:USDT"
    solo_eth = srv.watchlist_scan(symbols=["ETH"])
    assert solo_eth["mejor_candidato"] is None and "75%" in solo_eth["mejor_candidato_motivo"]


def test_la_gestion_de_trade_no_usa_cache(b, monkeypatch, tmp_path):
    monkeypatch.setattr(srv, "ACTIVE_FILE", tmp_path / "active.json")
    srv.multi_tf_snapshot("WLD", ["5m", "15m", "1h"])        # ANALISIS: llena el cache
    assert b.n("/klines") == 3
    srv.multi_tf_snapshot("WLD", ["5m", "15m", "1h"])        # analisis otra vez: del cache
    assert b.n("/klines") == 3 and srv._guardia["cache_hits"] == 3
    r0 = srv.trade_open("WLD", "long", 204.0, entry_tf="5m") # ciclo de trade: velas frescas
    assert r0["venue_at_open"] == "binanceusdm" and b.n("/klines") == 4
    r = srv.actualizame()                                    # 5m/15m/1h del snapshot + trailing
    assert r["trigger"] == "actualizame(trade)" and r["venue_now"] == "binanceusdm"
    assert b.n("/klines") == 8 and srv._guardia["cache_hits"] == 3, "el trade abierto no lee el cache"


def test_cache_sirve_un_limite_menor_desde_uno_mayor_y_declara_edad(b):
    ex = srv._mk("binanceusdm")
    d1 = srv._fetch_any(ex, "WLD/USDT:USDT", "1h", 210, cache=True)
    d2 = srv._fetch_any(ex, "WLD/USDT:USDT", "1h", 130, cache=True)
    assert b.n("/klines") == 1 and len(d1) == 210 and len(d2) == 130
    assert d1.attrs["edad_s"] == 0.0
    ts, lim, filas = srv._ohlcv_cache[("binanceusdm", "WLD/USDT:USDT", "1h")]
    srv._ohlcv_cache[("binanceusdm", "WLD/USDT:USDT", "1h")] = (ts - 12, lim, filas)
    d3 = srv._fetch_any(ex, "WLD/USDT:USDT", "1h", 130, cache=True)
    assert 11.5 <= d3.attrs["edad_s"] <= 14 and b.n("/klines") == 1
    srv._ohlcv_cache[("binanceusdm", "WLD/USDT:USDT", "1h")] = (ts - 60, lim, filas)   # vencido (TTL 45 s)
    srv._fetch_any(ex, "WLD/USDT:USDT", "1h", 130, cache=True)
    assert b.n("/klines") == 2, "un cache vencido no se sirve"
    srv._fetch_any(ex, "WLD/USDT:USDT", "1h", 130, cache=True, refrescar=True)
    assert b.n("/klines") == 3, "refrescar=True no lee el cache"
    assert list(d2["t"]) == list(d1["t"])[-130:]
    srv._fetch_any(ex, "WLD/USDT:USDT", "1h", 400, cache=True)   # pide mas de lo cacheado
    assert b.n("/klines") == 4


def test_ttl_de_velas_queda_muy_por_debajo_de_la_vela():
    assert srv._ttl_velas("1m") == 10.0 and srv._ttl_velas("5m") == 15.0
    assert srv._ttl_velas("15m") == 45.0 and srv._ttl_velas("1d") == 45.0


def test_venue_health_expone_la_guardia(b):
    b.peso = 321
    h = srv.venue_health()
    assert h["guardia"]["peso_ip_1m"] == 321 and h["guardia"]["limite"] == 2400
    assert h["guardia"]["corte"] is None and "v5.2" in h["build"]


def test_un_corte_de_la_guardia_no_lo_acorta_el_castigo_generico(b):
    b.fallar_desde = 2
    b.fallo = (429, {"code": -1003, "msg": "Too many requests"}, {"Retry-After": "300"})
    srv.watchlist_scan()
    assert srv._venue_ban_until["binanceusdm"] - time.time() > 290, "Retry-After de 300 s pisado por los 120 s genericos"


# ---------------- regresiones de la revision adversarial (2026-10-06) ----------------

def test_418_sin_banned_until_es_ban_de_120s(b):
    b.fallar_desde = 3
    b.fallo = (418, {"code": -1003, "msg": "Way too many requests."}, {})
    srv.watchlist_scan()
    assert srv._corte["motivo"] == "ban_418" and srv._guardia["cortes_418"] == 1
    assert 115 <= srv._corte["hasta"] - time.time() <= 121


def test_429_tiene_piso_de_30s_aunque_retry_after_diga_menos(b):
    b.fallar_desde = 3
    b.fallo = (429, {"code": -1003, "msg": "Too many requests"}, {"Retry-After": "5"})
    srv.watchlist_scan()
    assert srv._corte["motivo"] == "rate_limit_429"
    assert 28 <= srv._corte["hasta"] - time.time() <= 31


def test_un_timeout_con_418_en_la_url_no_es_ban(b):
    b.timeout_si = lambda path, q, i: path.endswith("/aggTrades")
    r = srv.realtime_pulse("WLD", trades_n=418)             # la URL lleva &limit=418
    assert srv._guardia["cortes_418"] == 0 and srv._guardia["cortes_429"] == 0
    assert srv._corte_motivo() is None, "un timeout suelto no corta nada en la guardia"
    assert r["motivo"] == "fallo_transitorio"


def test_dos_fallos_de_red_seguidos_cortan_45s_y_el_barrido_no_se_cuelga(b):
    b.timeout_si = lambda path, q, i: path.endswith("/klines")
    r = srv.watchlist_scan()
    assert b.n("/klines") == 2, "tras 2 timeouts seguidos no se manda ni una vela mas"
    assert srv._guardia["cortes_red"] == 1 and srv._corte["motivo"] == "red_inestable"
    assert 40 <= srv._corte["hasta"] - time.time() <= 46
    assert r["error"] == "binance_no_disponible" and r["motivo"] == "red_inestable"


def test_un_request_bueno_rompe_la_racha_de_fallos_de_red(b):
    b.timeout_si = lambda path, q, i: path.endswith("/klines") and q["interval"] == "1h"
    ex = srv._mk("binanceusdm")
    for tf in ("15m", "1h", "4h", "1h", "15m"):             # timeout, bueno, timeout: nunca 2 seguidos
        try:
            ex.fetch_ohlcv("BTC/USDT:USDT", timeframe=tf, limit=30)
        except ccxt.RequestTimeout:
            pass
    assert srv._guardia["cortes_red"] == 0 and not srv._cortado()
    for _ in range(2):                                      # ahora si: dos seguidos
        with pytest.raises(ccxt.RequestTimeout):
            ex.fetch_ohlcv("BTC/USDT:USDT", timeframe="1h", limit=30)
    assert srv._guardia["cortes_red"] == 1 and srv._corte_motivo() == "red_inestable"
    with pytest.raises(ccxt.RateLimitExceeded):             # y el siguiente ya no sale a la red
        ex.fetch_ohlcv("BTC/USDT:USDT", timeframe="15m", limit=30)


def test_los_requests_a_binance_salen_de_uno_en_uno(b):
    import threading
    estado = {"en_vuelo": 0, "maximo": 0}
    candado = threading.Lock()
    real = b.request

    def lento(method, url, **kw):
        with candado:
            estado["en_vuelo"] += 1
            estado["maximo"] = max(estado["maximo"], estado["en_vuelo"])
        time.sleep(0.01)
        try:
            return real(method, url, **kw)
        finally:
            with candado:
                estado["en_vuelo"] -= 1

    ex = srv._mk("binanceusdm")
    ex.session.request = lento
    ex.load_markets()
    hilos = [threading.Thread(target=ex.fetch_ohlcv, args=("BTC/USDT:USDT",), kwargs={"timeframe": tf, "limit": 30})
             for tf in ("1m", "5m", "15m", "1h", "4h", "1d")]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    assert b.n("/klines") == 6 and estado["maximo"] == 1


def test_venue_health_no_acorta_un_corte_de_la_guardia(b):
    b.fallar_desde = 2                  # time + exchangeInfo bien; las velas del probe dan 429
    b.fallo = (429, {"code": -1003, "msg": "Too many requests"}, {"Retry-After": "300"})
    h = srv.venue_health()
    assert h["binance"] is False and h["guardia"]["corte"]["motivo"] == "rate_limit_429"
    assert srv._venue_ban_until["binanceusdm"] - time.time() > 290
    assert 290 < h["guardia"]["corte"]["reintentar_en_s"] <= 301


def test_lectura_del_minuto_anterior_no_frena_un_minuto_entero(b, monkeypatch):
    reloj = Reloj(1_800_000_000 - 1_800_000_000 % 60 + 59.5)   # hh:mm:59.5
    monkeypatch.setattr(srv, "time", reloj)
    b.peso = 2100
    ex = srv._mk("binanceusdm")
    ex.fetch_time()                    # lectura 2100 tomada en mm:59.5
    reloj.t += 0.8                     # mm+1:00.3 -> Binance ya reinicio el peso
    b.peso = 5
    ex.fetch_time()                    # espera lo que falta para mm+1:01 y pide
    assert reloj.dormido and 0.0 <= reloj.dormido[0] <= 1.0
    assert len(b.reqs) == 2 and srv._guardia["peso"] == 5 and not srv._cortado()


def test_una_lectura_de_peso_vieja_caduca_y_no_frena(b):
    ex = srv._mk("binanceusdm")
    srv._guardia.update({"peso": 2300, "peso_ts": time.time() - 180})   # de hace 3 minutos
    ex.fetch_time()
    assert len(b.reqs) == 1 and srv._guardia["frenos_tope"] == 0 and srv._guardia["peso"] == 10


def test_el_freno_preventivo_no_saca_de_binance_a_un_trade(b, monkeypatch, tmp_path):
    _con_bybit(monkeypatch, tmp_path)
    monkeypatch.setattr(srv, "_ESPERA_MAX_S", 0.0)
    b.peso = 1700                       # 71% del limite: sobre el tope de analisis, bajo el de trade
    assert srv._ex().id == "binanceusdm"
    a = srv.watchlist_scan()            # el ANALISIS si se frena
    assert a["motivo"] == "tope_preventivo"
    r = srv.trade_open("WLD", "long", 100.0)
    assert r["venue_at_open"] == "binanceusdm", "el ciclo de trade sigue en Binance"
    assert srv._modo_trade[0] == 0


def test_el_trade_si_se_frena_sobre_su_propio_tope_y_no_alarga_el_corte(b, monkeypatch, tmp_path):
    _con_bybit(monkeypatch, tmp_path)
    monkeypatch.setattr(srv, "_ESPERA_MAX_S", 0.0)
    b.peso = 2300                       # sobre el tope de trade (2200): no se dispara
    assert srv._ex().id == "binanceusdm"
    r = srv.trade_open("WLD", "long", 100.0)
    assert r["venue_at_open"] == "bybit"
    assert srv._corte["motivo"] == "tope_preventivo"
    assert srv._corte["hasta"] - time.time() <= 61, "el failover no estira el freno a 120 s"
    assert abs(srv._venue_ban_until["binanceusdm"] - srv._corte["hasta"]) < 1


def test_un_timeout_en_analisis_no_saca_de_binance_al_trade(b, monkeypatch, tmp_path):
    _con_bybit(monkeypatch, tmp_path)
    assert srv._ex().id == "binanceusdm"
    estado = {"n": 0}

    def una_vez(path, q, i):
        if path.endswith("/aggTrades") and estado["n"] == 0:
            estado["n"] = 1
            return True
        return False

    b.timeout_si = una_vez
    a = srv.realtime_pulse("WLD")
    assert a["motivo"] == "fallo_transitorio"            # castigo generico de 120 s: solo analisis
    assert srv.watchlist_scan()["error"] == "binance_no_disponible"
    r = srv.trade_open("WLD", "long", 204.0)
    assert r["venue_at_open"] == "binanceusdm"


def test_tres_de_cuatro_alineados_no_es_candidato(b):
    b.velas_rotas = {("BTC", "1d")}
    r = srv.watchlist_scan(symbols=["BTC"], timeframes=["15m", "1h", "4h", "1d"])
    f = r["ranking"][0]
    assert f["alineacion_pct"] == 75 and f["completo"] is False
    assert r["mejor_candidato"] is None


def test_timeframes_duplicados_no_rompen_la_cobertura(b):
    r = srv.watchlist_scan(symbols=["BTC"], timeframes=["15m", "15m", "1h"])
    assert r["timeframes"] == ["15m", "1h"] and r["ranking"][0]["cobertura"] == "2/2"


def test_el_plan_pide_fresco_el_tf_de_entrada_y_declara_su_edad(b):
    srv.watchlist_scan(symbols=["WLD"])                     # deja 15m/1h/4h en cache
    for k in list(srv._ohlcv_cache):
        ts, lim, filas = srv._ohlcv_cache[k]
        srv._ohlcv_cache[k] = (ts - 30, lim, filas)         # el cache tiene 30 s
    n = b.n("/klines")
    r = srv.trade_plan("WLD", direction="long", entry_tf="15m")
    pedidas = [p for p in b.reqs if p.endswith("/klines")][n:]
    assert len(pedidas) == 2, "15m (fresco) + 1d (no estaba); 1h y 4h del cache"
    plan = r.get("plan")
    if plan:                                                 # si hubo estructura para TP
        assert plan["precio_edad_s"] == 0.0
    snap = srv.multi_tf_snapshot("WLD", ["15m"])
    assert snap["timeframes"]["15m"]["edad_s"] < 5, "el fresco quedo escrito en el cache"


def test_el_corte_conserva_el_detalle_original_del_429(b):
    b.fallar_desde = 2
    b.fallo = (429, {"code": -1003, "msg": "Too many requests"}, {"Retry-After": "40"})
    srv.watchlist_scan()
    srv.watchlist_scan()
    assert "429" in srv._venue_errors["binanceusdm"] and "Too many requests" in srv._venue_errors["binanceusdm"]


def test_hueco_minimo_entre_requests(b, monkeypatch):
    dormidas = []
    monkeypatch.setattr(srv, "_GAP_S", 0.05)
    reloj = Reloj(1_800_000_010.0)
    monkeypatch.setattr(srv, "time", reloj)
    ex = srv._mk("binanceusdm")
    ex.fetch_time()
    ex.fetch_time()
    assert reloj.dormido and abs(reloj.dormido[-1] - 0.05) < 1e-6


def test_requests_minuto_cuenta_solo_el_minuto_en_curso(b):
    srv.watchlist_scan(symbols=["BTC"])
    e = srv._guardia_estado()
    assert e["requests_minuto"] == e["requests"] > 0
    srv._guardia["req_min_id"] -= 1                          # como si hubiera cambiado el minuto
    assert srv._guardia_estado()["requests_minuto"] == 0


def test_mk_real_instala_la_guardia_solo_en_binance():
    ex = srv._mk("binanceusdm")                              # _mk REAL, sin fixture
    assert getattr(ex, "_guardia_on", False) and ex.fetch.__name__ == "fetch_guardado"
    envuelto = ex.fetch
    assert srv._instalar_guardia(ex) is ex and ex.fetch is envuelto, "no se envuelve dos veces"
    assert not getattr(srv._mk("bybit"), "_guardia_on", False), "la guardia es solo para Binance"


def test_un_timeout_no_relee_las_cabeceras_de_la_respuesta_anterior(b):
    ex = srv._mk("binanceusdm")
    b.peso = 500
    ex.fetch_time()
    ts = srv._guardia["peso_ts"]
    b.timeout_si = lambda path, q, i: True
    time.sleep(0.02)
    with pytest.raises(ccxt.RequestTimeout):
        ex.fetch_time()
    assert srv._guardia["peso_ts"] == ts, "el peso no se da por recien leido tras un timeout"
