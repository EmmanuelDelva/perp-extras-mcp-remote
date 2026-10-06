# delva-perp-extras (remote MCP)

MCP de day trading de perpetuos Binance USDT-M, hosteado como connector remoto (streamable-http) para claude.ai **web y móvil** — mismo patrón que `tradingview-mcp-remote` (incl. fix HTTP 421 en `launcher.py`).

## 18 tools

| Grupo | Tools |
|---|---|
| Análisis | `multi_tf_snapshot` (todos los TFs nativos en paralelo + 45m/3h resampleados), `resample_ohlcv` |
| Plan | `trade_plan` (entrada mercado/pullback con hora, stop, 3 TP con ETA y hora, caducidad del setup, sizing) |
| Posicionamiento | `long_short_and_oi`, `positioning` (top traders vs retail, taker ratio, basis, countdown funding) |
| Ahora | `realtime_pulse` (order-flow REST + libro) |
| Triggers skill | `wldlive`, `wldlivenow`, `wldlivefull` (compat) + genéricos multi-moneda `live`, `livenow`, `livefull` + `watchlist_scan` (radar T1/T2/T3 en 1 llamada) |
| Trade lifecycle | `trade_open`, `actualizame` (context-aware; detector de cambio de tendencia 0-6 con setup contrario), `trade_close`, `journal` |
| Diagnóstico | `venue_health` (venue activo, cadena, bans, build desplegado) |

## Resiliencia (v4, 2026-07-10)

- **Democión en caliente**: si el venue activo empieza a fallar a media sesión (p.ej. ban 418
  de Binance sobre la IP compartida de Render), se marca su ventana de ban, se re-sondea la
  cadena `binanceusdm→bybit→okx` y el builder se reintenta UNA vez en el siguiente venue vivo.
  (Antes: el proceso quedaba clavado en Binance muerto → `KeyError 'price'` en los gatillos.)
- **Barrera `venue_at_open`**: un trade abierto con precio de Binance nunca se gestiona en
  silencio con precio de Bybit — aviso arriba, sin auto-BE/trailing cross-venue, y cerrar con
  venue cruzado exige el fill real (`exit_price`).
- **Símbolos laxos** (`BTC`, `btcusdt`) + resolución por venue con **seguridad de escala**
  (Binance `1000SHIB` ↔ Bybit `SHIB1000`; nunca `1000X → X` silencioso).
- Degradación siempre rotulada (`venue`/`degraded` en cada respuesta); dato faltante → `null`.

## Guardia de rate-limit (v5.2, 2026-10-06)

Binance cuenta el peso **por IP** y la IP de salida de Render es compartida con otros
servicios. Tres mañanas seguidas (4, 5 y 6 de octubre) la IP amaneció baneada (418) justo
antes del NY Open. La guardia envuelve `ex.fetch` —el único punto por el que ccxt sale a la
red— y hace cuatro cosas:

- **Lee el peso real de la IP** (`X-MBX-USED-WEIGHT-1M`) en cada respuesta y no dispara si
  ya pasó el tope (`PERP_WEIGHT_CEIL`, 1600 de 2400). Si falta poco para el cambio de minuto
  espera; si no, responde `motivo: tope_preventivo` con los segundos exactos.
- **Corta todo el proceso al primer 429 o 418**, hasta `Retry-After` / `banned until`. Antes,
  un 429 se guardaba como error de un timeframe y el barrido seguía pidiendo: eso es lo que
  Binance escala a ban.
- **Corta 45 s tras dos fallos de red seguidos.** Con los requests en serie, un Binance
  colgado haría esperar 10 s por cada uno de los ~36 requests de un barrido.
- **Cache corto de velas** (10–45 s, solo análisis; la gestión de un trade abierto siempre
  pide fresco). Cada timeframe declara `edad_s`; el TF de entrada de un plan se pide siempre
  fresco (`plan.precio_edad_s`).

**Un trade abierto manda sobre la protección de la IP.** Las tools del ciclo de trade
(`trade_open`, `actualizame` con trade, `trade_close`) no se frenan por el tope preventivo de
análisis: tienen su propio tope, más alto (`PERP_WEIGHT_CEIL_TRADE`). El castigo genérico de
120 s tras un fallo suelto sigue frenando solo el análisis, como en v5.1.

`watchlist_scan` ahora declara `cobertura` por fila, mide `alineacion_pct` sobre los
timeframes pedidos y solo nombra `mejor_candidato` con cobertura completa (antes, 1 señal de
1 daba 100% y le ganaba a un 3 de 3). Las respuestas `binance_no_disponible` traen
`rate_limit`, y mientras haya un corte o castigo vigente también `motivo` (`ban_418`,
`rate_limit_429`, `tope_preventivo`, `red_inestable` o `fallo_transitorio`) y
`reintentar_en_s`; un geo-block sin ventana conocida no los trae. `venue_health` trae el
bloque `guardia`.

| Variable | Default | Qué hace |
|---|---|---|
| `PERP_WEIGHT_LIMIT` | 2400 | Límite de peso por IP y minuto de fapi |
| `PERP_WEIGHT_CEIL` | 1600 | Por encima de este peso de la IP el análisis no envía nada |
| `PERP_WEIGHT_CEIL_TRADE` | 2200 | El mismo tope para el ciclo de trade (trade abierto) |
| `PERP_NET_CUT_S` | 45 | Duración del corte tras dos fallos de red seguidos |
| `PERP_MIN_GAP_MS` | 60 | Hueco mínimo entre requests a Binance |
| `PERP_MAX_WAIT_S` | 6 | Espera máxima al cambio de minuto antes de responder error |
| `PERP_OHLCV_TTL_MAX` | 45 | Techo del cache de velas en segundos; 0 lo apaga |

### IP compartida: lo que el código no arregla

La guardia impide que **este** servidor provoque o alargue un ban. No impide que otro
inquilino de la misma IP la sature. Para saber quién gasta la IP: `venue_health` →
compara `guardia.peso_ip_1m` (peso de toda la IP en el minuto) con `guardia.requests_minuto`
(requests de este servidor en ese mismo minuto; cada uno pesa 1–2, las velas de 210 pesan 2).
Si el peso de la IP es muy superior a lo que explican los requests propios, el peso es
ajeno. Si eso se confirma, la salida es una IP propia: la instancia local (IP de casa) o un
VPS pequeño.

Dos llamadas no pasan por la guardia porque no usan ccxt: `fundingInfo` (una vez cada 24 h)
y el espejo público de brackets. Ambas respetan la ventana de ban conocida.

### Pruebas

```
pip install "mcp[cli]>=1.6.0,<2" "ccxt>=4.4.75,<5" "pandas>=2.2.3,<4" pytest
python -m pytest tests/ -q
```

33 pruebas sin red: falsean `session.request` de ccxt, así se ejercita el camino real
(errores 429/418 y timeouts de ccxt incluidos). Cubren también el ciclo de trade con
failover a otro venue.

## Deploy (Render)

1. Crear repo en GitHub (privado) y push de esta carpeta.
2. Render → New + → Blueprint → elegir el repo → Apply.
3. URL del connector: `https://<servicio>.onrender.com/mcp`
4. claude.ai → Settings → Connectors → Add custom connector → pegar la URL.

## Seguridad y estado

- Solo datos públicos de mercado; **sin API keys de exchange, sin órdenes reales**.
- Disco de Render = efímero → el **journal remoto es de sesión**; la fuente de verdad del journal es la máquina local (Claude Desktop, `state/journal.jsonl`).
- `/mcp` es POST-only (`Accept: text/event-stream`); un GET del navegador da 404/406 y es normal.
