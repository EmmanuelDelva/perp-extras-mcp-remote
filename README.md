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

## Deploy (Render)

1. Crear repo en GitHub (privado) y push de esta carpeta.
2. Render → New + → Blueprint → elegir el repo → Apply.
3. URL del connector: `https://<servicio>.onrender.com/mcp`
4. claude.ai → Settings → Connectors → Add custom connector → pegar la URL.

## Seguridad y estado

- Solo datos públicos de mercado; **sin API keys de exchange, sin órdenes reales**.
- Disco de Render = efímero → el **journal remoto es de sesión**; la fuente de verdad del journal es la máquina local (Claude Desktop, `state/journal.jsonl`).
- `/mcp` es POST-only (`Accept: text/event-stream`); un GET del navegador da 404/406 y es normal.
