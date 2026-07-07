# delva-perp-extras (remote MCP)

MCP de day trading de perpetuos Binance USDT-M, hosteado como connector remoto (streamable-http) para claude.ai **web y móvil** — mismo patrón que `tradingview-mcp-remote` (incl. fix HTTP 421 en `launcher.py`).

## 12 tools

| Grupo | Tools |
|---|---|
| Análisis | `multi_tf_snapshot` (todos los TFs nativos en paralelo + 45m/3h resampleados), `resample_ohlcv` |
| Plan | `trade_plan` (entrada mercado/pullback con hora, stop, 3 TP con ETA y hora, caducidad del setup, sizing) |
| Posicionamiento | `long_short_and_oi`, `positioning` (top traders vs retail, taker ratio, basis, countdown funding) |
| Ahora | `realtime_pulse` (order-flow REST + libro) |
| Triggers skill | `wldlive`, `wldlivenow`, `wldlivefull` |
| Trade lifecycle | `trade_open`, `actualizame` (context-aware; detector de cambio de tendencia 0-6 con setup contrario), `trade_close`, `journal` |

## Deploy (Render)

1. Crear repo en GitHub (privado) y push de esta carpeta.
2. Render → New + → Blueprint → elegir el repo → Apply.
3. URL del connector: `https://<servicio>.onrender.com/mcp`
4. claude.ai → Settings → Connectors → Add custom connector → pegar la URL.

## Seguridad y estado

- Solo datos públicos de mercado; **sin API keys de exchange, sin órdenes reales**.
- Disco de Render = efímero → el **journal remoto es de sesión**; la fuente de verdad del journal es la máquina local (Claude Desktop, `state/journal.jsonl`).
- `/mcp` es POST-only (`Accept: text/event-stream`); un GET del navegador da 404/406 y es normal.
