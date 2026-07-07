FROM python:3.13-slim

# delva-perp-extras MCP — day trading de perpetuos Binance USDT-M (12 tools).
RUN pip install --no-cache-dir "mcp[cli]>=1.6.0" "ccxt>=4.4.75" "pandas>=2.2.3"

WORKDIR /app
COPY main.py /app/main.py
# launcher.py neutraliza el check Host/Origin del SDK (si no, HTTP 421 detrás de Render).
COPY launcher.py /app/launcher.py

ENV PORT=8000
ENV HOST=0.0.0.0
EXPOSE 8000

# Modo remoto (streamable-http). Endpoint MCP: /mcp
# /mcp es POST-only y requiere Accept: text/event-stream (GET normal da 404/406
# por diseño -> NO configurar health check HTTP).
CMD ["python", "-u", "/app/launcher.py"]
