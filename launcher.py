"""Remote (streamable-http) launcher for hosting behind a public domain (Render, etc.).

Same pattern as tradingview-mcp-remote: the MCP Python SDK's DNS-rebinding
protection validates the incoming ``Host``/``Origin`` headers and returns
HTTP 421 "Invalid Host header" for any public hostname (e.g. *.onrender.com).
This server exposes only public market data + a per-session paper-trade state
(no auth, no exchange keys, no writes to any account), so neutralising the
Host/Origin checks is safe here. Content-Type validation is left untouched.

NOTE state: en Render el disco es efímero -> el journal remoto es de sesión.
La fuente de verdad del journal es la máquina local (Claude Desktop).
"""
import os
import sys

import mcp.server.transport_security as _ts

# Accept any Host / Origin header (fix HTTP 421 behind Render).
_ts.TransportSecurityMiddleware._validate_host = lambda self, host: True
_ts.TransportSecurityMiddleware._validate_origin = lambda self, origin: True

import main as srv  # registra los 12 tools en srv.mcp

srv.mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
srv.mcp.settings.port = int(os.environ.get("PORT", "8000"))

try:
    n = len(srv.mcp._tool_manager._tools)
    sys.stderr.write(f"[launcher] delva-perp-extras remote: {n} tools registered, "
                     f"port {srv.mcp.settings.port}\n")
except Exception:
    pass
sys.stderr.flush()

if __name__ == "__main__":
    # MCP endpoint en /mcp (POST-only, Accept: text/event-stream).
    srv.mcp.run(transport="streamable-http")
