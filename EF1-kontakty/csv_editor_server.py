#!/usr/bin/env python3
"""
Jednoduchý lokální server pro csv_editor.html:
- servíruje statické soubory ze složky projektu
- umí uložit CSV zpět do vybraného CSV přes POST /save (whitelist)

Spuštění:
  cd "/Users/helenamich/Desktop/KONTAKTY EF1 čištění"
  python3 csv_editor_server.py
  otevři http://localhost:8000/csv_editor.html
"""

from __future__ import annotations

import json
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


DEFAULT_FILE = "kontakty_unified.csv"
ALLOWED_FILES = {
    "kontakty_unified.csv",
    "kontakty_poptavky.csv",
    "deals_complete.csv",
    "deals_poptavky.csv",
    "poptavky_deals_filtered.csv",
}


class Handler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        # při vývoji nechceme cache (jinak se v prohlížeči drží starý JS/HTML a chování vypadá “náhodně”)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _send_json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        # allow fetch from same-origin; harmless to allow local tools too
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):  # noqa: N802
        if self.path.rstrip("/") != "/save":
            return self._send_json(404, {"ok": False, "error": "Not found"})

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0

        raw = self.rfile.read(length) if length > 0 else b""
        ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()

        filename = DEFAULT_FILE
        csv_text = ""

        if ctype == "application/json":
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "Invalid JSON"})
            filename = (payload.get("filename") or DEFAULT_FILE).strip()
            csv_text = payload.get("csv") or ""
        else:
            # fallback: raw CSV body
            csv_text = raw.decode("utf-8", errors="replace")

        # bezpečnost: povolíme uložit jen do whitelistu (žádné jiné cesty)
        filename = os.path.basename(filename)
        if filename not in ALLOWED_FILES:
            return self._send_json(
                400,
                {
                    "ok": False,
                    "error": f"File not allowed. Allowed: {sorted(ALLOWED_FILES)}",
                },
            )

        try:
            with open(filename, "w", encoding="utf-8", newline="") as f:
                f.write(csv_text)
        except Exception as e:
            return self._send_json(500, {"ok": False, "error": f"Write failed: {e}"})

        return self._send_json(200, {"ok": True, "saved": filename, "bytes": len(csv_text.encode("utf-8"))})


def main() -> None:
    # servírujeme z adresáře skriptu
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    # Pozn.: na macOS může `localhost` preferovat IPv6 (::1). Když bindneme jen na 127.0.0.1,
    # některé prohlížeče pak `http://localhost:8000` neotevřou. Proto bindneme na všechny IPv4 adresy.
    host = "0.0.0.0"
    port = 8000
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"CSV editor server running on http://127.0.0.1:{port}/csv_editor.html")
    print(f"CSV editor server running on http://localhost:{port}/csv_editor.html")
    httpd.serve_forever()


if __name__ == "__main__":
    main()

