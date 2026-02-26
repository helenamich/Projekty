#!/usr/bin/env python3
"""
Propojí HR kontakty s Klienty do pole HR Kontakt.
"""

import json
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote
from collections import defaultdict

import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"
BATCH_SIZE = 10


def get_token() -> str:
    with open(Path.home() / ".cursor" / "mcp.json") as f:
        return json.load(f)["mcpServers"]["airtable"]["env"]["AIRTABLE_API_KEY"]


def headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def request_with_backoff(method: str, url: str, *, hdrs: dict, json_data=None, params=None) -> dict:
    delay = 1.0
    for attempt in range(1, 8):
        resp = requests.request(method, url, headers=hdrs, json=json_data, params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay)
            delay = min(delay * 2, 20)
            continue
        if not resp.ok:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text[:500]}")
        return resp.json()
    raise RuntimeError("Airtable API still failing after retries")


def chunked(items: List, size: int) -> List[List]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Najdi všechny HR kontakty
    print("🔎 Hledám HR kontakty...")
    kontakty_url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    
    hr_by_klient = defaultdict(list)  # klient_id -> [kontakt_ids]
    offset = None
    total_hr = 0
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            oddeleni = fields.get("Oddělení", [])
            klienti = fields.get("Klienti", [])
            
            # Je to HR kontakt?
            if "HR" in oddeleni and klienti:
                total_hr += 1
                for klient_id in klienti:
                    hr_by_klient[klient_id].append(rec["id"])
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Nalezeno {total_hr} HR kontaktů pro {len(hr_by_klient)} klientů")
    
    if not hr_by_klient:
        print("\n⚠️ Žádné HR kontakty k propojení!")
        print("   Tip: Označ kontakty jako HR v poli 'Oddělení'")
        return
    
    # 2. Aktualizuj Klienty
    print("\n⬆️ Propojuji HR kontakty s Klienty...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    updates = [
        {"id": klient_id, "fields": {"HR Kontakt": kontakt_ids}}
        for klient_id, kontakt_ids in hr_by_klient.items()
    ]
    
    updated = 0
    for batch in chunked(updates, BATCH_SIZE):
        request_with_backoff("PATCH", klienti_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        time.sleep(0.2)
    
    print(f"\n✅ Propojeno {updated} klientů s HR kontakty!")


if __name__ == "__main__":
    main()
