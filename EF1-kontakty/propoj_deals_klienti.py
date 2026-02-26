#!/usr/bin/env python3
"""
Propojí Deals - doplněk s Klienty podle názvu firmy.
"""

import json
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"
BATCH_SIZE = 10


def get_token() -> str:
    mcp_path = Path.home() / ".cursor" / "mcp.json"
    with open(mcp_path, "r") as f:
        config = json.load(f)
    return config["mcpServers"]["airtable"]["env"]["AIRTABLE_API_KEY"]


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


def normalize_company(s):
    s = (s or "").strip().lower()
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',']:
        s = s.replace(suffix, '')
    return s.strip()


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti všechny Klienty
    print("🔎 Načítám Klienty...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    klienti_by_firma = {}  # normalized_firma -> record_id
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", klienti_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            firma = rec.get("fields", {}).get("Firma", "").strip()
            if firma:
                klienti_by_firma[normalize_company(firma)] = rec["id"]
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   {len(klienti_by_firma)} klientů")
    
    # 2. Načti Deals - doplněk a propoj
    print("\n🔎 Načítám Deals - doplněk...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals - doplněk', safe='')}"
    
    deals_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            firma = fields.get("Firma", "").strip()
            firma_norm = normalize_company(firma)
            current_klienti = fields.get("Klienti", [])
            
            # Pokud má firmu a najdeme klienta, propojíme
            if firma_norm and firma_norm in klienti_by_firma and not current_klienti:
                klient_id = klienti_by_firma[firma_norm]
                deals_to_update.append({
                    "id": rec["id"],
                    "fields": {"Klienti": [klient_id]}
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   K propojení: {len(deals_to_update)} deals")
    
    if not deals_to_update:
        print("\n✅ Všechny deals jsou už propojené s klienty!")
        return
    
    # 3. Aktualizuj
    print(f"\n🔗 Propojuji...")
    
    updated = 0
    for batch in chunked(deals_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        if updated % 50 == 0:
            print(f"   ... {updated}/{len(deals_to_update)}")
        time.sleep(0.2)
    
    print(f"\n✅ Propojeno {updated} deals s klienty!")


if __name__ == "__main__":
    main()
