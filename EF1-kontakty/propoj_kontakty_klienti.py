#!/usr/bin/env python3
"""
Propojí kontakty s klienty podle názvu firmy.
"""

import json
import time
import re
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

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


def normalize_company(s):
    """Normalizuje název firmy pro porovnání."""
    s = (s or "").strip().lower()
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' ltd.', ' inc', ' n.v.', ' ag', ' se',
                   ' czech republic', ' česká republika', ' cz', ' sk',
                   ' czech', ' slovakia', ' group', ' holding']:
        s = s.replace(suffix, '')
    s = re.sub(r'[,\.\-\(\)]', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti Klienty a vytvoř mapu
    print("🔎 Načítám Klienty...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    klienti_by_name = {}  # normalized name -> record ID
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", klienti_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            firma = rec.get("fields", {}).get("Firma", "")
            if firma:
                firma_norm = normalize_company(firma)
                if firma_norm:
                    klienti_by_name[firma_norm] = rec["id"]
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   {len(klienti_by_name)} klientů")
    
    # 2. Načti Kontakty bez linku na Klienta
    print("\n🔎 Hledám kontakty bez linku na Klienta...")
    kontakty_url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    
    to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            
            # Má už link na Klienta?
            if fields.get("Klienti"):
                continue
            
            # Má firmu?
            firma = fields.get("Společnost / Firma", "")
            if not firma:
                continue
            
            # Najdi klienta
            firma_norm = normalize_company(firma)
            klient_id = klienti_by_name.get(firma_norm)
            
            if not klient_id:
                # Zkus částečnou shodu
                for k_name, k_id in klienti_by_name.items():
                    if firma_norm in k_name or k_name in firma_norm:
                        klient_id = k_id
                        break
            
            if klient_id:
                to_update.append({
                    "id": rec["id"],
                    "fields": {
                        "Klienti": [klient_id]
                    }
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   K propojení: {len(to_update)} kontaktů")
    
    if not to_update:
        print("\n✅ Všechny kontakty jsou propojené!")
        return
    
    # 3. Aktualizuj
    print(f"\n⬆️ Propojuji...")
    
    updated = 0
    for batch in chunked(to_update, BATCH_SIZE):
        request_with_backoff("PATCH", kontakty_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        time.sleep(0.2)
    
    print(f"\n✅ Propojeno {updated} kontaktů s Klienty!")


if __name__ == "__main__":
    main()
