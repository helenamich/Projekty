#!/usr/bin/env python3
"""
Propojí Deals s Kontakty podle emailu.
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


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti všechny Kontakty
    print("🔎 Načítám Kontakty...")
    kontakty_url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    
    kontakty_by_email = {}  # email -> record_id
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            email = (rec.get("fields", {}).get("E-mail") or "").strip().lower()
            if email:
                kontakty_by_email[email] = rec["id"]
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   {len(kontakty_by_email)} kontaktů s emailem")
    
    # 2. Načti Deals
    print("\n🔎 Načítám Deals...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    deals_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            email = (fields.get("Email") or "").strip().lower()
            current_kontakt = fields.get("Kontakt", [])
            
            # Pokud má email a najdeme kontakt, propojíme
            if email and email in kontakty_by_email and not current_kontakt:
                kontakt_id = kontakty_by_email[email]
                deals_to_update.append({
                    "id": rec["id"],
                    "fields": {"Kontakt": [kontakt_id]}
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   K propojení: {len(deals_to_update)} deals")
    
    if not deals_to_update:
        print("\n✅ Všechny deals jsou už propojené nebo nemají odpovídající kontakt!")
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
    
    print(f"\n✅ Propojeno {updated} deals s kontakty!")


if __name__ == "__main__":
    main()
