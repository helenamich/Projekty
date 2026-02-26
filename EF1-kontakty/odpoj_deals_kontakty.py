#!/usr/bin/env python3
"""
Odstraní propojení deals s kontakty.
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
    
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    # Najdi deals s propojením
    print("🔎 Hledám deals s propojením...")
    deals_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            kontakt = fields.get("Kontakt", [])
            
            if kontakt:  # Má propojení
                deals_to_update.append({
                    "id": rec["id"],
                    "fields": {
                        "Kontakt": []  # Odstraň propojení
                    }
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Nalezeno {len(deals_to_update)} deals s propojením")
    
    if not deals_to_update:
        print("\n✅ Žádné propojení k odstranění!")
        return
    
    # Odstraň propojení
    print(f"\n🔄 Odstraňuji propojení...")
    
    updated = 0
    for batch in chunked(deals_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        time.sleep(0.2)
    
    print(f"\n✅ Odstraněno propojení u {updated} deals!")


if __name__ == "__main__":
    main()
