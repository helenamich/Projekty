#!/usr/bin/env python3
"""
Doplní rok 2025 k datům v Deals - doplněk.
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


def add_year_to_date(text: str) -> str:
    """Přidá rok 2025 k datům bez roku."""
    if not text:
        return text
    
    # Vzory pro datum bez roku: "Datum: 26.8." nebo "9.7." nebo "14.10. - 16.12."
    # Přidáme 2025 za datum ve formátu D.M. nebo DD.MM.
    
    # Pattern pro datum bez roku (den.měsíc. bez roku)
    # Např: "26.8." -> "26.8.2025"
    # Ale ne pokud už má rok: "26.8.2025" zůstane
    
    def replace_date(match):
        date = match.group(0)
        # Zkontroluj jestli za tím není rok
        return date + "2025"
    
    # Najdi datumy ve formátu D.M. nebo DD.MM. (končící tečkou, bez roku za ní)
    # Negativní lookahead pro číslo (rok) za tečkou
    result = re.sub(r'(\d{1,2}\.\d{1,2}\.)(?!\d)', r'\g<1>2025', text)
    
    return result


def main():
    token = get_token()
    hdrs = headers(token)
    
    print("🔎 Načítám Deals - doplněk...")
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
            poznamka = fields.get("Poznámka", "")
            
            if not poznamka:
                continue
            
            # Zkontroluj jestli obsahuje datum bez roku
            if re.search(r'\d{1,2}\.\d{1,2}\.(?!\d)', poznamka):
                new_poznamka = add_year_to_date(poznamka)
                if new_poznamka != poznamka:
                    deals_to_update.append({
                        "id": rec["id"],
                        "fields": {"Poznámka": new_poznamka},
                        "_old": poznamka[:50]
                    })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   K aktualizaci: {len(deals_to_update)} deals")
    
    if not deals_to_update:
        print("\n✅ Všechna data už mají rok!")
        return
    
    # Ukázka
    print("\n📋 Ukázka změn:")
    for rec in deals_to_update[:5]:
        old = rec["_old"]
        new = rec["fields"]["Poznámka"][:50]
        print(f"   {old}...")
        print(f"   → {new}...")
        print()
    
    # Odstraň pomocné pole
    for rec in deals_to_update:
        del rec["_old"]
    
    # Aktualizuj
    print(f"⬆️ Aktualizuji...")
    
    updated = 0
    for batch in chunked(deals_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        time.sleep(0.2)
    
    print(f"\n✅ Doplněn rok 2025 u {updated} deals!")


if __name__ == "__main__":
    main()
