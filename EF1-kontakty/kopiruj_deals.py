#!/usr/bin/env python3
"""
Zkopíruje záznamy z Deals do Deals - doplněk.
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


def normalize(s):
    return (s or "").strip().lower()


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti existující záznamy z Deals - doplněk (abychom neduplikovali)
    print("🔎 Načítám existující záznamy z Deals - doplněk...")
    doplnek_url = f"{API_BASE}/{BASE_ID}/{quote('Deals - doplněk', safe='')}"
    
    existing_keys = set()  # (email, firma) -> pro detekci duplicit
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", doplnek_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            email = normalize(fields.get("Email", ""))
            firma = normalize(fields.get("Firma", ""))
            jmeno = normalize(fields.get("Jméno a příjmení", ""))
            existing_keys.add((email, firma, jmeno))
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   {len(existing_keys)} existujících záznamů")
    
    # 2. Načti záznamy z Deals
    print("\n🔎 Načítám záznamy z Deals...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    records_to_copy = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            email = normalize(fields.get("Email", ""))
            firma = normalize(fields.get("Firma", ""))
            jmeno = normalize(fields.get("Jméno a příjmení", ""))
            
            # Přeskoč duplicity
            if (email, firma, jmeno) in existing_keys:
                continue
            
            # Připrav nový záznam (bez link polí - ta se musí vytvořit znovu)
            new_fields = {}
            for key in ["Jméno a příjmení", "Email", "Firma", "Co poptávali", 
                       "Komu určeno / Nabídnut pro realizaci", "Reakce/výsledek", "Poznámka"]:
                if key in fields and fields[key]:
                    new_fields[key] = fields[key]
            
            if new_fields:
                records_to_copy.append({"fields": new_fields})
                existing_keys.add((email, firma, jmeno))
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   {len(records_to_copy)} záznamů ke zkopírování")
    
    if not records_to_copy:
        print("\n✅ Vše už je zkopírované!")
        return
    
    # Ukázka
    print("\n📋 Ukázka (prvních 5):")
    for rec in records_to_copy[:5]:
        f = rec["fields"]
        print(f"   {f.get('Jméno a příjmení', '')} - {f.get('Firma', '')}")
    
    # 3. Vytvoř v Deals - doplněk
    print(f"\n➕ Kopíruji {len(records_to_copy)} záznamů...")
    
    created = 0
    for batch in chunked(records_to_copy, BATCH_SIZE):
        request_with_backoff("POST", doplnek_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        created += len(batch)
        if created % 50 == 0:
            print(f"   ... {created}/{len(records_to_copy)}")
        time.sleep(0.2)
    
    print(f"\n✅ Zkopírováno {created} záznamů do Deals - doplněk!")
    print("\n📌 Teď můžeš:")
    print("   1. Smazat tabulku 'Deals'")
    print("   2. Přejmenovat 'Deals - doplněk' na 'Deals'")


if __name__ == "__main__":
    main()
