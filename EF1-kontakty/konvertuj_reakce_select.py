#!/usr/bin/env python3
"""
Konvertuje pole Reakce/výsledek na single select s barvami.
"""

import json
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"
TABLE_ID = "tblOOAzDQbnOg1KRd"
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


# Mapování hodnot na normalizované + barvy
VALUE_MAP = {
    "Deal": "Deal",
    "Bez reakce": "Bez reakce",
    "Odmítnuto": "Odmítnuto",
    "Filip nedostupný": "Filip nedostupný",
    "Předáno dál": "Předáno dál",
    "Jiné": "Jiné",
    "Vycouvali": "Vycouvali",
    "Malý budget": "Malý budget",
    "Nevyšlo (tendr/konkurence)": "Nevyšlo (tendr/konkurence)",
    "Nedohodli se": "Nedohodli se",
    "V řešení": "V řešení",
    "v řešení": "V řešení",  # normalizace
    "FAIL": "Nevyšlo (tendr/konkurence)",  # sloučení
}

# Barvy pro jednotlivé možnosti
CHOICES = [
    {"name": "Deal", "color": "greenLight2"},
    {"name": "V řešení", "color": "cyanLight2"},
    {"name": "Bez reakce", "color": "grayLight2"},
    {"name": "Filip nedostupný", "color": "yellowLight2"},
    {"name": "Předáno dál", "color": "blueLight2"},
    {"name": "Odmítnuto", "color": "redLight2"},
    {"name": "Vycouvali", "color": "orangeLight2"},
    {"name": "Malý budget", "color": "orangeLight1"},
    {"name": "Nevyšlo (tendr/konkurence)", "color": "redLight1"},
    {"name": "Nedohodli se", "color": "pinkLight2"},
    {"name": "Jiné", "color": "purpleLight2"},
]


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Vytvoř nové single select pole
    print("📋 Vytvářím nové pole 'Výsledek' (single select)...")
    
    create_field_url = f"{API_BASE}/meta/bases/{BASE_ID}/tables/{TABLE_ID}/fields"
    field_data = {
        "name": "Výsledek",
        "type": "singleSelect",
        "options": {
            "choices": CHOICES
        }
    }
    
    try:
        result = request_with_backoff("POST", create_field_url, hdrs=hdrs, json_data=field_data)
        new_field_id = result.get("id")
        print(f"   ✅ Vytvořeno pole s ID: {new_field_id}")
    except Exception as e:
        if "DUPLICATE" in str(e):
            print("   ⚠️ Pole 'Výsledek' už existuje, pokračuji...")
        else:
            raise
    
    # 2. Načti všechny záznamy a přepiš hodnoty
    print("\n🔄 Přenáším data...")
    
    deals_url = f"{API_BASE}/{BASE_ID}/{TABLE_ID}"
    records_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            old_value = rec.get("fields", {}).get("Reakce/výsledek", "")
            if old_value:
                new_value = VALUE_MAP.get(old_value.strip(), old_value.strip())
                records_to_update.append({
                    "id": rec["id"],
                    "fields": {
                        "Výsledek": new_value
                    }
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Nalezeno {len(records_to_update)} záznamů k aktualizaci")
    
    # Aktualizuj po dávkách
    updated = 0
    for batch in chunked(records_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        print(f"   Aktualizováno: {updated}/{len(records_to_update)}", end="\r")
        time.sleep(0.2)
    
    print(f"\n   ✅ Aktualizováno {updated} záznamů")
    
    # 3. Smaž staré pole
    print("\n🗑️ Mažu staré pole 'Reakce/výsledek'...")
    
    # Nejdřív najdi ID starého pole
    tables_url = f"{API_BASE}/meta/bases/{BASE_ID}/tables"
    tables_data = request_with_backoff("GET", tables_url, hdrs=hdrs)
    
    old_field_id = None
    for table in tables_data.get("tables", []):
        if table["id"] == TABLE_ID:
            for field in table.get("fields", []):
                if field["name"] == "Reakce/výsledek":
                    old_field_id = field["id"]
                    break
    
    if old_field_id:
        delete_url = f"{API_BASE}/meta/bases/{BASE_ID}/tables/{TABLE_ID}/fields/{old_field_id}"
        request_with_backoff("DELETE", delete_url, hdrs=hdrs)
        print(f"   ✅ Smazáno staré pole")
    
    # 4. Přejmenuj nové pole
    print("\n✏️ Přejmenovávám pole na 'Reakce/výsledek'...")
    
    # Najdi ID nového pole
    tables_data = request_with_backoff("GET", tables_url, hdrs=hdrs)
    new_field_id = None
    for table in tables_data.get("tables", []):
        if table["id"] == TABLE_ID:
            for field in table.get("fields", []):
                if field["name"] == "Výsledek":
                    new_field_id = field["id"]
                    break
    
    if new_field_id:
        rename_url = f"{API_BASE}/meta/bases/{BASE_ID}/tables/{TABLE_ID}/fields/{new_field_id}"
        request_with_backoff("PATCH", rename_url, hdrs=hdrs, 
                            json_data={"name": "Reakce/výsledek"})
        print(f"   ✅ Přejmenováno")
    
    print("\n✅ Hotovo! Pole 'Reakce/výsledek' je nyní single select s barvami.")


if __name__ == "__main__":
    main()
