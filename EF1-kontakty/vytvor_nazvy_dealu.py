#!/usr/bin/env python3
"""
Vytvoří smysluplné názvy dealů z dostupných informací.
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


def extract_date(text: str) -> str:
    """Extrahuje datum z poznámky."""
    if not text:
        return ""
    # Hledej formát DD.MM.YYYY nebo DD.MM.
    match = re.search(r'(\d{1,2}\.\d{1,2}\.(?:\d{4})?)', text)
    if match:
        date = match.group(1)
        # Zkrať na měsíc/rok
        parts = date.split('.')
        if len(parts) >= 2:
            month = parts[1]
            year = parts[2] if len(parts) > 2 and parts[2] else ""
            if year and len(year) == 4:
                return f"{month}/{year[2:]}"  # např. "10/25"
            return ""
    return ""


def shorten_type(co_poptavali: str) -> str:
    """Zkrátí typ poptávky."""
    if not co_poptavali:
        return ""
    
    mapping = {
        "Přednáška / keynote": "Přednáška",
        "Workshop": "Workshop",
        "Školení": "Školení",
        "Konzultace": "Konzultace",
        "Jiné (interní program apod.)": "Program",
    }
    return mapping.get(co_poptavali, co_poptavali)


def create_deal_name(firma: str, co_poptavali: str, poznamka: str) -> str:
    """Vytvoří název dealu."""
    if not firma:
        return "Neznámý deal"
    
    # Zkrať název firmy pokud je moc dlouhý
    firma_short = firma
    if len(firma) > 30:
        # Zkrať a přidej ...
        firma_short = firma[:27] + "..."
    
    # Přidej typ
    typ = shorten_type(co_poptavali)
    
    # Přidej datum pokud je
    date = extract_date(poznamka)
    
    # Sestav název
    parts = [firma_short]
    if typ:
        parts.append(typ)
    if date:
        parts.append(date)
    
    return " - ".join(parts)


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti všechny deals
    print("🔎 Načítám deals...")
    deals_url = f"{API_BASE}/{BASE_ID}/{TABLE_ID}"
    
    records_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            
            # Název dealu je teď primary field s názvem firmy
            firma = fields.get("Název dealu", "")
            co_poptavali = fields.get("Co poptávali", "")
            poznamka = fields.get("Poznámka / Detaily", "")
            
            if firma:  # Máme firmu, vytvoříme název
                deal_name = create_deal_name(firma, co_poptavali, poznamka)
                records_to_update.append({
                    "id": rec["id"],
                    "fields": {
                        "Název dealu": deal_name  # Aktualizujeme primary field
                    },
                    "_old": firma,
                    "_new": deal_name
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Nalezeno {len(records_to_update)} deals k aktualizaci")
    
    # Ukázka
    print("\n📋 Ukázka nových názvů:")
    for rec in records_to_update[:15]:
        print(f"   {rec['_old'][:25]:<25} → {rec['_new']}")
    if len(records_to_update) > 15:
        print(f"   ... a dalších {len(records_to_update) - 15}")
    
    # Odstraň pomocná pole
    for rec in records_to_update:
        del rec["_old"]
        del rec["_new"]
    
    # 2. Aktualizuj
    print(f"\n⬆️ Aktualizuji názvy...")
    
    updated = 0
    for batch in chunked(records_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        print(f"   Aktualizováno: {updated}/{len(records_to_update)}", end="\r")
        time.sleep(0.2)
    
    print(f"\n\n✅ Aktualizováno {updated} názvů dealů!")


if __name__ == "__main__":
    main()
