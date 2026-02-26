#!/usr/bin/env python3
"""
Vytvoří smysluplné názvy dealů z linkovaných klientů a dalších informací.
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


def extract_date(text: str) -> str:
    """Extrahuje datum z poznámky."""
    if not text:
        return ""
    match = re.search(r'(\d{1,2}\.\d{1,2}\.(?:\d{4})?)', text)
    if match:
        date = match.group(1)
        parts = date.split('.')
        if len(parts) >= 3 and parts[2]:
            return f"{parts[0]}.{parts[1]}."  # DD.MM.
    return ""


def shorten_type(co_poptavali: str) -> str:
    """Zkrátí typ poptávky."""
    mapping = {
        "Přednáška / keynote": "Přednáška",
        "Workshop": "Workshop",
        "Školení": "Školení",
        "Konzultace": "Konzultace",
        "Jiné (interní program apod.)": "Program",
    }
    return mapping.get(co_poptavali, "")


def extract_company_from_note(note: str) -> str:
    """Zkusí extrahovat název firmy z poznámky."""
    if not note:
        return ""
    # Vezmi první část před " - " nebo před datem
    parts = note.split(" - ")
    if parts:
        first = parts[0].strip()
        # Odstraň datum pokud tam je
        first = re.sub(r'\d{1,2}\.\d{1,2}\.\d{4}', '', first).strip()
        if first and len(first) > 2 and len(first) < 50:
            return first
    return ""


def create_deal_name(firma: str, co_poptavali: str, poznamka: str) -> str:
    """Vytvoří název dealu."""
    if not firma:
        # Zkus extrahovat z poznámky
        firma = extract_company_from_note(poznamka)
    
    if not firma:
        return ""
    
    # Zkrať název firmy
    firma_short = firma[:35] if len(firma) > 35 else firma
    
    # Přidej typ
    typ = shorten_type(co_poptavali)
    
    # Přidej datum
    date = extract_date(poznamka)
    
    # Sestav název
    parts = [firma_short]
    if typ:
        parts.append(typ)
    if date:
        parts.append(date)
    
    return " | ".join(parts)


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti všechny Klienty (pro mapování ID -> název firmy)
    print("🔎 Načítám Klienty...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    klienti_map = {}  # ID -> název firmy
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", klienti_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            firma = rec.get("fields", {}).get("Firma", "")
            if firma:
                klienti_map[rec["id"]] = firma
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Načteno {len(klienti_map)} klientů")
    
    # 2. Načti všechny Deals
    print("\n🔎 Načítám Deals...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    records_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            
            # Získej název firmy z linkovaných Klientů
            klienti_ids = fields.get("Klienti", [])
            firma = ""
            for kid in klienti_ids:
                if kid in klienti_map:
                    firma = klienti_map[kid]
                    break  # Vezmi prvního
            
            co_poptavali = fields.get("Co poptávali", "")
            poznamka = fields.get("Poznámka / Detaily", "")
            
            # Vytvoř název dealu
            deal_name = create_deal_name(firma, co_poptavali, poznamka)
            
            if deal_name:
                records_to_update.append({
                    "id": rec["id"],
                    "fields": {
                        "Název dealu": deal_name
                    },
                    "_preview": deal_name
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Nalezeno {len(records_to_update)} deals k aktualizaci")
    
    # Ukázka
    print("\n📋 Ukázka nových názvů:")
    for rec in records_to_update[:20]:
        print(f"   {rec['_preview']}")
    if len(records_to_update) > 20:
        print(f"   ... a dalších {len(records_to_update) - 20}")
    
    # Odstraň pomocná pole
    for rec in records_to_update:
        del rec["_preview"]
    
    # 3. Aktualizuj
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
