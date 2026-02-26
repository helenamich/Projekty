#!/usr/bin/env python3
"""
Opraví názvy dealů - zajistí že datum obsahuje rok.
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


def extract_full_date(text: str) -> str:
    """Extrahuje datum s rokem z poznámky."""
    if not text:
        return ""
    
    # Hledej plné datum DD.MM.YYYY
    match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
    if match:
        day, month, year = match.groups()
        return f"{int(month)}/{year}"  # např. "3/2025"
    
    # Hledej datum DD.MM. + rok někde v textu
    match_date = re.search(r'(\d{1,2})\.(\d{1,2})\.', text)
    match_year = re.search(r'\b(202[3-9])\b', text)
    
    if match_date and match_year:
        month = match_date.group(2)
        year = match_year.group(1)
        return f"{int(month)}/{year}"
    
    # Jen rok
    if match_year:
        year = match_year.group(1)
        
        # Zkus najít období
        text_lower = text.lower()
        if 'jaro' in text_lower:
            return f"jaro {year}"
        elif 'léto' in text_lower:
            return f"léto {year}"
        elif 'podzim' in text_lower:
            return f"podzim {year}"
        elif 'zima' in text_lower:
            return f"zima {year}"
        
        return year  # Jen rok
    
    return ""


def shorten_type(co_poptavali: str) -> str:
    mapping = {
        "Přednáška / keynote": "Přednáška",
        "Workshop": "Workshop",
        "Školení": "Školení",
        "Konzultace": "Konzultace",
        "Jiné (interní program apod.)": "Program",
    }
    return mapping.get(co_poptavali, "")


def create_deal_name(firma: str, co_poptavali: str, poznamka: str) -> str:
    """Vytvoří název dealu s kompletním datem."""
    if not firma:
        return ""
    
    firma_short = firma[:35] if len(firma) > 35 else firma
    typ = shorten_type(co_poptavali)
    date_info = extract_full_date(poznamka)
    
    parts = [firma_short]
    if typ:
        parts.append(typ)
    if date_info:
        parts.append(date_info)
    
    return " | ".join(parts)


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti Klienty
    print("🔎 Načítám Klienty...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    klienti_map = {}
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
    
    print(f"   {len(klienti_map)} klientů")
    
    # 2. Načti a aktualizuj Deals
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
            
            klienti_ids = fields.get("Klienti", [])
            firma = ""
            for kid in klienti_ids:
                if kid in klienti_map:
                    firma = klienti_map[kid]
                    break
            
            co_poptavali = fields.get("Co poptávali", "")
            poznamka = fields.get("Poznámka / Detaily", "")
            old_name = fields.get("Název dealu", "")
            
            new_name = create_deal_name(firma, co_poptavali, poznamka)
            
            if new_name and new_name != old_name:
                records_to_update.append({
                    "id": rec["id"],
                    "fields": {"Název dealu": new_name},
                    "_old": old_name,
                    "_new": new_name
                })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   {len(records_to_update)} dealů ke změně")
    
    # Ukázka změn
    print("\n📋 Změny:")
    for rec in records_to_update[:20]:
        if rec["_old"] != rec["_new"]:
            print(f"   {rec['_old'][:40]:<40} → {rec['_new']}")
    
    for rec in records_to_update:
        del rec["_old"]
        del rec["_new"]
    
    # 3. Aktualizuj
    if records_to_update:
        print(f"\n⬆️ Aktualizuji {len(records_to_update)} dealů...")
        
        updated = 0
        for batch in chunked(records_to_update, BATCH_SIZE):
            request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                                json_data={"records": batch, "typecast": True})
            updated += len(batch)
            time.sleep(0.2)
        
        print(f"\n✅ Aktualizováno {updated} dealů!")
    else:
        print("\n✅ Všechny názvy jsou v pořádku.")


if __name__ == "__main__":
    main()
