#!/usr/bin/env python3
"""
Aktualizuje názvy dealů s datem/rokem/obdobím.
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


def extract_date_info(text: str) -> str:
    """Extrahuje datum z poznámky - vrací formát jako '3/2025' nebo 'jaro 2025'."""
    if not text:
        return ""
    
    # Hledej plné datum DD.MM.YYYY
    match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
    if match:
        day, month, year = match.groups()
        return f"{month}/{year}"  # např. "3/2025"
    
    # Hledej rok samostatně
    match = re.search(r'\b(202[3-9])\b', text)
    if match:
        year = match.group(1)
        
        # Zkus najít období
        text_lower = text.lower()
        if 'jaro' in text_lower or 'spring' in text_lower:
            return f"jaro {year}"
        elif 'léto' in text_lower or 'summer' in text_lower:
            return f"léto {year}"
        elif 'podzim' in text_lower or 'fall' in text_lower or 'autumn' in text_lower:
            return f"podzim {year}"
        elif 'zima' in text_lower or 'winter' in text_lower:
            return f"zima {year}"
        elif 'leden' in text_lower or 'january' in text_lower or 'únor' in text_lower or 'february' in text_lower or 'březen' in text_lower or 'march' in text_lower:
            return f"Q1 {year}"
        elif 'duben' in text_lower or 'april' in text_lower or 'květen' in text_lower or 'may' in text_lower or 'červen' in text_lower or 'june' in text_lower:
            return f"Q2 {year}"
        elif 'červenec' in text_lower or 'july' in text_lower or 'srpen' in text_lower or 'august' in text_lower or 'září' in text_lower or 'september' in text_lower:
            return f"Q3 {year}"
        elif 'říjen' in text_lower or 'october' in text_lower or 'listopad' in text_lower or 'november' in text_lower or 'prosinec' in text_lower or 'december' in text_lower:
            return f"Q4 {year}"
        
        return year
    
    # Hledej měsíc v textu (např. "říjen", "10.")
    months = {
        'leden': 1, 'únor': 2, 'březen': 3, 'duben': 4, 'květen': 5, 'červen': 6,
        'červenec': 7, 'srpen': 8, 'září': 9, 'říjen': 10, 'listopad': 11, 'prosinec': 12
    }
    text_lower = text.lower()
    for month_name, month_num in months.items():
        if month_name in text_lower:
            return f"{month_num}/2025"  # Předpokládej 2025
    
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


def create_deal_name(firma: str, co_poptavali: str, poznamka: str) -> str:
    """Vytvoří název dealu s datem."""
    if not firma:
        return ""
    
    # Zkrať název firmy
    firma_short = firma[:35] if len(firma) > 35 else firma
    
    # Typ
    typ = shorten_type(co_poptavali)
    
    # Datum/období
    date_info = extract_date_info(poznamka)
    
    # Sestav název
    parts = [firma_short]
    if typ:
        parts.append(typ)
    if date_info:
        parts.append(date_info)
    
    return " | ".join(parts)


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti Klienty pro mapování
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
    
    print(f"   Načteno {len(klienti_map)} klientů")
    
    # 2. Načti Deals
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
            
            # Získej firmu z linkovaných Klientů
            klienti_ids = fields.get("Klienti", [])
            firma = ""
            for kid in klienti_ids:
                if kid in klienti_map:
                    firma = klienti_map[kid]
                    break
            
            co_poptavali = fields.get("Co poptávali", "")
            poznamka = fields.get("Poznámka / Detaily", "")
            
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
    
    print(f"   Nalezeno {len(records_to_update)} deals")
    
    # Ukázka
    print("\n📋 Ukázka názvů s datem:")
    for rec in records_to_update[:25]:
        print(f"   {rec['_preview']}")
    
    # Odstraň pomocná pole
    for rec in records_to_update:
        del rec["_preview"]
    
    # 3. Aktualizuj
    print(f"\n⬆️ Aktualizuji...")
    
    updated = 0
    for batch in chunked(records_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        time.sleep(0.2)
    
    print(f"\n✅ Aktualizováno {updated} dealů s datem/obdobím!")


if __name__ == "__main__":
    main()
