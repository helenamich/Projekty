#!/usr/bin/env python3
"""
Najde potenciální duplicity v Deals tabulce.
"""

import json
import time
import re
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote
from collections import defaultdict

import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"


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


def normalize_company(s):
    """Normalizuje název firmy pro porovnání."""
    s = (s or "").strip().lower()
    # Odstraň právní formy a zkratky
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',',
                   ' czech republic', ' česká republika', ' cz', ' sk',
                   ' pharma', ' group', ' holding', '(eng)', '(sk)',
                   ' konference', ' firemní', ' workshop', ' přednáška']:
        s = s.replace(suffix, '')
    # Odstraň závorky a jejich obsah
    s = re.sub(r'\([^)]*\)', '', s)
    # Odstraň speciální znaky
    s = re.sub(r'[^a-záčďéěíňóřšťúůýž0-9\s]', '', s)
    return s.strip()


def extract_date(text):
    """Extrahuje datum z textu."""
    if not text:
        return None
    # Hledej formát DD.MM.YYYY nebo DD.MM.
    match = re.search(r'(\d{1,2}\.\d{1,2}\.(?:\d{4})?)', text)
    if match:
        return match.group(1)
    return None


def main():
    token = get_token()
    hdrs = headers(token)
    
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    # Načti všechny deals
    print("🔎 Načítám všechny deals...")
    all_deals = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        all_deals.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Celkem {len(all_deals)} deals")
    
    # Seskup podle normalizované firmy
    by_company = defaultdict(list)
    for rec in all_deals:
        fields = rec.get("fields", {})
        firma = fields.get("Firma", "")
        firma_norm = normalize_company(firma)
        if firma_norm:
            by_company[firma_norm].append({
                "id": rec["id"],
                "firma": firma,
                "jmeno": fields.get("Jméno a příjmení", ""),
                "email": fields.get("Email", ""),
                "co_poptavali": fields.get("Co poptávali", ""),
                "reakce": fields.get("Reakce/výsledek", ""),
                "poznamka": fields.get("Poznámka / Detaily", "")[:200] if fields.get("Poznámka / Detaily") else ""
            })
    
    # Najdi duplicity
    print("\n🔍 Hledám duplicity...\n")
    duplicates = []
    
    for firma_norm, deals in by_company.items():
        if len(deals) > 1:
            # Ověř, že to jsou skutečné duplicity (podobné datum/akce)
            # Porovnej datumy v poznámkách
            dates = []
            for d in deals:
                date = extract_date(d["poznamka"])
                dates.append(date)
            
            # Pokud mají stejné nebo žádné datum, jsou to pravděpodobně duplicity
            duplicates.append({
                "firma_norm": firma_norm,
                "deals": deals,
                "dates": dates
            })
    
    if not duplicates:
        print("✅ Žádné duplicity nenalezeny!")
        return
    
    print(f"📋 Nalezeno {len(duplicates)} skupin potenciálních duplicit:\n")
    print("=" * 80)
    
    real_duplicates = []
    
    for dup in duplicates:
        deals = dup["deals"]
        dates = dup["dates"]
        
        # Ověř, zda jsou to skutečné duplicity
        # Stejný email = určitě duplicita
        emails = [d["email"] for d in deals if d["email"]]
        unique_emails = set(emails)
        
        # Stejné nebo podobné datum = pravděpodobně duplicita
        non_null_dates = [d for d in dates if d]
        
        is_duplicate = False
        reason = ""
        
        if len(unique_emails) == 1 and len(emails) > 1:
            is_duplicate = True
            reason = f"Stejný email: {emails[0]}"
        elif len(non_null_dates) > 1:
            # Porovnej datumy
            date_set = set(non_null_dates)
            if len(date_set) == 1:
                is_duplicate = True
                reason = f"Stejné datum: {non_null_dates[0]}"
            else:
                # Různá data = různé akce, ne duplicita
                continue
        elif len(deals) == 2:
            # Pokud jeden má detaily a druhý ne, pravděpodobně duplicita
            details = [len(d["poznamka"]) for d in deals]
            if max(details) > 50 and min(details) < 30:
                is_duplicate = True
                reason = "Jeden záznam má více detailů"
        
        if is_duplicate:
            real_duplicates.append(dup)
            print(f"\n🔴 DUPLICITA: {dup['firma_norm'].upper()}")
            print(f"   Důvod: {reason}")
            for i, d in enumerate(deals):
                print(f"\n   [{i+1}] {d['firma']}")
                print(f"       ID: {d['id']}")
                if d['jmeno']:
                    print(f"       Kontakt: {d['jmeno']} ({d['email']})")
                if d['co_poptavali']:
                    print(f"       Typ: {d['co_poptavali']}")
                if d['reakce']:
                    print(f"       Výsledek: {d['reakce']}")
                if d['poznamka']:
                    print(f"       Poznámka: {d['poznamka'][:100]}...")
            print("-" * 80)
    
    print(f"\n\n📊 Celkem {len(real_duplicates)} skutečných duplicit k řešení.")


if __name__ == "__main__":
    main()
