#!/usr/bin/env python3
"""
Přeuspořádá Deals - doplněk: nejdřív původní Deals, pak ostatní.
"""

import csv
import json
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import requests

BASE_DIR = Path(__file__).parent
PIPEDRIVE = BASE_DIR / "deals-16044442-64.csv"
FILIP_AKCE = BASE_DIR / "Filip akce - poptávky - List 1.csv"

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


def normalize_company(s):
    s = normalize(s)
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',']:
        s = s.replace(suffix, '')
    return s.strip()


def main():
    token = get_token()
    hdrs = headers(token)
    doplnek_url = f"{API_BASE}/{BASE_ID}/{quote('Deals - doplněk', safe='')}"
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    # 1. Smaž všechny záznamy z Deals - doplněk
    print("🗑️ Mažu všechny záznamy z Deals - doplněk...")
    
    all_ids = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", doplnek_url, hdrs=hdrs, params=params)
        all_ids.extend([rec["id"] for rec in data.get("records", [])])
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Mažu {len(all_ids)} záznamů...")
    for batch in chunked(all_ids, BATCH_SIZE):
        params = {"records[]": batch}
        request_with_backoff("DELETE", doplnek_url, hdrs=hdrs, params=params)
        time.sleep(0.2)
    print("   ✅ Smazáno")
    
    # 2. Načti a vlož původní Deals (první)
    print("\n📋 Načítám původní Deals...")
    
    original_deals = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            new_fields = {}
            for key in ["Jméno a příjmení", "Email", "Firma", "Co poptávali", 
                       "Komu určeno / Nabídnut pro realizaci", "Reakce/výsledek", "Poznámka"]:
                if key in fields and fields[key]:
                    new_fields[key] = fields[key]
            if new_fields:
                original_deals.append({"fields": new_fields})
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   {len(original_deals)} původních deals")
    
    print("\n➕ Vkládám původní Deals (budou první)...")
    created = 0
    for batch in chunked(original_deals, BATCH_SIZE):
        request_with_backoff("POST", doplnek_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        created += len(batch)
        time.sleep(0.2)
    print(f"   ✅ Vloženo {created}")
    
    # 3. Načti a vlož Pipedrive a Filip akce (pak)
    print("\n📋 Načítám Pipedrive a Filip akce...")
    
    seen_keys = set()
    for rec in original_deals:
        f = rec["fields"]
        key = (normalize(f.get("Email", "")), normalize_company(f.get("Firma", "")))
        seen_keys.add(key)
    
    additional_deals = []
    
    # Pipedrive
    with open(PIPEDRIVE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row.get("Deal - Organizace", "").strip()
            contact = row.get("Deal - Kontaktní osoba", "").strip()
            deal_name = row.get("Deal - Název", "").strip()
            
            emails = [row.get("Osoba - E-mail - Práce", ""), 
                     row.get("Osoba - E-mail - Domov", ""),
                     row.get("Osoba - E-mail - Ostatní", "")]
            email = next((e.strip() for e in emails if e and '@' in e), "")
            
            key = (normalize(email), normalize_company(company))
            if key in seen_keys:
                continue
            
            # Urči typ
            typ = ""
            dl = deal_name.lower()
            if "workshop" in dl:
                typ = "Workshop"
            elif "přednáška" in dl or "keynote" in dl:
                typ = "Přednáška / keynote"
            elif "školení" in dl or "kurz" in dl:
                typ = "Školení"
            elif "webinář" in dl:
                typ = "Přednáška / keynote"
            elif "program" in dl or "masterclass" in dl:
                typ = "Jiné (interní program apod.)"
            
            fields = {"Firma": company, "Poznámka": deal_name}
            if contact:
                fields["Jméno a příjmení"] = contact
            if email:
                fields["Email"] = email
            if typ:
                fields["Co poptávali"] = typ
            
            if company or contact:
                additional_deals.append({"fields": fields})
                seen_keys.add(key)
    
    # Filip akce
    with open(FILIP_AKCE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            firma = row[0].strip()
            if not firma:
                continue
            
            key = ("", normalize_company(firma))
            if key in seen_keys:
                continue
            
            datum = row[1].strip() if len(row) > 1 else ""
            misto = row[2].strip() if len(row) > 2 else ""
            typ = row[3].strip() if len(row) > 3 else ""
            kategorie = row[5].strip() if len(row) > 5 else ""
            popis = row[7].strip() if len(row) > 7 else ""
            cena = row[8].strip() if len(row) > 8 else ""
            vysledek = row[11].strip() if len(row) > 11 else ""
            
            # Map typ
            typ_mapped = ""
            tl = (typ + kategorie).lower()
            if "workshop" in tl:
                typ_mapped = "Workshop"
            elif "přednáška" in tl or "keynote" in tl or "konference" in tl:
                typ_mapped = "Přednáška / keynote"
            elif "školení" in tl:
                typ_mapped = "Školení"
            elif "masterclass" in tl or "program" in tl:
                typ_mapped = "Jiné (interní program apod.)"
            
            # Map vysledek
            vys = ""
            vl = (vysledek or "").lower()
            if "potvrzeno" in vl or "deal" in vl:
                vys = "Deal"
            elif "odmítnuto" in vl or "zrušeno" in vl:
                vys = "Odmítnuto"
            
            poznamka_parts = []
            if datum:
                poznamka_parts.append(f"Datum: {datum}")
            if misto:
                poznamka_parts.append(f"Místo: {misto}")
            if popis:
                poznamka_parts.append(popis)
            if cena:
                poznamka_parts.append(f"Cena: {cena}")
            
            fields = {
                "Firma": firma,
                "Komu určeno / Nabídnut pro realizaci": "Filip",
                "Poznámka": " | ".join(poznamka_parts)
            }
            if typ_mapped:
                fields["Co poptávali"] = typ_mapped
            if vys:
                fields["Reakce/výsledek"] = vys
            
            additional_deals.append({"fields": fields})
            seen_keys.add(key)
    
    print(f"   {len(additional_deals)} dodatečných deals")
    
    print("\n➕ Vkládám Pipedrive a Filip akce...")
    created = 0
    for batch in chunked(additional_deals, BATCH_SIZE):
        request_with_backoff("POST", doplnek_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        created += len(batch)
        if created % 50 == 0:
            print(f"   ... {created}/{len(additional_deals)}")
        time.sleep(0.2)
    print(f"   ✅ Vloženo {created}")
    
    print(f"\n✅ Hotovo! Celkem {len(original_deals) + len(additional_deals)} záznamů")
    print("   - Původní Deals: první")
    print("   - Pipedrive + Filip akce: na konci")


if __name__ == "__main__":
    main()
