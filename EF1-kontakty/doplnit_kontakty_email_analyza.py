#!/usr/bin/env python3
"""
Doplní kontakty z analýzy emailů k deals bez kontaktů.
"""

import csv
import json
import time
import re
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import requests

BASE_DIR = Path(__file__).parent
EMAIL_CSV = Path.home() / "Downloads" / "analyza_emailu_poptavky_firemni_s_info a výsledky - analyza_emailu_poptavky_firemni_s_info.csv"

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


def normalize_company(s):
    s = (s or "").strip().lower()
    # Odstraň právní formy a zkratky
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',',
                   ' czech republic', ' česká republika', ' cz', ' sk',
                   ' pharma', ' group', ' holding']:
        s = s.replace(suffix, '')
    return s.strip()


def parse_email_csv() -> Dict[str, dict]:
    """Načte kontakty z CSV analýzy emailů podle firmy."""
    by_company = {}
    
    with open(EMAIL_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Různé varianty názvu firmy
            firma1 = row.get("firma_extrahovaná", "").strip()
            firma2 = row.get("firma", "").strip()
            jmeno = row.get("jméno_příjmení", "").strip()
            email = row.get("email", "").strip()
            
            if not email or '@' not in email or not jmeno:
                continue
            
            # Normalizuj obě firmy
            for firma in [firma1, firma2]:
                if firma:
                    firma_norm = normalize_company(firma)
                    if firma_norm and firma_norm not in by_company:
                        by_company[firma_norm] = {
                            "contact": jmeno,
                            "email": email,
                            "firma_original": firma
                        }
    
    return by_company


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti kontakty z CSV
    print("📋 Načítám kontakty z analýzy emailů...")
    email_contacts = parse_email_csv()
    print(f"   {len(email_contacts)} firem s kontakty")
    
    # Ukázka firem
    print("\n   Příklady firem:", list(email_contacts.keys())[:10])
    
    # 2. Načti deals bez kontaktů z Airtable
    print("\n🔎 Hledám deals bez kontaktů...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    deals_to_update = []
    all_deals_without_contact = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            firma = fields.get("Firma", "").strip()
            jmeno = fields.get("Jméno a příjmení", "").strip()
            email = fields.get("Email", "").strip()
            
            # Hledáme deals bez kontaktu
            if firma and not jmeno and not email:
                all_deals_without_contact.append(firma)
                firma_norm = normalize_company(firma)
                
                # Zkus najít v email CSV
                if firma_norm in email_contacts:
                    ec = email_contacts[firma_norm]
                    deals_to_update.append({
                        "id": rec["id"],
                        "fields": {
                            "Jméno a příjmení": ec["contact"],
                            "Email": ec["email"]
                        },
                        "_firma": firma
                    })
                else:
                    # Zkus najít částečnou shodu
                    for ec_firma, ec_data in email_contacts.items():
                        if firma_norm in ec_firma or ec_firma in firma_norm:
                            deals_to_update.append({
                                "id": rec["id"],
                                "fields": {
                                    "Jméno a příjmení": ec_data["contact"],
                                    "Email": ec_data["email"]
                                },
                                "_firma": firma
                            })
                            break
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Celkem deals bez kontaktu: {len(all_deals_without_contact)}")
    print(f"   Nalezeno kontaktů pro: {len(deals_to_update)} deals")
    
    if all_deals_without_contact:
        print(f"\n   Firmy bez kontaktu: {all_deals_without_contact[:20]}")
    
    if not deals_to_update:
        print("\n✅ Žádné další kontakty k doplnění!")
        return
    
    # Ukázka
    print("\n📋 Nalezené kontakty:")
    for rec in deals_to_update:
        f = rec["fields"]
        print(f"   {rec['_firma'][:30]:<30} → {f['Jméno a příjmení']} ({f['Email']})")
    
    # Odstraň pomocné pole
    for rec in deals_to_update:
        del rec["_firma"]
    
    # 3. Aktualizuj
    print(f"\n⬆️ Aktualizuji {len(deals_to_update)} deals...")
    
    updated = 0
    for batch in chunked(deals_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        time.sleep(0.2)
    
    print(f"\n✅ Doplněno {updated} kontaktů!")


if __name__ == "__main__":
    main()
