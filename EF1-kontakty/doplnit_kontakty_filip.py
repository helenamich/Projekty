#!/usr/bin/env python3
"""
Doplní kontakty z Pipedrive k deals z Filip akce.
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
PIPEDRIVE = BASE_DIR / "deals-16044442-64.csv"

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
                   ' czech republic', ' česká republika', ' cz', ' sk']:
        s = s.replace(suffix, '')
    return s.strip()


def get_best_email(emails: List[str]) -> str:
    for e in emails:
        if e and '@' in e:
            return e.strip()
    return ""


def get_best_phone(phones: List[str]) -> str:
    for p in phones:
        if p and len(re.sub(r'\D', '', p)) >= 9:
            return p.strip()
    return ""


def parse_pipedrive() -> Dict[str, dict]:
    """Načte Pipedrive kontakty podle firmy."""
    by_company = {}
    
    with open(PIPEDRIVE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row.get("Deal - Organizace", "").strip()
            contact = row.get("Deal - Kontaktní osoba", "").strip()
            
            if not company or not contact:
                continue
            
            emails = [
                row.get("Osoba - E-mail - Práce", ""),
                row.get("Osoba - E-mail - Domov", ""),
                row.get("Osoba - E-mail - Ostatní", "")
            ]
            email = get_best_email(emails)
            
            phones = [
                row.get("Osoba - Telefon - Práce", ""),
                row.get("Osoba - Telefon - Mobil", ""),
                row.get("Osoba - Telefon - Domov", ""),
                row.get("Osoba - Telefon - Ostatní", "")
            ]
            phone = get_best_phone(phones)
            
            company_norm = normalize_company(company)
            
            # Ulož jen pokud máme email
            if email and company_norm not in by_company:
                by_company[company_norm] = {
                    "contact": contact,
                    "email": email,
                    "phone": phone
                }
    
    return by_company


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti kontakty z Pipedrive
    print("📋 Načítám kontakty z Pipedrive...")
    pipedrive = parse_pipedrive()
    print(f"   {len(pipedrive)} firem s kontakty")
    
    # 2. Načti deals bez kontaktů z Airtable
    print("\n🔎 Hledám deals bez kontaktů...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    deals_to_update = []
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
                firma_norm = normalize_company(firma)
                
                # Zkus najít v Pipedrive
                if firma_norm in pipedrive:
                    pip = pipedrive[firma_norm]
                    deals_to_update.append({
                        "id": rec["id"],
                        "fields": {
                            "Jméno a příjmení": pip["contact"],
                            "Email": pip["email"]
                        },
                        "_firma": firma
                    })
                else:
                    # Zkus najít částečnou shodu
                    for pip_firma, pip_data in pipedrive.items():
                        if firma_norm in pip_firma or pip_firma in firma_norm:
                            deals_to_update.append({
                                "id": rec["id"],
                                "fields": {
                                    "Jméno a příjmení": pip_data["contact"],
                                    "Email": pip_data["email"]
                                },
                                "_firma": firma
                            })
                            break
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Nalezeno kontaktů pro: {len(deals_to_update)} deals")
    
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
