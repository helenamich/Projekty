#!/usr/bin/env python3
"""
Doplní tabulku Deals o záznamy z Pipedrive a Filip akce.
"""

import csv
import json
import time
import re
from pathlib import Path
from typing import Dict, List, Set
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


def map_typ_to_choice(typ: str, kategorie: str = "") -> str:
    """Mapuje typ na hodnoty v Airtable single select."""
    typ_lower = (typ or "").lower()
    kat_lower = (kategorie or "").lower()
    
    if "workshop" in typ_lower or "workshop" in kat_lower:
        return "Workshop"
    if "školení" in typ_lower or "training" in typ_lower:
        return "Školení"
    if "přednáška" in typ_lower or "keynote" in typ_lower or "speech" in typ_lower:
        return "Přednáška / keynote"
    if "konzultace" in typ_lower or "mentoring" in typ_lower:
        return "Konzultace"
    if "masterclass" in typ_lower or "hackathon" in typ_lower or "program" in typ_lower:
        return "Jiné (interní program apod.)"
    if "konference" in kat_lower:
        return "Přednáška / keynote"
    
    return ""


def map_vysledek(vysledek: str, status: str = "") -> str:
    """Mapuje výsledek."""
    v = (vysledek or status or "").lower()
    
    if "deal" in v or "potvrzeno" in v:
        return "Deal"
    if "odmítnuto" in v or "zrušeno" in v:
        return "Odmítnuto"
    if "nereagují" in v or "bez reakce" in v:
        return "Bez reakce"
    if "malý budget" in v:
        return "Malý budget"
    if "poptávka" in v or "nabídka" in v or "v řešení" in v:
        return "V jednání"
    
    return vysledek or status or ""


def get_best_email(emails: List[str]) -> str:
    for e in emails:
        if e and '@' in e:
            return e.strip()
    return ""


def parse_pipedrive() -> List[dict]:
    """Načte Pipedrive deals."""
    records = []
    
    with open(PIPEDRIVE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            deal_name = row.get("Deal - Název", "").strip()
            company = row.get("Deal - Organizace", "").strip()
            contact = row.get("Deal - Kontaktní osoba", "").strip()
            value = row.get("Deal - Hodnota", "").strip()
            
            emails = [
                row.get("Osoba - E-mail - Práce", ""),
                row.get("Osoba - E-mail - Domov", ""),
                row.get("Osoba - E-mail - Ostatní", "")
            ]
            email = get_best_email(emails)
            
            # Urči typ z názvu dealu
            typ = ""
            deal_lower = deal_name.lower()
            if "workshop" in deal_lower:
                typ = "Workshop"
            elif "přednáška" in deal_lower or "keynote" in deal_lower:
                typ = "Přednáška / keynote"
            elif "školení" in deal_lower or "kurz" in deal_lower:
                typ = "Školení"
            elif "webinář" in deal_lower or "webinar" in deal_lower:
                typ = "Přednáška / keynote"
            elif "program" in deal_lower or "masterclass" in deal_lower:
                typ = "Jiné (interní program apod.)"
            
            if company or contact:
                records.append({
                    "jmeno": contact,
                    "email": email,
                    "firma": company,
                    "typ": typ,
                    "poznamka": deal_name,
                    "zdroj": "Pipedrive"
                })
    
    return records


def parse_filip_akce() -> List[dict]:
    """Načte Filip akce."""
    records = []
    
    with open(FILIP_AKCE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            
            firma = row[0].strip() if len(row) > 0 else ""
            if not firma:
                continue
            
            datum = row[1].strip() if len(row) > 1 else ""
            misto = row[2].strip() if len(row) > 2 else ""
            typ = row[3].strip() if len(row) > 3 else ""
            kategorie = row[5].strip() if len(row) > 5 else ""
            popis = row[7].strip() if len(row) > 7 else ""
            cena = row[8].strip() if len(row) > 8 else ""
            vysledek = row[11].strip() if len(row) > 11 else ""
            
            typ_mapped = map_typ_to_choice(typ, kategorie)
            vysledek_mapped = map_vysledek(vysledek)
            
            poznamka_parts = []
            if datum:
                poznamka_parts.append(f"Datum: {datum}")
            if misto:
                poznamka_parts.append(f"Místo: {misto}")
            if popis:
                poznamka_parts.append(popis)
            if cena:
                poznamka_parts.append(f"Cena: {cena}")
            
            records.append({
                "jmeno": "",
                "email": "",
                "firma": firma,
                "typ": typ_mapped,
                "komu": "Filip",
                "vysledek": vysledek_mapped,
                "poznamka": " | ".join(poznamka_parts),
                "zdroj": "Filip akce"
            })
    
    return records


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti existující Deals
    print("🔎 Načítám existující Deals z Airtable...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    new_deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals - doplněk', safe='')}"
    
    existing_emails = set()
    existing_companies = set()
    
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            email = (fields.get("Email") or "").strip().lower()
            firma = (fields.get("Firma") or "").strip()
            
            if email:
                existing_emails.add(email)
            if firma:
                existing_companies.add(normalize_company(firma))
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Existující: {len(existing_emails)} emailů, {len(existing_companies)} firem")
    
    # 2. Načti Pipedrive
    print("\n📋 Načítám Pipedrive...")
    pipedrive = parse_pipedrive()
    print(f"   {len(pipedrive)} záznamů")
    
    # 3. Načti Filip akce
    print("\n📋 Načítám Filip akce...")
    filip = parse_filip_akce()
    print(f"   {len(filip)} záznamů")
    
    # 4. Filtruj nové záznamy
    deals_to_create = []
    
    # Z Pipedrive
    for rec in pipedrive:
        email = normalize(rec["email"])
        firma_norm = normalize_company(rec["firma"])
        
        # Přeskoč pokud už existuje
        if email and email in existing_emails:
            continue
        if firma_norm and firma_norm in existing_companies:
            continue
        
        # Přidej
        fields = {
            "Jméno a příjmení": rec["jmeno"],
            "Firma": rec["firma"],
            "Poznámka": rec["poznamka"]
        }
        if rec["email"]:
            fields["Email"] = rec["email"]
        if rec["typ"]:
            fields["Co poptávali"] = rec["typ"]
        
        deals_to_create.append({"fields": fields})
        
        if email:
            existing_emails.add(email)
        if firma_norm:
            existing_companies.add(firma_norm)
    
    # Z Filip akce
    for rec in filip:
        firma_norm = normalize_company(rec["firma"])
        
        if firma_norm in existing_companies:
            continue
        
        fields = {
            "Firma": rec["firma"],
            "Poznámka": rec["poznamka"]
        }
        if rec["typ"]:
            fields["Co poptávali"] = rec["typ"]
        if rec.get("komu"):
            fields["Komu určeno / Nabídnut pro realizaci"] = rec["komu"]
        if rec.get("vysledek"):
            fields["Reakce/výsledek"] = rec["vysledek"]
        
        deals_to_create.append({"fields": fields})
        existing_companies.add(firma_norm)
    
    print(f"\n📊 Nových deals k vytvoření: {len(deals_to_create)}")
    
    if not deals_to_create:
        print("\n✅ Žádné nové deals k přidání!")
        return
    
    # Ukázka
    print("\n📋 Ukázka (prvních 10):")
    for rec in deals_to_create[:10]:
        f = rec["fields"]
        print(f"   {f.get('Firma', f.get('Jméno a příjmení', '?'))[:40]} - {f.get('Co poptávali', '?')}")
    
    # 5. Vytvoř do nové tabulky "Deals - doplněk"
    print(f"\n➕ Vytvářím {len(deals_to_create)} deals do tabulky 'Deals - doplněk'...")
    
    created = 0
    for batch in chunked(deals_to_create, BATCH_SIZE):
        request_with_backoff("POST", new_deals_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        created += len(batch)
        if created % 50 == 0:
            print(f"   ... {created}/{len(deals_to_create)}")
        time.sleep(0.2)
    
    print(f"\n✅ Vytvořeno {created} nových deals!")


if __name__ == "__main__":
    main()
