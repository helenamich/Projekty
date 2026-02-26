#!/usr/bin/env python3
"""
Importuje deals_complete.csv do Airtable:
- Vytvoří/aktualizuje Kontakty
- Vytvoří/aktualizuje Klienti (firmy)
- Vytvoří záznamy v Projekty / Poptávky s propojením
"""

import csv
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import quote

import requests

BASE_DIR = Path(__file__).parent
DEALS_CSV = BASE_DIR / "deals_complete.csv"

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


def normalize_email(s: str) -> str:
    return (s or "").strip().lower()


def normalize_company(s: str) -> str:
    s = (s or "").strip().lower()
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', ' gmbh', ' ltd', ' inc', ',']:
        s = s.replace(suffix, '')
    return s.strip()


def get_existing_records(token: str, table: str, key_field: str) -> Dict[str, str]:
    """Vrátí mapu: normalized_key -> record_id."""
    url = f"{API_BASE}/{BASE_ID}/{quote(table, safe='')}"
    hdrs = headers(token)
    existing = {}
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", url, hdrs=hdrs, params=params)
        for rec in data.get("records", []):
            key = (rec.get("fields", {}).get(key_field) or "").strip()
            if key:
                existing[key.lower()] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
    return existing


def create_records(token: str, table: str, records: List[dict]) -> List[dict]:
    """Vytvoří záznamy, vrátí vytvořené."""
    url = f"{API_BASE}/{BASE_ID}/{quote(table, safe='')}"
    hdrs = headers(token)
    created = []
    
    for batch in chunked(records, BATCH_SIZE):
        data = request_with_backoff("POST", url, hdrs=hdrs, json_data={"records": batch, "typecast": True})
        created.extend(data.get("records", []))
        time.sleep(0.2)
    
    return created


def update_records(token: str, table: str, records: List[dict]) -> List[dict]:
    """Aktualizuje záznamy."""
    url = f"{API_BASE}/{BASE_ID}/{quote(table, safe='')}"
    hdrs = headers(token)
    updated = []
    
    for batch in chunked(records, BATCH_SIZE):
        data = request_with_backoff("PATCH", url, hdrs=hdrs, json_data={"records": batch, "typecast": True})
        updated.extend(data.get("records", []))
        time.sleep(0.2)
    
    return updated


def main():
    token = get_token()
    
    # Načti CSV
    print("📋 Načítám deals_complete.csv...")
    with open(DEALS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        deals = list(reader)
    print(f"   {len(deals)} záznamů")
    
    # 1. Načti existující Kontakty (podle emailu)
    print("\n🔎 Načítám existující Kontakty z Airtable...")
    existing_kontakty = get_existing_records(token, "Kontakty", "E-mail")
    print(f"   {len(existing_kontakty)} existujících kontaktů")
    
    # 2. Načti existující Klienti (podle firmy)
    print("\n🔎 Načítám existující Klienty z Airtable...")
    existing_klienti = get_existing_records(token, "Klienti", "Firma")
    print(f"   {len(existing_klienti)} existujících firem")
    
    # 3. Připrav data
    # Kontakty k vytvoření/aktualizaci
    kontakty_to_create = []
    kontakty_to_update = []
    
    # Klienti k vytvoření
    klienti_to_create = []
    new_klienti_names = set()
    
    # Poptávky k vytvoření
    poptavky = []
    
    for deal in deals:
        email = normalize_email(deal.get("Email", ""))
        firma = (deal.get("Firma") or "").strip()
        firma_norm = normalize_company(firma)
        kontakt = (deal.get("Kontakt") or "").strip()
        telefon = (deal.get("Telefon") or "").strip()
        stav_email = (deal.get("Stav emailu") or "").strip()
        
        # Kontakt
        kontakt_id = None
        if email and email in existing_kontakty:
            kontakt_id = existing_kontakty[email]
            # Aktualizuj telefon a stav pokud chybí
            if telefon or stav_email:
                kontakty_to_update.append({
                    "id": kontakt_id,
                    "fields": {
                        **({"Telefon": telefon} if telefon else {}),
                        **({"Stav": stav_email} if stav_email in ["Aktivní", "Neaktivní"] else {})
                    }
                })
        elif email and kontakt:
            # Nový kontakt - rozdělíme jméno
            parts = kontakt.split()
            jmeno = parts[0] if parts else ""
            prijmeni = " ".join(parts[1:]) if len(parts) > 1 else ""
            
            kontakty_to_create.append({
                "fields": {
                    "Jméno": jmeno,
                    "Příjmení": prijmeni,
                    "E-mail": email,
                    "Telefon": telefon,
                    "Společnost / Firma": firma,
                    "Stav": stav_email if stav_email in ["Aktivní", "Neaktivní"] else "Aktivní"
                }
            })
        
        # Klient (firma)
        klient_id = None
        if firma_norm and firma_norm in existing_klienti:
            klient_id = existing_klienti[firma_norm]
        elif firma and firma_norm not in new_klienti_names:
            klienti_to_create.append({"fields": {"Firma": firma}})
            new_klienti_names.add(firma_norm)
        
        # Poptávka
        poptavka_nazev = deal.get("Poznámky", "")[:100] or f"Poptávka - {firma}"
        poptavky.append({
            "nazev": poptavka_nazev,
            "firma": firma,
            "firma_norm": firma_norm,
            "email": email,
            "co_poptavali": deal.get("Co poptávali", ""),
            "komu_nabidnuto": deal.get("Komu nabídnuto", ""),
            "reakce": deal.get("Reakce / výsledek", ""),
            "cena": deal.get("Hodnota", ""),
            "poznamky": deal.get("Poznámky", ""),
            "zdroj": deal.get("Zdroj", "")
        })
    
    # 4. Vytvoř nové Kontakty
    if kontakty_to_create:
        print(f"\n➕ Vytvářím {len(kontakty_to_create)} nových kontaktů...")
        created = create_records(token, "Kontakty", kontakty_to_create)
        for rec in created:
            email = normalize_email(rec.get("fields", {}).get("E-mail", ""))
            if email:
                existing_kontakty[email] = rec["id"]
        print(f"   Vytvořeno: {len(created)}")
    
    # 5. Aktualizuj existující Kontakty
    if kontakty_to_update:
        # Filtruj prázdné aktualizace
        kontakty_to_update = [r for r in kontakty_to_update if r.get("fields")]
        if kontakty_to_update:
            print(f"\n♻️ Aktualizuji {len(kontakty_to_update)} kontaktů...")
            update_records(token, "Kontakty", kontakty_to_update)
    
    # 6. Vytvoř nové Klienty
    if klienti_to_create:
        print(f"\n➕ Vytvářím {len(klienti_to_create)} nových firem...")
        created = create_records(token, "Klienti", klienti_to_create)
        for rec in created:
            firma = (rec.get("fields", {}).get("Firma") or "").strip()
            if firma:
                existing_klienti[normalize_company(firma)] = rec["id"]
        print(f"   Vytvořeno: {len(created)}")
    
    # 7. Vytvoř Poptávky s propojením
    print(f"\n➕ Vytvářím {len(poptavky)} poptávek...")
    poptavky_records = []
    
    for p in poptavky:
        fields = {
            "Název": p["nazev"],
            "Co poptávali": p["co_poptavali"],
            "Komu nabídnuto": p["komu_nabidnuto"],
            "Reakce / výsledek": p["reakce"],
            "Cena": p["cena"],
            "Poznámky": p["poznamky"],
            "Zdroj": p["zdroj"]
        }
        
        # Propoj s Klientem
        klient_id = existing_klienti.get(p["firma_norm"])
        if klient_id:
            fields["Objednatel"] = [klient_id]
        
        # Propoj s Kontaktem
        kontakt_id = existing_kontakty.get(p["email"])
        if kontakt_id:
            fields["Kontakt"] = [kontakt_id]
        
        poptavky_records.append({"fields": fields})
    
    created = create_records(token, "Projekty / Poptávky", poptavky_records)
    print(f"   Vytvořeno: {len(created)}")
    
    print("\n✅ Hotovo!")


if __name__ == "__main__":
    main()
