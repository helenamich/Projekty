#!/usr/bin/env python3
"""
Vytvoří záznamy v tabulce Klienti pro všechny unikátní firmy z Kontaktů
a propojí kontakty s jejich firmami.
"""

import csv
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import quote

import requests

API_BASE = "https://api.airtable.com/v0"
BATCH_SIZE = 10

# Názvy firem, které ignorovat (nejsou to skutečné firmy)
INVALID_COMPANIES = {
    "", "-", "#ERROR!", "tbd", "TBD", "n/a", "N/A", "?", "nezaměstnaný", 
    "nezaměstnaná", "OSVČ", "soukromá osoba", "soukromý", "vlastní podnikání"
}


def get_token() -> str:
    """Načte token z MCP konfigurace."""
    mcp_path = Path.home() / ".cursor" / "mcp.json"
    with open(mcp_path, "r") as f:
        config = json.load(f)
    return config["mcpServers"]["airtable"]["env"]["AIRTABLE_API_KEY"]


def airtable_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def request_with_backoff(method: str, url: str, *, headers: dict, json_data=None, params=None) -> dict:
    delay = 1.0
    for attempt in range(1, 8):
        resp = requests.request(method, url, headers=headers, json=json_data, params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay)
            delay = min(delay * 2, 20)
            continue
        if not resp.ok:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text[:500]}")
        return resp.json()
    raise RuntimeError(f"Airtable API still failing after retries")


def chunked(items: List, size: int) -> List[List]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def normalize_company(name: str) -> str:
    """Normalizuje název firmy pro porovnání."""
    return (name or "").strip().lower()


def is_valid_company(name: str) -> bool:
    """Kontroluje, zda je název firmy validní."""
    normalized = normalize_company(name)
    if not normalized:
        return False
    if name.strip() in INVALID_COMPANIES:
        return False
    if normalized in {c.lower() for c in INVALID_COMPANIES}:
        return False
    return True


def get_unique_companies_from_csv(csv_path: Path) -> Set[str]:
    """Vrátí množinu unikátních firem z CSV."""
    companies = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = (row.get("Společnost / Firma") or "").strip()
            if is_valid_company(company):
                companies.add(company)
    return companies


def get_existing_klienti(token: str, base_id: str) -> Dict[str, str]:
    """Vrátí mapu: normalized_firma -> record_id."""
    url = f"{API_BASE}/{base_id}/{quote('Klienti', safe='')}"
    headers = airtable_headers(token)
    existing = {}
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", url, headers=headers, params=params)
        for rec in data.get("records", []):
            firma = (rec.get("fields", {}).get("Firma") or "").strip()
            if firma:
                existing[normalize_company(firma)] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
    return existing


def get_kontakty_by_company(token: str, base_id: str) -> Dict[str, List[str]]:
    """Vrátí mapu: normalized_firma -> [contact_record_ids]."""
    url = f"{API_BASE}/{base_id}/{quote('Kontakty', safe='')}"
    headers = airtable_headers(token)
    company_contacts: Dict[str, List[str]] = {}
    offset = None
    page = 0
    
    while True:
        page += 1
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", url, headers=headers, params=params)
        recs = data.get("records", [])
        print(f"   … stránka {page}: {len(recs)} kontaktů", flush=True)
        
        for rec in recs:
            firma = (rec.get("fields", {}).get("Společnost / Firma") or "").strip()
            if is_valid_company(firma):
                norm = normalize_company(firma)
                if norm not in company_contacts:
                    company_contacts[norm] = []
                company_contacts[norm].append(rec["id"])
        
        offset = data.get("offset")
        if not offset:
            break
    
    return company_contacts


def create_klienti(token: str, base_id: str, companies: List[str]) -> Dict[str, str]:
    """Vytvoří záznamy v Klienti, vrátí mapu: normalized_firma -> record_id."""
    url = f"{API_BASE}/{base_id}/{quote('Klienti', safe='')}"
    headers = airtable_headers(token)
    created = {}
    
    records_to_create = [{"fields": {"Firma": company}} for company in companies]
    
    for i, batch in enumerate(chunked(records_to_create, BATCH_SIZE)):
        print(f"   Vytvářím firmy: batch {i+1}/{(len(records_to_create) + BATCH_SIZE - 1) // BATCH_SIZE}", flush=True)
        data = request_with_backoff("POST", url, headers=headers, json_data={"records": batch, "typecast": True})
        for rec in data.get("records", []):
            firma = rec.get("fields", {}).get("Firma", "")
            created[normalize_company(firma)] = rec["id"]
        time.sleep(0.2)
    
    return created


def update_klienti_links(token: str, base_id: str, klient_id: str, contact_ids: List[str]) -> None:
    """Aktualizuje Klienti záznam s odkazy na Kontakty."""
    url = f"{API_BASE}/{base_id}/{quote('Klienti', safe='')}"
    headers = airtable_headers(token)
    
    record = {
        "id": klient_id,
        "fields": {
            "Kontakty": contact_ids
        }
    }
    
    request_with_backoff("PATCH", url, headers=headers, json_data={"records": [record], "typecast": True})


def main():
    token = get_token()
    base_id = "appEXpqOEIElHzScl"
    csv_path = Path(__file__).parent / "kontakty_unified.csv"
    
    print("📋 Načítám unikátní firmy z CSV…")
    csv_companies = get_unique_companies_from_csv(csv_path)
    print(f"   Nalezeno {len(csv_companies)} unikátních firem v CSV")
    
    print("\n🔎 Načítám existující Klienty z Airtable…")
    existing_klienti = get_existing_klienti(token, base_id)
    print(f"   Nalezeno {len(existing_klienti)} existujících firem v Airtable")
    
    # Firmy k vytvoření
    companies_to_create = []
    for company in csv_companies:
        if normalize_company(company) not in existing_klienti:
            companies_to_create.append(company)
    
    print(f"\n➕ K vytvoření: {len(companies_to_create)} nových firem")
    
    if companies_to_create:
        print("\n⬆️  Vytvářím nové firmy v Klienti…")
        new_klienti = create_klienti(token, base_id, companies_to_create)
        existing_klienti.update(new_klienti)
        print(f"   Vytvořeno {len(new_klienti)} nových firem")
    
    print("\n🔗 Načítám kontakty pro propojení s firmami…")
    company_contacts = get_kontakty_by_company(token, base_id)
    print(f"   Nalezeno {len(company_contacts)} firem s kontakty")
    
    print("\n🔗 Propojuji firmy s kontakty…")
    linked = 0
    batches = list(company_contacts.items())
    
    for i, (norm_company, contact_ids) in enumerate(batches):
        klient_id = existing_klienti.get(norm_company)
        if klient_id:
            update_klienti_links(token, base_id, klient_id, contact_ids)
            linked += 1
            if (i + 1) % 50 == 0:
                print(f"   … propojeno {i + 1}/{len(batches)} firem", flush=True)
            time.sleep(0.2)
    
    print(f"\n✅ Hotovo! Propojeno {linked} firem s jejich kontakty.")


if __name__ == "__main__":
    main()
