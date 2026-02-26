#!/usr/bin/env python3
"""
Doplní "Co poptává" do Klientů podle dat z Deals a Deals - doplněk.
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import quote

import requests

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
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',']:
        s = s.replace(suffix, '')
    return s.strip()


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti všechny Deals (obě tabulky)
    print("🔎 Načítám Deals...")
    
    company_poptavky = {}  # normalized_company -> set of poptávky
    
    # Deals (původní)
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    offset = None
    deals_count = 0
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            firma = fields.get("Firma", "").strip()
            co_poptavali = fields.get("Co poptávali", "")
            
            if firma and co_poptavali:
                firma_norm = normalize_company(firma)
                if firma_norm not in company_poptavky:
                    company_poptavky[firma_norm] = set()
                company_poptavky[firma_norm].add(co_poptavali)
                deals_count += 1
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Deals: {deals_count} záznamů s poptávkou")
    
    # Deals - doplněk
    deals2_url = f"{API_BASE}/{BASE_ID}/{quote('Deals - doplněk', safe='')}"
    offset = None
    deals2_count = 0
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals2_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            firma = fields.get("Firma", "").strip()
            co_poptavali = fields.get("Co poptávali", "")
            
            if firma and co_poptavali:
                firma_norm = normalize_company(firma)
                if firma_norm not in company_poptavky:
                    company_poptavky[firma_norm] = set()
                company_poptavky[firma_norm].add(co_poptavali)
                deals2_count += 1
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Deals - doplněk: {deals2_count} záznamů s poptávkou")
    print(f"   Celkem firem s poptávkami: {len(company_poptavky)}")
    
    # 2. Načti Klienty
    print("\n🔎 Načítám Klienty...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    klienti_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", klienti_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            firma = fields.get("Firma", "").strip()
            firma_norm = normalize_company(firma)
            current_poptavky = set(fields.get("Co poptává", []))
            
            if firma_norm in company_poptavky:
                new_poptavky = company_poptavky[firma_norm]
                # Přidej nové k existujícím
                combined = current_poptavky | new_poptavky
                
                if combined != current_poptavky:
                    klienti_to_update.append({
                        "id": rec["id"],
                        "fields": {"Co poptává": list(combined)}
                    })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   K aktualizaci: {len(klienti_to_update)} klientů")
    
    if not klienti_to_update:
        print("\n✅ Všichni klienti už mají aktuální poptávky!")
        return
    
    # Ukázka
    print("\n📋 Ukázka:")
    for rec in klienti_to_update[:5]:
        print(f"   → {rec['fields']['Co poptává']}")
    
    # 3. Aktualizuj
    print(f"\n⬆️ Aktualizuji...")
    
    updated = 0
    for batch in chunked(klienti_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", klienti_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        if updated % 50 == 0:
            print(f"   ... {updated}/{len(klienti_to_update)}")
        time.sleep(0.2)
    
    print(f"\n✅ Aktualizováno {updated} klientů!")


if __name__ == "__main__":
    main()
