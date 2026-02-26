#!/usr/bin/env python3
"""
Označí kontakty podle toho, jestli byl deal realizovaný nebo jen poptávka.
"""

import json
import time
from pathlib import Path
from typing import Dict, List
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


def is_deal(reakce: str) -> bool:
    """Určí, jestli výsledek je Deal nebo jen Poptávka."""
    if not reakce:
        return False
    
    reakce_lower = reakce.lower()
    
    # Jasné dealy
    if "deal" in reakce_lower:
        return True
    if reakce_lower in ["realizováno", "uskutečněno", "proběhlo"]:
        return True
    
    # Vše ostatní je poptávka
    return False


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti všechny Deals
    print("🔎 Načítám Deals...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    deals = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        deals.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    print(f"   {len(deals)} deals")
    
    # 2. Analyzuj výsledky
    deals_info = {}  # email -> "Deal" nebo "Poptávka"
    deal_count = 0
    poptavka_count = 0
    
    for deal in deals:
        fields = deal.get("fields", {})
        email = (fields.get("Email") or "").strip().lower()
        reakce = fields.get("Reakce/výsledek", "")
        
        if not email:
            continue
        
        if is_deal(reakce):
            deals_info[email] = "Deal"
            deal_count += 1
        else:
            deals_info[email] = "Poptávka"
            poptavka_count += 1
    
    print(f"   Dealy: {deal_count}, Poptávky: {poptavka_count}")
    
    # 3. Načti Kontakty
    print("\n🔎 Načítám Kontakty...")
    kontakty_url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    
    kontakty_to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            email = (fields.get("E-mail") or "").strip().lower()
            
            if email in deals_info:
                current_programs = fields.get("Program / Deal / Poptávka", [])
                status = deals_info[email]
                
                # Zkontroluj, jestli už tam není
                if status not in current_programs:
                    new_programs = current_programs + [status]
                    kontakty_to_update.append({
                        "id": rec["id"],
                        "fields": {"Program / Deal / Poptávka": new_programs}
                    })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   K aktualizaci: {len(kontakty_to_update)} kontaktů")
    
    if not kontakty_to_update:
        print("\n✅ Všechny kontakty jsou už označené!")
        return
    
    # 4. Aktualizuj
    print(f"\n⬆️ Aktualizuji...")
    
    updated = 0
    for batch in chunked(kontakty_to_update, BATCH_SIZE):
        request_with_backoff("PATCH", kontakty_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        if updated % 50 == 0:
            print(f"   ... {updated}/{len(kontakty_to_update)}")
        time.sleep(0.2)
    
    print(f"\n✅ Označeno {updated} kontaktů!")
    
    # Statistika
    deal_contacts = sum(1 for rec in kontakty_to_update 
                        if "Deal" in rec["fields"]["Program / Deal / Poptávka"])
    poptavka_contacts = sum(1 for rec in kontakty_to_update 
                            if "Poptávka" in rec["fields"]["Program / Deal / Poptávka"])
    
    print(f"\n📊 Statistika:")
    print(f"   Označeno jako Deal: {deal_contacts}")
    print(f"   Označeno jako Poptávka: {poptavka_contacts}")


if __name__ == "__main__":
    main()
