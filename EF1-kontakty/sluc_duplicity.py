#!/usr/bin/env python3
"""
Sloučí duplicitní deals do jednoho záznamu s max informacemi.
"""

import json
import time
from pathlib import Path
from typing import Dict
from urllib.parse import quote

import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"
TABLE_ID = "tblOOAzDQbnOg1KRd"


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


def get_record(rec_id: str, hdrs: dict) -> dict:
    url = f"{API_BASE}/{BASE_ID}/{TABLE_ID}/{rec_id}"
    return request_with_backoff("GET", url, hdrs=hdrs)


def update_record(rec_id: str, fields: dict, hdrs: dict):
    url = f"{API_BASE}/{BASE_ID}/{TABLE_ID}/{rec_id}"
    request_with_backoff("PATCH", url, hdrs=hdrs, json_data={"fields": fields, "typecast": True})


def delete_record(rec_id: str, hdrs: dict):
    url = f"{API_BASE}/{BASE_ID}/{TABLE_ID}/{rec_id}"
    request_with_backoff("DELETE", url, hdrs=hdrs)


def merge_fields(rec1: dict, rec2: dict) -> dict:
    """Sloučí pole z obou záznamů, preferuje delší/detailnější hodnoty."""
    f1 = rec1.get("fields", {})
    f2 = rec2.get("fields", {})
    
    merged = {}
    
    # Textová pole - vezmi delší nebo neprázdnou hodnotu
    text_fields = ["Jméno a příjmení", "Email", "Firma", "Reakce/výsledek"]
    for field in text_fields:
        v1 = f1.get(field, "") or ""
        v2 = f2.get(field, "") or ""
        if len(v1) >= len(v2):
            if v1:
                merged[field] = v1
        else:
            if v2:
                merged[field] = v2
    
    # Poznámka - slouč obě, pokud jsou různé
    p1 = f1.get("Poznámka / Detaily", "") or ""
    p2 = f2.get("Poznámka / Detaily", "") or ""
    
    if p1 and p2 and p1 != p2:
        # Zkontroluj, jestli jedna není obsažena v druhé
        if p1 in p2:
            merged["Poznámka / Detaily"] = p2
        elif p2 in p1:
            merged["Poznámka / Detaily"] = p1
        else:
            # Slouč obě
            merged["Poznámka / Detaily"] = p1 + "\n---\n" + p2
    elif p1:
        merged["Poznámka / Detaily"] = p1
    elif p2:
        merged["Poznámka / Detaily"] = p2
    
    # Select pole - preferuj neprázdnou hodnotu
    if f1.get("Co poptávali"):
        merged["Co poptávali"] = f1["Co poptávali"]
    elif f2.get("Co poptávali"):
        merged["Co poptávali"] = f2["Co poptávali"]
    
    if f1.get("Komu určeno / Nabídnut pro realizaci"):
        merged["Komu určeno / Nabídnut pro realizaci"] = f1["Komu určeno / Nabídnut pro realizaci"]
    elif f2.get("Komu určeno / Nabídnut pro realizaci"):
        merged["Komu určeno / Nabídnut pro realizaci"] = f2["Komu určeno / Nabídnut pro realizaci"]
    
    # Linked records - slouč
    klienti1 = f1.get("Klienti", []) or []
    klienti2 = f2.get("Klienti", []) or []
    all_klienti = list(set(klienti1 + klienti2))
    if all_klienti:
        merged["Klienti"] = all_klienti
    
    return merged


# Definice duplicit k sloučení
# Format: (keep_id, delete_id, firma)
DUPLICATES = [
    # Reshoper - keep detailed one
    ("rec2Y9xskk3PjtbtX", "recLyk4QsrIG80Qpo", "Reshoper"),
    
    # Inovační centrum Ústeckého kraje - keep detailed one
    ("rec8Ji0BHNs92Bee2", "rec3E63OFMXuTydoy", "Inovační centrum Ústeckého kraje"),
    
    # DHL Supply Chain - keep one with more info
    ("recaABrdpDJm2AnNE", "rec49pXfCCBtbiJra", "DHL Supply Chain"),
    
    # Positiva Futuro Brazil - keep one with dates
    ("recJ8gF8NX4ccEa5e", "recWBeFnV3QG6nkMK", "Positiva Futuro Brazil"),
    
    # BDO - keep one with more info
    ("receAkwZWXurh5daK", "recRq42VxXhEnzXbe", "BDO"),
    
    # mBank - keep one with details
    ("reck2xFCRpkynfY1v", "recxVyDd11iJPbbIj", "mBank"),
]


def main():
    token = get_token()
    hdrs = headers(token)
    
    print("🔄 Slučuji duplicity...\n")
    
    for keep_id, delete_id, firma in DUPLICATES:
        print(f"📋 {firma}")
        
        try:
            # Načti oba záznamy
            rec_keep = get_record(keep_id, hdrs)
            rec_delete = get_record(delete_id, hdrs)
            
            # Slouč data
            merged = merge_fields(rec_keep, rec_delete)
            
            # Aktualizuj hlavní záznam
            if merged:
                update_record(keep_id, merged, hdrs)
                print(f"   ✅ Aktualizován: {keep_id}")
            
            # Smaž duplicitu
            delete_record(delete_id, hdrs)
            print(f"   🗑️  Smazán: {delete_id}")
            
            time.sleep(0.3)
            
        except Exception as e:
            print(f"   ❌ Chyba: {e}")
    
    print("\n✅ Hotovo! Sloučeno 6 duplicit.")


if __name__ == "__main__":
    main()
