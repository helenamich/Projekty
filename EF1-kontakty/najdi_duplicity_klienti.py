#!/usr/bin/env python3
"""
Najde duplicitní klienty v Airtable.
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
    with open(Path.home() / ".cursor" / "mcp.json") as f:
        return json.load(f)["mcpServers"]["airtable"]["env"]["AIRTABLE_API_KEY"]


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
    # Odstraň právní formy
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' ltd.', ' inc', ' n.v.', ' ag', ' se',
                   ' czech republic', ' česká republika', ' cz', ' sk',
                   ' czech', ' slovakia', ' group', ' holding']:
        s = s.replace(suffix, '')
    # Odstraň speciální znaky
    s = re.sub(r'[,\.\-\(\)]', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def main():
    token = get_token()
    hdrs = headers(token)
    
    # Načti všechny klienty
    print("🔎 Načítám Klienty...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    all_klienti = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", klienti_url, hdrs=hdrs, params=params)
        all_klienti.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Celkem {len(all_klienti)} klientů")
    
    # Seskup podle normalizovaného názvu
    by_name = defaultdict(list)
    for rec in all_klienti:
        firma = rec.get("fields", {}).get("Firma", "")
        if firma:
            firma_norm = normalize_company(firma)
            if len(firma_norm) > 2:  # Ignoruj příliš krátké
                by_name[firma_norm].append({
                    "id": rec["id"],
                    "firma": firma,
                    "deals": len(rec.get("fields", {}).get("Deals", [])),
                    "kontakty": len(rec.get("fields", {}).get("Kontakty", []))
                })
    
    # Najdi duplicity
    print("\n🔍 Hledám duplicity...\n")
    
    duplicates = []
    for firma_norm, klienti in by_name.items():
        if len(klienti) > 1:
            duplicates.append({
                "norm": firma_norm,
                "klienti": klienti
            })
    
    # Seřaď podle počtu duplicit
    duplicates.sort(key=lambda x: -len(x["klienti"]))
    
    print(f"📋 Nalezeno {len(duplicates)} skupin duplicit:\n")
    print("=" * 80)
    
    for dup in duplicates[:30]:  # Ukaž prvních 30
        print(f"\n🔴 {dup['norm'].upper()}")
        for k in dup["klienti"]:
            deals_info = f"({k['deals']} deals, {k['kontakty']} kontaktů)"
            print(f"   • {k['firma'][:50]:<50} {deals_info}")
    
    if len(duplicates) > 30:
        print(f"\n   ... a dalších {len(duplicates) - 30} skupin")
    
    print(f"\n\n📊 Celkem {len(duplicates)} skupin duplicit k vyčištění.")
    
    # Spočítej celkový počet záznamů k odstranění
    total_to_remove = sum(len(d["klienti"]) - 1 for d in duplicates)
    print(f"   Potenciálně {total_to_remove} záznamů k sloučení/smazání.")


if __name__ == "__main__":
    main()
