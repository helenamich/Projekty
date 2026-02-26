#!/usr/bin/env python3
"""
Sloučí duplicitní klienty - převede linky a smaže duplicity.
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
BATCH_SIZE = 10


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


def chunked(items: List, size: int) -> List[List]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def normalize_company(s):
    """Normalizuje název firmy pro porovnání."""
    s = (s or "").strip().lower()
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' ltd.', ' inc', ' n.v.', ' ag', ' se',
                   ' czech republic', ' česká republika', ' cz', ' sk',
                   ' czech', ' slovakia', ' group', ' holding']:
        s = s.replace(suffix, '')
    s = re.sub(r'[,\.\-\(\)]', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti všechny klienty
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
    
    # 2. Seskup podle normalizovaného názvu
    by_name = defaultdict(list)
    for rec in all_klienti:
        fields = rec.get("fields", {})
        firma = fields.get("Firma", "")
        if firma:
            firma_norm = normalize_company(firma)
            if len(firma_norm) > 2:
                by_name[firma_norm].append({
                    "id": rec["id"],
                    "firma": firma,
                    "deals": fields.get("Deals", []),
                    "kontakty": fields.get("Kontakty", []),
                    "score": len(fields.get("Deals", [])) + len(fields.get("Kontakty", []))
                })
    
    # 3. Najdi duplicity a připrav sloučení
    print("\n🔄 Připravuji sloučení...")
    
    to_update = []  # Záznamy k aktualizaci (přidání linků)
    to_delete = []  # Záznamy ke smazání
    
    for firma_norm, klienti in by_name.items():
        if len(klienti) <= 1:
            continue
        
        # Vyber nejlepší záznam (nejvíc linků + nejkratší název)
        klienti.sort(key=lambda x: (-x["score"], len(x["firma"])))
        best = klienti[0]
        duplicates = klienti[1:]
        
        # Sesbírej všechny linky z duplicit
        all_deals = set(best["deals"])
        all_kontakty = set(best["kontakty"])
        
        for dup in duplicates:
            all_deals.update(dup["deals"])
            all_kontakty.update(dup["kontakty"])
            to_delete.append(dup["id"])
        
        # Pokud jsou nové linky, aktualizuj best
        if len(all_deals) > len(best["deals"]) or len(all_kontakty) > len(best["kontakty"]):
            to_update.append({
                "id": best["id"],
                "fields": {
                    "Deals": list(all_deals) if all_deals else None,
                    "Kontakty": list(all_kontakty) if all_kontakty else None
                }
            })
    
    print(f"   K aktualizaci: {len(to_update)} záznamů")
    print(f"   Ke smazání: {len(to_delete)} duplicit")
    
    if not to_delete:
        print("\n✅ Žádné duplicity k odstranění!")
        return
    
    # 4. Aktualizuj hlavní záznamy (přidej linky)
    if to_update:
        print("\n⬆️ Přenáším linky...")
        updated = 0
        for batch in chunked(to_update, BATCH_SIZE):
            # Odstraň None hodnoty
            clean_batch = []
            for rec in batch:
                clean_fields = {k: v for k, v in rec["fields"].items() if v is not None}
                if clean_fields:
                    clean_batch.append({"id": rec["id"], "fields": clean_fields})
            
            if clean_batch:
                request_with_backoff("PATCH", klienti_url, hdrs=hdrs, 
                                    json_data={"records": clean_batch})
            updated += len(batch)
            time.sleep(0.2)
        print(f"   Aktualizováno {updated} záznamů")
    
    # 5. Smaž duplicity
    print("\n🗑️ Mažu duplicity...")
    deleted = 0
    for batch in chunked(to_delete, BATCH_SIZE):
        # Airtable delete API expects records[] parameter
        params = "&".join([f"records[]={rec_id}" for rec_id in batch])
        delete_url = f"{klienti_url}?{params}"
        request_with_backoff("DELETE", delete_url, hdrs=hdrs)
        deleted += len(batch)
        print(f"   Smazáno: {deleted}/{len(to_delete)}", end="\r")
        time.sleep(0.2)
    
    print(f"\n\n✅ Sloučeno! Smazáno {deleted} duplicitních klientů.")


if __name__ == "__main__":
    main()
