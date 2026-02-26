#!/usr/bin/env python3
"""
Normalizuje hodnoty ve sloupci "Co poptávali" v Airtable 
na předefinované kategorie pro multiple choice.
"""

import json
import time
import re
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote

import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"
TABLE_ID = "tblN14nLVWXQ7jLbG"  # Projekty / Poptávky
BATCH_SIZE = 10

# Validní kategorie
VALID_OPTIONS = {
    "Přednáška / keynote",
    "Školení",
    "Jiné (interní program apod.)",
    "Konzultace",
    "Workshop"
}


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


def classify_poptavka(text: str) -> Optional[str]:
    """
    Klasifikuje text do jedné z kategorií.
    """
    if not text:
        return None
    
    lower = text.lower().strip()
    
    # Přesné shody (case insensitive)
    exact_map = {
        "přednáška / keynote": "Přednáška / keynote",
        "prednaska / keynote": "Přednáška / keynote",
        "keynote": "Přednáška / keynote",
        "přednáška": "Přednáška / keynote",
        "prednaska": "Přednáška / keynote",
        "školení": "Školení",
        "skoleni": "Školení",
        "workshop": "Workshop",
        "konzultace": "Konzultace",
        "jiné (interní program apod.)": "Jiné (interní program apod.)",
        "jiné": "Jiné (interní program apod.)",
        "jine": "Jiné (interní program apod.)",
        "interní program": "Jiné (interní program apod.)",
        "mentoring": "Konzultace",
    }
    
    if lower in exact_map:
        return exact_map[lower]
    
    # Už je to validní hodnota
    if text in VALID_OPTIONS:
        return text
    
    # Heuristiky podle klíčových slov
    # Priorita: Workshop > Školení > Přednáška > Konzultace > Jiné
    
    # Workshop
    if "workshop" in lower:
        return "Workshop"
    
    # Školení
    if "školení" in lower or "skoleni" in lower or "training" in lower or "vzdělávací program" in lower:
        return "Školení"
    
    # Přednáška / keynote
    if any(kw in lower for kw in ["přednáška", "prednaska", "keynote", "speech", "talk", "inspirativní"]):
        return "Přednáška / keynote"
    
    # Konzultace
    if any(kw in lower for kw in ["konzultace", "mentoring", "poradenství", "consulting"]):
        return "Konzultace"
    
    # Hackathon, interní akce apod.
    if any(kw in lower for kw in ["hackathon", "interní", "program", "setkání", "meetup", "webinář", "webinar"]):
        return "Jiné (interní program apod.)"
    
    # Pokud obsahuje nějaký relevantní text, zkusíme odhadnout
    # Pokud je to něco jako "AI budoucnost" - pravděpodobně přednáška
    if len(lower) > 20 and any(kw in lower for kw in ["ai ", "budoucnost", "future", "trend"]):
        return "Přednáška / keynote"
    
    # Nerozpoznáno - vrátíme None, hodnotu ponecháme
    return None


def main():
    token = get_token()
    
    print("🔎 Načítám záznamy z Projekty / Poptávky...")
    url = f"{API_BASE}/{BASE_ID}/{quote('Projekty / Poptávky', safe='')}"
    hdrs = headers(token)
    
    to_update = []
    stats = {"already_valid": 0, "to_normalize": 0, "unrecognized": 0, "empty": 0}
    unrecognized_values = []
    
    offset = None
    total = 0
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        
        data = request_with_backoff("GET", url, hdrs=hdrs, params=params)
        records = data.get("records", [])
        total += len(records)
        
        for rec in records:
            fields = rec.get("fields", {})
            original = fields.get("Co poptávali", "")
            
            if not original:
                stats["empty"] += 1
                continue
            
            if original in VALID_OPTIONS:
                stats["already_valid"] += 1
                continue
            
            # Zkus klasifikovat
            new_value = classify_poptavka(original)
            
            if new_value:
                stats["to_normalize"] += 1
                to_update.append({
                    "id": rec["id"],
                    "fields": {"Co poptávali": new_value},
                    "_original": original
                })
            else:
                stats["unrecognized"] += 1
                unrecognized_values.append((rec["id"], original))
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"\n📊 Statistika ({total} záznamů celkem):")
    print(f"   ✅ Už validní: {stats['already_valid']}")
    print(f"   🔄 K normalizaci: {stats['to_normalize']}")
    print(f"   ❓ Nerozpoznáno: {stats['unrecognized']}")
    print(f"   ⬜ Prázdné: {stats['empty']}")
    
    if to_update:
        print(f"\n📋 Ukázka normalizací:")
        for rec in to_update[:10]:
            print(f"   '{rec['_original']}' → '{rec['fields']['Co poptávali']}'")
    
    if unrecognized_values:
        print(f"\n⚠️ Nerozpoznané hodnoty (ponechám prázdné nebo původní):")
        for rec_id, val in unrecognized_values[:10]:
            print(f"   - {val[:80]}...")
    
    if not to_update:
        print("\n✅ Vše je již normalizované!")
        return
    
    # Aktualizace - automaticky
    
    print(f"\n⬆️ Aktualizuji...")
    
    # Odstraníme pomocné pole
    for rec in to_update:
        del rec["_original"]
    
    updated = 0
    for batch in chunked(to_update, BATCH_SIZE):
        request_with_backoff("PATCH", url, hdrs=hdrs, json_data={"records": batch, "typecast": True})
        updated += len(batch)
        if updated % 50 == 0:
            print(f"   ... {updated}/{len(to_update)}")
        time.sleep(0.2)
    
    print(f"\n✅ Normalizováno {updated} záznamů!")
    
    # Nerozpoznané hodnoty ponecháme - můžeš je opravit ručně v Airtable


if __name__ == "__main__":
    main()
