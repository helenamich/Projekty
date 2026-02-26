#!/usr/bin/env python3
"""
Audit Airtable databáze - kontrola konzistence a návrhy na zlepšení.
"""

import json
import time
from pathlib import Path
from typing import Dict
from urllib.parse import quote
from collections import Counter

import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"


def get_token() -> str:
    with open(Path.home() / ".cursor" / "mcp.json") as f:
        return json.load(f)["mcpServers"]["airtable"]["env"]["AIRTABLE_API_KEY"]


def headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def request_with_backoff(method: str, url: str, *, hdrs: dict, params=None) -> dict:
    delay = 1.0
    for attempt in range(1, 8):
        resp = requests.request(method, url, headers=hdrs, params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay)
            delay = min(delay * 2, 20)
            continue
        if not resp.ok:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text[:500]}")
        return resp.json()
    raise RuntimeError("Airtable API still failing after retries")


def load_all_records(table_name: str, hdrs: dict) -> list:
    url = f"{API_BASE}/{BASE_ID}/{quote(table_name, safe='')}"
    records = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", url, hdrs=hdrs, params=params)
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    
    return records


def main():
    token = get_token()
    hdrs = headers(token)
    
    print("=" * 70)
    print("📊 AUDIT AIRTABLE DATABÁZE")
    print("=" * 70)
    
    # Načti všechna data
    print("\n🔎 Načítám data...")
    kontakty = load_all_records("Kontakty", hdrs)
    klienti = load_all_records("Klienti", hdrs)
    deals = load_all_records("Deals", hdrs)
    
    print(f"   Kontakty: {len(kontakty)}")
    print(f"   Klienti: {len(klienti)}")
    print(f"   Deals: {len(deals)}")
    
    # === KONTAKTY ===
    print("\n" + "=" * 70)
    print("👤 KONTAKTY")
    print("=" * 70)
    
    # Prázdné emaily
    no_email = sum(1 for k in kontakty if not k.get("fields", {}).get("E-mail"))
    print(f"\n⚠️  Bez e-mailu: {no_email}")
    
    # Duplicitní emaily
    emails = [k.get("fields", {}).get("E-mail", "").lower() for k in kontakty if k.get("fields", {}).get("E-mail")]
    email_counts = Counter(emails)
    duplicates = {e: c for e, c in email_counts.items() if c > 1}
    print(f"⚠️  Duplicitní e-maily: {len(duplicates)}")
    if duplicates:
        for email, count in list(duplicates.items())[:5]:
            print(f"      {email}: {count}x")
    
    # Bez oslovení
    no_osloveni = sum(1 for k in kontakty if not k.get("fields", {}).get("Oslovení"))
    print(f"⚠️  Bez oslovení: {no_osloveni}")
    
    # Bez firmy
    no_firma = sum(1 for k in kontakty if not k.get("fields", {}).get("Společnost / Firma"))
    print(f"⚠️  Bez firmy: {no_firma}")
    
    # Bez linku na Klienta
    no_klient_link = sum(1 for k in kontakty if not k.get("fields", {}).get("Klienti"))
    print(f"⚠️  Bez linku na Klienta: {no_klient_link}")
    
    # Stav emailu
    stav_counts = Counter(k.get("fields", {}).get("Stav - e-mail", "Nevyplněno") for k in kontakty)
    print(f"\n📈 Stav e-mailu:")
    for stav, count in stav_counts.most_common():
        print(f"      {stav or 'Nevyplněno'}: {count}")
    
    # === KLIENTI ===
    print("\n" + "=" * 70)
    print("🏢 KLIENTI")
    print("=" * 70)
    
    # Bez kontaktů
    no_contacts = sum(1 for k in klienti if not k.get("fields", {}).get("Kontakty"))
    print(f"\n⚠️  Bez kontaktů: {no_contacts}")
    
    # Bez dealů
    no_deals = sum(1 for k in klienti if not k.get("fields", {}).get("Deals"))
    print(f"⚠️  Bez dealů: {no_deals}")
    
    # Počet zaměstnanců
    pocet_zam = Counter(k.get("fields", {}).get("Počet zaměstnanců", "Nevyplněno") for k in klienti)
    print(f"\n📈 Počet zaměstnanců:")
    for p, count in pocet_zam.most_common():
        print(f"      {p or 'Nevyplněno'}: {count}")
    
    # === DEALS ===
    print("\n" + "=" * 70)
    print("💼 DEALS")
    print("=" * 70)
    
    # Bez kontaktu
    no_contact = sum(1 for d in deals if not d.get("fields", {}).get("Kontakt"))
    print(f"\n⚠️  Bez kontaktu: {no_contact}")
    
    # Bez klienta
    no_klient = sum(1 for d in deals if not d.get("fields", {}).get("Klienti"))
    print(f"⚠️  Bez klienta: {no_klient}")
    
    # Bez typu (Co poptávali)
    no_type = sum(1 for d in deals if not d.get("fields", {}).get("Co poptávali"))
    print(f"⚠️  Bez typu: {no_type}")
    
    # Bez výsledku
    no_result = sum(1 for d in deals if not d.get("fields", {}).get("Reakce / Výsledek"))
    print(f"⚠️  Bez výsledku: {no_result}")
    
    # Reakce/Výsledek
    reakce_counts = Counter(d.get("fields", {}).get("Reakce / Výsledek", "Nevyplněno") for d in deals)
    print(f"\n📈 Reakce/Výsledek:")
    for r, count in reakce_counts.most_common():
        print(f"      {r or 'Nevyplněno'}: {count}")
    
    # Co poptávali
    typ_counts = Counter(d.get("fields", {}).get("Co poptávali", "Nevyplněno") for d in deals)
    print(f"\n📈 Co poptávali:")
    for t, count in typ_counts.most_common():
        print(f"      {t or 'Nevyplněno'}: {count}")
    
    # === NÁVRHY NA ZLEPŠENÍ ===
    print("\n" + "=" * 70)
    print("💡 NÁVRHY NA ZLEPŠENÍ")
    print("=" * 70)
    
    suggestions = []
    
    if no_email > 0:
        suggestions.append(f"1. Doplnit e-maily u {no_email} kontaktů")
    
    if len(duplicates) > 0:
        suggestions.append(f"2. Sloučit {len(duplicates)} duplicitních kontaktů (podle emailu)")
    
    if no_osloveni > 50:
        suggestions.append(f"3. Doplnit oslovení u {no_osloveni} kontaktů")
    
    if no_klient_link > 100:
        suggestions.append(f"4. Propojit {no_klient_link} kontaktů s Klienty")
    
    if no_contact > 10:
        suggestions.append(f"5. Doplnit kontakty u {no_contact} dealů")
    
    if no_result > 50:
        suggestions.append(f"6. Vyplnit výsledek u {no_result} dealů")
    
    if no_type > 30:
        suggestions.append(f"7. Vyplnit typ poptávky u {no_type} dealů")
    
    # Zbytečná pole v Klientech
    suggestions.append("8. Smazat zbytečná textová pole v Klientech (Co poptávali, Projekty/Poptávky, Kontakt HR) - info je v linkovaných Deals")
    
    # View návrhy
    suggestions.append("9. Vytvořit Views: 'Aktivní dealy', 'Bez reakce', 'Dealy 2025', 'VIP klienti'")
    
    print()
    for s in suggestions:
        print(f"   {s}")
    
    print("\n" + "=" * 70)
    print("✅ AUDIT DOKONČEN")
    print("=" * 70)


if __name__ == "__main__":
    main()
