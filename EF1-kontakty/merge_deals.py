#!/usr/bin/env python3
"""
Sloučí tři CSV soubory do jednoho:
- Filip akce - poptávky
- Final mejling 2025 (bounced emails)
- AT - Deals

Vytvoří jednotný přehled poptávek/dealů s označením neaktivních emailů.
"""

import csv
import re
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent

# Soubory
FILIP_AKCE = BASE_DIR / "Filip akce - poptávky - List 1.csv"
MEJLING = BASE_DIR / "Final mejling 2025 (AIP a AImpact) - List 1.csv"
DEALS = BASE_DIR / "AT - Deals - List 1.csv"
OUTPUT = BASE_DIR / "poptavky_deals_merged.csv"


def normalize_email(email: str) -> str:
    """Normalizuje email pro porovnání."""
    return (email or "").strip().lower().rstrip()


def parse_filip_akce():
    """Parsuje Filip akce - poptávky."""
    records = []
    with open(FILIP_AKCE, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    # Tento soubor nemá header, je to tabulka bez záhlaví
    # Sloupce: Firma, Datum, Místo, Typ, Status poptávky, Kategorie, Účastníci, Poznámky, Cena, ?, ?, Výsledek
    
    current_record = None
    for line in lines:
        line = line.rstrip('\n')
        if not line.strip():
            continue
        
        parts = line.split(',')
        
        # Pokud řádek začíná firmou (první sloupec není prázdný a vypadá jako firma)
        if parts[0] and not parts[0].startswith('"') and len(parts) >= 5:
            # Nový záznam
            if current_record:
                records.append(current_record)
            
            # Parse - některé záznamy jsou přes více řádků
            firma = parts[0].strip()
            datum = parts[1].strip() if len(parts) > 1 else ""
            misto = parts[2].strip() if len(parts) > 2 else ""
            typ = parts[3].strip().replace('"', '') if len(parts) > 3 else ""
            status_poptavky = parts[4].strip() if len(parts) > 4 else ""
            kategorie = parts[5].strip() if len(parts) > 5 else ""
            ucastnici = parts[6].strip() if len(parts) > 6 else ""
            poznamky = parts[7].strip().replace('"', '') if len(parts) > 7 else ""
            cena = parts[8].strip() if len(parts) > 8 else ""
            vysledek = parts[-1].strip() if parts[-1].strip() in ["Potvrzeno", "Zrušeno / odmítnuto klientem", "Odmítnuto", "Nereagují", ""] else ""
            
            current_record = {
                "Firma": firma,
                "Datum": datum,
                "Místo": misto,
                "Typ": typ.strip(),
                "Status poptávky": status_poptavky,
                "Kategorie": kategorie,
                "Účastníci": ucastnici,
                "Cena": cena,
                "Výsledek": vysledek,
                "Poznámky": poznamky,
                "Zdroj": "Filip akce"
            }
        elif current_record and parts[0].startswith('"'):
            # Pokračování poznámky z předchozího řádku
            current_record["Poznámky"] += " " + line.replace('"', '').strip()
    
    if current_record:
        records.append(current_record)
    
    return records


def parse_mejling():
    """Parsuje mejling - extrahuje bounced emaily."""
    bounced = {}  # email -> reason
    replied = set()  # emaily, které odpověděly
    
    with open(MEJLING, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        
        for row in reader:
            if len(row) < 4:
                continue
            email = normalize_email(row[0])
            status = row[3].strip() if len(row) > 3 else ""
            reason = row[5].strip() if len(row) > 5 else ""
            
            if not email:
                continue
            
            if status == "EMAIL_BOUNCED":
                bounced[email] = reason or "bounced"
            elif status == "EMAIL_REPLIED":
                replied.add(email)
    
    return bounced, replied


def parse_deals():
    """Parsuje AT - Deals."""
    records = []
    
    with open(DEALS, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    # Nemá header, sloupce: Jméno, Email, Firma, Typ, Přiřazeno, Status, Poznámky
    current_record = None
    
    for line in lines:
        line = line.rstrip('\n')
        if not line.strip():
            continue
        
        parts = line.split(',')
        
        # Nový záznam pokud první sloupec obsahuje jméno nebo email
        first = parts[0].strip()
        if first and not first.startswith('"'):
            if current_record:
                records.append(current_record)
            
            jmeno = parts[0].strip() if len(parts) > 0 else ""
            email = parts[1].strip() if len(parts) > 1 else ""
            firma = parts[2].strip() if len(parts) > 2 else ""
            typ = parts[3].strip() if len(parts) > 3 else ""
            prirazeno = parts[4].strip() if len(parts) > 4 else ""
            status = parts[5].strip() if len(parts) > 5 else ""
            poznamky = ','.join(parts[6:]).strip().replace('"', '') if len(parts) > 6 else ""
            
            current_record = {
                "Kontakt": jmeno,
                "Email": email,
                "Firma": firma,
                "Typ": typ,
                "Přiřazeno": prirazeno,
                "Status dealu": status,
                "Poznámky": poznamky,
                "Zdroj": "AT Deals"
            }
        elif current_record and (first.startswith('"') or not first):
            # Pokračování poznámky
            current_record["Poznámky"] += " " + line.replace('"', '').strip()
    
    if current_record:
        records.append(current_record)
    
    return records


def main():
    print("📋 Načítám data...")
    
    # Načti bounced emaily
    bounced_emails, replied_emails = parse_mejling()
    print(f"   Bounced emailů: {len(bounced_emails)}")
    print(f"   Replied emailů: {len(replied_emails)}")
    
    # Načti deals
    deals = parse_deals()
    print(f"   Dealů z AT: {len(deals)}")
    
    # Načti Filip akce
    filip_akce = parse_filip_akce()
    print(f"   Akcí od Filipa: {len(filip_akce)}")
    
    # Sloučení - primárně bereme deals a doplňujeme info
    merged = []
    
    # Nejdřív zpracuj deals
    for deal in deals:
        email = normalize_email(deal.get("Email", ""))
        
        # Určení stavu emailu
        if email in bounced_emails:
            stav_email = "Neaktivní"
            duvod = bounced_emails[email]
        elif email in replied_emails:
            stav_email = "Aktivní (odpověděl/a)"
            duvod = ""
        elif email:
            stav_email = "Aktivní"
            duvod = ""
        else:
            stav_email = "Bez emailu"
            duvod = ""
        
        merged.append({
            "Kontakt": deal.get("Kontakt", ""),
            "Email": deal.get("Email", ""),
            "Firma": deal.get("Firma", ""),
            "Typ": deal.get("Typ", ""),
            "Přiřazeno": deal.get("Přiřazeno", ""),
            "Status dealu": deal.get("Status dealu", ""),
            "Stav emailu": stav_email,
            "Důvod neaktivity": duvod,
            "Poznámky": deal.get("Poznámky", ""),
            "Zdroj": "AT Deals"
        })
    
    # Přidej Filip akce (ty nemají emaily, ale mají firmy)
    for akce in filip_akce:
        merged.append({
            "Kontakt": "",
            "Email": "",
            "Firma": akce.get("Firma", ""),
            "Typ": akce.get("Typ", "") or akce.get("Kategorie", ""),
            "Přiřazeno": "Filip",
            "Status dealu": akce.get("Výsledek", "") or akce.get("Status poptávky", ""),
            "Stav emailu": "",
            "Důvod neaktivity": "",
            "Poznámky": f"{akce.get('Datum', '')} | {akce.get('Místo', '')} | {akce.get('Cena', '')} | {akce.get('Poznámky', '')}".strip(" |"),
            "Zdroj": "Filip akce"
        })
    
    # Ulož
    print(f"\n💾 Ukládám {len(merged)} záznamů do {OUTPUT.name}...")
    
    fieldnames = ["Kontakt", "Email", "Firma", "Typ", "Přiřazeno", "Status dealu", 
                  "Stav emailu", "Důvod neaktivity", "Poznámky", "Zdroj"]
    
    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged)
    
    # Statistiky
    neaktivni = sum(1 for r in merged if r["Stav emailu"] == "Neaktivní")
    aktivni = sum(1 for r in merged if "Aktivní" in r["Stav emailu"])
    replied = sum(1 for r in merged if "odpověděl" in r["Stav emailu"])
    deals_count = sum(1 for r in merged if r["Status dealu"] == "Deal")
    
    print(f"\n📊 Statistiky:")
    print(f"   Celkem záznamů: {len(merged)}")
    print(f"   Neaktivní emaily: {neaktivni}")
    print(f"   Aktivní emaily: {aktivni}")
    print(f"   Odpověděli na email: {replied}")
    print(f"   Uzavřené dealy: {deals_count}")
    
    # Vypíš bounced emaily
    print(f"\n❌ Bounced emaily ({len(bounced_emails)}):")
    for email, reason in sorted(bounced_emails.items()):
        print(f"   {email} - {reason}")
    
    print(f"\n✅ Hotovo! Výstup: {OUTPUT}")


if __name__ == "__main__":
    main()
