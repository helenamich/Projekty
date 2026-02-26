#!/usr/bin/env python3
"""
Komplexní merge všech deal/poptávka dat:
1. AT - Deals (základ)
2. Pipedrive export (emaily, telefony)
3. Filip akce - poptávky (detaily)
4. Final mejling (bounced = neaktivní)
"""

import csv
import re
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent

# Soubory
AT_DEALS = BASE_DIR / "AT - Deals - List 1.csv"
PIPEDRIVE = BASE_DIR / "deals-16044442-64.csv"
FILIP_AKCE = BASE_DIR / "Filip akce - poptávky - List 1.csv"
MEJLING = BASE_DIR / "Final mejling 2025 (AIP a AImpact) - List 1.csv"
OUTPUT = BASE_DIR / "deals_complete.csv"


def normalize(s):
    """Normalizuje string pro porovnání."""
    return (s or "").strip().lower()


def normalize_email(s):
    """Normalizuje email."""
    return normalize(s).rstrip()


def normalize_company(s):
    """Normalizuje název firmy pro matching."""
    s = normalize(s)
    # Odstraň právní formy
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',']:
        s = s.replace(suffix, '')
    return s.strip()


def get_best_email(emails):
    """Vybere nejlepší email z listu."""
    for e in emails:
        if e and '@' in e:
            # Preferuj pracovní emaily
            if not any(x in e.lower() for x in ['gmail', 'seznam', 'email.cz', 'centrum.cz']):
                return e.strip()
    for e in emails:
        if e and '@' in e:
            return e.strip()
    return ""


def get_best_phone(phones):
    """Vybere nejlepší telefon z listu."""
    for p in phones:
        if p and len(re.sub(r'\D', '', p)) >= 9:
            return p.strip()
    return ""


def parse_bounced_emails():
    """Načte bounced emaily z mejlingu."""
    bounced = set()
    with open(MEJLING, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if len(row) >= 4:
                email = normalize_email(row[0])
                status = row[3].strip() if len(row) > 3 else ""
                if status == "EMAIL_BOUNCED" and email:
                    bounced.add(email)
    return bounced


def parse_pipedrive():
    """Načte Pipedrive export - vrátí dict by company a by email."""
    by_company = defaultdict(list)
    by_email = {}
    
    with open(PIPEDRIVE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            deal_name = row.get("Deal - Název", "").strip()
            company = row.get("Deal - Organizace", "").strip()
            contact = row.get("Deal - Kontaktní osoba", "").strip()
            value = row.get("Deal - Hodnota", "").strip()
            
            # Emaily
            emails = [
                row.get("Osoba - E-mail - Práce", ""),
                row.get("Osoba - E-mail - Domov", ""),
                row.get("Osoba - E-mail - Ostatní", "")
            ]
            email = get_best_email(emails)
            
            # Telefony
            phones = [
                row.get("Osoba - Telefon - Práce", ""),
                row.get("Osoba - Telefon - Mobil", ""),
                row.get("Osoba - Telefon - Domov", ""),
                row.get("Osoba - Telefon - Ostatní", "")
            ]
            phone = get_best_phone(phones)
            
            record = {
                "deal_name": deal_name,
                "company": company,
                "contact": contact,
                "email": email,
                "phone": phone,
                "value": value,
                "source": "Pipedrive"
            }
            
            if company:
                by_company[normalize_company(company)].append(record)
            if email:
                by_email[normalize_email(email)] = record
    
    return by_company, by_email


def parse_at_deals():
    """Načte AT - Deals."""
    records = []
    
    with open(AT_DEALS, "r", encoding="utf-8") as f:
        content = f.read()
    
    lines = content.split('\n')
    current = None
    
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        
        parts = line.split(',')
        first = parts[0].strip() if parts else ""
        
        # Nový záznam - buď má kontakt, nebo má firmu (3. sloupec)
        # Řádky začínající uvozovkou jsou pokračování poznámek
        is_continuation = first.startswith('"')
        has_data = len(parts) >= 3 and (first or (len(parts) > 2 and parts[2].strip()))
        
        if not is_continuation and has_data:
            if current:
                records.append(current)
            
            current = {
                "kontakt": parts[0].strip() if len(parts) > 0 else "",
                "email": parts[1].strip() if len(parts) > 1 else "",
                "firma": parts[2].strip() if len(parts) > 2 else "",
                "typ": parts[3].strip() if len(parts) > 3 else "",
                "prirazeno": parts[4].strip() if len(parts) > 4 else "",
                "status": parts[5].strip() if len(parts) > 5 else "",
                "poznamky": ','.join(parts[6:]).strip().replace('"', '') if len(parts) > 6 else "",
                "source": "AT Deals"
            }
        elif current and is_continuation:
            current["poznamky"] += " " + line.replace('"', '').strip()
    
    if current:
        records.append(current)
    
    return records


def parse_filip_akce():
    """Načte Filip akce - poptávky."""
    records = []
    
    with open(FILIP_AKCE, "r", encoding="utf-8") as f:
        content = f.read()
    
    lines = content.split('\n')
    current = None
    
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        
        parts = line.split(',')
        first = parts[0].strip() if parts else ""
        
        # Zjistíme jestli je to nový záznam (firma na začátku)
        if first and not first.startswith('"') and len(parts) >= 4:
            if current:
                records.append(current)
            
            # Parsování - sloupce: Firma, Datum, Místo, Typ, Status, Kategorie, Účastníci, Poznámky, Cena, ?, ?, Výsledek
            current = {
                "firma": parts[0].strip(),
                "datum": parts[1].strip() if len(parts) > 1 else "",
                "misto": parts[2].strip() if len(parts) > 2 else "",
                "typ": parts[3].strip().replace('"', '') if len(parts) > 3 else "",
                "status_poptavky": parts[4].strip() if len(parts) > 4 else "",
                "kategorie": parts[5].strip() if len(parts) > 5 else "",
                "ucastnici": parts[6].strip() if len(parts) > 6 else "",
                "poznamky": parts[7].strip().replace('"', '') if len(parts) > 7 else "",
                "cena": parts[8].strip() if len(parts) > 8 else "",
                "vysledek": parts[-1].strip() if len(parts) > 10 else "",
                "source": "Filip akce"
            }
        elif current:
            current["poznamky"] += " " + line.replace('"', '').strip()
    
    if current:
        records.append(current)
    
    return records


def main():
    print("📋 Načítám data...")
    
    # 1. Bounced emaily
    bounced = parse_bounced_emails()
    print(f"   Bounced emailů: {len(bounced)}")
    
    # 2. Pipedrive
    pipedrive_by_company, pipedrive_by_email = parse_pipedrive()
    print(f"   Pipedrive: {len(pipedrive_by_email)} kontaktů s emailem")
    
    # 3. AT Deals
    at_deals = parse_at_deals()
    print(f"   AT Deals: {len(at_deals)} záznamů")
    
    # 4. Filip akce
    filip_akce = parse_filip_akce()
    print(f"   Filip akce: {len(filip_akce)} záznamů")
    
    # === MERGE ===
    print("\n🔀 Merguji data...")
    
    merged = []
    seen_emails = set()
    seen_companies = set()
    
    # A) Nejdřív zpracuj Pipedrive (nejkompletnější kontaktní data)
    for email, rec in pipedrive_by_email.items():
        email_norm = normalize_email(email)
        company_norm = normalize_company(rec["company"])
        
        # Stav podle bounce
        stav = "Neaktivní" if email_norm in bounced else "Aktivní"
        
        merged.append({
            "Kontakt": rec["contact"],
            "Email": rec["email"],
            "Telefon": rec["phone"],
            "Firma": rec["company"],
            "Co poptávali": "",  # doplníme z AT Deals nebo Filip
            "Komu nabídnuto": "",
            "Reakce / výsledek": "",
            "Hodnota": rec["value"],
            "Stav emailu": stav,
            "Poznámky": rec["deal_name"],
            "Zdroj": "Pipedrive"
        })
        seen_emails.add(email_norm)
        if company_norm:
            seen_companies.add(company_norm)
    
    # B) Doplň z AT Deals (přidá typ, přiřazení, status)
    for rec in at_deals:
        email = rec.get("email", "").strip()
        email_norm = normalize_email(email)
        company_norm = normalize_company(rec.get("firma", ""))
        
        # Pokud už máme tento email, aktualizuj
        if email_norm and email_norm in seen_emails:
            # Najdi a aktualizuj
            for m in merged:
                if normalize_email(m["Email"]) == email_norm:
                    if rec.get("typ"):
                        m["Co poptávali"] = rec["typ"]
                    if rec.get("prirazeno"):
                        m["Komu nabídnuto"] = rec["prirazeno"]
                    if rec.get("status"):
                        m["Reakce / výsledek"] = rec["status"]
                    if rec.get("poznamky"):
                        m["Poznámky"] = (m["Poznámky"] + " | " + rec["poznamky"]).strip(" |")
                    m["Zdroj"] = "Pipedrive + AT Deals"
                    break
        else:
            # Nový záznam
            stav = "Neaktivní" if email_norm in bounced else ("Aktivní" if email else "Bez emailu")
            
            # Zkus najít telefon z Pipedrive podle firmy
            phone = ""
            if company_norm and company_norm in pipedrive_by_company:
                for prec in pipedrive_by_company[company_norm]:
                    if prec.get("phone"):
                        phone = prec["phone"]
                        break
            
            merged.append({
                "Kontakt": rec.get("kontakt", ""),
                "Email": email,
                "Telefon": phone,
                "Firma": rec.get("firma", ""),
                "Co poptávali": rec.get("typ", ""),
                "Komu nabídnuto": rec.get("prirazeno", ""),
                "Reakce / výsledek": rec.get("status", ""),
                "Hodnota": "",
                "Stav emailu": stav,
                "Poznámky": rec.get("poznamky", ""),
                "Zdroj": "AT Deals"
            })
            if email_norm:
                seen_emails.add(email_norm)
            if company_norm:
                seen_companies.add(company_norm)
    
    # C) Doplň z Filip akce (detaily k firmám)
    for rec in filip_akce:
        company = rec.get("firma", "").strip()
        company_norm = normalize_company(company)
        
        if not company:
            continue
        
        # Zkus najít v merged podle firmy
        found = False
        for m in merged:
            if normalize_company(m["Firma"]) == company_norm:
                # Aktualizuj detaily
                if rec.get("typ") and not m["Co poptávali"]:
                    m["Co poptávali"] = rec["typ"]
                if rec.get("datum"):
                    m["Poznámky"] = f"{rec['datum']} {rec.get('misto', '')} | {m['Poznámky']}".strip(" |")
                if rec.get("cena") and not m["Hodnota"]:
                    m["Hodnota"] = rec["cena"]
                if rec.get("vysledek"):
                    m["Reakce / výsledek"] = rec["vysledek"] if not m["Reakce / výsledek"] else m["Reakce / výsledek"]
                m["Zdroj"] = m["Zdroj"] + " + Filip"
                found = True
                break
        
        # Pokud firma není v merged, přidej jako nový záznam
        if not found and company_norm not in seen_companies:
            merged.append({
                "Kontakt": "",
                "Email": "",
                "Telefon": "",
                "Firma": company,
                "Co poptávali": rec.get("typ", "") or rec.get("kategorie", ""),
                "Komu nabídnuto": "Filip",
                "Reakce / výsledek": rec.get("vysledek", "") or rec.get("status_poptavky", ""),
                "Hodnota": rec.get("cena", ""),
                "Stav emailu": "",
                "Poznámky": f"{rec.get('datum', '')} {rec.get('misto', '')} | {rec.get('poznamky', '')}".strip(" |"),
                "Zdroj": "Filip akce"
            })
            seen_companies.add(company_norm)
    
    # Filtruj - necháme jen záznamy s nějakým smysluplným obsahem
    final = []
    for m in merged:
        has_identity = m["Kontakt"] or m["Firma"]
        has_content = m["Email"] or m["Co poptávali"] or m["Reakce / výsledek"] or m["Poznámky"] or m["Hodnota"]
        # Pokud má firmu a nějaký kontext, je to validní záznam
        if has_identity and has_content:
            final.append(m)
        elif m["Firma"] and (m["Komu nabídnuto"] or m["Zdroj"]):
            # Záznamy bez kontaktu ale s firmou (např. T-Mobile deal bez jména)
            final.append(m)
    
    print(f"   Celkem po merge: {len(merged)}")
    print(f"   Po filtraci (smysluplné): {len(final)}")
    
    # Statistiky
    aktivni = sum(1 for r in final if r["Stav emailu"] == "Aktivní")
    neaktivni = sum(1 for r in final if r["Stav emailu"] == "Neaktivní")
    s_emailem = sum(1 for r in final if r["Email"])
    s_telefonem = sum(1 for r in final if r["Telefon"])
    
    print(f"\n📊 Statistiky:")
    print(f"   S emailem: {s_emailem}")
    print(f"   S telefonem: {s_telefonem}")
    print(f"   Aktivní: {aktivni}")
    print(f"   Neaktivní (bounced): {neaktivni}")
    
    # Uložení
    fieldnames = ["Kontakt", "Email", "Telefon", "Firma", "Co poptávali", "Komu nabídnuto", 
                  "Reakce / výsledek", "Hodnota", "Stav emailu", "Poznámky", "Zdroj"]
    
    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final)
    
    print(f"\n✅ Uloženo do {OUTPUT.name}")
    
    # Vypíš neaktivní
    neaktivni_list = [r for r in final if r["Stav emailu"] == "Neaktivní"]
    if neaktivni_list:
        print(f"\n❌ Neaktivní kontakty ({len(neaktivni_list)}):")
        for r in neaktivni_list[:15]:
            print(f"   {r['Email']} - {r['Firma']}")
        if len(neaktivni_list) > 15:
            print(f"   ... a dalších {len(neaktivni_list) - 15}")


if __name__ == "__main__":
    main()
