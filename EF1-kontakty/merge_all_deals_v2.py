#!/usr/bin/env python3
"""
Komplexní merge všech deal/poptávka dat - VERZE 2 (opravené parsování)

Zdroje:
1. AT - Deals (základ - ruční evidence)
2. Pipedrive export (CRM data)
3. Filip akce - poptávky (kalendář akcí)
4. Final mejling (bounced = neaktivní)

Výstup: čisté CSV s jasnou strukturou
"""

import csv
import re
from pathlib import Path
from collections import defaultdict
from io import StringIO

BASE_DIR = Path(__file__).parent

# Soubory
AT_DEALS = BASE_DIR / "AT - Deals - List 1.csv"
PIPEDRIVE = BASE_DIR / "deals-16044442-64.csv"
FILIP_AKCE = BASE_DIR / "Filip akce - poptávky - List 1.csv"
MEJLING = BASE_DIR / "Final mejling 2025 (AIP a AImpact) - List 1.csv"
OUTPUT = BASE_DIR / "deals_complete.csv"


def normalize(s):
    return (s or "").strip().lower()


def normalize_email(s):
    return normalize(s)


def normalize_company(s):
    s = normalize(s)
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',']:
        s = s.replace(suffix, '')
    return s.strip()


def clean_name(s):
    """Vyčistí název - odstraní datumy, ceny, statusy."""
    if not s:
        return ""
    # Odstraň datum na začátku (např. "26.8. (9 - 12:30)")
    s = re.sub(r'^\d{1,2}\.\d{1,2}\.?\s*(\(\d.*?\))?\s*', '', s)
    # Odstraň cenu
    s = re.sub(r'\d+[\s,]*\d*\s*(Kč|EUR|USD|000)', '', s, flags=re.IGNORECASE)
    # Odstraň statusy
    for status in ['OK - DEAL', 'DEAL', 'OK', '| ', ' |']:
        s = s.replace(status, '')
    return s.strip()


def extract_price(s):
    """Extrahuje cenu z textu."""
    if not s:
        return ""
    # Hledej vzory jako "85 000 Kč", "2500 EUR", "350 000 Kč"
    match = re.search(r'(\d[\d\s,]*)\s*(Kč|EUR|USD)', s, re.IGNORECASE)
    if match:
        return f"{match.group(1).strip()} {match.group(2)}"
    return ""


def get_best_email(emails):
    for e in emails:
        if e and '@' in e:
            if not any(x in e.lower() for x in ['gmail', 'seznam', 'email.cz', 'centrum.cz']):
                return e.strip()
    for e in emails:
        if e and '@' in e:
            return e.strip()
    return ""


def get_best_phone(phones):
    for p in phones:
        if p and len(re.sub(r'\D', '', p)) >= 9:
            return p.strip()
    return ""


def parse_bounced_emails():
    """Načte bounced emaily."""
    bounced = set()
    with open(MEJLING, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) >= 4:
                email = normalize_email(row[0])
                status = row[3].strip() if len(row) > 3 else ""
                if status == "EMAIL_BOUNCED" and email:
                    bounced.add(email)
    return bounced


def parse_pipedrive():
    """Načte Pipedrive - vrací dict by email."""
    by_email = {}
    by_company = defaultdict(list)
    
    with open(PIPEDRIVE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            deal_name = row.get("Deal - Název", "").strip()
            company = row.get("Deal - Organizace", "").strip()
            contact = row.get("Deal - Kontaktní osoba", "").strip()
            value = row.get("Deal - Hodnota", "").strip()
            
            emails = [
                row.get("Osoba - E-mail - Práce", ""),
                row.get("Osoba - E-mail - Domov", ""),
                row.get("Osoba - E-mail - Ostatní", "")
            ]
            email = get_best_email(emails)
            
            phones = [
                row.get("Osoba - Telefon - Práce", ""),
                row.get("Osoba - Telefon - Mobil", ""),
                row.get("Osoba - Telefon - Domov", ""),
                row.get("Osoba - Telefon - Ostatní", "")
            ]
            phone = get_best_phone(phones)
            
            record = {
                "nazev": clean_name(deal_name),
                "firma": company,
                "kontakt": contact,
                "email": email,
                "telefon": phone,
                "hodnota": value,
                "zdroj": "Pipedrive"
            }
            
            if email:
                by_email[normalize_email(email)] = record
            if company:
                by_company[normalize_company(company)].append(record)
    
    return by_email, by_company


def parse_at_deals():
    """Načte AT - Deals pomocí csv modulu (správné parsování uvozovek)."""
    records = []
    
    with open(AT_DEALS, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            
            kontakt = row[0].strip() if len(row) > 0 else ""
            email = row[1].strip() if len(row) > 1 else ""
            firma = row[2].strip() if len(row) > 2 else ""
            typ = row[3].strip() if len(row) > 3 else ""
            prirazeno = row[4].strip() if len(row) > 4 else ""
            status = row[5].strip() if len(row) > 5 else ""
            poznamky = row[6].strip() if len(row) > 6 else ""
            
            # Přeskoč řádky bez dat
            if not kontakt and not firma and not email:
                continue
            
            records.append({
                "kontakt": kontakt,
                "email": email,
                "firma": firma,
                "typ": typ,
                "prirazeno": prirazeno,
                "status": status,
                "poznamky": poznamky,
                "zdroj": "AT Deals"
            })
    
    return records


def parse_filip_akce():
    """Načte Filip akce pomocí csv modulu (správné parsování víceřádkových polí)."""
    records = []
    
    with open(FILIP_AKCE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            
            firma = row[0].strip() if len(row) > 0 else ""
            if not firma:
                continue
            
            datum = row[1].strip() if len(row) > 1 else ""
            misto = row[2].strip() if len(row) > 2 else ""
            typ = row[3].strip() if len(row) > 3 else ""
            status_poptavky = row[4].strip() if len(row) > 4 else ""
            kategorie = row[5].strip() if len(row) > 5 else ""
            ucastnici = row[6].strip() if len(row) > 6 else ""
            popis = row[7].strip() if len(row) > 7 else ""
            cena = row[8].strip() if len(row) > 8 else ""
            potvrzeni = row[9].strip() if len(row) > 9 else ""
            interni_pozn = row[10].strip() if len(row) > 10 else ""
            vysledek = row[11].strip() if len(row) > 11 else ""
            
            records.append({
                "firma": firma,
                "datum": datum,
                "misto": misto,
                "typ": typ,  # workshop, přednáška, etc.
                "status_poptavky": status_poptavky,
                "kategorie": kategorie,  # Interní přednáška/workshop, Veřejná konference, etc.
                "ucastnici": ucastnici,
                "popis": popis,
                "cena": cena,
                "potvrzeni": potvrzeni,
                "interni_pozn": interni_pozn,
                "vysledek": vysledek,
                "zdroj": "Filip akce"
            })
    
    return records


def map_typ_to_category(typ, kategorie=""):
    """Mapuje typ na standardní kategorie."""
    typ_lower = (typ or "").lower()
    kat_lower = (kategorie or "").lower()
    
    if "workshop" in typ_lower:
        return "Workshop"
    if "školení" in typ_lower or "training" in typ_lower:
        return "Školení"
    if "přednáška" in typ_lower or "keynote" in typ_lower or "speech" in typ_lower:
        return "Přednáška / keynote"
    if "konzultace" in typ_lower or "mentoring" in typ_lower:
        return "Konzultace"
    if "masterclass" in typ_lower or "hackathon" in typ_lower or "program" in typ_lower:
        return "Jiné (interní program apod.)"
    if "aca" in typ_lower:
        return "Jiné (interní program apod.)"
    
    # Zkus z kategorie
    if "workshop" in kat_lower:
        return "Workshop"
    if "konference" in kat_lower or "přednáška" in kat_lower:
        return "Přednáška / keynote"
    
    return ""


def map_vysledek(vysledek, status=""):
    """Mapuje výsledek na standardní hodnoty."""
    v = (vysledek or status or "").lower()
    
    if "potvrzeno" in v or "deal" in v:
        return "Deal"
    if "odmítnuto" in v or "zrušeno" in v:
        return "Odmítnuto"
    if "nereagují" in v or "bez reakce" in v:
        return "Bez reakce"
    if "poptávka" in v or "nabídka" in v:
        return "V jednání"
    
    return vysledek or status or ""


def main():
    print("📋 Načítám data...")
    
    # 1. Bounced emaily
    bounced = parse_bounced_emails()
    print(f"   Bounced emailů: {len(bounced)}")
    
    # 2. Pipedrive
    pipedrive_by_email, pipedrive_by_company = parse_pipedrive()
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
    
    # A) Pipedrive - základní kontaktní data
    for email_norm, rec in pipedrive_by_email.items():
        company_norm = normalize_company(rec["firma"])
        stav = "Neaktivní" if email_norm in bounced else "Aktivní"
        
        merged.append({
            "Název": rec["nazev"],
            "Kontakt": rec["kontakt"],
            "Email": rec["email"],
            "Telefon": rec["telefon"],
            "Firma": rec["firma"],
            "Co poptávali": "",
            "Komu nabídnuto": "",
            "Reakce / výsledek": "",
            "Cena": rec["hodnota"],
            "Datum": "",
            "Místo": "",
            "Stav emailu": stav,
            "Poznámky": "",
            "Zdroj": "Pipedrive"
        })
        seen_emails.add(email_norm)
        if company_norm:
            seen_companies.add(company_norm)
    
    # B) AT Deals - doplnění typu, přiřazení, statusu
    for rec in at_deals:
        email = rec.get("email", "").strip()
        email_norm = normalize_email(email)
        company_norm = normalize_company(rec.get("firma", ""))
        
        # Mapuj typ na kategorii
        co_poptavali = map_typ_to_category(rec.get("typ", ""))
        reakce = map_vysledek(rec.get("status", ""))
        
        if email_norm and email_norm in seen_emails:
            # Aktualizuj existující
            for m in merged:
                if normalize_email(m["Email"]) == email_norm:
                    if co_poptavali and not m["Co poptávali"]:
                        m["Co poptávali"] = co_poptavali
                    if rec.get("prirazeno") and not m["Komu nabídnuto"]:
                        m["Komu nabídnuto"] = rec["prirazeno"]
                    if reakce and not m["Reakce / výsledek"]:
                        m["Reakce / výsledek"] = reakce
                    if rec.get("poznamky"):
                        m["Poznámky"] = rec["poznamky"]
                    m["Zdroj"] = "Pipedrive + AT Deals"
                    break
        else:
            # Nový záznam
            stav = "Neaktivní" if email_norm in bounced else ("Aktivní" if email else "")
            
            # Vytvoř čistý název
            nazev = rec.get("firma", "") or rec.get("kontakt", "")
            
            merged.append({
                "Název": nazev,
                "Kontakt": rec.get("kontakt", ""),
                "Email": email,
                "Telefon": "",
                "Firma": rec.get("firma", ""),
                "Co poptávali": co_poptavali,
                "Komu nabídnuto": rec.get("prirazeno", ""),
                "Reakce / výsledek": reakce,
                "Cena": "",
                "Datum": "",
                "Místo": "",
                "Stav emailu": stav,
                "Poznámky": rec.get("poznamky", ""),
                "Zdroj": "AT Deals"
            })
            if email_norm:
                seen_emails.add(email_norm)
            if company_norm:
                seen_companies.add(company_norm)
    
    # C) Filip akce - detaily k firmám nebo nové záznamy
    for rec in filip_akce:
        company = rec.get("firma", "").strip()
        company_norm = normalize_company(company)
        
        if not company:
            continue
        
        co_poptavali = map_typ_to_category(rec.get("typ", ""), rec.get("kategorie", ""))
        reakce = map_vysledek(rec.get("vysledek", ""), rec.get("status_poptavky", ""))
        cena = rec.get("cena", "")
        
        # Sestavení poznámek
        poznamky_parts = []
        if rec.get("popis"):
            poznamky_parts.append(rec["popis"])
        if rec.get("ucastnici"):
            poznamky_parts.append(f"Účastníků: {rec['ucastnici']}")
        if rec.get("interni_pozn"):
            poznamky_parts.append(rec["interni_pozn"])
        poznamky = " | ".join(poznamky_parts)
        
        # Najdi v merged podle firmy
        found = False
        for m in merged:
            if normalize_company(m["Firma"]) == company_norm:
                # Aktualizuj
                if co_poptavali and not m["Co poptávali"]:
                    m["Co poptávali"] = co_poptavali
                if not m["Komu nabídnuto"]:
                    m["Komu nabídnuto"] = "Filip"
                if reakce and not m["Reakce / výsledek"]:
                    m["Reakce / výsledek"] = reakce
                if cena and not m["Cena"]:
                    m["Cena"] = cena
                if rec.get("datum") and not m["Datum"]:
                    m["Datum"] = rec["datum"]
                if rec.get("misto") and not m["Místo"]:
                    m["Místo"] = rec["misto"]
                if poznamky:
                    if m["Poznámky"]:
                        m["Poznámky"] += " | " + poznamky
                    else:
                        m["Poznámky"] = poznamky
                m["Zdroj"] = m["Zdroj"] + " + Filip" if "Filip" not in m["Zdroj"] else m["Zdroj"]
                found = True
                break
        
        # Nový záznam z Filip akce
        if not found and company_norm not in seen_companies:
            merged.append({
                "Název": company,
                "Kontakt": "",
                "Email": "",
                "Telefon": "",
                "Firma": company,
                "Co poptávali": co_poptavali,
                "Komu nabídnuto": "Filip",
                "Reakce / výsledek": reakce,
                "Cena": cena,
                "Datum": rec.get("datum", ""),
                "Místo": rec.get("misto", ""),
                "Stav emailu": "",
                "Poznámky": poznamky,
                "Zdroj": "Filip akce"
            })
            seen_companies.add(company_norm)
    
    # Filtruj - jen záznamy se smysluplným obsahem
    final = []
    for m in merged:
        has_identity = m["Kontakt"] or m["Firma"] or m["Email"]
        has_content = m["Co poptávali"] or m["Reakce / výsledek"] or m["Poznámky"] or m["Cena"]
        if has_identity and (has_content or m["Zdroj"]):
            final.append(m)
    
    print(f"   Celkem po merge: {len(merged)}")
    print(f"   Po filtraci: {len(final)}")
    
    # Statistiky
    aktivni = sum(1 for r in final if r["Stav emailu"] == "Aktivní")
    neaktivni = sum(1 for r in final if r["Stav emailu"] == "Neaktivní")
    s_emailem = sum(1 for r in final if r["Email"])
    
    print(f"\n📊 Statistiky:")
    print(f"   S emailem: {s_emailem}")
    print(f"   Aktivní: {aktivni}")
    print(f"   Neaktivní (bounced): {neaktivni}")
    
    # Uložení
    fieldnames = ["Název", "Kontakt", "Email", "Telefon", "Firma", "Co poptávali", 
                  "Komu nabídnuto", "Reakce / výsledek", "Cena", "Datum", "Místo",
                  "Stav emailu", "Poznámky", "Zdroj"]
    
    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final)
    
    print(f"\n✅ Uloženo do {OUTPUT.name}")
    
    # Ukázka prvních záznamů z Filip akce
    print("\n📋 Ukázka dat z Filip akce:")
    filip_records = [r for r in final if "Filip" in r["Zdroj"]][:5]
    for r in filip_records:
        print(f"   {r['Název'][:40]:<40} | {r['Co poptávali']:<20} | {r['Reakce / výsledek']}")


if __name__ == "__main__":
    main()
