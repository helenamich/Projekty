#!/usr/bin/env python3
"""
Obohacení Airtable dat:
1. Doplní Deals o data z Filip akce a Pipedrive
2. Vytvoří/aktualizuje Kontakty (jméno, příjmení, oslovení, telefon)
3. Vytvoří/propojí Klienty (firmy)
"""

import csv
import json
import time
import re
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import quote

import requests

BASE_DIR = Path(__file__).parent
FILIP_AKCE = BASE_DIR / "Filip akce - poptávky - List 1.csv"
PIPEDRIVE = BASE_DIR / "deals-16044442-64.csv"

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


def normalize(s):
    return (s or "").strip().lower()


def normalize_company(s):
    s = normalize(s)
    for suffix in [' s.r.o.', ' a.s.', ' s.r.o', ' a.s', ' spol.', ' k.s.', 
                   ' gmbh', ' ltd', ' inc', ' n.v.', ' ag', ' se', ',']:
        s = s.replace(suffix, '')
    return s.strip()


def split_name(full_name: str) -> tuple:
    """Rozdělí celé jméno na jméno a příjmení."""
    if not full_name:
        return "", ""
    
    full_name = full_name.strip()
    
    # Odstraň prefixy jako "CCL-", "ing.", "Mgr." atd.
    full_name = re.sub(r'^[A-Z]{2,}-', '', full_name)
    full_name = re.sub(r'^(Ing\.|Mgr\.|Bc\.|PhDr\.|MUDr\.|JUDr\.|RNDr\.|Doc\.|Prof\.)\s*', '', full_name, flags=re.IGNORECASE)
    
    parts = full_name.split()
    if len(parts) == 0:
        return "", ""
    elif len(parts) == 1:
        return parts[0], ""
    else:
        # Pokud je první část velkými písmeny (PŘÍJMENÍ), tak otočíme
        if parts[0].isupper() and not parts[1].isupper():
            return parts[1], parts[0].title()
        # Standardně: Jméno Příjmení
        return parts[0], " ".join(parts[1:])


def vocative_czech(name: str) -> str:
    """Převede křestní jméno do 5. pádu."""
    if not name:
        return ""
    
    name = name.strip()
    lower = name.lower()
    
    # Speciální případy
    special = {
        "jan": "Jane", "pavel": "Pavle", "petr": "Petře", "jiří": "Jiří",
        "ondřej": "Ondřeji", "tomáš": "Tomáši", "lukáš": "Lukáši",
        "matěj": "Matěji", "david": "Davide", "jakub": "Jakube",
        "adam": "Adame", "martin": "Martine", "marek": "Marku",
        "filip": "Filipe", "michal": "Michale", "milan": "Milane",
        "josef": "Josefe", "jaroslav": "Jaroslave", "zdeněk": "Zdeňku",
        "václav": "Václave", "karel": "Karle", "radek": "Radku",
        "roman": "Romane", "daniel": "Danieli", "vladimír": "Vladimíre",
        "stanislav": "Stanislave", "miroslav": "Miroslave", "robert": "Roberte",
        "aleš": "Aleši", "miloš": "Miloši", "richard": "Richarde",
        "patrik": "Patriku", "dominik": "Dominiku", "vojtěch": "Vojtěchu",
        "štěpán": "Štěpáne", "viktor": "Viktore", "boris": "Borisi",
        "honza": "Honzo", "jirka": "Jirko", "kuba": "Kubo",
        "jana": "Jano", "marie": "Marie", "eva": "Evo", "anna": "Anno",
        "hana": "Hano", "lenka": "Lenko", "kateřina": "Kateřino",
        "lucie": "Lucie", "petra": "Petro", "martina": "Martino",
        "věra": "Věro", "alena": "Aleno", "ivana": "Ivano",
        "monika": "Moniko", "tereza": "Terezo", "michaela": "Michaelo",
        "barbora": "Barboro", "markéta": "Markéto", "jitka": "Jitko",
        "helena": "Heleno", "dagmar": "Dagmar", "renata": "Renato",
        "irena": "Ireno", "zuzana": "Zuzano", "blanka": "Blanko",
        "daniela": "Danielo", "andrea": "Andreo", "nicole": "Nicole",
        "kristýna": "Kristýno", "simona": "Simono", "veronika": "Veroniko",
        "klára": "Kláro", "šárka": "Šárko", "silvie": "Silvie",
        "natálie": "Natálie", "adéla": "Adélo", "vendula": "Vendulo",
        "radka": "Radko", "iveta": "Iveto", "olga": "Olgo",
        "soňa": "Soňo", "diana": "Diano", "lucia": "Luci",
        "gabriela": "Gabrielo", "denisa": "Deniso", "linda": "Lindo",
        "milena": "Mileno", "karolína": "Karolíno", "ester": "Ester",
        "magdalena": "Magdaléno", "magdaléna": "Magdaléno",
        "adriana": "Adriano", "edita": "Edito", "erich": "Erichu",
        "svetozar": "Svetozare", "brunclík": "Brunclíku",
    }
    
    if lower in special:
        return special[lower]
    
    # Obecná pravidla
    if lower.endswith('a'):
        return name[:-1] + 'o'
    if lower.endswith('ie') or lower.endswith('ije'):
        return name
    if lower.endswith('e'):
        return name
    if lower.endswith('ek'):
        return name[:-2] + 'ku'
    if lower.endswith('el'):
        return name + 'e'
    if lower.endswith('an') or lower.endswith('in') or lower.endswith('en'):
        return name + 'e'
    if lower.endswith('áš') or lower.endswith('eš') or lower.endswith('oš') or lower.endswith('iš'):
        return name + 'i'
    if lower.endswith('r') or lower.endswith('l') or lower.endswith('n') or lower.endswith('d') or lower.endswith('t'):
        return name + 'e'
    if lower.endswith('k'):
        return name + 'u'
    
    return name


def get_best_phone(phones):
    """Vybere nejlepší telefon."""
    for p in phones:
        if p and len(re.sub(r'\D', '', p)) >= 9:
            phone = p.strip()
            # Přidej + před 420 pokud chybí
            if phone.startswith('420'):
                phone = '+' + phone
            return phone
    return ""


def parse_filip_akce() -> Dict[str, dict]:
    """Načte Filip akce - vrací dict by normalized company name."""
    by_company = {}
    
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
            cena = row[8].strip() if len(row) > 8 else ""
            vysledek = row[11].strip() if len(row) > 11 else ""
            popis = row[7].strip() if len(row) > 7 else ""
            
            company_norm = normalize_company(firma)
            by_company[company_norm] = {
                "firma": firma,
                "datum": datum,
                "misto": misto,
                "typ": typ,
                "cena": cena,
                "vysledek": vysledek,
                "popis": popis
            }
    
    return by_company


def parse_pipedrive() -> Dict[str, dict]:
    """Načte Pipedrive - vrací dict by normalized company name."""
    by_company = {}
    by_email = {}
    
    with open(PIPEDRIVE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row.get("Deal - Organizace", "").strip()
            contact = row.get("Deal - Kontaktní osoba", "").strip()
            value = row.get("Deal - Hodnota", "").strip()
            deal_name = row.get("Deal - Název", "").strip()
            
            emails = [
                row.get("Osoba - E-mail - Práce", ""),
                row.get("Osoba - E-mail - Domov", ""),
                row.get("Osoba - E-mail - Ostatní", "")
            ]
            email = next((e.strip() for e in emails if e and '@' in e), "")
            
            phones = [
                row.get("Osoba - Telefon - Práce", ""),
                row.get("Osoba - Telefon - Mobil", ""),
                row.get("Osoba - Telefon - Domov", ""),
                row.get("Osoba - Telefon - Ostatní", "")
            ]
            phone = get_best_phone(phones)
            
            record = {
                "kontakt": contact,
                "email": email,
                "telefon": phone,
                "firma": company,
                "hodnota": value,
                "deal_name": deal_name
            }
            
            if company:
                company_norm = normalize_company(company)
                if company_norm not in by_company:
                    by_company[company_norm] = record
            
            if email:
                by_email[normalize(email)] = record
    
    return by_company, by_email


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti data z CSV
    print("📋 Načítám data z CSV...")
    filip_by_company = parse_filip_akce()
    print(f"   Filip akce: {len(filip_by_company)} firem")
    
    pipedrive_by_company, pipedrive_by_email = parse_pipedrive()
    print(f"   Pipedrive: {len(pipedrive_by_company)} firem, {len(pipedrive_by_email)} emailů")
    
    # 2. Načti existující Deals z Airtable
    print("\n🔎 Načítám Deals z Airtable...")
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
    
    # 3. Načti existující Kontakty
    print("\n🔎 Načítám Kontakty z Airtable...")
    kontakty_url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    
    existing_kontakty = {}  # email -> record_id
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        for rec in data.get("records", []):
            email = (rec.get("fields", {}).get("E-mail") or "").strip().lower()
            if email:
                existing_kontakty[email] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
    print(f"   {len(existing_kontakty)} kontaktů")
    
    # 4. Načti existující Klienty
    print("\n🔎 Načítám Klienty z Airtable...")
    klienti_url = f"{API_BASE}/{BASE_ID}/{quote('Klienti', safe='')}"
    
    existing_klienti = {}  # normalized_company -> record_id
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", klienti_url, hdrs=hdrs, params=params)
        for rec in data.get("records", []):
            firma = (rec.get("fields", {}).get("Firma") or "").strip()
            if firma:
                existing_klienti[normalize_company(firma)] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
    print(f"   {len(existing_klienti)} klientů")
    
    # 5. Zpracuj Deals - vytvoř/aktualizuj Kontakty a Klienty
    print("\n🔄 Zpracovávám Deals...")
    
    kontakty_to_create = []
    kontakty_to_update = []
    klienti_to_create = []
    new_klienti_names = set()
    
    for deal in deals:
        fields = deal.get("fields", {})
        full_name = fields.get("Jméno a příjmení", "").strip()
        email = (fields.get("Email") or "").strip().lower()
        firma = fields.get("Firma", "").strip()
        firma_norm = normalize_company(firma)
        
        # Doplň data z Pipedrive
        telefon = ""
        if email and email in pipedrive_by_email:
            pip = pipedrive_by_email[email]
            telefon = pip.get("telefon", "")
        elif firma_norm and firma_norm in pipedrive_by_company:
            pip = pipedrive_by_company[firma_norm]
            telefon = pip.get("telefon", "")
        
        # Zpracuj kontakt
        if full_name and email:
            jmeno, prijmeni = split_name(full_name)
            osloveni = vocative_czech(jmeno)
            
            if email in existing_kontakty:
                # Aktualizuj existující - doplň telefon pokud chybí
                if telefon:
                    kontakty_to_update.append({
                        "id": existing_kontakty[email],
                        "fields": {"Telefon": telefon}
                    })
            else:
                # Vytvoř nový kontakt
                kontakty_to_create.append({
                    "fields": {
                        "Jméno": jmeno,
                        "Příjmení": prijmeni,
                        "Oslovení": osloveni,
                        "E-mail": email,
                        "Telefon": telefon,
                        "Společnost / Firma": firma,
                        "Stav": "Aktivní"
                    }
                })
                existing_kontakty[email] = "pending"  # Mark as pending
        
        # Zpracuj klienta (firmu)
        if firma and firma_norm not in existing_klienti and firma_norm not in new_klienti_names:
            klienti_to_create.append({"fields": {"Firma": firma}})
            new_klienti_names.add(firma_norm)
    
    # 6. Doplň data z Filip akce (firmy bez kontaktů v Deals)
    print("\n📋 Doplňuji data z Filip akce...")
    for company_norm, filip_data in filip_by_company.items():
        if company_norm not in existing_klienti and company_norm not in new_klienti_names:
            klienti_to_create.append({"fields": {"Firma": filip_data["firma"]}})
            new_klienti_names.add(company_norm)
    
    # 7. Vytvoř nové Kontakty
    if kontakty_to_create:
        print(f"\n➕ Vytvářím {len(kontakty_to_create)} nových kontaktů...")
        for batch in chunked(kontakty_to_create, BATCH_SIZE):
            result = request_with_backoff("POST", kontakty_url, hdrs=hdrs, 
                                         json_data={"records": batch, "typecast": True})
            for rec in result.get("records", []):
                email = (rec.get("fields", {}).get("E-mail") or "").strip().lower()
                if email:
                    existing_kontakty[email] = rec["id"]
            time.sleep(0.2)
        print(f"   ✅ Vytvořeno")
    
    # 8. Aktualizuj existující Kontakty (telefony)
    if kontakty_to_update:
        # Filtruj prázdné aktualizace
        kontakty_to_update = [r for r in kontakty_to_update if r.get("fields", {}).get("Telefon")]
        if kontakty_to_update:
            print(f"\n♻️ Aktualizuji {len(kontakty_to_update)} kontaktů (telefony)...")
            for batch in chunked(kontakty_to_update, BATCH_SIZE):
                request_with_backoff("PATCH", kontakty_url, hdrs=hdrs, 
                                    json_data={"records": batch, "typecast": True})
                time.sleep(0.2)
            print(f"   ✅ Aktualizováno")
    
    # 9. Vytvoř nové Klienty
    if klienti_to_create:
        print(f"\n➕ Vytvářím {len(klienti_to_create)} nových klientů...")
        for batch in chunked(klienti_to_create, BATCH_SIZE):
            result = request_with_backoff("POST", klienti_url, hdrs=hdrs, 
                                         json_data={"records": batch, "typecast": True})
            for rec in result.get("records", []):
                firma = (rec.get("fields", {}).get("Firma") or "").strip()
                if firma:
                    existing_klienti[normalize_company(firma)] = rec["id"]
            time.sleep(0.2)
        print(f"   ✅ Vytvořeno")
    
    # 10. Propoj Kontakty s Klienty
    print("\n🔗 Propojuji Kontakty s Klienty...")
    
    # Znovu načti kontakty pro propojení
    kontakty_to_link = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            firma = (fields.get("Společnost / Firma") or "").strip()
            firma_norm = normalize_company(firma)
            current_klienti = fields.get("Klienti", [])
            
            if firma_norm and firma_norm in existing_klienti and not current_klienti:
                klient_id = existing_klienti[firma_norm]
                kontakty_to_link.append({
                    "id": rec["id"],
                    "fields": {"Klienti": [klient_id]}
                })
        offset = data.get("offset")
        if not offset:
            break
    
    if kontakty_to_link:
        print(f"   Propojuji {len(kontakty_to_link)} kontaktů...")
        for batch in chunked(kontakty_to_link, BATCH_SIZE):
            request_with_backoff("PATCH", kontakty_url, hdrs=hdrs, 
                                json_data={"records": batch, "typecast": True})
            time.sleep(0.2)
        print(f"   ✅ Propojeno")
    
    print("\n✅ Hotovo!")
    print(f"\n📊 Souhrn:")
    print(f"   Nových kontaktů: {len(kontakty_to_create)}")
    print(f"   Aktualizovaných kontaktů: {len(kontakty_to_update)}")
    print(f"   Nových klientů: {len(klienti_to_create)}")
    print(f"   Propojených kontaktů: {len(kontakty_to_link)}")


if __name__ == "__main__":
    main()
