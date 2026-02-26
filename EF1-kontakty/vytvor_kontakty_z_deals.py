#!/usr/bin/env python3
"""
Vytvoří kontakty z deals - pokud ještě neexistují.
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


def split_name(full_name: str) -> tuple:
    """Rozdělí celé jméno na jméno a příjmení."""
    if not full_name:
        return "", ""
    
    # Odstraň tituly
    full_name = re.sub(r'^(Ing\.|Mgr\.|Bc\.|PhDr\.|MUDr\.|JUDr\.|RNDr\.|Doc\.|Prof\.|Dr\.)\s*', '', full_name, flags=re.IGNORECASE)
    full_name = re.sub(r',?\s*(Ph\.?D\.?|MBA|MSc\.?|CSc\.?)$', '', full_name, flags=re.IGNORECASE)
    
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    
    # Pokud je příjmení VELKÝMI PÍSMENY
    if parts[-1].isupper() and len(parts[-1]) > 2:
        return " ".join(parts[:-1]), parts[-1].title()
    
    # Standardně: první = jméno, zbytek = příjmení
    return parts[0], " ".join(parts[1:])


def vocative_czech(name: str) -> str:
    """Vrátí oslovení (5. pád) pro české jméno."""
    if not name:
        return ""
    
    name = name.strip()
    name_lower = name.lower()
    
    # Speciální případy - mužská jména
    special_male = {
        "jan": "Jane", "pavel": "Pavle", "karel": "Karle", "josef": "Josefe",
        "petr": "Petře", "tomáš": "Tomáši", "martin": "Martine", "jakub": "Jakube",
        "ondřej": "Ondřeji", "david": "Davide", "adam": "Adame", "michal": "Michale",
        "lukáš": "Lukáši", "filip": "Filipe", "marek": "Marku", "jiří": "Jiří",
        "vojtěch": "Vojtěchu", "matěj": "Matěji", "daniel": "Danieli", "radek": "Radku",
        "milan": "Milane", "jaroslav": "Jaroslave", "zdeněk": "Zdeňku", "václav": "Václave",
        "vladimír": "Vladimíre", "stanislav": "Stanislave", "roman": "Romane",
        "aleš": "Aleši", "libor": "Libore", "oldřich": "Oldřichu", "miroslav": "Miroslave",
        "ladislav": "Ladislave", "patrik": "Patriku", "richard": "Richarde",
        "robert": "Roberte", "viktor": "Viktore", "štěpán": "Štěpáne",
        "dominik": "Dominiku", "matyáš": "Matyáši", "šimon": "Šimone",
        "antonín": "Antoníne", "františek": "Františku", "bohumil": "Bohumile",
        "igor": "Igore", "boris": "Borisi", "denis": "Denisi",
    }
    
    if name_lower in special_male:
        return special_male[name_lower]
    
    # Speciální případy - ženská jména
    special_female = {
        "jana": "Jano", "marie": "Marie", "eva": "Evo", "anna": "Anno",
        "hana": "Hano", "lenka": "Lenko", "kateřina": "Kateřino", "lucie": "Lucie",
        "petra": "Petro", "martina": "Martino", "tereza": "Terezo", "michaela": "Michaelo",
        "veronika": "Veroniko", "barbora": "Barbaro", "markéta": "Markéto",
        "alena": "Aleno", "helena": "Heleno", "ivana": "Ivano", "monika": "Moniko",
        "zuzana": "Zuzano", "jitka": "Jitko", "věra": "Věro", "daniela": "Danielo",
        "simona": "Simono", "renata": "Renato", "nicole": "Nicole", "natalie": "Natálie",
        "kristýna": "Kristýno", "adéla": "Adélo", "nikola": "Nikolo",
        "karolína": "Karolíno", "eliška": "Eliško", "vendula": "Vendulo",
        "klára": "Kláro", "šárka": "Šárko", "diana": "Diano",
    }
    
    if name_lower in special_female:
        return special_female[name_lower]
    
    # Obecná pravidla
    # Ženská jména končící na -a
    if name_lower.endswith('a'):
        if name_lower.endswith('ka'):
            return name[:-1] + 'o'
        elif name_lower.endswith('na') or name_lower.endswith('la') or name_lower.endswith('ra'):
            return name[:-1] + 'o'
        elif name_lower.endswith('ia') or name_lower.endswith('ie'):
            return name  # Marie, Lucie - nemění se
        else:
            return name[:-1] + 'o'
    
    # Mužská jména končící na souhlásku
    if name_lower.endswith(('k', 'g', 'h', 'ch')):
        return name + 'u'
    elif name_lower.endswith(('c', 'č', 'š', 'ž', 'ř', 'j')):
        return name + 'i'
    elif name_lower.endswith(('s', 'x', 'z')):
        return name + 'i'
    elif name_lower.endswith(('b', 'd', 'f', 'l', 'm', 'n', 'p', 'r', 't', 'v', 'w')):
        return name + 'e'
    
    return name


def main():
    token = get_token()
    hdrs = headers(token)
    
    # 1. Načti existující kontakty (podle emailu)
    print("🔎 Načítám existující kontakty...")
    kontakty_url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    
    existing_emails = set()
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            email = rec.get("fields", {}).get("E-mail", "")
            if email:
                existing_emails.add(email.lower().strip())
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Existujících kontaktů s emailem: {len(existing_emails)}")
    
    # 2. Načti deals s kontakty
    print("\n🔎 Načítám kontakty z Deals...")
    deals_url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    new_contacts = []
    duplicates = 0
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", deals_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            full_name = fields.get("Jméno a příjmení", "").strip()
            email = fields.get("Email", "").strip()
            firma = fields.get("Firma", "").strip()
            
            if not email or not full_name:
                continue
            
            # Zkontroluj duplicitu
            if email.lower() in existing_emails:
                duplicates += 1
                continue
            
            # Přidej do seznamu nových
            existing_emails.add(email.lower())  # Aby se nepřidávaly duplicity v rámci deals
            
            jmeno, prijmeni = split_name(full_name)
            osloveni = vocative_czech(jmeno)
            
            new_contacts.append({
                "fields": {
                    "Jméno": jmeno,
                    "Příjmení": prijmeni,
                    "Oslovení": osloveni,
                    "E-mail": email,
                    "Společnost / Firma": firma,
                    "Program / Deal / Poptávka": ["Poptávka"]  # Označí jako poptávku
                }
            })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Nových kontaktů k vytvoření: {len(new_contacts)}")
    print(f"   Přeskočeno (už existují): {duplicates}")
    
    if not new_contacts:
        print("\n✅ Žádné nové kontakty k vytvoření!")
        return
    
    # Ukázka
    print("\n📋 Ukázka nových kontaktů:")
    for c in new_contacts[:10]:
        f = c["fields"]
        print(f"   {f['Jméno']} {f['Příjmení']} ({f['Oslovení']}) - {f['E-mail']} - {f['Společnost / Firma']}")
    if len(new_contacts) > 10:
        print(f"   ... a dalších {len(new_contacts) - 10}")
    
    # 3. Vytvoř nové kontakty
    print(f"\n⬆️ Vytvářím {len(new_contacts)} nových kontaktů...")
    
    created = 0
    for batch in chunked(new_contacts, BATCH_SIZE):
        request_with_backoff("POST", kontakty_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        created += len(batch)
        print(f"   Vytvořeno: {created}/{len(new_contacts)}", end="\r")
        time.sleep(0.2)
    
    print(f"\n\n✅ Vytvořeno {created} nových kontaktů!")


if __name__ == "__main__":
    main()
