#!/usr/bin/env python3
"""
Doplní oslovení (5. pád křestního jména) do Airtable kontaktů.
"""

import json
import time
import re
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import requests

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


def vocative_czech(name: str) -> str:
    """
    Převede české křestní jméno do 5. pádu (vokativ).
    """
    if not name:
        return ""
    
    name = name.strip()
    if not name:
        return ""
    
    # Převod na lowercase pro analýzu, zachováme original pro výstup
    lower = name.lower()
    
    # Speciální případy - nepravidelná jména
    special = {
        # Mužská
        "jan": "Jane",
        "pavel": "Pavle",
        "petr": "Petře",
        "jiří": "Jiří",
        "jiri": "Jiří",
        "ondřej": "Ondřeji",
        "ondrej": "Ondřeji",
        "tomáš": "Tomáši",
        "tomas": "Tomáši",
        "lukáš": "Lukáši",
        "lukas": "Lukáši",
        "matěj": "Matěji",
        "matej": "Matěji",
        "david": "Davide",
        "jakub": "Jakube",
        "adam": "Adame",
        "martin": "Martine",
        "marek": "Marku",
        "filip": "Filipe",
        "michal": "Michale",
        "milan": "Milane",
        "josef": "Josefe",
        "jaroslav": "Jaroslave",
        "zdeněk": "Zdeňku",
        "zdenek": "Zdeňku",
        "václav": "Václave",
        "vaclav": "Václave",
        "karel": "Karle",
        "radek": "Radku",
        "roman": "Romane",
        "daniel": "Danieli",
        "vladimír": "Vladimíre",
        "vladimir": "Vladimíre",
        "stanislav": "Stanislave",
        "miroslav": "Miroslave",
        "robert": "Roberte",
        "aleš": "Aleši",
        "ales": "Aleši",
        "miloš": "Miloši",
        "milos": "Miloši",
        "ladislav": "Ladislave",
        "bohumil": "Bohumile",
        "oldřich": "Oldřichu",
        "oldrich": "Oldřichu",
        "richard": "Richarde",
        "patrik": "Patriku",
        "dominik": "Dominiku",
        "vojtěch": "Vojtěchu",
        "vojtech": "Vojtěchu",
        "štěpán": "Štěpáne",
        "stepan": "Štěpáne",
        "viktor": "Viktore",
        "igor": "Igore",
        "boris": "Borisi",
        "denis": "Denisi",
        "honza": "Honzo",
        "míra": "Míro",
        "mira": "Míro",
        "jirka": "Jirko",
        "péťa": "Péťo",
        "peta": "Péťo",
        "kuba": "Kubo",
        "tonda": "Tondo",
        "franta": "Franto",
        "vašek": "Vašku",
        "vasek": "Vašku",
        "leoš": "Leoši",
        "leos": "Leoši",
        "otakar": "Otakare",
        "svatopluk": "Svatopluku",
        "bronislav": "Bronislave",
        "arkadiusz": "Arkadiuszi",
        "thomas": "Thomasi",
        "higor": "Higore",
        "darko": "Darko",
        "szymon": "Szymone",
        "bogdan": "Bogdane",
        
        # Ženská
        "jana": "Jano",
        "marie": "Marie",
        "eva": "Evo",
        "anna": "Anno",
        "hana": "Hano",
        "lenka": "Lenko",
        "kateřina": "Kateřino",
        "katerina": "Kateřino",
        "lucie": "Lucie",
        "petra": "Petro",
        "martina": "Martino",
        "věra": "Věro",
        "vera": "Věro",
        "alena": "Aleno",
        "ivana": "Ivano",
        "monika": "Moniko",
        "tereza": "Terezo",
        "michaela": "Michaelo",
        "barbora": "Báro",
        "markéta": "Markéto",
        "marketa": "Markéto",
        "jitka": "Jitko",
        "helena": "Heleno",
        "dagmar": "Dagmar",
        "renata": "Renato",
        "irena": "Ireno",
        "zuzana": "Zuzano",
        "blanka": "Blanko",
        "daniela": "Danielo",
        "andrea": "Andreo",
        "nicole": "Nicole",
        "kristýna": "Kristýno",
        "kristyna": "Kristýno",
        "simona": "Simono",
        "veronika": "Veroniko",
        "klára": "Kláro",
        "klara": "Kláro",
        "šárka": "Šárko",
        "sarka": "Šárko",
        "silvie": "Silvie",
        "natálie": "Natálie",
        "natalie": "Natálie",
        "adéla": "Adélo",
        "adela": "Adélo",
        "vendula": "Vendulo",
        "radka": "Radko",
        "iveta": "Iveto",
        "olga": "Olgo",
        "soňa": "Soňo",
        "sona": "Soňo",
        "diana": "Diano",
        "lucia": "Luci",
        "karla": "Karlo",
        "zlata": "Zlato",
        "magdaléna": "Magdaléno",
        "magdalena": "Magdaléno",
        "gabriela": "Gabrielo",
        "denisa": "Deniso",
        "terezie": "Terezie",
        "alice": "Alice",
        "linda": "Lindo",
        "milena": "Mileno",
        "daria": "Dario",
        "mariia": "Mariio",
        "tatiana": "Tatiano",
        "katarína": "Katko",
        "katarina": "Katko",
        "natália": "Natálio",
        "natalia": "Natálio",
        "vanda": "Vando",
        "greta": "Greto",
        "bára": "Báro",
        "bara": "Báro",
    }
    
    if lower in special:
        return special[lower]
    
    # Obecná pravidla pro ženská jména (končí na -a)
    if lower.endswith('a'):
        # -ka → -ko
        if lower.endswith('ka'):
            return name[:-1] + 'o'
        # -na → -no
        if lower.endswith('na'):
            return name[:-1] + 'o'
        # -la → -lo
        if lower.endswith('la'):
            return name[:-1] + 'o'
        # -ra → -ro
        if lower.endswith('ra'):
            return name[:-1] + 'o'
        # -da → -do
        if lower.endswith('da'):
            return name[:-1] + 'o'
        # -ta → -to
        if lower.endswith('ta'):
            return name[:-1] + 'o'
        # -sa/-za → -so/-zo
        if lower.endswith('sa') or lower.endswith('za'):
            return name[:-1] + 'o'
        # obecně -a → -o
        return name[:-1] + 'o'
    
    # Ženská jména končící na -ie/-ije zůstávají
    if lower.endswith('ie') or lower.endswith('ije'):
        return name
    
    # Ženská jména končící na -e
    if lower.endswith('e'):
        return name
    
    # Mužská jména končící na souhlásku
    # -ek → -ku
    if lower.endswith('ek'):
        return name[:-2] + 'ku'
    
    # -ec → -če
    if lower.endswith('ec'):
        return name[:-2] + 'če'
    
    # -el → -le (Karel → Karle, Daniel → Danieli)
    if lower.endswith('el'):
        return name + 'e'
    
    # -il → -ile
    if lower.endswith('il'):
        return name + 'e'
    
    # -an → -ane
    if lower.endswith('an'):
        return name + 'e'
    
    # -in → -ine
    if lower.endswith('in'):
        return name + 'e'
    
    # -en → -ene
    if lower.endswith('en'):
        return name + 'e'
    
    # -ín → -íne
    if lower.endswith('ín'):
        return name + 'e'
    
    # -ír → -íře
    if lower.endswith('ír') or lower.endswith('ir'):
        return name + 'e'
    
    # -áš/-aš → -áši/-aši
    if lower.endswith('áš') or lower.endswith('aš') or lower.endswith('as'):
        return name + 'i'
    
    # -eš → -eši
    if lower.endswith('eš') or lower.endswith('es'):
        return name + 'i'
    
    # -oš → -oši
    if lower.endswith('oš') or lower.endswith('os'):
        return name + 'i'
    
    # -iš/-is → -iši
    if lower.endswith('iš') or lower.endswith('is'):
        return name + 'i'
    
    # -ř → -ři
    if lower.endswith('ř'):
        return name + 'i'
    
    # -r → -re
    if lower.endswith('r'):
        return name + 'e'
    
    # -l → -le
    if lower.endswith('l'):
        return name + 'e'
    
    # -n → -ne
    if lower.endswith('n'):
        return name + 'e'
    
    # -d → -de
    if lower.endswith('d'):
        return name + 'e'
    
    # -t → -te
    if lower.endswith('t'):
        return name + 'e'
    
    # -k → -ku
    if lower.endswith('k'):
        return name + 'u'
    
    # -p → -pe
    if lower.endswith('p'):
        return name + 'e'
    
    # -b → -be
    if lower.endswith('b'):
        return name + 'e'
    
    # -f → -fe
    if lower.endswith('f'):
        return name + 'e'
    
    # -v → -ve
    if lower.endswith('v'):
        return name + 'e'
    
    # Ostatní - vrátíme původní
    return name


def main():
    token = get_token()
    
    # Načti kontakty bez oslovení
    print("🔎 Načítám kontakty z Airtable...")
    url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    hdrs = headers(token)
    
    to_update = []
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
            jmeno = (fields.get("Jméno") or "").strip()
            osloveni = (fields.get("Oslovení") or "").strip()
            
            # Pokud má jméno ale nemá oslovení, doplníme
            if jmeno and not osloveni:
                new_osloveni = vocative_czech(jmeno)
                if new_osloveni and new_osloveni != jmeno:
                    to_update.append({
                        "id": rec["id"],
                        "fields": {"Oslovení": new_osloveni}
                    })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   Celkem kontaktů: {total}")
    print(f"   K doplnění oslovení: {len(to_update)}")
    
    if not to_update:
        print("\n✅ Všechny kontakty už mají oslovení!")
        return
    
    # Ukázka
    print("\n📋 Ukázka (prvních 10):")
    for rec in to_update[:10]:
        print(f"   → {rec['fields']['Oslovení']}")
    
    # Aktualizace
    print(f"\n⬆️ Aktualizuji {len(to_update)} kontaktů...")
    
    updated = 0
    for batch in chunked(to_update, BATCH_SIZE):
        request_with_backoff("PATCH", url, hdrs=hdrs, json_data={"records": batch, "typecast": True})
        updated += len(batch)
        if updated % 100 == 0:
            print(f"   ... {updated}/{len(to_update)}")
        time.sleep(0.2)
    
    print(f"\n✅ Doplněno oslovení u {updated} kontaktů!")


if __name__ == "__main__":
    main()
