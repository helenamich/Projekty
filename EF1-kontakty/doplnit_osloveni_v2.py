#!/usr/bin/env python3
"""
Doplní oslovení pro kontakty, které ho nemají.
Řeší i případy kde je jméno ve formátu "Příjmení Jméno".
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


def extract_first_name(jmeno_field: str) -> str:
    """Extrahuje křestní jméno - řeší formáty jako 'Příjmení Jméno' nebo 'Jméno'."""
    if not jmeno_field:
        return ""
    
    # Odstraň čárky a tituly
    jmeno_field = re.sub(r',', ' ', jmeno_field)
    jmeno_field = re.sub(r'^(Ing\.|Mgr\.|Bc\.|PhDr\.|MUDr\.|JUDr\.|RNDr\.|Doc\.|Prof\.|Dr\.)\s*', '', jmeno_field, flags=re.IGNORECASE)
    
    parts = jmeno_field.strip().split()
    if len(parts) == 0:
        return ""
    if len(parts) == 1:
        return parts[0]
    
    # Pokud první slovo končí na -ová, -ský, -cká, -ek atd. -> je to příjmení
    first = parts[0]
    if re.search(r'(ová|ský|ská|cký|cká|ek|ec|ík|ič|ač|ář|eř|íř|ůř|ej)$', first, re.IGNORECASE):
        # První je příjmení, druhé je jméno
        return parts[1] if len(parts) > 1 else first
    
    # Jinak první je jméno
    return first


def vocative_czech(name: str) -> str:
    """Vrátí oslovení (5. pád) pro české jméno."""
    if not name:
        return ""
    
    name = name.strip().title()
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
        "igor": "Igore", "boris": "Borisi", "denis": "Denisi", "michael": "Michaeli",
        "radim": "Radime", "hendrich": "Hendrichu", "ognen": "Ognene",
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
        "simona": "Simono", "renata": "Renato", "nicole": "Nicole", "natálie": "Natálie",
        "kristýna": "Kristýno", "adéla": "Adélo", "nikola": "Nikolo",
        "karolína": "Karolíno", "eliška": "Eliško", "vendula": "Vendulo",
        "klára": "Kláro", "šárka": "Šárko", "diana": "Diano", "silvie": "Silvie",
        "olga": "Olgo", "vanda": "Vando", "miriam": "Miriam", "natálie": "Natálie",
    }
    
    if name_lower in special_female:
        return special_female[name_lower]
    
    # Obecná pravidla
    if name_lower.endswith('a'):
        if name_lower.endswith('ia') or name_lower.endswith('ie'):
            return name
        return name[:-1] + 'o'
    
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
    
    print("🔎 Načítám kontakty bez oslovení...")
    kontakty_url = f"{API_BASE}/{BASE_ID}/{quote('Kontakty', safe='')}"
    
    to_update = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", kontakty_url, hdrs=hdrs, params=params)
        
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            jmeno = fields.get("Jméno", "").strip()
            osloveni = fields.get("Oslovení", "").strip()
            
            if jmeno and not osloveni:
                # Extrahuj křestní jméno
                first_name = extract_first_name(jmeno)
                new_osloveni = vocative_czech(first_name)
                
                if new_osloveni:
                    to_update.append({
                        "id": rec["id"],
                        "fields": {"Oslovení": new_osloveni},
                        "_jmeno": jmeno,
                        "_osloveni": new_osloveni
                    })
        
        offset = data.get("offset")
        if not offset:
            break
    
    print(f"   K doplnění: {len(to_update)} kontaktů")
    
    if not to_update:
        print("\n✅ Všechny kontakty mají oslovení!")
        return
    
    # Ukázka
    print("\n📋 Ukázka:")
    for rec in to_update[:15]:
        print(f"   {rec['_jmeno']:<25} → {rec['_osloveni']}")
    
    # Odstraň pomocná pole
    for rec in to_update:
        del rec["_jmeno"]
        del rec["_osloveni"]
    
    # Aktualizuj
    print(f"\n⬆️ Doplňuji oslovení...")
    
    updated = 0
    for batch in chunked(to_update, BATCH_SIZE):
        request_with_backoff("PATCH", kontakty_url, hdrs=hdrs, 
                            json_data={"records": batch, "typecast": True})
        updated += len(batch)
        time.sleep(0.2)
    
    print(f"\n✅ Doplněno {updated} oslovení!")


if __name__ == "__main__":
    main()
