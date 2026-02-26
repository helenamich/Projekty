#!/usr/bin/env python3
"""
Opraví kontakty kde je prohozené jméno/příjmení a doplní správné oslovení.
"""

import json
import re
import time
from pathlib import Path
from urllib.parse import quote
import requests

token = json.load(open(Path.home() / '.cursor' / 'mcp.json'))['mcpServers']['airtable']['env']['AIRTABLE_API_KEY']
hdrs = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

BASE_ID = 'appEXpqOEIElHzScl'
url = f'https://api.airtable.com/v0/{BASE_ID}/{quote("Kontakty", safe="")}'


def is_likely_surname(name):
    """Vrátí True pokud jméno vypadá jako příjmení."""
    if not name:
        return False
    name = name.strip().rstrip(',')
    # Typické koncovky příjmení
    return bool(re.search(r'(ová|ový|ský|ská|cký|cká|ník|čík|řík|dík|lík|tík|vík|ač|eč|ič|oč|uč|ář|íř|ůř|eř|oř|ej|aj|ůj|ek|ec|ík)$', name, re.IGNORECASE))


def is_likely_firstname(name):
    """Vrátí True pokud jméno vypadá jako křestní jméno."""
    if not name:
        return False
    name = name.strip().lower()
    # Typická ženská jména
    female = ['simona', 'olga', 'adéla', 'nicole', 'petra', 'vanda', 'lucie', 'natálie', 
              'marie', 'jana', 'eva', 'hana', 'anna', 'lenka', 'kateřina', 'martina',
              'michaela', 'veronika', 'barbora', 'markéta', 'alena', 'helena', 'ivana',
              'monika', 'zuzana', 'jitka', 'věra', 'daniela', 'renata', 'kristýna',
              'karolína', 'eliška', 'vendula', 'klára', 'šárka', 'diana', 'silvie', 'miriam',
              'tereza', 'nikola', 'andrea', 'gabriela', 'aneta', 'denisa', 'pavla', 'radka']
    # Typická mužská jména
    male = ['jan', 'pavel', 'karel', 'josef', 'petr', 'tomáš', 'martin', 'jakub',
            'ondřej', 'david', 'adam', 'michal', 'lukáš', 'filip', 'marek', 'jiří',
            'vojtěch', 'matěj', 'daniel', 'radek', 'milan', 'jaroslav', 'zdeněk',
            'václav', 'vladimír', 'stanislav', 'roman', 'aleš', 'libor', 'oldřich',
            'miroslav', 'ladislav', 'patrik', 'richard', 'robert', 'viktor', 'štěpán',
            'dominik', 'matyáš', 'šimon', 'antonín', 'františek', 'michael', 'radim',
            'igor', 'boris', 'denis', 'ognen', 'hendrich', 'jiří', 'tomáš']
    return name in female or name in male


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
        "radim": "Radime", "ognen": "Ognene",
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
        "olga": "Olgo", "vanda": "Vando", "miriam": "Miriam",
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


def request_with_retry(method, url, **kwargs):
    """Request s retry logikou."""
    for attempt in range(5):
        try:
            resp = requests.request(method, url, timeout=30, **kwargs)
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 4:
                print(f"   Připojení selhalo, zkouším znovu ({attempt+1}/5)...")
                time.sleep(2 ** attempt)
            else:
                raise


print('🔎 Hledám kontakty s prohozeným jménem/příjmením...')

to_fix = []
offset = None

while True:
    params = {'pageSize': 100}
    if offset:
        params['offset'] = offset
    resp = request_with_retry('GET', url, headers=hdrs, params=params)
    data = resp.json()
    
    for rec in data.get('records', []):
        fields = rec.get('fields', {})
        jmeno = fields.get('Jméno', '').strip()
        prijmeni = fields.get('Příjmení', '').strip()
        
        if not jmeno:
            continue
            
        new_jmeno = None
        new_prijmeni = None
        
        # Formát "Příjmení, Jméno" nebo "Příjmení Jméno"
        if ',' in jmeno:
            parts = [p.strip() for p in jmeno.split(',', 1)]
            if len(parts) == 2 and parts[0] and parts[1]:
                new_prijmeni = parts[0]
                new_jmeno = parts[1]
        elif len(jmeno.split()) == 2:
            parts = jmeno.split()
            if is_likely_surname(parts[0]) and (is_likely_firstname(parts[1]) or not is_likely_surname(parts[1])):
                new_prijmeni = parts[0]
                new_jmeno = parts[1]
        # Prohozené: Jméno je příjmení, Příjmení je křestní jméno
        elif is_likely_surname(jmeno) and is_likely_firstname(prijmeni):
            new_jmeno = prijmeni
            new_prijmeni = jmeno
        
        if new_jmeno and new_prijmeni:
            # Normalize case
            new_jmeno = new_jmeno.title()
            new_prijmeni = new_prijmeni.title()
            
            osloveni = vocative_czech(new_jmeno)
            
            to_fix.append({
                'id': rec['id'],
                'old_jmeno': jmeno,
                'old_prijmeni': prijmeni,
                'new_jmeno': new_jmeno,
                'new_prijmeni': new_prijmeni,
                'osloveni': osloveni
            })

print(f'   Nalezeno {len(to_fix)} kontaktů k opravě\n')

if not to_fix:
    print('✅ Všechna jména jsou správně!')
    exit()

print('📋 Změny:')
for f in to_fix:
    print(f"   {f['old_jmeno']:<20} {f['old_prijmeni']:<15} → {f['new_jmeno']:<15} {f['new_prijmeni']:<15} (oslovení: {f['osloveni']})")

# Aktualizace
print(f'\n⬆️ Opravuji {len(to_fix)} kontaktů...')

for i, f in enumerate(to_fix, 1):
    update = {
        'fields': {
            'Jméno': f['new_jmeno'],
            'Příjmení': f['new_prijmeni'],
            'Oslovení': f['osloveni']
        }
    }
    resp = request_with_retry('PATCH', f"{url}/{f['id']}", headers=hdrs, json=update)
    if not resp.ok:
        print(f"   ❌ Chyba u {f['old_jmeno']}: {resp.text[:100]}")
    else:
        print(f"   ✓ {i}/{len(to_fix)} {f['new_jmeno']} {f['new_prijmeni']}")
    time.sleep(0.3)

print(f'\n✅ Opraveno {len(to_fix)} kontaktů!')
