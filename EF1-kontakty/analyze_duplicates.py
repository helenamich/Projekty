#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyzuje kontakty v Airtable a hledá:
1. Duplicitní telefony - kontakty ke sloučení
2. Stejné e-mailové domény - firmy ke sloučení
"""

import json
import re
from collections import defaultdict

# Načtení dat z exportovaných souborů
contacts_file = "/Users/helenamich/.cursor/projects/Users-helenamich-Desktop-KONTAKTY-EF1-i-t-n/agent-tools/bd08aa75-5714-40a6-bc6f-0d5936e73b1e.txt"
clients_file = "/Users/helenamich/.cursor/projects/Users-helenamich-Desktop-KONTAKTY-EF1-i-t-n/agent-tools/31d26fba-d6ab-4bd6-a41e-8f97935ee6c5.txt"

def normalize_phone(phone):
    """Normalizuje telefonní číslo pro porovnání"""
    if not phone or phone in ['#ERROR!', 'x', '']:
        return None
    # Odstraní vše kromě číslic
    digits = re.sub(r'\D', '', str(phone))
    # Odstraní předvolby
    if digits.startswith('00420'):
        digits = digits[5:]
    elif digits.startswith('420'):
        digits = digits[3:]
    elif digits.startswith('00421'):
        digits = digits[5:]
    elif digits.startswith('421'):
        digits = digits[3:]
    # Pokud je příliš krátké, ignoruj
    if len(digits) < 9:
        return None
    return digits

def get_email_domain(email):
    """Extrahuje doménu z e-mailu"""
    if not email or '@' not in email:
        return None
    domain = email.lower().split('@')[1]
    # Ignoruj obecné domény
    ignored_domains = ['gmail.com', 'seznam.cz', 'email.cz', 'outlook.com', 
                      'hotmail.com', 'yahoo.com', 'icloud.com', 'me.com',
                      'centrum.cz', 'post.cz', 'volny.cz', 'atlas.cz']
    if domain in ignored_domains:
        return None
    return domain

# Načti kontakty
with open(contacts_file, 'r', encoding='utf-8') as f:
    contacts_data = json.load(f)

contacts = contacts_data.get('records', [])
print(f"Načteno {len(contacts)} kontaktů\n")

# 1. ANALÝZA DUPLICITNÍCH TELEFONŮ
print("="*80)
print("1. DUPLICITNÍ TELEFONY (kontakty ke sloučení)")
print("="*80)

phone_groups = defaultdict(list)
for contact in contacts:
    fields = contact.get('fields', {})
    phone = fields.get('Telefon')
    norm_phone = normalize_phone(phone)
    if norm_phone:
        phone_groups[norm_phone].append({
            'id': contact['id'],
            'jmeno': fields.get('Jméno', ''),
            'prijmeni': fields.get('Příjmení', ''),
            'email': fields.get('E-mail', ''),
            'telefon': phone,
            'firma': fields.get('Společnost / Firma', ''),
            'pozice': fields.get('Pracovní pozice', ''),
            'programy': fields.get('Programy', []),
            'klienti': fields.get('Klienti', [])
        })

# Najdi duplicity
phone_duplicates = {k: v for k, v in phone_groups.items() if len(v) > 1}
print(f"\nNalezeno {len(phone_duplicates)} skupin s duplicitním telefonem:\n")

for phone, group in sorted(phone_duplicates.items(), key=lambda x: -len(x[1])):
    print(f"\n📞 Telefon: {phone} ({len(group)} kontaktů)")
    print("-" * 60)
    for c in group:
        print(f"  • {c['jmeno']} {c['prijmeni']}")
        print(f"    E-mail: {c['email']}")
        print(f"    Firma: {c['firma']}")
        print(f"    ID: {c['id']}")

# 2. ANALÝZA E-MAILOVÝCH DOMÉN
print("\n\n" + "="*80)
print("2. STEJNÉ E-MAILOVÉ DOMÉNY (firmy ke sloučení)")
print("="*80)

domain_groups = defaultdict(list)
for contact in contacts:
    fields = contact.get('fields', {})
    email = fields.get('E-mail')
    domain = get_email_domain(email)
    if domain:
        domain_groups[domain].append({
            'id': contact['id'],
            'jmeno': fields.get('Jméno', ''),
            'prijmeni': fields.get('Příjmení', ''),
            'email': email,
            'firma': fields.get('Společnost / Firma', ''),
            'klienti': fields.get('Klienti', [])
        })

# Najdi domény s různými názvy firem
print(f"\nAnalýza domén s více kontakty a různými názvy firem:\n")

domain_issues = []
for domain, group in domain_groups.items():
    if len(group) >= 2:
        # Získej unikátní názvy firem (bez None a prázdných)
        company_names = set()
        for c in group:
            if c['firma']:
                company_names.add(c['firma'])
        
        # Pokud jsou různé názvy firem pro stejnou doménu
        if len(company_names) > 1:
            domain_issues.append({
                'domain': domain,
                'companies': company_names,
                'contacts': group
            })

print(f"Nalezeno {len(domain_issues)} domén s různými názvy firem:\n")

for issue in sorted(domain_issues, key=lambda x: -len(x['contacts'])):
    print(f"\n🌐 Doména: @{issue['domain']}")
    print(f"   Názvy firem: {', '.join(issue['companies'])}")
    print(f"   Kontakty ({len(issue['contacts'])}):")
    for c in issue['contacts']:
        print(f"     • {c['jmeno']} {c['prijmeni']} - {c['firma']} ({c['email']})")

# SOUHRN
print("\n\n" + "="*80)
print("SOUHRN")
print("="*80)
print(f"• Duplicitní telefony: {len(phone_duplicates)} skupin")
print(f"• Domény s různými názvy firem: {len(domain_issues)}")

# Export pro další zpracování
output = {
    'phone_duplicates': [],
    'domain_issues': []
}

for phone, group in phone_duplicates.items():
    output['phone_duplicates'].append({
        'phone': phone,
        'contacts': group
    })

for issue in domain_issues:
    output['domain_issues'].append({
        'domain': issue['domain'],
        'companies': list(issue['companies']),
        'contact_ids': [c['id'] for c in issue['contacts']]
    })

with open('/Users/helenamich/Desktop/KONTAKTY EF1 čištění/duplicates_analysis.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print("\n✅ Výsledky uloženy do: duplicates_analysis.json")
