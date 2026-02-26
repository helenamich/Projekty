# Workflow: Správa kontaktů v databázi

## 1. Zdroje dat

| Zdroj | Použití |
|-------|---------|
| **Airtable** | Hlavní databáze kontaktů, klientů a dealů |
| **LinkedIn** (Apify scraper) | Aktuální pozice a firma z experience sekce |
| **Attio** | Doplňkové vyhledávání (volitelné) |

---

## 2. Kvalita dat - základní kontroly

### 2.1 Formátování jmen
| Kontrola | Akce |
|----------|------|
| Jméno/Příjmení prohozené | Prohodit pole |
| VELKÁ PÍSMENA | Opravit na správný formát (první velké) |
| malá písmena na začátku | Opravit na správný formát |
| Chybí diakritika | Doplnit (Vsechovsky → Všechovský) |

### 2.2 Oslovení
| Kontrola | Akce |
|----------|------|
| Více oslovení (oddělených ; , /) | Vybrat jedno preferované |
| Oslovení = příjmení místo jména | Opravit |
| Nekonzistentní tvar (Lucko vs Lucie) | Sjednotit podle preference |

### 2.3 Duplicity
| Typ | Kontrola |
|-----|----------|
| Kontakty | Stejné jméno + příjmení, stejný email |
| Klienti | Podobné názvy firem (ČEZ vs ČEZ, a.s.) |
| Telefony | Stejné číslo u více kontaktů |
| Dealy | Stejný název, klient+typ, datum |

---

## 3. Aktualizace z LinkedIn

### 3.1 Scraping LinkedIn
- Použít **experience sekci** (ne headline!)
- Hledat pozici kde `endDate.text == "Present"`
- Extrahovat: **pozici**, **firmu**

### 3.2 Kontrola koncového data
| Situace | Akce |
|---------|------|
| Má "Present" | Aktualizovat pozici a firmu |
| Nemá "Present" (skončil) | Vymazat pozici, případně firmu |

---

## 4. Aktualizace polí kontaktu

| Pole | Akce |
|------|------|
| `Pracovní pozice` | Aktualizovat z LinkedIn experience |
| `Společnost / Firma` | Aktualizovat z LinkedIn |
| `LinkedIn profil` | Opravit pokud chybí https:// |
| `Oddělení` | Klasifikovat podle pozice (viz 4.1) |
| `Aktuální - checked 02/2026` | ✅ Zaškrtnout |
| `E-mail` | Ponechat beze změny (viz sekce 6) |

### 4.1 Klasifikace Oddělení

| Oddělení | Klíčová slova v pozici |
|----------|------------------------|
| **HR** | hr, human resource, people, talent, recruiting, recruitment, personalist, lidské zdroje, nábor, hrbp, chief people, vp people, head of people, people & culture, people operations, L&D |
| **C-level Manager** | ceo, cfo, coo, cto, cmo, cio, cpo, chief, c-level, generální ředitel, finanční ředitel, provozní ředitel, výkonný ředitel, managing director, general manager |
| **Owner** | founder, co-founder, owner, zakladatel, spoluzakladatel, majitel, vlastník |
| **IT** | developer, software, engineer, programmer, devops, data, architect, tech lead, technical, it manager, it director, vývojář, programátor, architekt |

> **Poznámka:** Pole je multiselect - kontakt může mít více kategorií (např. Owner + C-level)

---

## 5. Změna firmy (LinkedIn ≠ Airtable)

### 5.1 Workflow při změně firmy

```
1. Odpojit kontakt od starého klienta
   └─ Pole "Klienti" → odebrat starý záznam

2. Aktualizovat textové pole "Společnost / Firma"
   └─ Nastavit novou firmu z LinkedIn

3. Zkontrolovat existenci nového klienta
   ├─ Existuje → připojit kontakt
   └─ Neexistuje → vytvořit nového klienta, pak připojit

4. Odpojit kontakt od dealů starého klienta
   └─ Pole "Kontakt" v tabulce Deals → odebrat kontakt
   └─ Deal zůstává přiřazen starému klientovi!

5. Zkontrolovat starého klienta
   ├─ Má jiné kontakty nebo dealy → ponechat
   └─ Je prázdný (0 kontaktů, 0 dealů) → SMAZAT
```

### 5.2 E-mail při změně firmy

Při změně firmy zkontrolovat doménu e-mailu:

| Typ domény | Příklad | Akce |
|------------|---------|------|
| **Osobní email** | gmail.com, outlook.com, seznam.cz, centrum.cz, yahoo.com, icloud.com | ✅ Ponechat |
| **Osobní doména** (jméno osoby) | novak.cz, jan@kucerajan.com | ✅ Ponechat |
| **Email NOVÉ firmy** | nidec.com (u Nidec Power) | ✅ Ponechat |
| **Email STARÉ firmy** | t-mobile.cz (odešel z T-Mobile) | ❌ Smazat + nastavit "Chybí - kontaktovat přes Li" |

**⚠️ DŮLEŽITÉ: Kontrola data účasti v programu**

Před smazáním emailu staré firmy porovnej:
- **Kdy změnil práci?** (z LinkedIn experience - startDate nové pozice)
- **Kdy se zúčastnil posledního programu?** (pole "Programy" v Airtable)

| Situace | Akce |
|---------|------|
| Změna práce **PO** účasti v programu | ❌ Smazat email (měl starý email při registraci) |
| Změna práce **PŘED** účastí v programu | ✅ Ponechat email (už se hlásil s novým emailem) |

### 5.3 Co zůstává beze změny
- **Telefon** - osobní kontakt
- **Programy** - historická účast
- **Poznámka** - zachovat historii

---

## 6. Správa e-mailů a nedoručitelnost

### 6.1 Nedoručitelný e-mail (bounce)

> **⚠️ DŮLEŽITÉ PRAVIDLO:**
> Pokud se e-mail vrátí jako nedoručitelný, **smažeme ho** z pole E-mail.

| Má LinkedIn? | Akce |
|--------------|------|
| ✅ Ano | Smazat email, nastavit `Stav - e-mail = "Chybí - kontaktovat přes Li"` |
| ❌ Ne | **Smazat celý kontakt z databáze** |

### 6.2 Kontrola shody email-firma

Pravidelně kontrolovat, zda firemní email odpovídá aktuální firmě:

| Situace | Příklad | Akce |
|---------|---------|------|
| Email odpovídá firmě | jan@csob.cz pracuje v ČSOB | ✅ OK |
| Email neodpovídá firmě | jan@jt.cz pracuje v ČSOB | ⚠️ Ověřit, pravděpodobně smazat email |
| Osobní email | jan@gmail.com | ✅ Ponechat vždy |

---

## 7. HR kontakt u klienta

Když je kontakt klasifikován jako **HR**:

1. Nastavit `Oddělení = "HR"` na kontaktu
2. Kontakt se automaticky zobrazí v poli `HR Kontakty (linked)` u klienta
   - Pole je typu linked record, propojené s kontakty kde Oddělení = HR

---

## 8. Správa klientů

### 8.1 Prázdní klienti
Pravidelně mazat klienty, kteří nemají:
- Žádné kontakty
- Žádné dealy
- Žádné dealy přes agenturu

### 8.2 Duplicitní názvy klientů
| Problém | Řešení |
|---------|--------|
| Více názvů oddělených ";" | Vybrat jeden kanonický název |
| Podobné firmy (ČEZ vs ČEZ, a.s.) | Sloučit pod jeden záznam |
| Dceřiné společnosti | Zvážit sloučení nebo ponechat samostatně |

### 8.3 Freelanceři / OSVČ
Kontakty kde firma vypadá jako:
- "Na volné noze"
- Jméno a příjmení osoby
- "Freelance", "OSVČ"

→ Smazat klienta, nastavit `Pracovní pozice = "Freelance"`, odpojit od klienta

---

## 9. Kontakty bez LinkedIn

### 9.1 Označení
| Situace | Checkbox |
|---------|----------|
| LinkedIn URL = "-" | ✅ `Nemá Li / nemožné ověřit` |
| LinkedIn URL = "-" + má soukromý email | ✅ `Bez Li, ale soukr. e-mail` |

### 9.2 Soukromé emaily
Domény považované za soukromé:
- gmail.com, googlemail.com
- yahoo.com, yahoo.cz
- hotmail.com, outlook.com, outlook.cz, live.com
- seznam.cz, email.cz, centrum.cz, volny.cz, atlas.cz
- icloud.com, me.com, mac.com

**Výjimka:** protonmail.com = považovat za firemní

---

## 10. Kontrolní checklist

### Pro každý kontakt:

- [ ] Má LinkedIn URL?
- [ ] Scrapnout LinkedIn (experience sekce)
- [ ] Má aktuální pozici (Present)?
- [ ] Aktualizovat pozici
- [ ] Změnila se firma?
  - [ ] Odpojit od starého klienta
  - [ ] Připojit k novému klientovi (vytvořit pokud neexistuje)
  - [ ] Odpojit od dealů starého klienta
  - [ ] Smazat starého klienta pokud prázdný
  - [ ] **Zkontrolovat email:**
    - [ ] Email staré firmy?
      - [ ] Změna práce PO posledním programu? → Smazat email
      - [ ] Změna práce PŘED posledním programem? → Ponechat
    - [ ] Osobní/nové firmy? → Ponechat
- [ ] Klasifikovat oddělení (HR/C-level/Owner/IT)
- [ ] Pokud HR → automaticky se propíše do klienta
- [ ] Zaškrtnout "Aktuální - checked 02/2026"

### Pravidelná údržba:

- [ ] Smazat prázdné klienty (bez kontaktů a dealů)
- [ ] Sloučit duplicitní klienty
- [ ] Zkontrolovat shodu email-firma
- [ ] Opravit formátování jmen (velká/malá písmena, diakritika)
- [ ] Sjednotit oslovení

---

## 11. Speciální případy

| Situace | Řešení |
|---------|--------|
| LinkedIn profil neexistuje | Označit checkbox, nechat na ruční kontrolu |
| Více "Present" pozic | Vzít první (nejnovější) |
| Firma stejná, jen jiný název | Ověřit ručně (např. T-Mobile vs T-Mobile Czech Republic) |
| Kontakt skončil (žádný Present) | Vymazat pozici, zvážit vymazání firmy |
| Duplicitní klient | Sloučit - přesunout kontakty a dealy, smazat duplicitu |
| **Nedoručitelný email** | Smazat email; bez LinkedIn = smazat kontakt |

---

*Poslední aktualizace: 19. února 2026*
