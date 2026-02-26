# Práce s databází KONTAKTY

Tento dokument popisuje strukturu databáze kontaktů v Airtable, workflow pro její údržbu a propojení s dalšími nástroji.

---

## 1. Struktura databáze

Databáze se skládá ze **3 propojených tabulek** v Airtable:

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│   KLIENTI   │◄─────►│  KONTAKTY   │◄─────►│    DEALS    │
│   (firmy)   │       │   (osoby)   │       │  (poptávky) │
└─────────────┘       └─────────────┘       └─────────────┘
```

### 1.1 Tabulka KLIENTI (firmy)

Obsahuje záznamy o firmách/organizacích.

| Pole | Typ | Popis |
|------|-----|-------|
| **Firma** | Text | Název firmy |
| **Kontakty** | Link → Kontakty | Všechny kontakty z této firmy |
| **HR Kontakty (linked)** | Link → Kontakty | Automaticky filtrované kontakty s Oddělení = HR |
| **Deals** | Link → Deals | Poptávky od této firmy |
| **Deals - přes agenturu** | Link → Deals | Poptávky, kde firma je koncový klient (přes agenturu) |
| **Typ klienta** | Select | Kategorie klienta |
| **Počet zaměstnanců** | Select | Velikost firmy |
| **Poznámka** | Text | Interní poznámky |

### 1.2 Tabulka KONTAKTY (osoby)

Obsahuje záznamy o konkrétních lidech.

| Pole | Typ | Popis |
|------|-----|-------|
| **Kontakt ID** | Formula | Automaticky: "Jméno Příjmení - Firma" |
| **Jméno** | Text | Křestní jméno |
| **Příjmení** | Text | Příjmení |
| **Oslovení** | Text | Jak oslovujeme v emailech (např. "Honzo", "Petře") |
| **Primární e-mail** | Email | Hlavní email (preferovaně pracovní) |
| **Sekundární e-mail** | Email | Záložní email (osobní) |
| **Telefon** | Phone | Telefonní číslo |
| **LinkedIn profil** | URL | Odkaz na LinkedIn profil |
| **Klienti** | Link → Klienti | Firma, kde kontakt pracuje |
| **Společnost / Firma** | Text | Textový název firmy (pro rychlý přehled) |
| **Pracovní pozice** | Text | Aktuální pozice |
| **Oddělení** | Multi-select | HR / C-level Manager / Owner / IT |
| **Stav - e-mail** | Select | Aktivní / Chybí - kontaktovat přes Li / atd. |
| **Programy** | Multi-select | Účast v programech (FAIL - archiv, FAIL - jaro 2026...) |
| **Deals** | Link → Deals | Poptávky, kde je kontaktní osobou |
| **Aktuální - checked 02/2026** | Checkbox | Označení, že kontakt byl ověřen |
| **Poznámka** | Text | Interní poznámky |
| **Nelze ověřit - prac. e-mail bez Li** | Checkbox | Má pracovní email, ale nemá LinkedIn |
| **Nelze ověřit - soukr. e-mail bez Li** | Checkbox | Má soukromý email, nemá LinkedIn |

### 1.3 Tabulka DEALS (poptávky)

Obsahuje záznamy o obchodních příležitostech.

| Pole | Typ | Popis |
|------|-----|-------|
| **Název dealu** | Text | Identifikace poptávky (např. "ČSOB \| Školení") |
| **Klienti** | Link → Klienti | Firma, která poptává |
| **Kontakt** | Link → Kontakty | Kontaktní osoba |
| **Co poptávali** | Select | Typ poptávky (Školení, Keynote, Program...) |
| **Komu určeno / Nabídnut pro realizaci** | Multi-select | Kdo z týmu má realizovat |
| **Reakce / Výsledek** | Select | Stav poptávky |
| **Poznámka / Detaily** | Text | Podrobnosti o poptávce |
| **Koncový klient** | Link → Klienti | Pokud poptávka přes agenturu - skutečný klient |
| **Aktualizováno / checked** | Checkbox | Označení aktuálnosti |

### 1.4 Vazby mezi tabulkami

```
KLIENTI ←──────────────────────────────────────→ KONTAKTY
    │   (1 klient má N kontaktů)                    │
    │   (1 kontakt patří k 1 klientovi)             │
    │                                               │
    │                                               │
    ▼                                               ▼
DEALS ◄────────────────────────────────────────────►
    (1 deal má 1 klienta + 1 kontakt)
    (1 klient/kontakt může mít N dealů)
```

**Speciální vazby:**
- **HR Kontakty (linked)** - automaticky zobrazuje kontakty, kde Oddělení obsahuje "HR"
- **Deals - přes agenturu** - dealy, kde firma je v poli "Koncový klient" (ne přímý klient)

---

## 2. Přidávání nových kontaktů

### 2.1 Způsob A: Automaticky z emailových poptávek

**Trigger:** Označení emailu štítkem `klienti-poptávky` v Gmailu

**Flow (k nastavení v Make/Zapier):**

```
1. Gmail: Nový email se štítkem "klienti-poptávky"
                    ↓
2. AI analýza emailu - extrakce:
   - Jméno a příjmení kontaktu
   - Email kontaktu
   - Telefon (pokud uveden)
   - Název firmy
   - Pracovní pozice
   - Co poptávají (typ poptávky)
   - Detaily poptávky
                    ↓
3. Airtable: Vyhledat klienta podle názvu firmy
   ├─ Existuje → použít existujícího
   └─ Neexistuje → vytvořit nového klienta
                    ↓
4. Airtable: Vyhledat kontakt podle emailu
   ├─ Existuje → aktualizovat údaje
   └─ Neexistuje → vytvořit nový kontakt
                    ↓
5. Airtable: Vytvořit nový Deal
   - Propojit s klientem
   - Propojit s kontaktem
   - Vyplnit typ poptávky a detaily
                    ↓
6. (Volitelné) Notifikace do Slacku/emailu
```

**Pole k extrakci z emailu:**
- Jméno, Příjmení
- Email
- Telefon
- Firma
- Pozice
- Typ poptávky (Školení / Keynote / Program / Jiné)
- Text poptávky → Poznámka / Detaily

> **Poznámka:** Tato automatizace zatím není nastavena. Po nastavení bude potřeba ruční kontrola nově vytvořených záznamů.

### 2.2 Způsob B: Ruční import po programu FAIL

Po skončení každého běhu programu FAIL se účastníci ručně importují do databáze.

**Postup:**

1. **Export účastníků** z registračního systému (CSV/Excel)

2. **Příprava dat:**
   - Zkontrolovat formát jmen (velká písmena, diakritika)
   - Doplnit oslovení
   - Ověřit emaily

3. **Import do Airtable:**
   - Použít Airtable import nebo ruční vložení
   - Každému kontaktu přiřadit program v poli **Programy** (např. "FAIL - jaro 2026")

4. **Propojení s klienty:**
   - Pro každý kontakt najít/vytvořit odpovídajícího klienta
   - Propojit kontakt s klientem

5. **Doplnění LinkedIn:**
   - Vyhledat LinkedIn profily účastníků
   - Doplnit URL do pole LinkedIn profil

---

## 3. Pravidelná aktualizace kontaktů (1× za 2-3 měsíce)

### 3.1 Automatizovaná kontrola přes Cursor AI

**Spuštění:** Otevřít tento projekt v Cursor a zadat příkaz:

```
Spusť aktualizaci kontaktů podle workflow.
```

**Co se stane:**

1. **Stažení všech kontaktů s LinkedIn profilem** z Airtable

2. **Scraping LinkedIn** (přes Apify):
   - Pro každý kontakt stáhnout aktuální data z LinkedIn
   - Kontrola sekce "Experience" - hledání pozice s "Present"

3. **Identifikace nesrovnalostí:**
   - Kontakt změnil firmu (jiná firma na LinkedIn než v Airtable)
   - Kontakt ukončil pozici (žádná "Present" pozice)
   - Kontakt má jinou pozici

4. **Výstup - seznam k ruční kontrole:**
   - Tabulka kontaktů se změnami
   - Pro každý: staré vs. nové údaje
   - Doporučená akce

### 3.2 Ruční kontrola a oprava

Po automatizované kontrole projít seznam a pro každý kontakt:

**Při změně firmy:**
1. Odpojit kontakt od starého klienta
2. Aktualizovat pole "Společnost / Firma"
3. Připojit k novému klientovi (vytvořit pokud neexistuje)
4. Odpojit od dealů starého klienta
5. Zkontrolovat email (viz 3.3)
6. Smazat starého klienta, pokud zůstal prázdný

**Při ukončení pozice (nezaměstnaný):**
1. Ověřit na LinkedIn, zda skutečně nemá práci
2. Pokud ano - vymazat pozici a firmu
3. Ponechat kontakt (může být stále relevantní)

### 3.3 Kontrola emailu při změně firmy

| Typ emailu | Příklad | Akce |
|------------|---------|------|
| **Osobní email** | gmail.com, seznam.cz | ✅ Ponechat |
| **Email NOVÉ firmy** | novafirma.cz | ✅ Ponechat |
| **Email STARÉ firmy** | starafirma.cz | ❌ Smazat, nastavit "Chybí - kontaktovat přes Li" |

### 3.4 Po dokončení aktualizace

1. Zaškrtnout **"Aktuální - checked MM/YYYY"** u všech zkontrolovaných kontaktů
2. Smazat prázdné klienty (bez kontaktů a dealů)
3. Aktualizovat datum v názvu checkboxu pro příští kontrolu

---

## 4. Propojení s Attio

### 4.1 Současný stav

- **Airtable** = hlavní (master) databáze
- **Attio** = zatím nepoužíváno, plánované nasazení pro tým

### 4.2 Plánované nastavení

**Směr synchronizace:** Airtable → Attio (jednosměrná)

**Co synchronizovat:**
- Kontakty (People)
- Klienti (Companies)
- Propojení mezi nimi

**Identifikace záznamů:**
- Primární klíč: **Email** (pro kontakty)
- Sekundární: Název firmy (pro klienty)

### 4.3 Čištění Attio od starých kontaktů

Při synchronizaci identifikovat kontakty v Attio, které:
- Mají jiný email než v Airtable (změnili email)
- Nejsou v Airtable vůbec (smazané kontakty)

**Postup:**
1. Export kontaktů z Attio
2. Porovnat s Airtable podle emailu
3. Kontakty pouze v Attio → kandidáti na smazání
4. Ruční kontrola před smazáním

> **TODO:** Nastavit automatizaci synchronizace (Make/Zapier nebo Attio API)

---

## 5. Propojení s Ecomail

### 5.1 Účel

Ecomail slouží pro email marketing. Kontakty se do něj přidávají automaticky s příslušnými štítky podle produktu/akce.

### 5.2 Produkty a štítky

| Produkt/Akce | Zdroj dat | Automatizace |
|--------------|-----------|--------------|
| **FAIL webinář** (zdarma) | Registrační formulář | ✅ Nastaveno |
| **State of AI** (zdarma) | Registrační formulář | ✅ Nastaveno |
| **Newsletter** | Registrační formulář | ✅ Nastaveno |
| **Nákup knihy** | Simpleshop | ❌ K nastavení |
| **Nákup AI Predictions** | Simpleshop | ❌ K nastavení |

### 5.3 Automatizace k nastavení

**Pro nákupy (kniha, AI Predictions):**

```
Simpleshop: Nová objednávka
        ↓
Ecomail: Přidat/aktualizovat kontakt
        - Email
        - Jméno
        - Štítek podle produktu
```


---

## 6. Správa vrácených emailů (bounce)

### 6.1 Zdroje informací o bounce

- **Ecomail** - bounce report po rozesílce
- **Vlastní email** - vrácené emaily při osobní komunikaci

### 6.2 Postup při vráceném emailu

```
Email se vrátil jako nedoručitelný
                ↓
        Má kontakt LinkedIn?
       /                    \
     ANO                    NE
      ↓                      ↓
Zkontrolovat LinkedIn    Má jiný email?
      ↓                  /          \
Stále pracuje          ANO          NE
ve firmě?               ↓            ↓
  /     \          Ověřit       SMAZAT
ANO     NE         druhý        KONTAKT
 ↓       ↓         email
Hledat   ↓
nový   Změnil
email  firmu?
       /    \
     ANO    NE
      ↓      ↓
  Postup   Označit
  změna    "Chybí -
  firmy    kontaktovat
  (kap.3)  přes Li"
```

### 6.3 Akce podle situace

| Situace | Akce |
|---------|------|
| **Má LinkedIn, stále ve firmě** | Pokusit se najít nový email (web firmy, LinkedIn) |
| **Má LinkedIn, změnil firmu** | Postup změny firmy (kapitola 3), smazat starý email |
| **Má LinkedIn, nezaměstnaný** | Smazat email, nastavit "Chybí - kontaktovat přes Li" |
| **Nemá LinkedIn, má jiný email** | Ověřit druhý email, pokud funguje - použít jako primární |
| **Nemá LinkedIn, nemá jiný email** | **SMAZAT KONTAKT** z databáze |

### 6.4 Po smazání kontaktu

Zkontrolovat, zda klient (firma) nezůstal prázdný:
- Nemá žádné kontakty
- Nemá žádné dealy

→ Pokud prázdný, **SMAZAT KLIENTA**

---

## 7. Klasifikace oddělení

Kontakty se automaticky klasifikují podle pozice:

| Oddělení | Klíčová slova v pozici |
|----------|------------------------|
| **HR** | hr, human resource, people, talent, recruiting, personalist, lidské zdroje, nábor, L&D, people & culture |
| **C-level Manager** | ceo, cfo, coo, cto, cmo, chief, generální ředitel, finanční ředitel, managing director |
| **Owner** | founder, co-founder, owner, zakladatel, majitel, vlastník |
| **IT** | developer, software, engineer, programmer, devops, data, architect, vývojář |

> **Poznámka:** Kontakt může mít více kategorií (např. Owner + C-level)

Kontakty s **Oddělení = HR** se automaticky zobrazují v poli "HR Kontakty" u příslušného klienta.

---

## 8. Pravidla pro oslovení

Standardní oslovení pro běžná jména:

| Jméno | Oslovení |
|-------|----------|
| Jakub | Jakube |
| Ondřej | Ondro |
| Magdaléna | Magdi |
| Petr | Petře |
| Jan | Honzo |
| ... | ... |

> **Poznámka:** Oslovení se zadává ručně při vytváření kontaktu. Při hromadném importu zkontrolovat a sjednotit.

---

## 9. Kontrolní checklist pro údržbu

### Týdenně:
- [ ] Zpracovat nové poptávky (štítek v Gmailu)
- [ ] Zkontrolovat bounce emaily

### Po každém programu FAIL:
- [ ] Importovat nové účastníky
- [ ] Přiřadit program
- [ ] Doplnit LinkedIn profily
- [ ] Propojit s klienty

### Každé 2-3 měsíce:
- [ ] Spustit automatizovanou kontrolu LinkedIn
- [ ] Projít seznam nesrovnalostí
- [ ] Aktualizovat změněné kontakty
- [ ] Smazat prázdné klienty
- [ ] Synchronizovat s Attio

### Ročně:
- [ ] Celková revize databáze
- [ ] Kontrola duplicit
- [ ] Aktualizace workflow dokumentace

---

*Poslední aktualizace: 4. února 2026*
