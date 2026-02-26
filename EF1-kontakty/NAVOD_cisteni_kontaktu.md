# Návod: Čištění kontaktů v Airtable

## Přehled úkolu
Procházíme tabulku **Kontakty** v Airtable a aktualizujeme údaje podle LinkedIn profilů.

---

## FÁZE 1: Neaktivní e-maily

**Cíl:** Opravit kontakty, kterým se vrátil email z posledních mailingů.

### Postup:
1. **Filtruj** tabulku Kontakty podle sloupce **"Stav - e-mail"** = neaktivní (nebo podobně)
2. Pro každý kontakt:
   - **Otevři LinkedIn** a vyhledej osobu podle jména + firmy
   - **Doplň/aktualizuj:**
     - `LinkedIn profil` - vlož URL profilu
     - `Společnost / Firma` - aktuální firma z LinkedIn
     - `Pracovní pozice` - aktuální pozice z LinkedIn
   - **Smaž email** (nebo ponech prázdný)
   - **Změň** "Stav - e-mail" na **"e-mail chybí"**
   - **Zaškrtni checkbox** "Aktuální" ✓

### Tipy:
- LinkedIn hledání: `jméno příjmení firma` (např. "Jan Novák Škoda Auto")
- Pokud osobu nenajdeš → ponech bez LinkedIn, ale stejně označ jako zkontrolované

---

## FÁZE 2: Kontakty s LinkedIn profilem

**Cíl:** Zkontrolovat a doplnit údaje u kontaktů, které už mají LinkedIn URL.

### Postup:
1. **Filtruj** tabulku podle: `LinkedIn profil` is not empty
2. Pro každý kontakt:
   - **Proklikni LinkedIn URL** v záznamu
   - **Zkontroluj/doplň:**
     - `Společnost / Firma` - sedí s LinkedIn?
     - `Pracovní pozice` - aktuální?
   - **Zaškrtni checkbox** "Aktuální" ✓

### Co aktualizovat:
| Pole | Akce |
|------|------|
| Firma se změnila | Aktualizuj na novou |
| Pozice se změnila | Aktualizuj na novou |
| Vše sedí | Jen zaškrtni checkbox |

---

## FÁZE 3: Zbytek (pokračování zítra)

Podle pokroku - bude upřesněno.

---

## Rychlý checklist

- [ ] Filtr nastaven správně
- [ ] LinkedIn URL doplněna
- [ ] Firma aktuální
- [ ] Pozice aktuální  
- [ ] E-mail smazán (u neaktivních)
- [ ] Stav e-mail změněn (u neaktivních)
- [ ] Checkbox "Aktuální" zaškrtnut ✓

---

## Poznámky
- Pokud si nejsi jistá → přeskoč a poznač si
- Pokud osoba nemá LinkedIn → zaškrtni jako zkontrolované, ale nech prázdné
- Při problémech se ozvi Heleně
