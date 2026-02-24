# FAIL - webinář - firmy

Projekt pro vyhledávání LinkedIn profilů kontaktů z CSV a nalezení HR/L&D kontaktů pro firmy.

## Obsah projektu

### Soubory

- **team_challenge_contacts.csv** - Hlavní databáze kontaktů (345 kontaktů)
  - Obsahuje: Firma, Email, Jméno, Poznámky, Tagy, LinkedIn Profile, Pozice
  - 158 kontaktů má vyplněný LinkedIn profil (144 přidáno v rámci tohoto projektu)

- **hr_contacts.csv** - Nové HR/L&D kontakty pro firmy bez HR kontaktu (13 kontaktů)
  - Obsahuje: Firma, Jméno, Pozice, LinkedIn Profile
  - Firmy: ABB, ARICOMA, Bosch, Generali, Komerční banka, O2 Czech Republic

## Statistiky

- Celkem kontaktů v hlavní databázi: 345
- S LinkedIn profilem: 158 (45.8%)
- Bez LinkedIn profilu: 187 (54.2%)
- Nových HR kontaktů nalezeno: 13

## Metodika

1. Vyhledávání LinkedIn profilů pomocí Apify aktoru `harvestapi/linkedin-profile-search-by-name`
2. Párování výsledků podle jména a firmy
3. Aktualizace CSV s nalezenými profily a pozicemi
4. Vyhledání HR/L&D kontaktů pro firmy bez HR kontaktu pomocí `apify/rag-web-browser`
