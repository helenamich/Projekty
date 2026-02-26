# Aktualizace LinkedIn pozic pomocí Apify

## Přehled
Skript `update_linkedin_positions.py` automaticky aktualizuje pracovní pozice kontaktů v CSV souboru pomocí Apify LinkedIn scraperu.

## Statistiky
- **Celkem kontaktů**: 1,391
- **S LinkedIn**: 331
- **S LinkedIn i pozicí**: 126
- **S LinkedIn BEZ pozice**: 205 (potenciál pro aktualizaci)

## Instalace

1. **Zaregistrujte se na Apify**
   - Jděte na https://apify.com
   - Vytvořte účet (free tier má 5 USD kreditu měsíčně)

2. **Získejte API token**
   - Přihlaste se na https://console.apify.com
   - Jděte na Account → Integrations
   - Zkopírujte API token

3. **Nainstalujte Apify klienta**
   ```bash
   pip install apify-client
   ```

## Použití

### Varianta 1: Environment variable
```bash
export APIFY_API_TOKEN='your-token-here'
python3 update_linkedin_positions.py
```

### Varianta 2: Přímý argument
```bash
python3 update_linkedin_positions.py 'your-token-here'
```

## Jak to funguje

1. Skript načte `kontakty_unified.csv`
2. Najde kontakty s LinkedIn URL ale bez pozice
3. Pro každý LinkedIn profil zavolá Apify API
4. Získá aktuální pozici z LinkedIn profilu
5. Aktualizuje CSV soubor

## Náklady

- Apify LinkedIn scraper: ~0.01 USD na profil
- 205 kontaktů = ~2.05 USD
- Free tier: 5 USD měsíčně (dost na ~500 profilů)

## Poznámky

- Skript má rate limiting (2 sekundy mezi požadavky)
- Aktualizuje pouze kontakty bez pozice
- Volitelně aktualizuje i firmu, pokud chybí
- Ukládá změny přímo do `kontakty_unified.csv`

## Alternativy

Pokud nechcete používat Apify, můžete:
- Manuálně aktualizovat v aplikaci (http://localhost:8000/csv_editor.html)
- Použít jiný LinkedIn scraper
- Použít LinkedIn API (vyžaduje LinkedIn Developer účet)
