#!/usr/bin/env python3
"""
Update LinkedIn positions, companies, and emails using Apify API.
Requires: pip install apify-client
Usage: Set APIFY_API_TOKEN environment variable or pass as argument

Updates:
- Pracovní pozice (if missing or changed)
- Společnost / Firma (if missing or changed)
- Email (if company/position changed and new email available)
"""

import csv
import os
import sys
import time
from pathlib import Path
from apify_client import ApifyClient

# Configuration
CSV_FILE = Path(__file__).parent / "kontakty_unified.csv"
# HarvestAPI LinkedIn Profile Scraper (No Cookies) - použijte ID pokud name nefunguje
APIFY_ACTOR = "LpVuK3Zozwuipa5bp"  # harvestapi/linkedin-profile-scraper

def normalize_firma(name: str) -> str:
    """Pro porovnání: malá písmena, bez s.r.o. / a.s., zkrácené mezery."""
    if not name:
        return ""
    s = (name or "").lower().strip()
    for suffix in (" s.r.o.", " a.s.", " s.r.o", " a.s", ", s.r.o.", ", a.s.", " spol. s r.o."):
        s = s.replace(suffix, "")
    s = " ".join(s.split())
    return s


def company_matches(csv_firma: str, linkedin_company: str) -> bool:
    """True, pokud se firma v CSV shoduje s firmou z LinkedIn (nebo CSV nemá firmu)."""
    if not (csv_firma or "").strip():
        return True
    if not (linkedin_company or "").strip():
        return False
    a = normalize_firma(csv_firma)
    b = normalize_firma(linkedin_company)
    if not a:
        return True
    # Shoda: celý název nebo alespoň významná slova
    words = [w for w in a.split() if len(w) > 2]
    return a in b or b in a or any(w in b for w in words)


def _looks_like_headline_not_title(text: str) -> bool:
    """True pokud text vypadá jako headline/citát, ne jako job title."""
    if not (text or "").strip():
        return True
    s = (text or "").strip()
    # Příliš dlouhé = spíš citát
    if len(s) > 80:
        return True
    # Osobní fráze typu „Pamela, je tu“
    if "je tu" in s.lower() or ", je " in s.lower():
        return True
    # Začíná uvozovkou = citát
    if s.startswith('"') or s.startswith("'"):
        return True
    # Typické citáty (education is the most powerful...)
    if "education is the most" in s.lower() or "change the world" in s.lower():
        return True
    return False


def _looks_titleish(text: str) -> bool:
    """Heuristika: text vypadá jako job title (ne věta/citát)."""
    s = (text or "").strip()
    if not s:
        return False
    if len(s) < 2 or len(s) > 60:
        return False
    # příliš mnoho slov = spíš headline/věta
    if len(s.split()) > 10:
        return False
    low = s.lower()
    # typické věty / osobní prohlášení
    for bad in (" i ", " i'm", " i’m", " passionate", " enthusiast", " lover", " dad", " mom"):
        if bad in f" {low} ":
            return False
    # větná interpunkce
    if any(ch in s for ch in (".", "!", "?", "\n")):
        return False
    # uvozovky/citáty
    if s.startswith(("“", "”", '"', "'")):
        return False
    return True


def extract_job_title_from_headline(headline: str) -> str:
    """
    Z LinkedIn headline vytáhne jen job title (např. část před ' at ' / ' @ ' / ' | ').
    Vrací prázdný řetězec, pokud headline vypadá jako citát nebo se nedá bezpečně zkrátit.
    """
    s = (headline or "").strip()
    if not s:
        return ""
    # citáty / osobní texty rovnou pryč
    if _looks_like_headline_not_title(s):
        return ""

    # typické separátory v headline; bereme pouze první segment = job title
    seps = [" at ", " @ ", " | "]
    for sep in seps:
        if sep in s:
            cand = s.split(sep, 1)[0].strip()
            if not cand:
                return ""
            # kandidát musí vypadat jako titul, ne jako citát
            if _looks_like_headline_not_title(cand):
                return ""
            # příliš krátké/obecné nechceme
            if len(cand) < 2:
                return ""
            return cand

    # bez separátoru: pokud headline vypadá jako čistý titul, vezmeme ho celý
    if _looks_titleish(s):
        return s
    return ""


def get_linkedin_username(url: str) -> str:
    """Extract LinkedIn username from URL"""
    if not url or "linkedin.com/in/" not in url:
        return ""
    
    # Extract username from URL like https://www.linkedin.com/in/username/
    parts = url.split("linkedin.com/in/")
    if len(parts) < 2:
        return ""
    
    username = parts[1].split("/")[0].split("?")[0].split("#")[0]
    return username.strip()

def scrape_linkedin_profile(client: ApifyClient, linkedin_url: str) -> dict:
    """
    Scrape LinkedIn profile using Apify
    Returns: dict with 'headline', 'currentPosition', 'company', etc.
    """
    username = get_linkedin_username(linkedin_url)
    if not username:
        return {}
    
    print(f"  Scraping: {username}...", end=" ", flush=True)
    
    try:
        run_input = {"urls": [linkedin_url]}
        run_result = client.actor(APIFY_ACTOR).call(run_input=run_input)
        default_dataset_id = run_result.get("defaultDatasetId")
        if not default_dataset_id:
            print("✗ (no dataset)")
            return {}
        dataset = client.dataset(default_dataset_id)
        items = list(dataset.iterate_items())
        
        if items and len(items) > 0:
            profile = items[0]
            # HarvestAPI: headline, currentPosition = list of {companyName, title?}; bereme JEN job title, ne headline
            headline = profile.get("headline", "") or ""
            curr = profile.get("currentPosition")
            if isinstance(curr, list) and curr:
                first = curr[0]
                company = (first.get("companyName") or first.get("company") or "") if isinstance(first, dict) else ""
                # Pouze title/position z aktuální pozice – nikdy headline (citáty, "Pamela, je tu" atd.)
                position = (first.get("title") or first.get("position") or "") if isinstance(first, dict) else ""
            else:
                company = profile.get("company", "") or profile.get("currentCompany", "")
                position = profile.get("title", "") or ""
            # Fallback: pokud API neposkytne title, zkusíme vytáhnout jen job title z headline
            if not position:
                position = extract_job_title_from_headline(headline)

            # finální kontrola: do CSV nechceme citáty / osobní texty
            if position and _looks_like_headline_not_title(position):
                position = ""
            result = {
                "headline": headline,
                "currentPosition": position,
                "company": company,
                "location": profile.get("location", ""),
                "email": profile.get("email", ""),
                "emails": profile.get("emails", []),
            }
            print("✓")
            return result
        else:
            print("✗ (no data)")
            return {}
            
    except Exception as e:
        print(f"✗ Error: {str(e)[:80]}")
        return {}

def main():
    # Get API token
    api_token = os.getenv("APIFY_API_TOKEN")
    if not api_token:
        print("❌ Error: APIFY_API_TOKEN environment variable not set")
        print("\nTo use Apify:")
        print("1. Sign up at https://apify.com")
        print("2. Get your API token from https://console.apify.com/account/integrations")
        print("3. Set it: export APIFY_API_TOKEN='your-token-here'")
        print("4. Or pass as argument: python update_linkedin_positions.py YOUR_TOKEN")
        sys.exit(1)
    
    client = ApifyClient(api_token)
    
    # Read CSV
    print(f"📖 Reading {CSV_FILE}...")
    rows = []
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        for row in reader:
            rows.append(row)
    
    # Kontakty s LinkedIn, kde chybí pozice (doplníme jen při shodě firmy)
    to_update = []
    for i, row in enumerate(rows):
        linkedin = row.get('LinkedIn profil', '').strip()
        pozice = row.get('Pracovní pozice', '').strip()
        if linkedin and not pozice:
            to_update.append((i, linkedin))
    
    print(f"\n📊 Kontakty s LinkedIn a bez pozice: {len(to_update)}")
    print(f"   Pozici doplním jen tam, kde se firma z LinkedIn shoduje s firmou v CSV.")
    print(f"\n💰 Odhad nákladů: ~{len(to_update) * 0.01:.2f} USD (Apify)")
    
    if not to_update:
        print("✅ U všech s LinkedIn je už pozice vyplněná.")
        return
    
    if "--limit" in sys.argv:
        try:
            li = sys.argv.index("--limit")
            if li + 1 < len(sys.argv):
                n = int(sys.argv[li + 1])
                to_update = to_update[:n]
                print(f"   (TEST: jen prvních {n} kontaktů)")
        except (ValueError, IndexError):
            pass
    
    # Potvrzení (přeskočí se s --yes)
    if "--yes" not in sys.argv and "-y" not in sys.argv:
        response = input(f"\n⚠️  Spustit aktualizaci pro {len(to_update)} kontaktů? (yes/no): ")
        if response.lower() != 'yes':
            print("Zrušeno.")
            return
    else:
        print(f"\n🚀 Spouštím aktualizaci ({len(to_update)} kontaktů)...")
    
    updated_positions = 0
    skipped_no_match = 0
    for idx, (row_idx, linkedin_url) in enumerate(to_update, 1):
        csv_firma = rows[row_idx].get('Společnost / Firma', '').strip()
        jmeno = f"{rows[row_idx].get('Jméno','')} {rows[row_idx].get('Příjmení','')}".strip()
        print(f"\n[{idx}/{len(to_update)}] {jmeno or '?'}…")
        
        profile_data = scrape_linkedin_profile(client, linkedin_url)
        
        if not profile_data:
            continue
        
        # Pouze skutečný job title – headline nepoužíváme
        new_position = (profile_data.get("currentPosition") or "").strip()
        new_company = (profile_data.get("company") or "").strip()
        
        if not new_position:
            print("  → LinkedIn bez job title (jen headline), přeskakuji")
            continue
        
        if not company_matches(csv_firma, new_company):
            skipped_no_match += 1
            print(f"  → Přeskočeno (firma neshoduje: CSV „{csv_firma[:30]}…“ vs LinkedIn „{new_company[:30]}…“)")
            continue
        
        # Doplnit pozici jen když v CSV chybí – nikdy nepřepisovat existující
        current_pos = (rows[row_idx].get('Pracovní pozice') or '').strip()
        if current_pos:
            print(f"  → Přeskočeno (pozice už vyplněná: „{current_pos[:40]}…“)")
            continue
        
        rows[row_idx]['Pracovní pozice'] = new_position
        updated_positions += 1
        print(f"  → Pozice: {new_position[:60]}")
        
        if not csv_firma and new_company:
            rows[row_idx]['Společnost / Firma'] = new_company
            print(f"  → Firma doplněna: {new_company[:50]}")
        
        if idx % 10 == 0 and updated_positions > 0:
            with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  💾 Průběžně uloženo ({updated_positions} pozic)")
        
        if idx < len(to_update):
            time.sleep(2)
    
    if updated_positions > 0:
        with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\n✅ Hotovo. Doplněno pozic: {updated_positions}")
        if skipped_no_match:
            print(f"   Přeskočeno (firma neshoduje): {skipped_no_match}")
    else:
        print("\n⚠️  Žádná pozice nebyla doplněna (nebo všechny přeskočeny – neshoda firmy).")

if __name__ == "__main__":
    if len(sys.argv) > 1 and "apify_api_" in (sys.argv[1] or ""):
        os.environ["APIFY_API_TOKEN"] = sys.argv[1]
        sys.argv.pop(1)
    main()
