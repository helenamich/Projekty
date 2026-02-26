#!/usr/bin/env python3
"""
Find real LinkedIn URLs from Google search links using Apify LinkedIn Profile Scraper.
Extracts name and company from Google search URLs and searches for LinkedIn profiles.
"""

import csv
import os
import sys
import time
import urllib.parse
from pathlib import Path
from apify_client import ApifyClient

CSV_FILE = Path(__file__).parent / "kontakty_unified.csv"
APIFY_ACTOR = "harvestapi/linkedin-profile-search"  # No Cookies, searchQuery for name

def extract_name_company_from_google_search(url: str) -> tuple:
    """Extract name and company from Google search URL"""
    try:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        q = params.get('q', [''])[0]
        
        # Parse query like "Jan Kodytek JPF Czech s.r.o. site:linkedin.com"
        parts = q.replace('site:linkedin.com', '').strip().split()
        if len(parts) >= 2:
            name = ' '.join(parts[:2])  # First name + last name
            company = ' '.join(parts[2:]) if len(parts) > 2 else ''
            return name.strip(), company.strip()
    except:
        pass
    return None, None

def search_linkedin_by_name_company(client: ApifyClient, name: str, company: str) -> dict:
    """
    Search for LinkedIn profile by name using HarvestAPI LinkedIn Profile Search (No Cookies).
    Uses searchQuery for fuzzy search by full name.
    """
    print(f"  Hledám: {name} @ {company}...", end=" ", flush=True)
    
    try:
        if not name or not name.strip():
            print("✗ (chybí jméno)")
            return {}
        
        run_input = {
            "profileScraperMode": "Short",
            "searchQuery": name.strip(),
            "maxItems": 10,
        }
        
        run = client.actor(APIFY_ACTOR).call(run_input=run_input)
        run_result = client.run(run["data"]["id"]).wait_for_finish()
        
        dataset = client.dataset(run_result["defaultDatasetId"])
        items = list(dataset.iterate_items())
        
        name_parts_lower = name.lower().split()
        for item in items:
            profile_name = (item.get("fullName") or item.get("name") or "").lower()
            profile_company = (item.get("currentCompany") or item.get("company") or "").lower()
            profile_url = item.get("profileUrl") or item.get("url") or item.get("linkedInUrl") or ""
            if not profile_url and item.get("publicIdentifier"):
                profile_url = f"https://www.linkedin.com/in/{item['publicIdentifier']}"
            
            name_ok = all(part in profile_name for part in name_parts_lower if len(part) > 2)
            company_ok = not company or company.lower() in profile_company
            
            if name_ok and company_ok and profile_url:
                if not profile_url.startswith("http"):
                    profile_url = "https://" + profile_url
                result = {
                    "linkedinUrl": profile_url,
                    "headline": item.get("headline", ""),
                    "currentPosition": item.get("title") or item.get("currentPosition", ""),
                    "company": item.get("currentCompany") or item.get("company", ""),
                }
                print("✓")
                return result
        
        print("✗ (nenalezeno)")
        return {}
    except Exception as e:
        err_msg = str(e)
        print(f"✗ Error: {err_msg[:120]}")
        if "authentication" in err_msg.lower() or "token" in err_msg.lower():
            print("   → Zkontrolujte: 1) APIFY_API_TOKEN 2) V Apify Console přidejte actor z Store (HarvestAPI LinkedIn Search)")
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
        sys.exit(1)
    
    client = ApifyClient(api_token)
    
    # Read CSV and find contacts from FAIL - jaro 2025 without LinkedIn profiles
    print(f"📖 Reading {CSV_FILE}...")
    rows = []
    contacts_to_find = []
    
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for i, row in enumerate(reader):
            linkedin = row.get('LinkedIn profil', '').strip()
            ucastnil = row.get('Účastnil se', '').strip()
            jmeno = row.get('Jméno', '').strip()
            prijmeni = row.get('Příjmení', '').strip()
            firma = row.get('Společnost / Firma', '').strip()
            
            # Find contacts from FAIL - jaro 2025 without LinkedIn profile
            if 'FAIL - jaro 2025' in ucastnil and not linkedin:
                if jmeno and prijmeni:  # Only if we have name
                    contacts_to_find.append({
                        'row_idx': i,
                        'jmeno': jmeno,
                        'prijmeni': prijmeni,
                        'firma': firma,
                        'name': f"{jmeno} {prijmeni}",
                    })
            
            rows.append(row)
    
    print(f"\n📊 Found {len(contacts_to_find)} contacts from FAIL - jaro 2025 without LinkedIn profiles")
    
    # Limit for testing: python3 find_linkedin_from_google_search.py --limit 5
    limit = None
    if "--limit" in sys.argv:
        i = sys.argv.index("--limit")
        if i + 1 < len(sys.argv):
            try:
                limit = int(sys.argv[i + 1])
                contacts_to_find = contacts_to_find[:limit]
                print(f"🧪 TEST MODE: processing only first {limit} contacts")
            except ValueError:
                pass
    
    print(f"💰 Estimated cost: ~{len(contacts_to_find) * 0.01:.2f} USD (Apify pricing)")
    
    if not contacts_to_find:
        print("✅ All contacts from FAIL - jaro 2025 already have LinkedIn profiles!")
        return
    
    # Auto-confirm (no interactive input needed)
    print(f"\n🚀 Starting search for LinkedIn URLs...")
    
    # Find LinkedIn URLs
    updated_count = 0
    failed_count = 0
    
    for idx, contact in enumerate(contacts_to_find, 1):
        print(f"\n[{idx}/{len(contacts_to_find)}] {contact['name']}")
        
        try:
            profile_data = search_linkedin_by_name_company(
                client,
                contact['name'],
                contact['firma']
            )
            
            if profile_data and profile_data.get("linkedinUrl"):
                linkedin_url = profile_data["linkedinUrl"]
                # Normalize URL
                if not linkedin_url.startswith("http"):
                    linkedin_url = "https://" + linkedin_url
                linkedin_url = linkedin_url.replace("cz.linkedin.com", "www.linkedin.com")
                linkedin_url = linkedin_url.replace("sk.linkedin.com", "www.linkedin.com")
                
                rows[contact['row_idx']]['LinkedIn profil'] = linkedin_url
                updated_count += 1
                print(f"  → Found: {linkedin_url[:60]}")
                
                # Also update position/company if available
                if profile_data.get("currentPosition"):
                    rows[contact['row_idx']]['Pracovní pozice'] = profile_data["currentPosition"]
                if profile_data.get("company") and not rows[contact['row_idx']].get('Společnost / Firma', '').strip():
                    rows[contact['row_idx']]['Společnost / Firma'] = profile_data["company"]
            else:
                failed_count += 1
        except Exception as e:
            print(f"  ✗ Exception: {str(e)[:100]}")
            failed_count += 1
        
        # Rate limiting - be nice to Apify
        if idx < len(contacts_to_find):
            time.sleep(2)  # 2 second delay between requests
        
        # Save progress every 10 contacts
        if idx % 10 == 0:
            print(f"\n💾 Saving progress... ({updated_count} found so far)")
            with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
    
    # Save updated CSV
    if updated_count > 0:
        print(f"\n💾 Saving updated CSV...")
        with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"\n✅ Updated {updated_count} LinkedIn URLs!")
        print(f"   Zbývá kontaktů bez LinkedIn: {len(contacts_to_find) - updated_count}")
        print(f"   Neúspěšných hledání: {failed_count}")
    else:
        print("\n⚠️  Nebyly nalezeny žádné LinkedIn URL.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        os.environ["APIFY_API_TOKEN"] = sys.argv[1]
    
    main()
