#!/usr/bin/env python3
"""
Pomocný skript pro snadné použití Apify API.
Umožňuje nastavit API token a spustit různé Apify úlohy.
"""

import os
import sys
from pathlib import Path

def set_api_token():
    """Interaktivně nastaví API token"""
    print("\n🔑 Nastavení Apify API tokenu")
    print("=" * 50)
    print("1. Zaregistrujte se na https://apify.com")
    print("2. Získejte API token z: https://console.apify.com/account/integrations")
    print("3. Zadejte token níže (nebo stiskněte Enter pro zrušení)\n")
    
    token = input("API token: ").strip()
    if token:
        os.environ["APIFY_API_TOKEN"] = token
        print("✅ Token nastaven!")
        return token
    else:
        print("❌ Zrušeno.")
        return None

def check_token():
    """Zkontroluje, jestli je nastaven API token"""
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("⚠️  API token není nastaven.")
        return set_api_token()
    return token

def main():
    print("\n🚀 Apify Helper")
    print("=" * 50)
    
    # Check token
    token = check_token()
    if not token:
        print("\n❌ API token není nastaven. Ukončuji.")
        sys.exit(1)
    
    print("\n📋 Dostupné úlohy:")
    print("1. Najít LinkedIn URL z Google search odkazů")
    print("2. Aktualizovat pozice a firmy z existujících LinkedIn profilů")
    print("3. Nastavit nový API token")
    print("0. Ukončit")
    
    choice = input("\nVyberte úlohu (0-3): ").strip()
    
    if choice == "1":
        print("\n🔍 Spouštím hledání LinkedIn URL z Google search odkazů...")
        os.system(f'python3 "{Path(__file__).parent / "find_linkedin_from_google_search.py"}"')
    elif choice == "2":
        print("\n📝 Spouštím aktualizaci pozic a firem z LinkedIn profilů...")
        os.system(f'python3 "{Path(__file__).parent / "update_linkedin_positions.py"}"')
    elif choice == "3":
        set_api_token()
        print("\n✅ Token aktualizován!")
    elif choice == "0":
        print("\n👋 Ukončuji.")
        sys.exit(0)
    else:
        print("\n❌ Neplatná volba.")

if __name__ == "__main__":
    # Allow passing token as argument
    if len(sys.argv) > 1:
        os.environ["APIFY_API_TOKEN"] = sys.argv[1]
        print(f"✅ Token nastaven z argumentu")
    
    main()
