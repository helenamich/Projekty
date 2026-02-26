#!/usr/bin/env python3
"""
Převede Google search odkazy na přímé LinkedIn profily (jen když se shoduje firma).
1. Načte FAIL - jaro 2025 - List 1.csv, najde řádky kde sloupec LinkedIn obsahuje google.com/search
2. Z URL vytáhne vyhledávací dotaz (parametr q) a firmu kontaktu (sloupec 7)
3. Zavolá Google Custom Search API; z výsledků vezme první odkaz na linkedin.com/in/.
   Je-li u kontaktu vyplněná firma, bere se jen výsledek se shodnou firmou v titulku/snippetu;
   bez firmy se bere první LinkedIn odkaz
4. Aktualizuje kontakty_unified.csv (podle emailu) – doplní LinkedIn profil

Potřeba: GOOGLE_API_KEY a GOOGLE_CSE_ID (Custom Search Engine).
Vytvoření: https://programmablesearchengine.google.com/ (vyhledávání po celém webu)
API klíč: https://console.cloud.google.com/ (Custom Search API)
"""

import csv
import os
import re
import sys
import time
import urllib.parse
import warnings
from pathlib import Path

warnings.filterwarnings("ignore", message=".*duckduckgo_search.*renamed.*")

try:
    import requests
except ImportError:
    requests = None
try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

DIR = Path(__file__).resolve().parent
FAIL_CSV = DIR / "FAIL - jaro 2025 - List 1.csv"
UNIFIED_CSV = DIR / "kontakty_unified.csv"
LINKEDIN_COL_INDEX = 45
EMAIL_COL_INDEX = 4
FIRMA_COL_INDEX = 7


def get_query_from_google_url(url: str) -> str:
    """Z Google search URL vrátí vyhledávací dotaz (parametr q)."""
    if not url or "google" not in url.lower():
        return ""
    try:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        q = params.get("q", [""])[0]
        return (q or "").strip()
    except Exception:
        return ""


def normalize_firma_for_match(firma: str) -> str:
    """Pro porovnání: malá písmena, bez s.r.o. / a.s. atd., zkrácené mezery."""
    if not firma:
        return ""
    s = firma.lower().strip()
    for suffix in (" s.r.o.", " a.s.", " s.r.o", " a.s", ", s.r.o.", ", a.s."):
        s = s.replace(suffix, "")
    s = " ".join(s.split())
    return s


def firma_matches(firma: str, title: str, snippet: str) -> bool:
    """
    Pokud firma není zadaná → True (bereme první výsledek).
    Pokud firma je zadaná → True jen když je firma (nebo její významná část) v title nebo snippet.
    """
    if not firma or not firma.strip():
        return True
    if not (title or snippet):
        return False
    norm = normalize_firma_for_match(firma)
    if not norm:
        return True
    text = ((title or "") + " " + (snippet or "")).lower()
    words = [w for w in norm.split() if len(w) > 2]
    if not words:
        return norm in text
    return any(w in text for w in words) or norm in text


def first_linkedin_from_google_search(
    api_key: str, cse_id: str, query: str, firma: str
) -> str:
    """
    Zavolá Google Custom Search API a vrátí první odkaz na linkedin.com/in/,
    u kterého se v titulku nebo snippetu shoduje firma.
    """
    if not query or not api_key or not cse_id:
        return ""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": api_key, "cx": cse_id, "q": query, "num": 10}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        for item in data.get("items", []):
            link = item.get("link", "")
            if not link or "linkedin.com/in/" not in link.lower():
                continue
            title = (item.get("title") or "").strip()
            snippet = (item.get("snippet") or "").strip()
            if not firma_matches(firma, title, snippet):
                continue
            link = link.split("?")[0].split("#")[0]
            if "linkedin.com" in link:
                return link
    except Exception as e:
        print(f"    API chyba: {e}")
    return ""


def first_linkedin_from_google_page(google_url: str) -> str:
    """
    Načte přímo stránku Google vyhledávání (URL z CSV) a z HTML vytáhne první odkaz na linkedin.com/in/.
    Bez API klíče – funguje, když Google vrátí normální výsledky (ne captcha).
    """
    if not google_url or not requests:
        return ""
    # Google často používá /url?q=SKUTECNA_URL – hledáme linkedin.com/in/ v href nebo v q=
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "cs,en;q=0.9",
    }
    try:
        r = requests.get(google_url, headers=headers, timeout=15)
        r.raise_for_status()
        html = r.text
        # 1) Odkazy ve tvaru /url?q=https://www.linkedin.com/in/...
        for m in re.finditer(r'/url\?q=(https?%3A%2F%2F[^&"\']+)|/url\?q=(https?://[^&"\']+)', html):
            raw = m.group(1) or m.group(2) or ""
            if raw:
                url = urllib.parse.unquote(raw) if "%" in raw else raw
                if "linkedin.com/in/" in url.lower():
                    url = url.split("?")[0].split("#")[0]
                    if url.startswith("http"):
                        return url
        # 2) Přímé href="https://www.linkedin.com/in/..."
        for m in re.finditer(r'href=["\'](https?://[^"\']*linkedin\.com/in/[^"\']+)["\']', html, re.I):
            url = m.group(1).split("?")[0].split("#")[0]
            if "linkedin.com" in url:
                return url
        # 3) Jakýkoli výskyt https://...linkedin.com/in/...
        for m in re.finditer(r'https?://(?:www\.)?linkedin\.com/in/[^\s"\'<>\)]+', html, re.I):
            url = m.group(0).split("?")[0].split("#")[0]
            if "linkedin.com" in url:
                return url
    except Exception as e:
        print(f"    Chyba načtení stránky: {e}")
    return ""


def _extract_first_linkedin_from_html(html: str) -> str:
    """Z libovolného HTML vytáhne první odkaz na linkedin.com/in/."""
    if not html:
        return ""
    # Odkazy ve tvaru /url?q=...
    for m in re.finditer(r'/url\?q=(https?%3A%2F%2F[^&"\']+)|/url\?q=(https?://[^&"\']+)', html):
        raw = m.group(1) or m.group(2) or ""
        if raw:
            url = urllib.parse.unquote(raw) if "%" in raw else raw
            if "linkedin.com/in/" in url.lower():
                url = url.split("?")[0].split("#")[0]
                if url.startswith("http"):
                    return url
    # Přímé href na LinkedIn
    for m in re.finditer(r'href=["\'](https?://[^"\']*linkedin\.com/in/[^"\']+)["\']', html, re.I):
        url = m.group(1).split("?")[0].split("#")[0]
        if "linkedin.com" in url:
            return url
    # Jakýkoli výskyt URL
    for m in re.finditer(r'https?://(?:www\.)?linkedin\.com/in/[^\s"\'<>\)]+', html, re.I):
        url = m.group(0).split("?")[0].split("#")[0]
        if "linkedin.com" in url:
            return url
    return ""


def first_linkedin_from_duckduckgo_html(query: str) -> str:
    """Načte DuckDuckGo HTML vyhledávání (bez API) a vrátí první LinkedIn odkaz."""
    if not query or not requests:
        return ""
    url = "https://html.duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = requests.post(url, data={"q": query}, headers=headers, timeout=15)
        r.raise_for_status()
        return _extract_first_linkedin_from_html(r.text)
    except Exception as e:
        print(f"    DDG HTML: {e}")
    return ""


def first_linkedin_from_bing_page(query: str) -> str:
    """Načte Bing vyhledávání (bez API) a vrátí první LinkedIn odkaz."""
    if not query or not requests:
        return ""
    url = "https://www.bing.com/search"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = requests.get(url, params={"q": query}, headers=headers, timeout=15)
        r.raise_for_status()
        return _extract_first_linkedin_from_html(r.text)
    except Exception as e:
        print(f"    Bing: {e}")
    return ""


def first_linkedin_from_duckduckgo(query: str, firma: str) -> str:
    """
    Vyhledá dotaz přes DuckDuckGo (bez API klíče) a vrátí první odkaz na linkedin.com/in/,
    u kterého se shoduje firma (pokud je zadaná).
    """
    if not query or DDGS is None:
        return ""
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=10):
                link = (r.get("href") or r.get("link") or "").strip()
                if not link or "linkedin.com/in/" not in link.lower():
                    continue
                title = (r.get("title") or "").strip()
                body = (r.get("body") or "").strip()
                if not firma_matches(firma, title, body):
                    continue
                link = link.split("?")[0].split("#")[0]
                if "linkedin.com" in link:
                    return link
    except Exception as e:
        print(f"    DuckDuckGo chyba: {e}")
    return ""


def main():
    api_key = os.getenv("GOOGLE_API_KEY")
    cse_id = os.getenv("GOOGLE_CSE_ID")
    use_google = api_key and cse_id
    if use_google and not requests:
        print("Nainstalujte: pip install requests")
        sys.exit(1)
    if not use_google:
        if not requests:
            print("Nainstalujte: pip install requests")
            sys.exit(1)
        print("Bez GOOGLE_API_KEY / GOOGLE_CSE_ID načtu přímo stránku Google (URL z CSV) a z ní vytáhnu první LinkedIn odkaz.\n")

    # 1) Načíst FAIL - jaro 2025: (email -> (query, firma)) kde sloupec 45 je Google search
    email_to_data = {}
    if not FAIL_CSV.exists():
        print(f"Soubor nenalezen: {FAIL_CSV}")
        sys.exit(1)
    with open(FAIL_CSV, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) <= max(LINKEDIN_COL_INDEX, EMAIL_COL_INDEX, FIRMA_COL_INDEX):
                continue
            cell = (row[LINKEDIN_COL_INDEX] or "").strip()
            if "google.com/search" in cell.lower() or "google.cz/search" in cell.lower():
                email = (row[EMAIL_COL_INDEX] or "").strip().lower()
                firma = (row[FIRMA_COL_INDEX] or "").strip()
                if email:
                    q = get_query_from_google_url(cell)
                    if q:
                        email_to_data[email] = (q, firma, cell)  # cell = celá Google URL

    if not email_to_data:
        print("Žádné Google search odkazy v FAIL - jaro 2025 (sloupec LinkedIn).")
        return

    print(f"Nalezeno {len(email_to_data)} kontaktů s Google search odkazem.")
    print("Doplním LinkedIn: u výsledků s firmou jen při shodě firmy, bez firmy první odkaz.\n")

    # Limit pro test: --limit 30
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            try:
                limit = int(sys.argv[idx + 1])
                email_to_data = dict(list(email_to_data.items())[:limit])
                print(f"(TEST: jen prvních {limit} kontaktů)\n")
            except ValueError:
                pass

    # 2) Pro každý email spustit vyhledávání, brát jen výsledek se shodnou firmou
    email_to_linkedin = {}
    rows = []
    headers = None
    with open(UNIFIED_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames)
        rows = list(reader)

    for i, (email, data) in enumerate(email_to_data.items(), 1):
        query, firma, google_url = data[0], data[1], data[2]
        print(f"[{i}/{len(email_to_data)}] {query[:50]}…")
        if use_google:
            link = first_linkedin_from_google_search(api_key, cse_id, query, firma)
        else:
            link = first_linkedin_from_google_page(google_url)
            if not link:
                link = first_linkedin_from_duckduckgo_html(query)
            if not link:
                link = first_linkedin_from_bing_page(query)
        if link:
            email_to_linkedin[email] = link
            print(f"    → LinkedIn: {link[:60]}…")
            for row in rows:
                if (row.get("Email") or "").strip().lower() == email and not (row.get("LinkedIn profil") or "").strip():
                    row["LinkedIn profil"] = link
                    break
        else:
            print("    → žádný vhodný LinkedIn")
        time.sleep(0.3)
        # Průběžné ukládání každých 10 kontaktů
        if i % 10 == 0 and headers and rows:
            with open(UNIFIED_CSV, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=headers)
                w.writeheader()
                w.writerows(rows)
            print(f"    💾 uloženo ({len(email_to_linkedin)} doplněno)")

    if not email_to_linkedin:
        print("\nNepodařilo se získat žádné LinkedIn URL.")
        return

    # 3) Finální zápis kontakty_unified.csv
    with open(UNIFIED_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    print(f"\nHotovo. Doplněno {len(email_to_linkedin)} LinkedIn profilů do kontakty_unified.csv.")


if __name__ == "__main__":
    main()
