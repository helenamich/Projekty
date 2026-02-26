#!/usr/bin/env python3
"""
Merge CSV files into unified contact database for Airtable import.
- Loads all relevant CSVs (excludes merged_emails*)
- Standardizes columns, merges on email
- Účastnil se: comma-separated program names; DLM 1-4, DLM 5, DLM 6 → DLM 1-6
- HR kontakt: extracted from note-like columns (patterns: name, email, HR)
- Output: single CSV with target columns
"""

import csv
import re
import os
from pathlib import Path
from collections import defaultdict

# --- Config: directory and exclude pattern ---
DIR = Path(__file__).resolve().parent
EXCLUDE_PATTERN = re.compile(r"merged_emails", re.I)

# Program name from filename; consolidation: DLM 1-4, DLM 5, DLM 6 → "DLM 1-6"
PROGRAM_ALIAS = {
    "dlm 1-4": "DLM 1-6 (2021-2023)",
    "dlm 1-6": "DLM 1-6 (2021-2023)",
    "dlm5": "DLM 1-6 (2021-2023)",
    "dlm6": "DLM 1-6 (2021-2023)",
    "dlm 1-6 (2021-2023)": "DLM 1-6 (2021-2023)",
}
def program_from_filename(name: str) -> str:
    base = Path(name).stem
    key = base.lower().replace(" - list 1", "").strip()
    return PROGRAM_ALIAS.get(key, base)

# Column mapping per file: (header_row: bool, cols: dict)
# cols: jmeno, prijmeni, email, telefon, linkedin, pozice, firma, note_indices (for HR)
def safe_get(row, i, default=""):
    if i is None or i < 0 or i >= len(row):
        return default
    v = row[i]
    return (v or "").strip()

def parse_dlm16_program(source_label: str) -> str:
    """Map source labels to consolidated program name."""
    lower = (source_label or "").lower()
    if "dlm" in lower and ("1" in lower or "2" in lower or "3" in lower or "4" in lower or "5" in lower or "6" in lower):
        if "7" not in lower and "jaro 2024" not in lower:
            return "DLM 1-6 (2021-2023)"
    return source_label or ""

# File configs: (has_header, email_col, jmeno_col, prijmeni_col, telefon_col, linkedin_col, pozice_col, firma_col, [note_col_indices])
# dlm 1-4: header Jméno,Email,Firma,Oslovení,Zdroj -> 0,1,2,3,4. Jméno is full name.
CONFIGS = {
    "ACA 2024 - List 1.csv": {
        "header": False,
        "email": 4, "jmeno": 2, "prijmeni": 1, "osloveni": 3, "telefon": 5, "linkedin": None, "pozice": None, "firma": 6,
        "notes": list(range(15, 30)),  # wide range for long text
        "program": "ACA 2024",
    },
    "AILM - List 1.csv": {
        "header": False,
        "email": 3, "jmeno": 0, "prijmeni": 1, "osloveni": 2, "telefon": 4, "linkedin": None, "pozice": None, "firma": 6,
        "notes": list(range(10, 25)),
        "program": "AILM - podzim 2024",
    },
    "dlm 1-4.csv": {
        "header": True,
        "email": 1, "jmeno": 0, "prijmeni": None, "osloveni": 3, "telefon": None, "linkedin": None, "pozice": None, "firma": 2,
        "notes": [],
        "program": "DLM 1-6 (2021-2023)",
    },
    "DLM5.csv": {
        "header": False,
        "email": 1, "jmeno": 0, "prijmeni": None, "osloveni": 3, "telefon": 4, "linkedin": None, "pozice": 5, "firma": 2,
        "notes": list(range(6, 12)),
        "program": "DLM 1-6 (2021-2023)",
    },
    "dlm6.csv": {
        "header": False,
        "email": 4, "jmeno": 2, "prijmeni": 1, "osloveni": 3, "telefon": 6, "linkedin": 8, "pozice": 7, "firma": 5,
        "notes": [],
        "program": "DLM 1-6 (2021-2023)",
    },
    "DLM7 - List 1.csv": {
        "header": False,
        "email": 6, "jmeno": 1, "prijmeni": 4, "osloveni": 2, "telefon": 7, "linkedin": [21, 22], "pozice": 8, "firma": 5,
        "notes": list(range(9, 25)),  # Extended to catch more
        "program": "DLM 7 - jaro 2024",
    },
    "FAIL - jaro 2025 - List 1.csv": {
        "header": False,
        "email": 4, "jmeno": 1, "prijmeni": 2, "osloveni": 3, "telefon": 5, "linkedin": 45, "pozice": None, "firma": 7,
        "notes": list(range(8, 25)),
        "program": "FAIL - jaro 2025",
    },
    "FAIL - podzim 2025 - List 1.csv": {
        "header": False,
        # Pozice je v exportu někdy v col 6, jindy v col 15 (např. "Advanced Business Consultant")
        "email": 3, "jmeno": 1, "prijmeni": 2, "osloveni": 4, "telefon": 5, "linkedin": [31, 32], "pozice": [6, 15], "firma": 7,
        "notes": list(range(8, 40)),  # Extended
        "program": "FAIL - podzim 2025",
    },
}

# Split "Jméno" full name when prijmeni not available (e.g. "Adriana Lososová" -> Jméno, Příjmení)
def split_full_name(full: str, jmeno: str, prijmeni: str):
    full = (full or "").strip()
    jmeno = (jmeno or "").strip()
    prijmeni = (prijmeni or "").strip()
    if prijmeni:
        return jmeno or full, prijmeni
    if not full:
        return jmeno, prijmeni
    parts = full.split(None, 1)
    if len(parts) == 1:
        return parts[0], prijmeni or ""
    return parts[0], parts[1]

# Normalize email for merge
def norm_email(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    # take first email if multiple
    for part in re.split(r"[\s,;]+", s):
        if "@" in part and "." in part:
            return part
    return s

# Clean LinkedIn URL - keep only direct LinkedIn profiles, remove Google search and invalid formats
def clean_linkedin_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    
    # Skip Google search URLs
    if "google.com/search" in url.lower() or "google.cz/search" in url.lower():
        return ""
    
    # Skip if it's not a LinkedIn URL
    if "linkedin.com" not in url.lower() and "linked.in" not in url.lower():
        return ""
    
    # Handle URLs without https:// prefix (add it)
    if url.startswith("linkedin.com") or url.startswith("www.linkedin.com"):
        url = "https://" + url
    
    # Extract direct LinkedIn URL
    # Match: https://www.linkedin.com/in/... or https://linkedin.com/in/... or https://cz.linkedin.com/in/...
    # Also handle URLs with special characters (URL encoded)
    match = re.search(r'(https?://(?:www\.|cz\.|sk\.|at\.)?linkedin\.com/in/[^\s<>"\'\)\?]+)', url, re.I)
    if match:
        cleaned = match.group(1)
        # Remove query parameters and fragments
        cleaned = cleaned.split('?')[0].split('#')[0]
        # Normalize to www.linkedin.com
        cleaned = re.sub(r'https?://(?:cz|sk|at|www)?\.?linkedin\.com', 'https://www.linkedin.com', cleaned, flags=re.I)
        # Handle double slashes
        cleaned = cleaned.replace('//www', '//www').replace('linkedin.com//', 'linkedin.com/')
        return cleaned
    
    # Try linked.in format
    match = re.search(r'(https?://linked\.in/[^\s<>"\'\)\?]+)', url, re.I)
    if match:
        return match.group(1).split('?')[0].split('#')[0]
    
    return ""

# Extract HR contacts from text: look for name-like + email, or "HR", "personalist", "kontakt"
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
HR_KEYWORDS = re.compile(r"\b(HR|personalist|personální|kontakt|fakturace|Tereza|Martina|Slezákov|Tyšerov|Slezakov|Tyserov)\b", re.I)

def extract_hr_from_text(text: str) -> list[str]:
    if not text or not isinstance(text, str):
        return []
    found = []
    # Emails in text
    for m in EMAIL_RE.finditer(text):
        email = m.group(0)
        start = max(0, m.start() - 80)
        snippet = text[start : m.end() + 40]
        if HR_KEYWORDS.search(snippet) or "kontakt" in snippet.lower() or "faktur" in snippet.lower():
            # try to get a name before email (capitalized word)
            name_cand = re.findall(r"([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+(?:\s+[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+)*)\s*[,:]?\s*" + re.escape(email), text)
            if name_cand:
                found.append(f"{name_cand[0].strip()} ({email})")
            else:
                found.append(email)
    return list(dict.fromkeys(found))

def read_csv_rows(path: Path, has_header: bool):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        if has_header:
            next(reader, None)
        for row in reader:
            yield row

def main():
    # 0) Load bounced emails
    bounced_emails = set()
    bounced_file = DIR / "merged_emails_old_dlm_2024 vcetne bounced.csv"
    if bounced_file.exists():
        with open(bounced_file, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = norm_email(row.get("Email", ""))
                bounced = row.get("Bounced", "").strip().lower()
                if email and bounced == "bounced":
                    bounced_emails.add(email)
        print(f"Načteno {len(bounced_emails)} bounced emailů")
    
    # 1) Collect all rows keyed by normalized email
    by_email = defaultdict(lambda: {
        "jmeno": "", "prijmeni": "", "email": "", "osloveni": set(), "telefon": set(), "linkedin": set(),
        "pozice": set(), "firma": set(), "programs": [], "hr_contacts": set(),
    })

    for fname, cfg in CONFIGS.items():
        path = DIR / fname
        if not path.exists():
            print(f"Skip (not found): {fname}")
            continue
        program = cfg["program"]
        notes_cols = cfg.get("notes", [])

        for row in read_csv_rows(path, cfg["header"]):
            if not row:
                continue
            
            # Filter dlm 1-4.csv: only include DLM participants (Zdroj column contains "DLM")
            if fname == "dlm 1-4.csv":
                zdroj = safe_get(row, 4)  # Zdroj is column index 4
                if not zdroj or "DLM" not in zdroj.upper():
                    continue
            
            email = norm_email(safe_get(row, cfg["email"]))
            if not email or "@" not in email:
                continue

            jmeno = safe_get(row, cfg["jmeno"])
            prijmeni = safe_get(row, cfg["prijmeni"])
            # When only one name column has full name (e.g. "Adriana Lososová"), split it
            if (cfg.get("prijmeni") is None or not prijmeni) and jmeno and " " in jmeno:
                jmeno, prijmeni = split_full_name(jmeno, "", "")

            rec = by_email[email]
            if not rec["email"]:
                rec["email"] = email
            if jmeno:
                rec["jmeno"] = rec["jmeno"] or jmeno
            if prijmeni:
                rec["prijmeni"] = rec["prijmeni"] or prijmeni

            osloveni = safe_get(row, cfg.get("osloveni"))
            if osloveni:
                rec["osloveni"].add(osloveni)

            t = safe_get(row, cfg["telefon"])
            if t:
                rec["telefon"].add(t)
            # Get LinkedIn from specified column(s) OR search in all columns
            linkedin_found = False
            
            # First try specified LinkedIn column(s)
            if cfg["linkedin"] is not None:
                linkedin_cols = cfg["linkedin"] if isinstance(cfg["linkedin"], list) else [cfg["linkedin"]]
                for col_idx in linkedin_cols:
                    ln = safe_get(row, col_idx)
                    if ln and "linkedin" in ln.lower():
                        # Handle LinkedIn URLs without https:// prefix
                        if ln.startswith("linkedin.com") or ln.startswith("www.linkedin.com"):
                            ln = "https://" + ln
                        cleaned_ln = clean_linkedin_url(ln)
                        if cleaned_ln:
                            rec["linkedin"].add(cleaned_ln)
                            linkedin_found = True
                            break
            
            # If not found in specified column, search ALL columns (including notes)
            if not linkedin_found:
                # Search in all columns for LinkedIn URLs
                for i in range(len(row)):
                    text = safe_get(row, i)
                    if text and isinstance(text, str):
                        # Look for LinkedIn URLs (with or without https://)
                        linkedin_match = re.search(r'(?:https?://)?(?:www\.)?(?:linkedin\.com/in/[^\s<>"\'\)\?]+|linked\.in/[^\s<>"\'\)\?]+)', text, re.I)
                        if linkedin_match:
                            ln_url = linkedin_match.group(0)
                            # Skip Google search URLs
                            if "google.com/search" in text.lower():
                                continue
                            # Add https:// if missing
                            if ln_url.startswith("linkedin.com") or ln_url.startswith("www.linkedin.com"):
                                ln_url = "https://" + ln_url
                            cleaned_ln = clean_linkedin_url(ln_url)
                            if cleaned_ln:
                                rec["linkedin"].add(cleaned_ln)
                                linkedin_found = True
                                break
            # Pozice může být v jednom sloupci nebo v seznamu sloupců (vezmeme první neprázdný)
            poz_cfg = cfg.get("pozice")
            if isinstance(poz_cfg, list):
                for col_idx in poz_cfg:
                    p = safe_get(row, col_idx)
                    if p:
                        rec["pozice"].add(p)
                        break
            else:
                p = safe_get(row, poz_cfg)
                if p:
                    rec["pozice"].add(p)
            fm = safe_get(row, cfg["firma"])
            if fm:
                rec["firma"].add(fm)

            rec["programs"].append(program)

            for i in notes_cols:
                text = safe_get(row, i)
                for hr in extract_hr_from_text(text):
                    rec["hr_contacts"].add(hr)

    # 2) Build output rows
    target_columns = [
        "Jméno", "Příjmení", "Email", "Oslovení", "Telefon", "LinkedIn profil", "Pracovní pozice",
        "Společnost / Firma", "Účastnil se", "HR kontakt", "Stav",
    ]
    out_rows = []

    # Sort: rows with name first (by příjmení, jméno), then by email
    def sort_key(item):
        rec = item[1]
        p = (rec["prijmeni"] or "zzz").lower()
        j = (rec["jmeno"] or "zzz").lower()
        return (p, j)

    for email, rec in sorted(by_email.items(), key=sort_key):
        # Účastnil se: unique program names, already consolidated (DLM 1-6)
        programs = list(dict.fromkeys(rec["programs"]))
        ucastnil = ", ".join(programs)

        osloveni = "; ".join(sorted(rec["osloveni"])) if rec["osloveni"] else ""
        telefon = "; ".join(sorted(rec["telefon"])) if rec["telefon"] else ""
        linkedin = "; ".join(sorted(rec["linkedin"])) if rec["linkedin"] else ""
        pozice = "; ".join(sorted(rec["pozice"])) if rec["pozice"] else ""
        firma = "; ".join(sorted(rec["firma"])) if rec["firma"] else ""
        hr = "; ".join(sorted(rec["hr_contacts"])) if rec["hr_contacts"] else ""

        # Determine status based on bounced emails
        stav = "Neaktivní" if email in bounced_emails else "Aktivní"
        
        out_rows.append({
            "Jméno": rec["jmeno"],
            "Příjmení": rec["prijmeni"],
            "Email": rec["email"],
            "Oslovení": osloveni,
            "Telefon": telefon,
            "LinkedIn profil": linkedin,
            "Pracovní pozice": pozice,
            "Společnost / Firma": firma,
            "Účastnil se": ucastnil,
            "HR kontakt": hr,
            "Stav": stav,
        })

    # 3) Write CSV
    out_path = DIR / "kontakty_unified.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=target_columns)
        w.writeheader()
        w.writerows(out_rows)

    print(f"Written {len(out_rows)} contacts to {out_path.name}")

if __name__ == "__main__":
    main()
