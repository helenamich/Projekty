#!/usr/bin/env python3
"""
Upsert kontakty_unified.csv do Airtable (podle Email).

Použití:
  cd "/Users/helenamich/Desktop/KONTAKTY EF1 čištění"
  export AIRTABLE_TOKEN="pat_..."            # Airtable Personal Access Token
  export AIRTABLE_BASE_ID="appXXXXXXXXXXXXXX"
  export AIRTABLE_TABLE="Kontakty"          # název tabulky (nebo tblXXXXXXXXXXXXXX)
  python3 airtable_upsert.py

Volby:
  --csv "/cesta/k/csv"          (default: kontakty_unified.csv vedle skriptu)
  --email-field "Email"         (default: Email)
  --limit 100                   (zpracovat jen prvních N řádků)
  --dry-run                     (nic nezapisovat, jen spočítat změny)
  --overwrite-empty             (posílat i prázdné hodnoty = může mazat data v Airtable)

Poznámky:
- Airtable limit: max 10 záznamů na request.
- Skript NEPOSÍLÁ prázdné hodnoty (aby omylem nemařil existující data), pokud nedáš --overwrite-empty.
- Předpokládá, že v Airtable existují pole se stejnými názvy jako CSV hlavičky.
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from urllib.parse import quote


API_BASE = "https://api.airtable.com/v0"
API_META_BASE = "https://api.airtable.com/v0/meta/bases"
BATCH_SIZE = 10


def norm_email(s: str) -> str:
    return (s or "").strip().lower()


def chunked(items: List[dict], size: int) -> List[List[dict]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def airtable_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def request_with_backoff(method: str, url: str, *, headers: dict, json: Optional[dict] = None, params: Optional[dict] = None) -> dict:
    delay = 1.0
    for attempt in range(1, 8):
        resp = requests.request(method, url, headers=headers, json=json, params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            # rate limit / transient
            time.sleep(delay)
            delay = min(delay * 2, 20)
            continue
        if not resp.ok:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text[:500]}")
        return resp.json()
    raise RuntimeError(f"Airtable API still failing after retries: {method} {url}")


def resolve_table_name(token: str, base_id: str, table: str) -> str:
    """
    Airtable data API typicky používá table NAME v URL. U některých setupů tableId `tbl...` nefunguje.
    Pokud uživatel zadá `tbl...`, zkusíme ho přeložit na název tabulky přes metadata API.
    Vyžaduje scope `schema.bases:read` a access na danou base.
    """
    table = (table or "").strip()
    if not table:
        return table
    if not table.startswith("tbl"):
        return table

    url = f"{API_META_BASE}/{base_id}/tables"
    headers = airtable_headers(token)
    data = request_with_backoff("GET", url, headers=headers)
    for t in data.get("tables", []) or []:
        if t.get("id") == table:
            return t.get("name") or table
    return table


def get_table_field_names(token: str, base_id: str, table_name: str) -> Set[str]:
    """Vrátí množinu názvů polí v tabulce (metadata API)."""
    url = f"{API_META_BASE}/{base_id}/tables"
    headers = airtable_headers(token)
    data = request_with_backoff("GET", url, headers=headers)
    for t in data.get("tables", []) or []:
        if (t.get("name") or "") == table_name:
            fields = t.get("fields", []) or []
            return {str(f.get("name") or "") for f in fields if (f.get("name") or "")}
    return set()


def clean_field_name(name: str) -> str:
    """Očistí názvy polí z CSV (BOM, uvozovky, whitespace)."""
    s = (name or "").strip()
    s = s.lstrip("\ufeff")
    # někdy se do názvu omylem dostane uvozovka na konci/začátku
    if s.startswith('"') and s.endswith('"') and len(s) >= 2:
        s = s[1:-1].strip()
    s = s.rstrip('"').lstrip('"').strip()
    return s


# Mapování CSV sloupců na Airtable pole
FIELD_MAPPING = {
    "Email": "E-mail",
    "Účastnil se": "Koupil / účastnil se",
    "HR kontakt": "HR Kontakt",
}

# Pole, která jsou v Airtable multiselect (hodnoty oddělené čárkou v CSV)
MULTISELECT_FIELDS = {"Koupil / účastnil se"}


def map_field_name(csv_name: str) -> str:
    """Převede název CSV sloupce na Airtable pole."""
    cleaned = clean_field_name(csv_name)
    return FIELD_MAPPING.get(cleaned, cleaned)


def convert_multiselect(value: str) -> list:
    """Převede čárkou oddělené hodnoty na seznam pro multiselect."""
    if not value or not value.strip():
        return []
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items


def list_existing_by_email(token: str, base_id: str, table: str, email_field: str) -> Dict[str, str]:
    """Vrátí mapu email -> recordId pro existující záznamy."""
    # table name může obsahovat mezery → encode do URL
    url = f"{API_BASE}/{base_id}/{quote(table, safe='')}"
    headers = airtable_headers(token)
    out: Dict[str, str] = {}
    offset = None
    page = 0

    while True:
        page += 1
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = request_with_backoff("GET", url, headers=headers, params=params)
        recs = data.get("records", []) or []
        print(f"   … stránka {page}: {len(recs)} záznamů (celkem načteno: {len(out)})", flush=True)
        for rec in recs:
            fields = rec.get("fields", {}) or {}
            em = norm_email(str(fields.get(email_field, "") or ""))
            if em:
                out[em] = rec.get("id")
        offset = data.get("offset")
        if not offset:
            break
    return out


def build_airtable_fields(row: dict, *, overwrite_empty: bool, allowed_fields: Optional[Set[str]] = None) -> dict:
    fields = {}
    for k, v in row.items():
        if k is None:
            continue
        csv_name = clean_field_name(str(k))
        if not csv_name:
            continue
        # Mapovat CSV název na Airtable název
        airtable_name = map_field_name(csv_name)
        if allowed_fields is not None and airtable_name not in allowed_fields:
            continue
        if v is None:
            if overwrite_empty:
                fields[airtable_name] = "" if airtable_name not in MULTISELECT_FIELDS else []
            continue
        s = str(v)
        if not s.strip():
            if overwrite_empty:
                fields[airtable_name] = "" if airtable_name not in MULTISELECT_FIELDS else []
            continue
        # Multiselect pole převést na seznam
        if airtable_name in MULTISELECT_FIELDS:
            fields[airtable_name] = convert_multiselect(s)
        else:
            fields[airtable_name] = s.strip()
    return fields


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", dest="csv_path", default=str(Path(__file__).parent / "kontakty_unified.csv"))
    ap.add_argument("--email-field", default="Email")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--overwrite-empty", action="store_true")
    ap.add_argument("--skip-unknown-fields", action="store_true", help="Ignorovat CSV sloupce, které v Airtable tabulce neexistují")
    args = ap.parse_args()

    token = os.getenv("AIRTABLE_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table = os.getenv("AIRTABLE_TABLE", "").strip()

    if not token or not base_id or not table:
        raise SystemExit(
            "Chybí konfigurace. Nastav env proměnné:\n"
            "  AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE\n"
            "Např.:\n"
            "  export AIRTABLE_TOKEN=\"pat_...\"\n"
            "  export AIRTABLE_BASE_ID=\"app...\"\n"
            "  export AIRTABLE_TABLE=\"Kontakty\""
        )

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise SystemExit(f"CSV nenalezeno: {csv_path}")

    # Normalize table identifier (allow tbl... by resolving to table name if possible)
    try:
        resolved_table = resolve_table_name(token, base_id, table)
    except RuntimeError as e:
        raise SystemExit(
            "Nepodařilo se načíst metadata tabulek pro převod `tbl...` → název.\n"
            "Zkontroluj, že token má scope `schema.bases:read` a má přístup k base.\n"
            f"Detaily: {e}"
        )
    if resolved_table != table:
        print(f"ℹ️  AIRTABLE_TABLE je ID ({table}), používám název tabulky: {resolved_table}")
    table = resolved_table

    # Preflight: zjistit pole v Airtable tabulce (kvůli chybě UNKNOWN_FIELD_NAME)
    allowed_fields: Optional[Set[str]] = None
    try:
        allowed_fields = get_table_field_names(token, base_id, table)
    except RuntimeError as e:
        # bez schema scope to nemusí jít; pokračujeme bez filtrace
        allowed_fields = None
        print("⚠️  Nepodařilo se načíst schema tabulky (pokračuji bez kontroly názvů polí).")
        print(f"   {e}")

    if allowed_fields:
        # Ověřit, že email field existuje v Airtable (mapovaný název)
        email_field_mapped = map_field_name(clean_field_name(args.email_field))
        if email_field_mapped not in allowed_fields:
            raise SystemExit(
                f"V Airtable tabulce neexistuje pole '{email_field_mapped}'.\n"
                "Nejrychlejší řešení:\n"
                "- v Airtable vytvoř sloupec přesně s názvem 'E-mail' (nebo použij --email-field s názvem existujícího pole)\n"
                "- nebo nejdřív importuj `kontakty_unified.csv` přes Airtable UI, aby se pole vytvořila automaticky.\n"
            )

    # Load CSV
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    # Index existing Airtable by email (použij mapovaný název pole)
    airtable_email_field = map_field_name(clean_field_name(args.email_field))
    print(f"🔎 Načítám existující záznamy z Airtable (email pole: {airtable_email_field})…")
    try:
        existing = list_existing_by_email(token, base_id, table, airtable_email_field)
    except RuntimeError as e:
        raise SystemExit(
            "Airtable vrátil chybu při čtení záznamů.\n"
            "Nejčastější příčiny:\n"
            "- token nemá přístup k base (Access v tokenu)\n"
            "- chybí scope `data.records:read`\n"
            "- AIRTABLE_TABLE je špatně (zkus dát název tabulky přesně „Kontakty“)\n"
            f"\nDetaily: {e}"
        )
    print(f"   Nalezeno existujících emailů v Airtable: {len(existing)}")

    to_create: List[dict] = []
    to_update: List[dict] = []
    skipped_no_email = 0

    for row in rows:
        email = norm_email(row.get(args.email_field, "") or "")
        if not email:
            skipped_no_email += 1
            continue

        fields = build_airtable_fields(
            row,
            overwrite_empty=args.overwrite_empty,
            allowed_fields=allowed_fields if (allowed_fields and args.skip_unknown_fields) else None,
        )
        # Airtable email field must exist; ensure it is present if available
        # Mapovat CSV email field na Airtable field name
        airtable_email_field = map_field_name(clean_field_name(args.email_field))
        if airtable_email_field not in fields and email:
            fields[airtable_email_field] = email

        rec_id = existing.get(email)
        if rec_id:
            to_update.append({"id": rec_id, "fields": fields})
        else:
            to_create.append({"fields": fields})

    print(f"📄 CSV řádků ke zpracování: {len(rows)} (bez emailu přeskočeno: {skipped_no_email})")
    print(f"➕ Create: {len(to_create)}")
    print(f"♻️ Update: {len(to_update)}")

    if args.dry_run:
        print("🧪 Dry-run: nic nezapisuji.")
        return

    url = f"{API_BASE}/{base_id}/{quote(table, safe='')}"
    headers = airtable_headers(token)

    # Create
    if to_create:
        print("⬆️  Vytvářím nové záznamy…")
        for batch in chunked(to_create, BATCH_SIZE):
            try:
                request_with_backoff("POST", url, headers=headers, json={"records": batch, "typecast": True})
            except RuntimeError as e:
                msg = str(e)
                if "UNKNOWN_FIELD_NAME" in msg:
                    raise SystemExit(
                        "Airtable odmítl zápis kvůli neznámému názvu pole.\n"
                        "Nejrychlejší fix: v Airtable nejdřív importuj `kontakty_unified.csv` (vytvoří sloupce),\n"
                        "nebo spusť skript se `--skip-unknown-fields`.\n"
                        f"\nDetaily: {e}"
                    )
                raise
            time.sleep(0.2)

    # Update
    if to_update:
        print("⬆️  Aktualizuji existující záznamy…")
        for batch in chunked(to_update, BATCH_SIZE):
            try:
                request_with_backoff("PATCH", url, headers=headers, json={"records": batch, "typecast": True})
            except RuntimeError as e:
                msg = str(e)
                if "UNKNOWN_FIELD_NAME" in msg:
                    raise SystemExit(
                        "Airtable odmítl update kvůli neznámému názvu pole.\n"
                        "Zkontroluj názvy sloupců v Airtable vs CSV (nejrychlejší je nejdřív CSV import v UI).\n"
                        f"\nDetaily: {e}"
                    )
                raise
            time.sleep(0.2)

    print("✅ Hotovo.")


if __name__ == "__main__":
    main()

