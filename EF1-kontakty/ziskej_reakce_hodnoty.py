#!/usr/bin/env python3
"""Zjistí unikátní hodnoty v poli Reakce/výsledek."""

import json
import time
from pathlib import Path
from collections import Counter
from urllib.parse import quote
import requests

API_BASE = "https://api.airtable.com/v0"
BASE_ID = "appEXpqOEIElHzScl"

def get_token():
    with open(Path.home() / ".cursor" / "mcp.json") as f:
        return json.load(f)["mcpServers"]["airtable"]["env"]["AIRTABLE_API_KEY"]

def main():
    hdrs = {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}
    url = f"{API_BASE}/{BASE_ID}/{quote('Deals', safe='')}"
    
    values = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=hdrs, params=params, timeout=60)
        data = resp.json()
        
        for rec in data.get("records", []):
            v = rec.get("fields", {}).get("Reakce/výsledek", "")
            if v:
                values.append(v.strip())
        
        offset = data.get("offset")
        if not offset:
            break
    
    print("📊 Unikátní hodnoty v 'Reakce/výsledek':\n")
    counts = Counter(values)
    for val, count in counts.most_common():
        print(f"   {count:3}x  {val}")
    
    print(f"\n   Celkem: {len(values)} záznamů, {len(counts)} unikátních hodnot")

if __name__ == "__main__":
    main()
