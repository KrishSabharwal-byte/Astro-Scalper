"""
Instrument Builder for Index Futures (NIFTY, BANKNIFTY, SENSEX).
Downloads Angel One OpenAPI Scrip Master and filters for active FUTIDX instruments
on NFO (NIFTY, BANKNIFTY) and BFO (SENSEX).
Generates scrips_index_futures.json.
"""

import os
import json
import logging
import urllib.request
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IndexInstrumentBuilder")

SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
TARGET_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrips_index_futures.json")


def build():
    logger.info("Downloading official Angel One OpenAPI Scrip Master for Index Futures...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
    }
    req = urllib.request.Request(SCRIP_MASTER_URL, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    logger.info(f"Total entries parsed from scrip master: {len(data)}")

    results = {
        "NIFTY": [],
        "BANKNIFTY": [],
        "SENSEX": [],
    }

    for item in data:
        exch = item.get("exch_seg")
        itype = item.get("instrumenttype")
        name = item.get("name")

        if itype == "FUTIDX":
            if exch == "NFO" and name in ("NIFTY", "BANKNIFTY"):
                results[name].append(item)
            elif exch == "BFO" and name == "SENSEX":
                results[name].append(item)

    for k, v in results.items():
        logger.info(f"Filtered {len(v)} contracts for {k}.")

    with open(TARGET_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Successfully saved {TARGET_FILE}")
    return results


if __name__ == "__main__":
    build()
