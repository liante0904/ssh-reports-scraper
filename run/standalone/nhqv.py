#!/usr/bin/env python3
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from run.standalone._runner import run_env_scraper
from scrapers.nhqv_core import scrape_nhqv

K = "NHQV_URLS_JSON"
NM = "NH투자증권"
if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    run_env_scraper(
        env_key=K,
        firm_name=NM,
        scrape_func=scrape_nhqv,
        target_date=target_date,
    )
