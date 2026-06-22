#!/usr/bin/env python3
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from run.standalone._runner import run_env_scraper
from scrapers.sks_core import scrape_sks

K = "SKS_URLS_JSON"
NM = "SK증권"
if __name__ == "__main__":
    run_env_scraper(env_key=K, firm_name=NM, scrape_func=scrape_sks)
