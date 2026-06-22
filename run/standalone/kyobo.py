#!/usr/bin/env python3
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from run.standalone._runner import run_env_scraper
from scrapers.kyobo_core import scrape_kyobo

K = "KYOBO_URLS_JSON"
NM = "교보증권"
if __name__ == "__main__":
    run_env_scraper(env_key=K, firm_name=NM, scrape_func=scrape_kyobo)
