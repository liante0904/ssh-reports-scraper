#!/usr/bin/env python3
"""View page에서 본문 텍스트 추출 (GA 백필용)"""
import json, re, sys, urllib.request, time

def extract_text(url):
    """URL에서 본문 텍스트 추출 (DS, 신한, 대신, 메리츠, 흥국)"""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=10)
        html = resp.read()
        for enc in ['utf-8', 'euc-kr', 'cp949']:
            try: return _parse(html.decode(enc), url); break
            except: continue
    except: pass
    return None

def _parse(html, url):
    # DS: div#bo_v_con
    m = re.search(r'id="bo_v_con"[^>]*>(.*?)</div>\s*</section', html, re.DOTALL)
    # 신한: mobile view.do 페이지
    if not m and 'shinhan' in url:
        divs = re.findall(r'<div[^>]*>(.*?)</div>', html, re.DOTALL)
        for d in divs:
            clean = re.sub(r'<[^>]+>', ' ', d)
            clean = re.sub(r'&nbsp;', ' ', clean)
            clean = re.sub(r'\s+', ' ', clean).strip()
            if len(clean) > 200:
                return clean[:10000]
    if not m: return None
    raw = m.group(1)
    clean = re.sub(r'<[^>]+>', ' ', raw)
    clean = re.sub(r'&nbsp;', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean[:10000] if len(clean) > 30 else None

if __name__ == "__main__":
    data = json.load(sys.stdin)
    results = {}
    for i, (rid, url) in enumerate(data):
        text = extract_text(url)
        if text:
            results[str(rid)] = text
        if (i+1) % 100 == 0: print(f"  {i+1}/{len(data)}", file=sys.stderr)
    json.dump(results, sys.stdout, ensure_ascii=False)
