from types import SimpleNamespace

import pytest

from modules import Koreainvestment_13 as koreainvestment


@pytest.mark.asyncio
async def test_koreainvestment_requires_shared_browser_endpoint(monkeypatch):
    class BrowserContext:
        async def __aenter__(self):
            return SimpleNamespace(chromium=SimpleNamespace(launch=lambda **_: None))

        async def __aexit__(self, *_):
            return False

    monkeypatch.delenv("PLAYWRIGHT_WS_ENDPOINT", raising=False)
    monkeypatch.setattr(koreainvestment.config, "get_urls", lambda *_: ["https://example.test"])
    monkeypatch.setattr(koreainvestment, "async_playwright", BrowserContext)

    with pytest.raises(RuntimeError, match="PLAYWRIGHT_WS_ENDPOINT is required"):
        await koreainvestment.Koreainvestment_playwright_checkNewArticle()
