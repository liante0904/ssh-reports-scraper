class ScraperConfigError(ValueError):
    """Raised when a scraper receives config in an unsupported shape."""


def normalize_cfg(cfg, *, firm_key: str):
    if isinstance(cfg, dict):
        return cfg
    if isinstance(cfg, list):
        return {"urls": cfg}
    if isinstance(cfg, str):
        return {"url": cfg}
    raise ScraperConfigError(
        f"{firm_key}: config must be JSON object or URL list, got {type(cfg).__name__}"
    )


def require_keys(cfg, keys, *, firm_key: str):
    missing = [key for key in keys if key not in cfg]
    if missing:
        raise ScraperConfigError(
            f"{firm_key}: full config dict required; missing keys: {', '.join(missing)}"
        )
    return cfg
