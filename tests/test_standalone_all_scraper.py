import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.standalone_all_scraper import GA_STANDALONE_FIRMS, IMPORT_MAP, select_target_firms


def test_select_target_firms_default_includes_ga_and_ls():
    targets = select_target_firms(None, server_only=False, skip_ls=False)

    assert targets == list(IMPORT_MAP)
    assert "LS_0" in targets
    assert GA_STANDALONE_FIRMS - set(targets) == set()


def test_select_target_firms_server_only_excludes_ga_standalones():
    targets = select_target_firms(None, server_only=True, skip_ls=False)

    assert "LS_0" not in targets
    assert not (GA_STANDALONE_FIRMS & set(targets))
    assert "ShinHanInvest_1" in targets


def test_select_target_firms_explicit_firms_overrides_filters():
    targets = select_target_firms("LS_0,KBsec_4,NO_SUCH", server_only=True, skip_ls=True)

    assert targets == ["LS_0", "KBsec_4"]
