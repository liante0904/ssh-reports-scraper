from utils.ls_pdf_verifier import normalize_ls_title, title_matches_pdf


def test_title_matching_accepts_exact_title_with_formatting_noise():
    expected = "[김윤정 ESG] 정책발 받는 시장4: 하반기 정책, MSCI 기준·대만 사례 비교"
    observed = "LS Securities Research\n정책발 받는 시장4: 하반기 정책, MSCI 기준 대만 사례 비교"
    assert title_matches_pdf(expected, observed)


def test_title_matching_rejects_a_different_report_with_some_generic_terms():
    expected = "정책발 받는 시장4: 하반기 정책, MSCI 기준·대만 사례 비교"
    observed = "테크/모빌리티 Weekly | 반도체 기술적 약세장 진입"
    assert not title_matches_pdf(expected, observed)


def test_normalize_title_removes_bracketed_author_and_punctuation():
    assert normalize_ls_title("[김윤정 ESG] 정책발: MSCI") == "정책발msci"
