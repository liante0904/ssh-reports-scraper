# `tbl_sec_reports` — 투자 데이터 제품 기준

## 목적

이 테이블은 단순 알림 로그가 아니라, 리서치 변화(종목·의견·목표가·근거)를
비교하는 투자 의사결정 입력이어야 한다. live catalog가 스키마 권위이며 이 문서는
DDL이 아닌 제품·운영 기준이다.

## 먼저 볼 단일 입력

```bash
bash scripts/ops_report_data_snapshot.sh --days 7
bash scripts/ops_report_daily_stats.sh --year 2026 --summary
```

첫 명령의 compact JSON만으로 freshness, URL 무결성, 투자 필드 커버리지와
stale firm을 판단한다. 전체 테이블 덤프, 소스 탐색, 장문의 운영 문서를 LLM의
첫 입력으로 사용하지 않는다. firm/day 원본은 장애 후보를 확인할 때만 연다.

## 컬럼을 세 층으로 분리한다

| 층 | 현재 컬럼 | 투자 판단에서의 역할 |
|---|---|---|
| 원천·식별 | `report_unique_key`, `firm_id`, `board_id`, `report_date`, `save_at`, `article_title`, `writer`, `pdf_url` | 중복 없는 시간축과 원문 증거 |
| 구조화 신호 | `stock_tickers`, `stock_names`, `sector`, `report_type`, `rating`, `target_price`, `revision_type`, `fnguide_summary_id` | 종목별 컨센서스 변화·필터·랭킹 |
| 근거·LLM | `article_text`, `gemini_summary`, `summary_model`, `summary_time`, `tags` | 투자 논리, 리스크, 촉매의 검증 가능한 요약 |

`telegram_sent`, `sync_status`, `pdf_sync_status`, `retry_count`, `archive_path`,
`gdrive_pdf_url`은 전달/아카이브 운영 상태다. 이를 투자 점수나 LLM 프롬프트의
핵심 신호로 섞지 않는다.

## 2026-07-13 live snapshot의 판단

- 최근 7일 1,672건은 제목·PDF URL·Telegram URL 누락이 0건이다. 수집/전달은 정상이다.
- 같은 기간 `article_text`와 LLM summary는 모두 0건, target price는 3건이다.
- 전체 311,165건 중 ticker 3,302건, 본문 1,877건, LLM summary 321건뿐이다.

따라서 현재 제품의 P0는 “더 많은 LLM 요약”이 아니라 **최신 리포트의 읽을 수 있는
근거를 안정적으로 확보하는 것**이다. `article_text`는 PDF 전문의 중복 저장이 아니라
증권사 사이트가 제공하는 HTML 요약/본문을 보존하는 용도다. 현재 PDF archive는 파일
메타데이터만 갖고 추출 본문을 저장하지 않으므로, 이 필드를 제거하면 유일한 텍스트
근거도 잃는다.

## 우선순위

1. **P0 — 최신 사이트 요약 확보와 실패 관측**: 사이트 HTML summary는 `article_text`에
   보존하고, PDF 전문 추출은 별도 archive/text 저장소로 둔다. 둘을 한 컬럼에 섞지 않는다.
   최근 7일 `article_text` 0건이면 해당 HTML-summary 수집 경로와 최신 PDF-text 추출 경로를
   각각 점검한다.
2. **P1 — 결정 필드의 fresh coverage**: 회사 리포트에 한해 ticker, rating, target price,
   revision type을 추출하고 `source_span`/추출시각/모델을 함께 저장한다. 값만 저장하면
   투자자가 근거를 확인할 수 없다.
3. **P2 — 비교 가능한 LLM 산출물**: 원문+결정 필드가 있는 행만 요약한다. 출력은
   `thesis`, `earnings_change`, `valuation_change`, `catalysts`, `risks`, `confidence`,
   `evidence_spans`의 구조화 JSON이어야 하며, 자유문장 하나로는 스크리닝을 만들지 않는다.
4. **P3 — UI/API**: 목록은 title/tag/목표가/의견/변경 방향만 전송한다. 원문과 evidence는
   report-detail API에서 필요할 때만 가져온다. 목록 응답에 `article_text`를 넣지 않는다.

## 펀드매니저용 최소 화면

각 회사 리포트 카드에서 `종목 · 의견 · 목표가 · 직전 대비 · 핵심 변화 · 리스크 · 근거`
를 한 화면에 보이고, `알 수 없음`을 빈값처럼 숨기지 않는다. 현재 데이터가 없는 경우
그 사실을 표시해야 LLM 결과를 과신하지 않는다.

## 다음 구현의 완료 기준

- 최근 7일 회사 리포트의 `article_text` 커버리지와 extraction 실패율을 매일 snapshot에 표시
- LLM summary는 원문·모델·추출시각·evidence를 가진 행만 생성
- 프론트 normalizer가 이미 API가 반환하는 `stock_tickers`, `target_price`, `rating`,
  `revision_type`, `report_type`를 버리지 않음
- detail API는 원문을 on-demand로 제공하고 목록 API payload를 비대하게 만들지 않음

## 현재 API/UI 경계 점검

- backend `v_reports_api`와 `SecReportResponse`는 구조화 투자 필드를 목록 API에
  제공한다. frontend `reportNormalizer`도 이를 보존해야 한다.
- `article_text`와 `gdrive_pdf_url`은 물리 테이블에는 있지만 현재 API view/schema에는
  없다. 목록 API에 추가하면 무한 스크롤과 LLM 입력 모두 불필요하게 비대해진다.
- 다음 backend 변경은 `GET /external/api/reports/{report_id}/context`처럼 한 리포트만
  반환하는 detail 경로여야 한다. 권한, 저작권 범위, 응답에서 허용할 원문 길이는 제품
  결정이므로 이 문서만으로 public endpoint를 만들지 않는다.
