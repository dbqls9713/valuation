# GOOGL Q2 — 검색의 AI 위협 정도 (조사 raw 데이터 archive)

**연결**: `GOOGL_research_questions.md` Q2 도달한 답·시나리오의 근거 자료
**조사 범위**: Search 광고 12분기 매출 + AI 경쟁자 데이터 + AI Overviews 영향 + DOJ 진행 + AI Agent + Gen Z + Apple deal
**조사일**: 2026-05-03

---

## 1. Google Search & Other 광고 매출 12분기

| 분기 | Search 매출 (B) | YoY% | 경영진 멘트 (Pichai · Schindler) |
| --- | --- | --- | --- |
| 2023 Q2 | 42.6 | +4.8% | Pre-AIO. SGE 실험 단계 |
| 2023 Q3 | 44.0 | +11.3% | SGE 확장, 광고 매출 회복 |
| 2023 Q4 | 48.0 | +12.7% | Bard 출시, SGE 120+ 국가 |
| 2024 Q1 | 46.2 | +14.4% | "Generative AI experiences in Search" |
| **2024 Q2** | **48.5** | **+13.8%** | **AI Overviews 출시 (5/14/2024)**. Pichai: "monetization approximately the same rate" |
| 2024 Q3 | 49.4 | +12.2% | Schindler: "ads in AIO are additive". Circle to Search 200M+ Android |
| 2024 Q4 | ~54.0 | +12.5% | AIO 1B+ MAU |
| 2025 Q1 | 50.7 | +9.8% | AIO 1.5B MAU. AI Mode 발표 (5/2025 I/O) |
| **2025 Q2** | 54.2 | +11.7% | **AI Mode GA (US/India)**, 100M+ MAU. 쿼리 2-3배 길어짐 |
| 2025 Q3 | 56.6 | +14.5% | Gemini 2.5 native in Search |
| 2025 Q4 | 63.1 | **+16.5%** | Gemini 3 launch. AI Mode daily 쿼리 출시 후 2배. "queries at all-time high" |
| **2026 Q1** | **60.4** | **+19.0%** | "Strong quarter, AI experiences driving usage". **11분기 연속 두 자릿수 성장** |

**핵심 패턴**: AI Overviews 출시 (2024.05) 후에도 매출 둔화 X — 오히려 가속. 12분기 만에 +4.8% → +19%.

---

## 2. AI 검색 경쟁자 데이터 (2025-2026)

| 서비스 | MAU/WAU | 쿼리/사용 | Monetization | Notes |
| --- | --- | --- | --- | --- |
| **Google AI Overviews** | **2B MAU** (2025.07) | 48% 검색 트리거 (BrightEdge 2026.02) | 광고 출시 (2024 후반) | Pichai: "monetizes ~at same rate as classic" |
| **Google Gemini App** | **750M MAU** (2025 Q4) | 10B tokens/min via API | 구독 (AI Pro/Ultra) | Gemini 3 출시 (2025.11) |
| ChatGPT | 900M WAU (2026.02) / ~1B MAU (est.) | 2.5B prompts/day, 18B msgs/week | 50M 유료, ChatGPT Search free (2025.02부터) | Search 트리거 34.5% (작년 46%에서 ↓ 정착) |
| Perplexity | 22M (2025.01) → **45M MAU** (early 2026) | 780M 쿼리 (2025.05) | $200M ARR (2026.02 est.), 광고 (2024부터) | $20B valuation (2025.09), $22.6B (2026.01) |
| Microsoft Bing/Copilot | Bing 1B MAU (early 2026) | US 점유 7.85% (2024) → 8.5% (2025.09) | 광고 매출 미공시 | Copilot에 GPT-4/5 통합 |
| Anthropic Claude | (미공시), 엔터프라이즈 점유 32% (2026.04, 2025 <15% 대비 2배) | Claude Code + Claude.ai | API 매출 ~$5-7B 추정 | Brave Search 백엔드 |

**핵심 비교**: AIO 2B + Gemini 750M = ~2.75B Google AI 사용자 vs ChatGPT 900M (3배). Google이 LLM 시대에도 사용자 base 압도적.

---

## 3. AI Overviews 영향

### Coverage·MAU 추이

- 2024.05: 출시
- 2024 Q4: 1B MAU
- 2025.03: 검색 ~20% 트리거 (Pew)
- 2025.07: 2B MAU
- 2026.02: **48% 트리거** (BrightEdge)
- 60% question-phrased 쿼리에 트리거

### CTR 영향 (각종 연구)

- **Pew (2025.03)**: AIO 쿼리 CTR 8% vs 일반 15%. 시민 1%만 인용 source 클릭
- **Seer Interactive (2025.09)**: organic CTR 61% drop on AIO 쿼리 (1.76% → 0.61%)
- **BrightEdge (2026)**: Search impressions +49% YoY but CTR -30%

### Monetization

- Pichai 반복 멘트: AIO 광고는 "approximately the same rate as traditional Search"
- AIO 광고 출시: 2024 후반 (mobile)
- AI Mode "Direct Offers" pilot: 2025-26

### Publisher 영향

- Zero-click 증가
- e-commerce 카테고리 organic traffic -35% (rankings 동일)
- Reddit·NYT·Wikipedia 트래픽 감소

---

## 4. DOJ Search 독점 사건 진행

### 타임라인

| 날짜 | 사건 |
| --- | --- |
| 2020.10 | DOJ + 11개 주가 Google 상대 소송 |
| 2023.09~11 | 재판 (10주, Judge Amit Mehta) |
| **2024.08.05** | **Liability ruling — "Google is monopolist"** |
| 2024 Q4~2025 Q3 | Remedy phase. DOJ 제안: Chrome 매각, Apple deal 금지, 데이터 공유 |
| **2025.09.02** | **Mehta remedy order — Chrome·Android 매각 X, Apple deal 유지 (1년 계약 한정), 데이터 일부 공유** |
| 2025.12.05/07 | 추가 detail: 모든 default 검색·Generative AI 계약 1년 한정, AI 학습 공시 |
| 2025 말~2026 초 | DOJ + Google 양쪽 항소 |
| 2026~2028 | DC Circuit 항소 판결 (12-24개월 예상) |

### 핵심 위반 행위 (Mehta 판결)

- Google이 Apple Safari, Mozilla Firefox, Android OEM에 매년 수십억 달러 지급해서 default 검색엔진 자리 봉쇄
- 미국 검색 시장 점유 ~90% 유지 메커니즘
- "Google is a monopolist, and it has acted as one to maintain its monopoly" (Judge Mehta)

### Apple deal 핵심 데이터

- 2022년 ~$26B 지급 (Pichai 증언, 2023.11)
- Safari 검색 매출의 36% 분배
- Yahoo Finance (2025.09): "Apple dodged $20B hit" — 판결로 deal 유지
- 단 1년 계약 한정 → 매년 재협상 → Apple 협상력 ↑

### Apple Contingencies

- Apple Intelligence ChatGPT integration 완료 (2024.10)
- "World Knowledge Answers" — Apple 자체 검색엔진 2026 목표
- Gemini-Siri 협상 보고
- Perplexity 인수 검토 후 shelved (2025)

### Loss 시나리오 (sell-side analysis)

- Direct cost 절감: +$20B/yr op profit (TAC 감소)
- Revenue at risk: -$10~20B Search 광고 (5-10% 잠식)
- **Net 영향**: net-positive 또는 slightly-negative (Bernstein·MS sell-side range)
- 이유: Apple Services 자체 손실 15-20% → Apple도 Google 유지 동기 강함

---

## 5. AI Agent 채택 트렌드

### 주요 제품

| 제품 | 출시 | 현재 상태 |
| --- | --- | --- |
| Anthropic Computer Use | 2024.10 → API GA 2024.12 | 2026 production-grade, usage-based pricing |
| OpenAI Operator | 2025.01 ($200/mo Pro) | 2025 mid → ChatGPT Agent |
| Google Project Mariner / Gemini agents | 2024.12 announced | Chrome 2026 통합 |
| Browser-native: Comet (Perplexity), Dia, Arc Search | 2025 | agentic browse-and-act |

### MCP (Model Context Protocol) — 표준화

- Anthropic 발의 (2024)
- OpenAI 채택 (2025)
- 2026.02: 9,400+ public servers, 78% 엔터프라이즈 채택

### 분석가 consensus

- **2026 = "production year"** (B2B 본격)
- Mainstream consumer use 여전히 **<10% ChatGPT WAU**
- Consumer 일반화 **2027~2028** 예상
- Bypass 사례: travel booking, shopping (Shopify checkout via Operator), research/PDF 합성

---

## 6. Gen Z 행동 데이터

| 데이터 | 결과 |
| --- | --- |
| Statista 2024: US Gen Z TikTok 검색 사용 | 64% |
| 2026 ALM: Gen Z TikTok > Google 선호도 | 2024 8% → **2026 4%** (50% 감소) |
| TikTok 검색 효과 (Gen Z 응답) | 65% 사용 / 25%만 효과적 |
| ChatGPT > Google 선호 (US 소비자) | 14% (TikTok 대비 2배) |
| Google 자체 데이터 (Pichai) | "Queries at all-time high" — 연령 cohort 미공시 |

---

## 7. 도메인 용어

- **DOJ (Department of Justice)** = 미국 법무부. 반독점 소송 담당
- **Sherman Antitrust Act (1890)** = 시장 독점·경쟁 봉쇄를 막는 미국법
- **Default Search Engine** = 브라우저·OS의 기본 검색엔진
- **Liability Ruling** = 책임 판결 (반독점법 위반 인정)
- **Remedy Phase** = 시정 조치 단계 (어떻게 처벌·시정할지)
- **CTR (Click-Through Rate, 클릭률)** = 검색 결과·광고가 보인 횟수 대비 클릭된 비율
- **AIO (AI Overviews)** = Google 검색 결과 상단 AI 답변 박스
- **AI Mode** = AIO보다 본격 conversational search 인터페이스 (2025.05 GA)
- **TAC (Traffic Acquisition Cost)** = Google이 traffic 끌어오는 데 쓰는 비용. Apple deal이 가장 큰 항목
- **Zero-Click Search** = 사용자가 검색 결과 페이지에서 답을 얻고 사이트 클릭 안 하는 검색. AIO가 zero-click을 늘림 → publisher 트래픽 ↓
- **MCP (Model Context Protocol)** = AI agent와 외부 툴 연결 표준 프로토콜. Anthropic 발의, OpenAI 채택. Cross-vendor 표준
- **Agentic Browser** = 사용자 명령으로 브라우저가 자동으로 사이트 방문·작업 수행
- **SGE (Search Generative Experience)** = AI Overviews 전신, 2023 실험 단계 명칭
- **Circle to Search** = Android에서 화면을 동그라미로 감싸 검색하는 기능 (200M+ 기기)

---

## 8. 1차 자료 출처

### Alphabet

- Q1 2026 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204426000043/googexhibit991q12026.htm>
- Q4 2025 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204426000012/googexhibit991q42025.htm>
- Q3 2025 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204425000087/googexhibit991q32025.htm>
- Q2 2025 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204425000056/googexhibit991q22025.htm>
- Q1 2025 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204425000040/googexhibit991q12025.htm>
- Q4 2024 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204425000010/googexhibit991q42024.htm>
- Q3 2024 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204424000115/googexhibit991q32024.htm>
- Q2 2024 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204424000076/googexhibit991q22024.htm>
- Q3 2023 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204423000088/googexhibit991q32023.htm>
- Q2 2023 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204423000067/googexhibit991q22023.htm>
- Q4 2023 release: <https://abc.xyz/assets/95/eb/9cef90184e09bac553796896c633/2023q4-alphabet-earnings-release.pdf>
- Pichai Q1 2026 remarks: <https://blog.google/company-news/inside-google/message-ceo/alphabet-earnings-q1-2026/>

### Search Engine Journal · ALM Corp · Captide

- Q1 2026 매출 commentary: <https://www.searchenginejournal.com/google-search-revenue-grew-19-in-q1-pichai-cites-ai/573378/>
- Q4 2025 ALM Corp: <https://almcorp.com/blog/google-search-63-billion-ai-mode-advertising-q4-2025/>
- AI Mode ad tests: <https://www.searchenginejournal.com/google-search-hits-63b-details-ai-mode-ad-tests/566613/>

### AI 경쟁자 데이터

- ChatGPT 900M WAU (TechCrunch): <https://techcrunch.com/2026/02/27/chatgpt-reaches-900m-weekly-active-users/>
- Perplexity $20B funding: <https://techcrunch.com/2025/09/10/perplexity-reportedly-raised-200m-at-20b-valuation/>
- Perplexity stats: <https://www.demandsage.com/perplexity-ai-statistics/>
- ChatGPT Search 출시: <https://openai.com/index/introducing-chatgpt-search/>

### DOJ 사건

- DOJ remedies CNBC: <https://www.cnbc.com/2025/09/02/google-antitrust-search-ruling.html>
- DOJ remedy 2025.12: <https://www.cnbc.com/2025/12/05/judge-finalize-remedies-in-google-antitrust-case.html>
- 1-year contract limit: <https://winbuzzer.com/2025/12/07/judge-orders-annual-renegotiation-for-exclusive-google-search-deal-with-apple-ending-forever-contracts-xcxwbn/>
- DOJ 항소 2026: <https://9to5mac.com/2026/02/03/apple-search-deal-with-google-could-face-renewed-scrutiny-as-doj-appeals-antitrust-ruling/>
- Apple $26B / 36% rev-share: <https://www.bloomberg.com/news/articles/2024-05-01/google-s-payments-to-apple-reached-20-billion-in-2022-cue-says>
- Apple 36% Pichai 증언: <https://www.cnbc.com/2023/11/14/google-pays-apple-36percent-of-safari-search-revenue-sundar-pichai.html>

### AI Overviews

- BrightEdge AIO 48%: <https://almcorp.com/blog/google-ai-overviews-surge-9-industries/>
- Pew AIO CTR: <https://searchengineland.com/google-ai-overviews-search-clicks-fell-report-455498>
- AIO 광고 monetization: <https://www.seroundtable.com/googles-pichai-ads-ai-overviews-baseline-39428.html>

### AI Agent

- OpenAI Operator launch: <https://www.technologyreview.com/2025/01/23/1110484/openai-launches-operator-an-agent-that-can-use-a-computer-for-you/>
- Computer Use vs CUA: <https://workos.com/blog/anthropics-computer-use-versus-openais-computer-using-agent-cua>

### Gen Z

- Gen Z TikTok decline: <https://www.searchenginejournal.com/gen-z-preference-for-tiktok-over-google-drops-50-data-shows/568267/>

### Apple Contingencies 출처

- Apple AI search 2026: <https://fortune.com/2025/09/04/apple-ai-siri-search-perplexity-chatgpt-world-knowledge-answers-feature/>

---

## 9. 데이터 gap / caveats

- SEC PDF/HTML 일부 403 에러 → Search Engine Land·SEJ·ALM·Captide 인용으로 triangulate. Q4 2024 매출은 13% YoY 멘트로 추정 (~$54B), 10-K verify 권고
- ChatGPT MAU 미공시 → 1B 추정은 Similarweb 기반
- Perplexity ARR ($200M, 2026.02) press leak, audited 아님
- Safari iPhone 검색 트래픽 = Google 검색 광고 매출 ratio 미공시 → 25-30% est. (sell-side range)
- Google AIO vs classic monetization 달러 단가 미공시 → "approximately the same rate" 멘트만 존재 (management assertion)
- AI Agent mainstream 채택 정량 지표 zero (e-commerce 거래 % 등)
- Gen Z Google 검색 쿼리 감소 → Google 자체 데이터는 ATH (연령 cohort 비공개). 외부 추정만 존재
- DOJ 항소 timeline 불확실. remedy enforcement는 DC Circuit 판결 후
