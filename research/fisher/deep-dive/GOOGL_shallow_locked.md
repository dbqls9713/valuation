# GOOGL Shallow Dive — Locked Sections (Anchoring Block)

**원본**: `research/shallow-dive/GOOGL_alphabet.md` (2026-04-26)
**목적**: Deep Dive Stream 1·2·3·4·6 작업 중 **절대 열지 않음**. 각 Stream Acceptance 충족 후 비교 시점에만 참조.

## 차단 매트릭스

| Stream | 차단 대상 | 비교 해제 시점 |
| --- | --- | --- |
| 1-A | (없음 — Phase 5 산업 지도는 1-B와 함께 차단) | — |
| 1-B 해자 분석 | Phase 5 + **Phase 6-A 버핏 해자** | Stream 1 종료 후 |
| 2 경영진 | Phase 4 (경영진 기본 프로필 + 1분 신뢰성 테스트 + 가이던스 성격) | Stream 2 종료 후 |
| 3 리스크·기회 | Phase 8 (1차 가설 + 리스크/기회 톱3) | Stream 3 종료 후 |
| 4 가격 평가 | Phase 10 (간이 3시나리오 Valuation 전부) | Stream 4 종료 후 |
| 6 결정 | Phase 11 (Deep 진입 결정 + 확신도 + Stream 매핑) | Stream 6 종료 후 |

**참조 가능** (차단 안 됨): Phase 1 (유니버스), Phase 2 (2분 테스트·사업모델), Phase 3 (정량 기초 5년), Phase 6-B/C/D (Fisher 15 정량·프리미엄·Lynch), Phase 7 (밸류에이션 위치), Phase 9 (Track Assignment 본 트랙 인계).

---

## Phase 4. 경영진 기본 프로필 (Stream 2 차단)

| 항목 | 내용 |
| --- | --- |
| CEO | Sundar Pichai, 2015.08~ Google CEO, 2019.12~ Alphabet CEO, IIT 카라그푸르·스탠퍼드·와튼, 2004 Google 입사 (Chrome·Android 사업 견인) |
| CFO | Anat Ashkenazi, 2024.07~ (Eli Lilly 전 CFO), 1.8년차 — MARGINAL 단축 재직 |
| CEO 자사주 보유 | ~$1.3B (2025년 말 기준, 0.03% 미만 — 빅테크 평균 수준) |
| 내부자 거래 패턴 | 정규 10b5-1 분할매도 위주 (Pichai·Brin 모두 정기 매도), 적극적 매수 부재 |
| 위기 대응 기록 | 2022 비용절감 + 2023.01 12,000명 정리해고, 2024.09 DOJ 검색 반독점 패배 후 즉시 항소·remedies 협상 → 2025.09 Chrome/Android 매각 회피 |
| 인상 문장 (Q4 2025 어닝콜) | "We were able to lower Gemini serving unit costs by 78% over 2025 through model optimizations" — AI 단가 효율화 강조, capex 정당화 논거 |

### 1분 신뢰성 테스트

| # | 항목 | 판정 | 근거 |
| --- | --- | --- | --- |
| 1 | ROE 10%+ | PASS (31.8% — 30%+ 영역, 지속 의심 항목이지만 Cloud 마진 확장 + 광고 본업 구조적 이유 있음) | |
| 2 | ROA 7%+ | PASS (132/595 = 22%) | |
| 3 | 재무상태표 의심 항목 | NORMAL — 무형자산 14B (총자산 595B 대비 2%로 작음), 매출채권 정상 | |
| 4 | 영업외/일회성 비용 패턴 | 2024 EU 반독점 과징금 항소 진행, DOJ 검색 remedy 복종 비용은 비계량 — 모니터 | |
| 5 | 주주환원 정책 | 배당 도입 2024.Q1 (~$8B/yr) + buyback ~$60B/yr (2025), payout ~52% | |

### 경영진 가이던스 성격 (Phase 9 D축 입력)

> **재투자·파이프라인 중심**. 2026 capex $175–185B 가이던스 (전년 91B → 2배 증가)로 AI 컴퓨트·DeepMind·Cloud demand에 압도적 베팅. 현금환원도 동시 (50%+ payout)이지만 메시지 톤은 명백히 "AI N+1 시장 확대".

---

## Phase 5. 산업 지도 (Stream 1-B 차단)

| 항목 | 내용 |
| --- | --- |
| 주요 경쟁사 (광고) | Meta, Amazon, Apple (privacy) |
| 주요 경쟁사 (Cloud) | AWS (1위, ~30%), Microsoft Azure (2위, ~25%), GCP (3위, ~12%) |
| 주요 경쟁사 (AI 모델) | OpenAI/MS, Anthropic, Meta Llama, xAI, DeepSeek, Mistral |
| 시장 집중도 | 디지털 광고 듀오폴리(Google+Meta 50%+), Cloud 빅3가 ~70%, 검색은 Google 90%+ |
| 산업 성장률 | 광고 ~10% CAGR, Cloud ~22% CAGR, AI 인프라 시장 ~40%+ CAGR (2025-2028) |
| Alphabet의 산업 내 위치 | 검색 1위 (압도), Cloud 3위 (가속 중 +48%), AI 모델 풀스택 플레이어 (TPU+모델+분포) |
| 해자 유형 후보 | **네트워크 + 전환비용 + 원가우위 (자체 TPU)** — Phase 6에서 판정 |

### 산업 성숙도 (Phase 9 C축 입력)

> 광고 본업: 산업 ~10% vs Alphabet 매출 17% — **상회**. Cloud 부문은 산업 22% vs Alphabet 48% — **명백 상회**. 회사 전체 17%는 산업 mix 가중평균 대비 **상회**.

---

## Phase 6-A. 버핏 해자 — 프랜차이즈 3조건 (Stream 1-B 차단)

| 조건 | 판정 | 근거 |
| --- | --- | --- |
| 1. 소비자가 필요로 하거나 욕망한다 | YES (검색·YouTube·Maps·Gmail·Android) | 글로벌 일일 사용 빈도 압도 |
| 2. 소비자 마음속에 대체재가 없다 | UNCERTAIN | 검색은 GenAI(ChatGPT, Perplexity)로 일부 대체 시작; YouTube/Maps는 대체재 부재; Cloud는 명백 대체재 (AWS/Azure) |
| 3. 가격 규제가 없다 | UNCERTAIN | DOJ 2025.09 remedies — 독점 계약 금지·검색 인덱스 데이터 공유 의무화로 가격 규제는 아니지만 **유통 구조 규제** 시작 |

**See's 테스트**:

- ROIC 22% (See's 60% 대비 낮지만 자본집약 테크 중에서는 강함)
- CAPEX/매출 22.7% (See's 1% 대비 매우 높음 — 자본 light 비즈니스 아님)
- OE > NI: CFO $164.7B > NI $132.2B → 좋음 (감가상각 + SBC 환원)

**해자 유형별 등급**:

| 유형 | 등급 | 근거 |
| --- | --- | --- |
| 브랜드 | HIGH | "Google" = "검색"의 동사화, YouTube = 영상의 표준 |
| 네트워크 | HIGH (YouTube), MODERATE (검색 — AI로 흔들림 가능성) | YouTube 크리에이터-시청자 양면 효과 |
| 전환비용 | HIGH (Android·Workspace), MODERATE (Cloud — 멀티클라우드 확산) | 디바이스·기업 ID 잠금 |
| 원가우위 | HIGH | 자체 TPU + 글로벌 광섬유 — Gemini 단가 1년 -78% 절감 |
| 수직통합 | HIGH | TPU(설계) + DC(운영) + 모델(DeepMind) + 분포(검색·YouTube) 풀스택 |

**해자 종합**: HIGH — 다축 보유. 단 검색 ad 부문에 대해 **AI 디스럽션 + DOJ 행위 제한**의 이중 충격이 시작 단계.

---

## Phase 8. 1차 가설 + 리스크·기회 톱3 (Stream 3 차단)

### 1차 가설

> Alphabet은 **검색 광고 머니머신 + YouTube/Android 분포 + 자체 TPU 인프라 + DeepMind/Gemini 모델**의 4축 풀스택 AI 플레이어로, 2026~2028년 동안 **Cloud 부문이 매출 mix의 30%+로 확장**하고 **Gemini 기반 신규 제품군이 광고 외 수익 채널을 형성**할 가능성이 있다. 단 매출 17% 복리는 광고 본업이 AI에 잠식당하지 않고 Cloud 가속이 capex ROI를 정당화한다는 두 축 모두에 의존하며, 현 가격은 이 시나리오를 대부분 가격에 반영한 것으로 보인다. Pichai 체제 10년+의 캡 효율 개선 트랙(2020→2025 마진 23%→32%)이 이 경로를 뒷받침한다.

### 리스크 톱3

| 순위 | 리스크 | 발생 시 영향 | 관찰 지표 |
| --- | --- | --- | --- |
| 1 | AI 검색 디스럽션 (ChatGPT, Perplexity가 Google 검색 이탈 가속) | 검색 광고 매출 성장 둔화 또는 절대 감소; ROE 31.8% 훼손 | Google 검색 점유율, AI Overviews CTR, Search 광고 매출 YoY |
| 2 | DOJ 검색 remedies 행위 제한 (2025.09 판결) | 검색 default 트래픽 일부 손실 + 경쟁사 데이터 격차 축소 → 장기 마진 악화 | Apple Safari·Samsung 디바이스 default 변경, EU·다른 국가 추가 조치 |
| 3 | Capex ROI 미달 ($175–185B/yr 2026 가이던스) | FCF 마진 18%→10% 추가 하락; AI 매출이 capex 흡수 못 하면 ROIC 15% 하향 | 분기 capex 가이던스 변경, Cloud 매출 가속/감속, Gemini 유료 전환 KPI |

### 기회 톱3

| 순위 | 기회 | 실현 시 영향 | 관찰 지표 |
| --- | --- | --- | --- |
| 1 | Cloud 가속 (Q4 2025 +48%, 백로그 $240B +55% QoQ) | Cloud 매출 비중 14%→25%, 마진 30%→40% → EBIT $50B+ 추가 기여 | Cloud RPO 백로그, 영업이익률, 신규 엔터프라이즈 계약 |
| 2 | Gemini App 750M MAU + 생성AI 매출 +400% YoY | 광고 외 신규 수익 채널 (구독·API·Agentic AI) 출현 | Gemini 유료 사용자, API 매출, Workspace AI 가격 인상 수용도 |
| 3 | Waymo 상업화 + Other Bets 대형화 | Other Bets <1% → 5%로 확장 시 새 카테고리 다이버시티 | Waymo 운행 도시 수, ride 수, 승차당 단가 |

---

## Phase 10. 간이 3시나리오 Valuation — Fisher 분기 (Stream 4 차단)

### 10-B. Fisher 기대 CAGR (ROE × PBR)

**현재 상태**: ROE 31.8%, PBR 9.93x, payout ~52% (배당 + buyback), 잔존 retention ~48%.

#### 시나리오 가정 매트릭스

| 변수 | Bull | Base | Bear |
| --- | :-: | :-: | :-: |
| 유지 ROE | 30% | 25% | 18% |
| 예상 지속 N | 10년 | 10년 | 10년 |
| 터미널 PBR | 8x | 5x | 3x |
| Retention | 50% | 50% | 50% |
| BV CAGR | 15% | 12.5% | 9% |

#### 계산 결과

| 시나리오 | 자본증식 CAGR | + payout yield | 종합 CAGR | 판정 |
| --- | :-: | :-: | :-: | :-: |
| Bull | 12.6% | +1.5% | **~14.1%** | 매수 가능 후보 |
| Base | 5.0% | +1.5% | **~6.5%** | 회색 |
| Bear | -3.3% | +1.5% | **~-1.8%** | 손실 가능 |

**확률가중 (Bull 30 / Base 50 / Bear 20)**: **7.1%**

### 10-C. Trigger 신호

- Bull CAGR 14.1% ≥ 10% → Deep 진입 가능
- 확률가중 7.1% < 10% 요구수익률 → 풍족한 마진 없음, Bull 의존

---

## Phase 11. Deep 진입 결정 + 확신도 (Stream 6 차단)

### 4갈래 결정

| 결과 | 본 종목 충족? |
| --- | --- |
| Deep Dive 진입 | ✅ — Fisher Deep Dive 진입 |

**충족 근거**:

- 유니버스 PASS ✓
- 2분 테스트 명료 ✓
- 정량 5/5 PASS (Phase 6-B) ✓
- Track Gate 확신도 = 중간 (3:1, ROIC 안정성이 약점) ✓
- Phase 10-B Bull CAGR 14.1% ≥ 4% (매수 금지 trigger 미발동) ✓ + ≥ 10% (Deep 가능 후보)

### 확신도: **중간**

### 과신 점검

> "왜 비싼가?" — AI 시대 풀스택 플레이어 4축 + Cloud 가속 + DOJ Chrome 매각 회피 (2025.09)로 시장이 **AI 승자 프리미엄**을 부여. 5년 PER 레인지 상단 + FCF Yield 5년 최저(1.78%)는 **모든 좋은 뉴스가 가격에 반영**된 시점.
>
> "왜 이 트랙인가?" — ROIC 안정·payout 50%는 Buffett 신호이지만, **재투자율 92.6% + 매출 17% 산업 상회 + capex 가이던스 +90%**가 압도. Fisher 트랙에서 형용사 누적·N+1차항 입증 후 가격 결정이 적합.
>
> "**놓치고 있는 것**?" — (a) AI 검색 디스럽션의 속도가 시장 가정보다 빠를 수 있음 (Perplexity·ChatGPT의 search-like UX), (b) DOJ remedies 시행 디테일에 따라 점진적 점유 손실 누적, (c) Cloud 가속이 OpenAI/Anthropic의 Azure/AWS 락인을 깨야 하는 구조적 challenge.

### Stream 매핑 (legacy A~F 명명)

- **Stream A (스카이텔링·소비자 관찰)**: Gemini App 사용 패턴, ChatGPT·Perplexity와의 retention 비교, Pixel·Workspace AI 채택률
- **Stream B (경영진 형용사)**: Pichai·Ashkenazi의 capex justification 발언 누적, Demis Hassabis (DeepMind) 공개 발언 톤
- **Stream C (10년 CAGR + 피어 터미널 PER)**: Microsoft·Meta·Apple·Amazon 동급 풀스택 빅테크 터미널 PER 수렴치 (15~25x), GOOGL의 30x 정당화 가능 여부
- **Stream D (자본배분 — buyback at PBR=10)**: 2025 buyback ~$60B를 PBR 10x에서 집행하는 합리성, retention 50%의 자기효율
- **Stream E (해자 깊이 — TPU·DeepMind·데이터)**: Trillium TPU 차세대 ROI, DeepMind Gemini vs OpenAI 모델 격차, 검색 인덱스 데이터 공유 의무 후 데이터 격차 약화 속도
- **Stream F (Monitoring KPI)**: 검색 점유율 (StatCounter), Cloud 분기 +YoY%, 분기 capex 실집행 vs 가이던스, DOJ remedies 시행 마일스톤, 신규 EU/한국·일본 반독점 동향
