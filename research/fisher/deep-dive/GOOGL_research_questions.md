# GOOGL Deep Dive — Research Questions (학습·답 형성 누적 문서)

**시작**: 2026-05-02
**목적**: GOOGL Stream 1·4의 핵심 판단 입력을 형성하기 위한 학습. 본인이 능동적으로 질문 던지고, 별도 세션에서 조사·대화·정리하면서 답을 형성. AI는 정보 조사·논점 자극 도구.

**연결**:

- 본 문서의 질문별 답 → `GOOGL_alphabet.md` Stream 1-A 직관 박스 + Stream 4 시나리오 입력
- 학습 진행 시 각 질문은 별도 세션으로 깊이 작업

## 운영 룰

- 한 질문 = 한 세션 (또는 여러 세션 누적)
- 각 sub-section 채울 순서: **본인 직관 (먼저)** → 조사 → 추가 직관 → 도달한 답
- AI는 사실 조사·논점 정리·반대 의견 제시. **답은 본인이 형성**
- 답이 형성되면 Stream 1-A 직관 박스 + Stream 4 시나리오 가정으로 옮김

## 우선순위

| 우선 | Q# | 질문 | Stream 영향 |
| --- | --- | --- | --- |
| 1 | Q1 | Cloud의 지속적 성장 가능성 | Stream 1-A 산업 + Stream 4 가격 |
| 1 | Q2 | 검색의 AI 위협 정도 | Stream 1-B 해자 + Stream 3 리스크 + Stream 4 |
| 1 | Q3 | CAPEX ROI 회수 가능성 | Stream 4 모든 시나리오 sanity check |
| 2 | Q4 | Gemini 모델 차별화 (월드모델·backbone 수요) | Stream 1-B 해자 (원가우위) + Stream 4 |
| 2 | Q5 | TPU 자체 chip 사용률·차별화 | Stream 1-B 해자 (수직통합) + Stream 4 (Cloud 마진) |
| 2 | Q6 | AI 답변 상업화의 미래 (광고 모델 변화) | Stream 1-B 해자 + Stream 4 |
| 3 | Q7 | YouTube 지속 성장 | Stream 4 (보조 매출 동인) |

---

## Q1. Cloud의 지속적 성장 가능성

### 질문

- Cloud 5년 CAGR +43%가 다음 5년 (2026-2030) 어떻게 변할까?
- AWS·Azure·GCP 시장 점유율 12% → ?% 가능 시나리오
- 어떤 조건이 충족되어야 +30%/yr 유지? +20% 둔화 시나리오?
- 마진 17% → ?% 확장 가능?

### 본인 직관 (작성 시점)

- **2026-05-02 ~ 05-03 (v2 형성, AI와 인풋 교환 후 정리)**

  **회사 thesis (GCP 자체)**:

  - **점유율 추월 가능**: AI/ML 자원을 AWS·Azure보다 훨씬 저렴하게 제공할 수 있다면 (TPU 자체 칩 + 수직통합 단가 우위가 핵심)
  - **Capex 회수 가능**: 실제 생산성 향상이 일어나고 있음 (본인 사례 — YouTube AI 요약 매일 사용, Workspace Gemini는 아직 조악하지만 발전 중). 미국 정부도 공급증대 → 인플레 완화 기대로 macro tailwind
  - **마진 장기 회복**: AWS·Azure 수준 (~37%)으로 수렴할 것. 다만 timeline은 5년·7~8년·10년 시나리오로 분리해서 봐야 함 (단일 가정 X)

  **시장 TAM 직관 (상충하는 두 방향, 조사로 weight 결정 필요)**:

  - (+) AI가 신규 서비스 런칭 허들을 낮춤 → Cloud 자원 수요 ↑ (정량 가정 아직 못 잡음)
  - (-) 개인·법인용 데이터센터 허들이 낮아지지 않을까? (NVIDIA DGX 등 on-premise/specialized cloud 트렌드 — 일부 세그먼트는 cloud → 자가호스팅 이동 가능성)

  **Bear 메커니즘 (시나리오 변동, thesis-breaker는 아님)**:

  - AI의 선제적·공격적 투자가 산업 병목을 만들고 있음 (GPU·메모리 가격 폭등) → **비효율 단가로 인프라 자산이 형성**되고 있음
  - AI가 세상을 비가역적으로 바꾸는 건 맞지만, 수요가 생각만큼 폭발적이지 않거나 거품 꺼지고 천천히 올라오면:
    - 비용 희석 timeline 지연 (역사적 cost로 산 자산이 그대로 감가상각)
    - 수익 창출 시점 지연
  - → 회수 timeline이 5년 → 7~10년으로 shift, 마진 회복도 늦어짐
  - 본인 판단: **이 시나리오는 valuation 하락 요인이지, 매도 트리거 (thesis-breaker) 는 아님** (AI의 비가역성에 대한 confidence가 깊음)

  **Thesis-breaker**: 현 시점 진짜 미지의 영역 — 조사하면서 형성 예정. 후보 (AI 자체 수요 절멸 / TPU 우위 붕괴 / 반독점 규제 / Gemini API·BigQuery 매출 미달)

### 조사 (raw 데이터·출처) → `GOOGL_q1_cloud_research.md`

**1단계 (12분기 1차 자료, 2026-05-03)**: Cloud 매출 8.0B → 20.0B (12분기 만에 2.5배), op margin 4.9% → 32.9% (AWS 37.7%와 격차 5%pt), Cloud RPO 462B (24개월 내 50% 매출 전환 가이던스). 12분기 모두 GCP 성장률 hyperscaler 1위 (4분기 연속). 3대 hyperscaler 동시 재가속 = AI 수요가 zero-sum 아닌 시장 expansion 증거.

**2단계 (AI Chip 격차, 2026-05-03)**: TPU v7 Ironwood vs NVIDIA GB300 ~50% TCO 우위 (SemiAnalysis 독립 검증). 단 (a) **TSMC CoWoS 할당 부족으로 Google 2026 TPU 생산 25% 감산** (NVIDIA 60%·Google ~9%), (b) **NVIDIA Rubin (2027 Q1) HBM4 22 TB/s가 TPU 메모리 대역폭 우위 erase 가능**, (c) Midjourney TPU → NVIDIA defection (software stack 한계). Anthropic은 multi-cloud (TPU·Trainium·NVIDIA 병행).

→ 상세 데이터·출처·도메인 용어 모두 별도 파일 `GOOGL_q1_cloud_research.md` 참조.

### 본인 추가 직관 v4 (조사 2단계 후, 2026-05-03)

조사 종합 후 sharpen된 직관:

1. **마진 회복 속도는 본인 v2 예상보다 훨씬 빠름** (17% → 32.9% in 6분기). 단 capacity constrained 환경의 일시적 프리미엄 가능성 — capacity 풀리는 시점이 thesis re-evaluation trigger.
2. **TPU 우위는 진짜지만 압도적 아님**. ~50% TCO (SemiAnalysis) 중 software 부분 ~30%, 하드웨어 순수 ~20~25% 추정. NVIDIA 사용자도 software 절감 가능 → 우위 일부 약화.
3. **Multi-vendor 균형이 base case** (Anthropic의 multi-cloud는 risk hedging이고 양 스택 기술 차이가 dramatic 하지 않다는 방증). 시장이 NVIDIA 80% 독점 방조 안 함. **Goldman의 TPU 35% 점유 시나리오가 본인 base에 가까움**.
4. **RPO 462B = capex 회수 visibility 보장**. 본인 직관에 없던 보너스. 단 "50% 24개월 전환" 가이던스 신뢰도는 별도 검증 필요.
5. **AI는 zero-sum 점유 재편 아니라 시장 자체 expansion** (3대 hyperscaler 동시 가속). 본인 시장 TAM 직관 (+ 방향)이 데이터로 지지받음.
6. **2027 Q1 (Rubin 출시) + 2025·2026 capex 본격 감가상각 = 이중 stress test 시점**. 본인 v3 직관의 정확한 timing은 2027.
7. **주가 반응**: 최근 GOOGL 주가 상승의 메인 driver는 RPO 462B + 마진 32.9% surprise. → **valuation Stream 4에서 "이미 가격에 반영됐는지"가 새 핵심 질문**.

#### Thesis-breaker re-frame (조사 후)

| 본인 v2 가설 | 조사 후 평가 |
| --- | --- |
| AWS Trainium·MS Maia가 TPU 우위 추월 | △ 부분 틀림. Trainium3는 추격하지만 추월 못 함 (2.52 vs 4.6 PF). Maia 200은 inference-only/Azure-locked → thesis-breaker 아님 |
| **(NEW) TSMC CoWoS 할당 부족** | ⚠️ **진짜 thesis-breaker 후보**. Google 2026 TPU 생산 25% 감산. 칩 공급이 매출 cap |
| **(NEW) NVIDIA Rubin (2027 Q1)** | ⚠️ **진짜 thesis-breaker 후보**. HBM4 22 TB/s가 TPU 메모리 대역폭 우위 erase |
| **(NEW) CUDA moat (비-frontier 고객)** | △ Software stack stickiness가 cost 우위 상쇄 가능. Midjourney defection이 증거 |

#### "잘 모르겠다" 영역 — 시나리오 trigger로 monitor

| 모르겠는 부분 | Bull 가정 | Base 가정 | Bear 가정 |
| --- | --- | --- | --- |
| CoWoS 해결법 | Broadcom·TSMC 협상 성공 | 부분 해결, 25% 감산 유지 | 해결 못 함, 추가 감산 |
| Rubin 2027 대응 | TPU v8 동시 출시 | 6~12개월 격차 | 1년+ 격차, NVIDIA 추월 |
| Midjourney defection 일반화 | 단발 사건, software 빨리 성숙 | 일부 중소 고객 NVIDIA 회귀 | 다수 defection, frontier lab 외 NVIDIA 우위 |

→ **모르는 건 시나리오 trigger로 monitor만 함**. 매 분기 어닝콜에서 (a) Google 칩 생산량, (b) Rubin 출시 영향, (c) 신규 고객 mix 추적.

### 도달한 답 + 시나리오 입력 (Q1 종결)

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| **매출 성장 (5y CAGR)** | +40% (capacity 풀려도 AI 시장 expansion으로 수요 지속, 시장 자체 확대) | +30% (정상화, multi-vendor 균형) | +20% (2027~2028 둔화 + 경쟁 격화 + CoWoS 제약 지속) |
| **Op Margin (5y 평균)** | 32.9% → 38% (AWS 수준 돌파, software + 하드웨어 우위 sustained) | 32.9% → 30% (소폭 후퇴 후 plateau, capacity 풀리며 일부 압박) | 32.9% → 22% (capacity 풀리고 capex 감가상각 본격, Rubin 추월) |
| **시장 점유 (5y 후)** | TPU 35%+ (Goldman aggressive case) | TPU 25~30% (multi-vendor 안정) | TPU 15~20% (CoWoS 제약 + Rubin · CUDA moat) |
| **Capex 회수** | RPO 50%+ 전환 + 신규 RPO 가속 | RPO 50% 전환 정상 | RPO 30~40% 전환 (가이던스 미달) |
| **Thesis 상태** | 강화 | 유지 (현 thesis 그대로) | 시나리오 변동 (매도 아님) |

**Thesis-breaker monitoring (분기별 추적)**:

- (1) CoWoS 할당 변화: NVIDIA·Google·Broadcom 분기 capex·출하량
- (2) Rubin 영향: 2027 Q1 출시 후 GCP 신규 고객·점유 변화
- (3) Software stack stickiness: PyTorch/TPU 채택률, 신규 frontier lab 칩 mix
- (4) Capacity constrained 멘트 변화: 어닝콜 표현이 "constrained" → "balanced" → "excess capacity"로 변하는 시점

**Stream 4 valuation 입력**:

- 위 시나리오 표 → `GOOGL_alphabet.md` Stream 4 (가격 평가) 가정
- 추가 핵심 질문: "현재 주가에 위 Bull 시나리오가 이미 반영됐는가?" (RPO·마진 surprise 후 주가 급등)

---

## Q2. 검색의 AI 위협 정도

### 질문

- ChatGPT·Perplexity·Claude·Apple Intelligence 결합 점유율이 검색 시장의 몇 %까지 잠식 가능?
- 5년 timeline에서 Google 검색 점유 90% → ?%
- AI Overviews가 잠식을 흡수할 수 있나? CTR·매출 단가 영향
- 검색 광고 단가 (CPC) 변화 시나리오

### 본인 직관 (작성 시점)

- **2026-05-03 (v2 형성, AI와 인풋 교환 후 정리)**

  **본인 사용 패턴 (자기 데이터)**:

  - 단순 fact → Google
  - 복잡한 분석·코딩 → **Claude**
  - 쇼핑 → YouTube + 쿠팡 + 네이버스토어 (Google 안 씀)
  - **함의**: 본인은 이미 검색 unbundling 상태 — Google이 슈퍼 도구가 아님. 자기 데이터 자체가 검색 점유 잠식의 evidence
  - 단 AI Overviews는 자연어 쿼리로 의도 트리거, 신뢰도 높음 → Google의 retention 메커니즘 작동

  **5년 점유 잠식 시나리오 (본인 직관)**:

  - (a) **LLM 회사들끼리 n빵**: ChatGPT·Claude·Gemini·Perplexity 등이 검색 트래픽 분산
    - 단 LLM은 어디서 검색? → 검색 인덱스는 Google·Bing·Brave 셋 뿐. **Google이 Gemini로 n빵 참여 + 인덱스 인프라 매출 일부 보유 가능** (Bull factor)
    - 다만 광고 매출은 검색 트래픽 ↓로 직격 (인덱스 매출이 cushion 못 함)
  - (b) **Agent가 직접 URL 데이터 fetch / 검색**: Anthropic Computer Use, OpenAI Operator
    - 검색 광고 표시 기회 자체 소멸 가능 (사용자가 검색을 안 거침)

  **광고 단가 (Bull)**:

  - 검색 인텐트 context가 풍부해짐에 따라 광고의 질·단가 ↑
  - AI 답변 시대에 single high-context ad가 multiple low-context CPC 대체

  **Thesis-breaker (combined)**:

  - **AI agent 일반화 + Gen Z 검색 행동 변화**
    - 본인 사용 패턴 (Claude → 코딩, 쿠팡 → 쇼핑) 이 Gen Z 일반화 가능성
    - Z 세대가 검색을 ChatGPT·TikTok·YouTube로 처음부터 학습 → Google 검색 학습 X
  - **DOJ Apple deal 금지** (정량 직격, ~$20-26B/yr) + **Chrome 매각** (정성 직격, distribution 손실)

  **종합 잠정 thesis**: 검색 unbundling은 진행 중이지만, AI Overviews·Gemini로 일부 흡수 + LLM 백엔드 매출 일부 보유 + 광고 단가 ↑가 buffer. 단 (Agent 일반화 + Gen Z 행동 변화) + (DOJ Apple deal 금지)가 동시 발현하면 thesis 깨짐.

### 조사 (raw 데이터·출처) → `GOOGL_q2_search_research.md`

**1단계 (12분기 + AI 경쟁자 + DOJ + Agent + Gen Z + Apple deal, 2026-05-03)**:

- **Search 매출 가속**: 2023 Q2 +4.8% → **2026 Q1 +19%** (11분기 연속 두 자릿수). AI Overviews 출시 (2024.05) 후에도 매출 둔화 X — 오히려 가속
- **AIO 영향**: 검색 트리거 48% (BrightEdge 2026.02), MAU 2B (2025.07). publisher CTR -30% but Google 매출 +19%
- **Google AI 사용자 base 압도**: AIO 2B + Gemini 750M = 2.75B (ChatGPT 900M의 3배)
- **DOJ 위협 본인 Bear 가정보다 약함**: 2025.09 Mehta — Chrome 매각 X, **Apple deal 유지 (1년 계약)**. "Apple dodged $20B hit". 항소 12-24개월
- **AI Agent**: 2026 = "production year" (B2B), mainstream consumer use <10% — 본인 thesis-breaker는 2027~2028
- **Gen Z**: TikTok > Google 선호 50% 감소 (2024 8% → 2026 4%). ChatGPT > Google 선호 14%로 제한적

→ 상세 데이터·출처·도메인 용어 모두 별도 파일 `GOOGL_q2_search_research.md` 참조.

### 조사 — 직접 사용 비교

- _본인이 ChatGPT·Perplexity·Gemini·Google 사용 비교 (일상 누적, 1~2주 의식적)_
- 자기 데이터 (2026-05-03): 단순 fact → Google, 분석·코딩 → Claude, 쇼핑 → YouTube/쿠팡/네이버
- Zero-click 패턴 인정 — 본인도 이렇게 되어가고 있음

### 본인 추가 직관 v3 (조사 후, 2026-05-03)

조사 결과를 본 후 형성된 추가 직관:

1. **Search 매출 가속 (12분기 +4.8% → +19%) 은 sustainable 아님**. Google이 publisher 손해 (CTR -30%) 를 단기로 감수 중 — **AI 시대 영토 (검색 점유) 방어가 우선**. 본인 직관: "지금은 시장에서의 영토를 잃지 않아야 할 때기 때문에 감내해야 하는 부분". 5년 지속 불가, 중장기에 redistribution 메커니즘 변경 필요 (publisher revenue share 도입, content licensing 비용 ↑, AIO quality 하락 risk).

2. **본인 사용 패턴이 일반화 신호**: Claude 엔터프라이즈 점유 32% (1년 만에 2배+) = 본인의 unbundling 사용 패턴이 B2B에서 빠르게 일반화 중. 본인 사례 = leading indicator.

3. **AI Overviews 신뢰도 + 영토 방어 = 본인 thesis 강화**. 단 publisher 관계 cost는 추후 op margin 압박.

4. **DOJ Apple deal 위협이 Bear 가정보다 약함**: Apple deal 유지 (1년 계약). Apple 자체 손실 동기 강해서 Google default 유지. 단 매년 협상 → TAC inflation 가능 (5년 후 $20B → $30B+?).

5. **Agent + Gen Z thesis-breaker는 long-term (2027~2028)**. 단기는 AI 광고 monetization upside가 더 큰 driver.

6. **Zero-click 트렌드 인정** (본인 사용 패턴): 일반화되면 publisher fresh content ↓ → AIO quality ↓ → 사용자 ChatGPT로 이탈. **Google 자기 발등 찍기 risk**.

7. **#3·#4·#5 미답** (TAC inflation 정량, 본인 패턴 일반화 시점, Pichai ATH 멘트 신뢰성) — 시나리오 trigger로 monitor 처리.

#### Thesis-breaker re-frame (조사 후)

| 본인 v2 가설 | 조사 후 평가 |
| --- | --- |
| AI agent 일반화 + Gen Z 행동 변화 (combined) | △ 진행 중이나 mainstream 2027~2028. 단기 영향 작음 |
| DOJ Apple deal 금지 | ✗ **부분 틀림** — 2025.09 판결로 deal 유지. 단 1년 계약 → TAC 압박 가능 |
| **(NEW) Publisher 관계 재조정** | ⚠️ **새 thesis-breaker 후보**. 5년 후 op margin 압박 (-3~5%pt) |
| **(NEW) AIO quality 하락 (publisher fresh content ↓)** | ⚠️ **새 thesis-breaker 후보**. 사용자가 ChatGPT로 이탈 시 thesis 깨짐 |

→ **현 시점 가장 critical thesis-breaker = Publisher 관계 재조정 + AIO quality 하락 (combined)**. Agent + Gen Z는 2027 이후 추적.

#### 미답 영역 — 시나리오 trigger로 monitor

| 모르는 부분 | Bull 가정 | Base 가정 | Bear 가정 |
| --- | --- | --- | --- |
| TAC inflation (Apple 1년 계약) | $20B 안정 | 매년 +5% (5y +28%) | 매년 +10% (5y +61%, ~$32B) |
| 본인 패턴 일반화 시점 | 5년+ (느림) | 3년 | 1~2년 (Claude 엔터 32% 시그널) |
| Pichai "queries ATH" 신뢰성 | 사실 | 부분 사실 (광고 쿼리만 ATH, organic 감소 가능) | 의도적 narrative |
| Publisher revenue share 도입 | 안 도입 | 부분 도입 (-2%pt op margin) | 본격 도입 (-5%pt op margin) |

### 도달한 답 + 시나리오 입력 (Q2 종결)

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| **Search 매출 5y CAGR** | +12% (현재 +19% → 정상화) | **+5~7%** (publisher cost 반영, redistribution 메커니즘) | +1~3% (Gen Z + Agent 본격, 2027~) |
| **AIO/Publisher** | 균형 sustain (현재 모델 유지) | 점진 redistribution 메커니즘 도입 (op margin -3~5%pt) | Publisher 이탈로 AIO quality ↓ → 사용자 ChatGPT 이탈 |
| **검색 점유 (5y)** | 88~90% 유지 (AIO retention) | 80~85% (일부 unbundling) | 65~70% (Gen Z + Agent 본격) |
| **Apple deal** | 유지 (1년 계약, 단가 안정 $20B) | 유지 but 매년 가격 협상 ($25~30B) | 항소심에서 금지 또는 ChatGPT 전환 |
| **광고 단가 (CPC)** | +5%/yr (context premium 지속) | 안정 | -10% (AIO 잠식 + UX 신뢰 ↓) |
| **Op Margin (검색 사업)** | 유지 | -3~5%pt (publisher cost) | -7~10%pt (publisher 이탈 + 단가 ↓) |
| **Thesis 상태** | 강화 | 유지 (현 thesis 그대로) | 시나리오 변동 (매도 X) |

**Thesis-breaker monitoring (분기별 추적)**:

- (1) Publisher revenue share 또는 content licensing 협상 진행 (Reddit·NYT·Reuters)
- (2) AIO quality 사용자 평가 (사용자 만족도 조사, ChatGPT 이탈률)
- (3) Apple deal 재협상 결과 (2026~ 매년)
- (4) DOJ DC Circuit 항소 판결 (2027~2028)
- (5) Agent mainstream 채택률 (Operator·Computer Use consumer MAU)
- (6) Gen Z Google 쿼리량 (Google이 cohort 데이터 공개하기 시작하면)

**Stream 4 valuation 입력**:

- 위 시나리오 표 → `GOOGL_alphabet.md` Stream 4-Pre Q2 입력
- 추가 핵심 질문: "현재 +19% 가속이 가격에 반영됐는가? 5년 +5~7% Base 시나리오를 시장이 가격에 반영 중인가?"

---

## Q3. CAPEX ROI 회수 가능성

### 질문

- 2026 capex $175-185B 가이던스 → 5년 누적 $1T+?
- 이 capex가 5년 후 매출·EBITDA로 회수되는 시나리오와 시점
- ROIC 22% → ?%
- 역사적 비교: 2000 telecom capex (실패) vs 2010 모바일 capex (성공) 어느 쪽?

### 본인 직관 (작성 시점)

- **2026-05-03 (v1)**

  **회수 메커니즘**:

  - **단기 (5y)**: Cloud + Gemini 포함 AI 구독 (Workspace AI, AI Pro/Ultra)
  - **장기 (7~10y)**: AI 광고 추가 (Q6 영역)
  - 광고는 나중 — 현재 +19% Search 매출은 publisher 손해 감수의 단기 결과 (Q2)이라 sustainable 의문

  **역사적 비교 직관**: **2010 모바일 패턴** (실제 생산성 변화 일어나고 있음, telecom 닷컴 패턴 아님)

  **회수 trigger** (2개):

  1. 사용자 retention (서비스 머무름 + 데이터 만들기 + 광고 노출 시간 유지) — Q2 thesis 직결
  2. 실제 AI-powered 생산성 제공 (기업·개인) + infra 잘 제공

- **2026-05-03 (v2 형성, AI 인풋·반대 의견 후)**

  **균형 잡힌 thesis (Bull + Bear 양면 인정)**:

  | 영역 | v2 직관 | Bull/Bear |
  | --- | --- | --- |
  | FCF 우위 5년 지속 | "그럴듯. Google 마이너스 되면 다른 회사 더 큼" | Bull |
  | Productivity Paradox | **(a) Solow Paradox + (c) Deployment 초기 단계** (overhyped 아님) | 장기 Bull, 단기 reality check |
  | Cloud +20% / Patel "no profits 2027" | "가능할 것 같은데" | Bear weight ↑ |
  | Aggregation Theory | **수직통합 우위** (capex 회수 자체 가능) | Bull |
  | Multiple compression -30~50% | "충분히 가능하다고 봄" | Stream 4 안전마진 |
  | Combined thesis-breaker 발생 확률 | **중간** | EV에 weight |

  **새 frame**: "Bullish on the company but bearish on the price" (Druckenmiller 패턴). 사업은 Bull이지만 가격은 Bear 가능성 인정 → **매수 가격을 Base + Bear 가중치 평균 기반 + 안전마진 크게**.

  **새로 등장한 thesis-breaker (combined)**:

  - (a) Cloud 성장률 +20% 둔화 + (b) Productivity 미실현 + (c) Multiple compression
  - 셋 다 동시 발현 시 닷컴 패턴 partial 재발 (살아남은 회사도 -60~95%, 회복 5~16년)
  - 본인 발생 확률 평가: **중간** (Base 30~50% weight 인정)

### 조사 (raw 데이터·출처) → `GOOGL_q3_capex_research.md`

**1단계 (12분기 + Hyperscaler 비교 + AI 구독 + 역사적 비교 + Productivity research, 2026-05-03)**:

- **Capex 5배 증가**: 6.9B (2023 Q2) → 35.7B (2026 Q1). 2024-26 누적 ~$329B
- **FCF 압박 가시화**: Q1 2026 FCF $10.1B (12분기 만에 둘째로 낮음). TTM FCF FY25 $73.3B → Q1 2026 $64.4B (-12%)
- **Big 4 hyperscaler 비교**: 2026 누적 capex ~$725B. **Alphabet 유일 positive FCF** (Amazon -$17~28B, Meta -90% YoY)
- **D&A 가속**: ~$3B → ~$8.5B/Q. **2027년 분기 ~$10-11B 예상**. CFO Ashkenazi: "2026 D&A 의미 있게 가속, 추가 useful-life extension 없음"
- **SP&D segment +19%, 350M 유료 구독자 (+25M Q/Q)**, Gemini Enterprise paid MAU +40% Q/Q
- **Recovery math**: $329B capex × D&A → 2027 분기 ~$42B/yr 추가 비용. Cloud RPO 462B의 50% 24개월 전환 = ~$231B 매출 lock-in. **회수 가능 (단 Cloud 성장률 +20% 유지 조건)**
- **2000 telecom vs 2010 mobile 비교**: 닷컴 시총 wipeout $2T+ vs capex $500B (5배 premium). **2024-26 AI는 11배 premium** ($15T 시총 vs $1.33T capex). 닷컴 살아남은 회사 정점-저점 -60~95%
- **Productivity Paradox**: NBER 2026.02 — 90% 기업 measurable 영향 zero. Solow Paradox 패턴 (10~20년 lag). SW eng만 명확한 uplift

→ 상세 데이터·출처·도메인 용어 모두 별도 파일 `GOOGL_q3_capex_research.md` 참조.

### 도달한 답 + 시나리오 입력 (Q3 종결)

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| **Capex 회수 (5y)** | RPO 60%+ 전환, Cloud +30%/yr 유지 | RPO 50% 전환, Cloud +25%/yr | **RPO 30~40%, Cloud +20% 둔화 (Patel scenario)** |
| **FCF (2027~2028)** | 압박 후 회복 (+$70B/yr) | flat ($50-60B/yr) | **negative possible** |
| **Productivity** | 2028~ Solow paradox 해소 | 점진 (5년+) | 측정 불가 지속, FOMO 둔화 |
| **Aggregation Theory** | 수직통합 우위 강화 | 유지 | 자본 부담 인식 |
| **Multiple Compression** | -10% 이내 | -10~20% (정상화) | **-30~50% (닷컴 partial 재발)** |
| **FCF 우위 (vs Big 4)** | 더 강화 | 유지 | 격차 좁혀짐 |
| **확률 weight (본인)** | ~30% | ~40% | **~30%** |
| **Thesis 상태** | 강화 | 유지 | 시나리오 변동 (매도 X, **추매 기회**) |

**Thesis-breaker monitoring**:

- (1) Cloud 분기 성장률 +20% 이상 유지 여부 (2026~2028)
- (2) NBER·McKinsey 연간 productivity follow-up 연구
- (3) 2027년 D&A 분기당 실제 수치 vs 매출 가속 비교
- (4) Pichai 어닝콜 멘트 ("ROI", "productivity", "FOMO" 키워드 빈도)
- (5) Hyperscaler FCF 차이 변화 (Amazon·Meta 압박 vs Alphabet 우위)
- (6) Combined 시나리오 발생 시 → 추매 기회 (fundamentals 살아있는 한)

**Stream 4 valuation 입력**:

- 위 시나리오 표 → `GOOGL_alphabet.md` Stream 4-Pre Q3 입력
- 핵심 추가 frame: **Bull 30% / Base 40% / Bear 30% 가중치 평균 기반 fair value**
- **안전마진 30%+ 적용** (Multiple compression risk 반영)

---

## Q4. Gemini 모델 차별화 (월드모델·backbone 수요)

### 질문

- Gemini가 LMSys 순위 2등이지만 차별화 영역 (월드모델·로보틱스·과학 AI)이 가치 창출로 이어지나?
- "1등 모델 아니어도 1등 분포로 가치 창출" 가설 검증
- Backbone 수요처 (검색·Android·YouTube·Workspace) 합산 가치
- DeepMind의 RL·multimodal 본질이 LLM 일변도 OpenAI와 어떻게 다른지

### 본인 직관 (작성 시점)

- **2026-05-02 (v1)**: "다른 provider가 안 가는 길 + backbone 수요처 = 가치 창출 가능. 단 실제 매출·해자 강화로 이어지는지는 별개"

- **2026-05-03 (v2 형성, light touch)**

  | sub-question | 본인 직관 |
  | --- | --- |
  | DeepMind 차별화 매출 채널 (Genie·Veo·Robotics·AlphaFold) | "잘 모르겠음" → 시나리오 trigger로 monitor |
  | Backbone vs Standalone 수익성 | **전자 (Backbone) 더 높음. 기존 서비스 해자 강화** ⭐ |
  | Apple Intelligence 통합 | "잘 모르겠음" → 시나리오 trigger |

  **핵심 통찰 (#2 — Stream 1-B 해자 분석의 frame)**:

  - Google AI 가치 창출 메커니즘 = **기존 platform 해자 강화** (검색 90% + Android 70% + YouTube 2.5B + Workspace 3B + Gmail에 AI 통합)
  - OpenAI는 **Standalone** — 새 사용자 base 구축 + 새 습관 형성 cost 큼
  - Google은 **Backbone Integration** — 사용자 행동 변경 X, 사용성만 ↑, **이탈 방지 + 해자 강화**
  - **측정 어려움** (회사도 "approximately the same rate" 정성 표현만) → **시장 underestimate 가능성, 숨은 가치**
  - Q1·Q2 데이터 검증: AIO 2B + Gemini 750M = 3배 ChatGPT, AI Overviews 광고 monetization same rate, SP&D +19% 가속

### 조사 — Q1·Q2 조사로 일부 검증 + 추가 backbone 데이터

- 이미 검증된 부분 (별도 조사 X):
  - Backbone 수요: AIO 2B MAU, Gemini App 750M MAU, Workspace 3B 사용자
  - Standalone 비교: ChatGPT 900M WAU, Gemini App 750M MAU
  - AI Overviews monetization "approximately the same rate" (Pichai)
  - Anthropic·Apple multi-cloud 채택 = TPU + Gemini 부분 lock-in (Q1 데이터)
- DeepMind 차별화 매출 채널 (Genie·Veo·Robotics·AlphaFold) — 별도 조사 안 함 (light touch). 시나리오 trigger로 monitor

### 도달한 답 + 시나리오 입력 (Q4 종결)

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| **Backbone Integration 효과** | 검색·Workspace·YouTube 해자 강화 → retention +5%pt, 갱신율 ↑ | 해자 유지 (이탈 방지) | Backbone 효과 미미 (OpenAI standalone 흡수) |
| **DeepMind 차별화 매출** | 월드모델·로보틱스 5년 내 새 매출 채널 ($10-30B) | 옵션 (R&D 지속, 매출 기여 미미) | R&D 비용만 발생 |
| **Apple Intelligence Gemini 라우팅** | 본격 통합 (default search·Siri 통합) — Gemini value +20% | 부분 통합 (선택 옵션) | ChatGPT default 유지 |
| **Thesis 상태** | 강화 (해자 + 차별화 영역) | 유지 (해자만) | 시나리오 변동 |

**Stream 1-B 해자 분석 입력 (★ 핵심)**:

> Google 해자의 AI 시대 새 메커니즘 = **Backbone Integration**. 기존 platform 해자가 AI로 강화 (retention·갱신율 ↑, 이탈 방지). OpenAI standalone과의 본질적 차이. 측정 어려워 시장 underestimate 가능 — **숨은 가치**.

**Thesis-breaker monitoring**:

- Apple Intelligence default routing 결정 (2026~2027)
- DeepMind 차별화 영역 매출 본격 발현 시점 (2027~2028)
- 본인 사용자 비교: Gemini in Workspace vs ChatGPT standalone 사용성

---

## Q5. TPU 자체 chip 사용률·차별화 (수율·전파율)

### 질문

- Gemini·Google 자체 모델 학습 compute 중 TPU 비중 (vs NVIDIA)
- Anthropic Claude의 TPU 사용률 (Google 파트너십)
- Cloud 고객 (외부 모델사) TPU 채택 가능성
- TPU 단가 우위 (Gemini -78%/yr 절감)의 지속 가능성
- 차세대 TPU v6/v7 vs NVIDIA Blackwell/Rubin 격차

### 본인 직관 (작성 시점)

> _별도 세션에서 작성_

### 조사 — 1차 정보

- Google ML Infrastructure 발표 (Google I/O 2024·2025)
- Cloud Next conference TPU 발표
- Pichai·Hassabis 어닝콜 TPU 관련 발언

### 조사 — 외부 추정

- SemiAnalysis (Dylan Patel) — chip·인프라 분석
- Epoch AI — AI compute 추적
- The Information GCP 시리즈

### 조사 — 비교

- NVIDIA 분기 매출 vs Google 자체 chip 비용 추정
- Microsoft Maia·AWS Trainium 자체 칩 비교
- Anthropic-Google 파트너십 ($4B 투자, 2024) 분석

### 조사 — 수율·생산 (TSMC)

- TSMC 5nm/3nm 공정에서 TPU·NVIDIA·Apple 칩 경쟁
- TPU 생산 capacity vs NVIDIA H100/B100

### 도달한 답 + 시나리오 입력

| Bull | Base | Bear |
| --- | --- | --- |
| TPU 우위 지속, 외부 모델사 (Claude 등) 채택 확대, NVIDIA 의존도 감소 | 자체 모델 100% TPU, 외부 일부 채택 | NVIDIA Blackwell 격차 좁혀 TPU 우위 약화 |

---

## Q6. AI 답변 상업화의 미래 (광고 모델 변화)

### 질문

- AI 답변에서 광고/sponsored 통합 가능성·시점
- 검색 광고 CPC vs AI commission 단가 격차
- 사용자 신뢰 trade-off (AI sponsored 답에 대한 의심)
- Google·OpenAI·Perplexity 누가 먼저 큰 매출 채널 만들까

### 본인 직관 (작성 시점)

- **2026-05-02 (v1)**: "AI 답변에 광고는 당연히 추후 구현. 광고 플랫폼 보완 효과"
- **2026-05-03 (v2, light touch)**: 광고 vs Commission 모델 비교 — **"잘 모르겠음"** → 시나리오 trigger로 monitor

### 조사 — Q2 조사로 일부 검증 (별도 깊이 X)

이미 검증된 부분:

- AI Overviews sponsored 광고 monetization "approximately the same rate as traditional Search" (Pichai)
- AI Mode "Direct Offers" pilot 진행 중 (2025-26)
- OpenAI ChatGPT × Shopify (2024.11), Apple Intelligence affiliate (2024.10)
- Perplexity sponsored related questions (2024.10), $200M ARR
- Anthropic Operator → OpenTable·Expedia 예약 commission 가능성

### 도달한 답 + 시나리오 입력 (Q6 종결, Q2와 통합)

Q6는 Q2 시나리오에 흡수. 별도 시나리오 X.

| Q2 시나리오와 통합 | Bull | Base | Bear |
| --- | --- | --- | --- |
| AI 답변 광고 통합 | 광고 + Commission 동시 작동 → Search 매출 +12% | 광고 모델만 (AIO sponsored) → +5~7% | 둘 다 미미 → +1~3% |
| Google advantage | 광고주 base 수백만 활용 → Commission 빠른 도입 | AIO sponsored 점진 확대 | OpenAI·Shopify에 시장 잠식 |

**Thesis-breaker monitoring**:

- OpenAI Shopify·Apple commission 매출 발현 (분기별)
- Perplexity sponsored 매출 가속 ($200M ARR → ?)
- Google AIO sponsored·AI Mode "Direct Offers" 매출 발현 (Pichai 어닝콜 멘트)
- Anthropic Operator commission 매출
- 광고주 적응 시간 (검색 광고는 10년+ 걸렸음 — AI 광고 같은 timeline?)

---

## Q7. YouTube 지속 성장

### 질문

- YouTube ads +18% CAGR 지속 가능성
- TikTok·Reels와의 점유 경쟁
- Premium·NFL Sunday Ticket 등 구독 매출 증가 추세
- Shorts 광고 단가 vs 인스트림 광고

### 본인 직관 (작성 시점)

- **2026-05-03 (lightest)**: "잘 모르겠음" → 시나리오 trigger로 monitor

### 도달한 답 + 시나리오 입력 (Q7 종결, lightest)

기준 데이터 (Stream 1-A): YouTube ads $38B (+18% CAGR), SP&D $50B (+20% CAGR, YouTube Premium·NFL·YouTube TV 포함). YouTube 합계 ~$60B+ (Alphabet 매출 ~15%).

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| YouTube ads 5y CAGR | +20% (Shorts 단가 회복 + 새 광고 포맷) | +15% (정상화) | +10% (TikTok·Reels 점유 잠식) |
| Premium·NFL·YouTube TV | 가속 (광고 free 선호 + 스포츠 통합) | 안정 +10% | 둔화 |
| YouTube 합계 5y | $60B → $130B+ (2배+) | $60B → $110B (1.8배) | $60B → $90B (1.5배) |

**Thesis-breaker monitoring**:

- TikTok·Reels 분기 점유 변화 (Sensor Tower)
- YouTube DAU 추이 (Neal Mohan 어닝콜 멘트)
- NFL Sunday Ticket 갱신율 (스포츠 lock-in)
- YouTube Music vs Spotify 점유 변화

---

## 학습 진행 로그

| 날짜 | 질문 | 진행 | 비고 |
| --- | --- | --- | --- |
| 2026-05-02 | (시작) | 문서 골격 작성, 본인 직관 일부 작성 (Q4·Q6) | |
| 2026-05-03 | Q1 | 직관 v2 형성 완료 (점유·회수·마진·시장TAM·Bear메커니즘). thesis-breaker는 조사 단계로 후속 | AI와 인풋 교환 (사업구조·경쟁구도·반대의견)으로 직관 sharpen |
| 2026-05-03 | Q1 | 1차 조사 완료 (12분기 GCP·AWS·Azure 데이터, 어닝콜 멘트). 추가 직관 v3 형성 (capacity constrained = thesis re-evaluation trigger). Thesis-breaker 후보를 (b) Trainium·Maia·TPU 격차로 sharpen | 다음 조사: (ii) Trainium·Maia vs TPU 격차 |
| 2026-05-03 | Q1 | **Q1 종결**. 2단계 조사 완료 (AI Chip 격차). 직관 v4 형성 (TPU ~50% TCO 우위지만 ~30%는 software, 하드웨어 ~20~25%). Thesis-breaker re-frame: Trainium/Maia → CoWoS·Rubin·CUDA moat. Multi-vendor 균형 (Goldman 35% TPU 점유) base case. Q1 raw 데이터 → `GOOGL_q1_cloud_research.md` 분리. Stream 1-A 직관 박스 + Stream 4-Pre 시나리오 입력 완료 | 다음: Q2 (검색 AI 위협) 진입 |
| 2026-05-03 | Q2 | 본인 직관 v2 형성 (사용 패턴 자체가 검색 unbundling evidence + AI Overviews retention + LLM n빵 + 광고 단가 Bull). Thesis-breaker = AI agent 일반화 + Gen Z 행동 변화 (combined) + DOJ Apple deal 금지. DOJ 사건 풀이 완료 | 다음 조사 (a): 검색 광고 12분기 매출 + AI 검색 외부 데이터 + DOJ remedy 진행 |
| 2026-05-03 | Q2 | **Q2 종결**. 1차 조사 완료 (Search 12분기 +4.8% → +19% 가속, AIO 2B MAU, DOJ Apple deal 유지, Gen Z TikTok 선호 50% 감소). 직관 v3 형성 (영토 방어 단기 감내 = sustainable 아님, publisher 관계 재조정 새 thesis-breaker). Q2 raw 데이터 → `GOOGL_q2_search_research.md` 분리. Stream 1-A 직관 박스 + Stream 4-Pre Q2 시나리오 입력 완료 | 다음: Q3 (Capex ROI 회수) 진입 |
| 2026-05-03 | Q3 | **Q3 종결**. 1차 조사 완료 (12분기 capex 5배 + Hyperscaler 비교 + AI 구독 + 2000/2010 역사적 비교 + Productivity research). 직관 v2 형성 — **균형 잡힌 thesis (Bull 30% / Base 40% / Bear 30%)**. Druckenmiller 패턴 "Bullish on the company but bearish on the price" 채택. Multiple compression -30~50% 가능 인정 → Stream 4 안전마진 30%+. Combined thesis-breaker 발생 확률 중간. Q3 raw 데이터 → `GOOGL_q3_capex_research.md` 분리. Stream 1-A Q3 직관 박스 + Stream 4-Pre Q3 시나리오 입력 완료 | 다음: Q4 (Gemini 차별화 — lighter touch) 또는 Stream 4 직접 진입 검토 |
| 2026-05-03 | Q4·Q5·Q6·Q7 | **Q4·Q6·Q7 종결 (light·lightest touch)**. Q5는 skip (Q1 chip 조사로 답). 핵심 통찰 — **Backbone Integration** (Google 해자 AI 시대 강화 메커니즘). Q6 광고 vs Commission 모델은 시나리오 trigger로 monitor. Q7 시나리오 Q2와 통합 | 다음: Stream 1-B (해자 분석) 진입 |
| 2026-05-03 | Stream 1-B + 회고 | **Stream 1-B 해자 build 완료** (Buffett 5분류 × 5개 사업 + Backbone Integration 횡단 해자). 전체 GOOGL 해자 = Wide (Search 51% Wide → Narrow 트랜지션 risk). Phase 5·6-A Anchoring 비교 — bias 없음. 본인 Backbone Integration frame이 Phase 6-A "수직통합 HIGH"의 AI 시대 sharpening. Stream 1-A·1-B 회고 작성 완료. Framework 변경: 직관 강요 → 정량 build + 본인 review | 다음: Stream 3 (위험) 또는 Stream 4 (가격 평가) 진입 |
| 2026-05-03 | Stream 3 + 회고 | **Stream 3 build 완료** (리스크 19 + 기회 20 매트릭스). 본인 Driver Tree 통찰 통합 (재무 결과 통합 frame, 39개 → 5개 top metric). 본인 4-Period 시나리오 통찰 통합 (5년 single timeline 한계 → 단기 1-2y / 중기 3-5y / 장기 5-10y / terminal). Phase 8 Anchoring 비교 — bias 없음, Phase 8 1차 가설을 sharpen. Monitoring Plan: 분기 Top 5 (5분) + Driver Tree 진단 (이상 시) + 외부 보고 (연 1-2회). 총 ~2-3시간/년 | 다음: Stream 4 (가격 평가) 본격 진입 — DCF + Fisher 기대수익률 + Multiple cross-check |
| 2026-05-03 | Stream 4 + 회고 | **Stream 4 build 완료** (DCF 보수+진보 OE + EPS×PER + Reverse 역산 + 과신 점검 + Phase 10 anchoring 비교). 종목 특성 진단: DCF (1순위) + EPS×PER (2순위). DCF 진보 OE 확률가중 IV $250 vs 현재가 $383 (-35%). EPS×PER 확률가중 CAGR 3.0% < RF 4.35%. **Druckenmiller 패턴 정량 검증** — 시장이 Bull case 88% 반영. **판정: 회색 (대기)**. 매수 trigger: $330 공격 / $230~260 현실 / $175 보수. Phase 10 Anchoring 비교 — bias 없음, Stream 4 더 정교 | 다음: Stream 5 (시장 의견) → Stream 6 (편입 결정 — watch list) |

## 답 형성 후 통합 위치

각 질문 답이 형성되면 다음 위치로 옮김:

- 본인 직관 → `GOOGL_alphabet.md` Stream 1-A 직관 박스 (Q1~Q5 신규)
- 시나리오 입력 → `GOOGL_alphabet.md` Stream 4 (가격 평가) Bull/Base/Bear 가정
- 해자 영향 → `GOOGL_alphabet.md` Stream 1-B 해자 분석
- 리스크/기회 → `GOOGL_alphabet.md` Stream 3
