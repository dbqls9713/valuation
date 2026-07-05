---
ticker: GOOGL
deep_date_started: 2026-05-02
shallow_date: 2026-04-26
track_verdict: fisher
confidence: medium
playbook_version: 통합 v1 (2026-04-30)
---

# Alphabet (GOOGL) Deep Dive — Started 2026-05-02

기준일: 2026-05-02 | Shallow: `research/shallow-dive/GOOGL_alphabet.md` (2026-04-26) | 트랙: Fisher (Phase 9 인계, 확신도 중간)

## Anchoring 차단 상태

- **차단 사본**: `research/fisher/deep-dive/GOOGL_shallow_locked.md`
- **차단 매트릭스**:
  - ~~Stream 1-B (해자 분석): Phase 5 (산업 지도) + Phase 6-A (버핏 해자) 차단~~ — **2026-05-03 해제 비교 완료** (Anchoring bias 없음, Stream 1-B build가 더 sharp)
  - Stream 2 (경영진): Phase 4 (경영진 프로필 + 1분 테스트 + 가이던스 성격) 차단 — Stream 2 종료 후 비교
  - Stream 3·4·6: 본문 진입 시점에 차단
- **참조 가능**: Phase 1·2·3·6-B/C/D·7·9
- **트랙 배정**: Fisher (Phase 9 인계). 재배정 가능성 — 재배정 플래그는 Stream 6 진입 전 점검

---

## Stream 1: 사업·산업·기술 + 해자 분석

### 1-A. 사업·산업·기술 이해

#### 매출 분해

##### 제품 기준 (FY2025 — Phase 3 정량 기초 + 외부 IR 자료)

매출 $402.8B (FY2025) 기준. SEC 10-K + IR 자료.

| 제품 | FY2025 매출 ($B) | 비중 | 5년 추이 | 본질 |
| --- | ---: | ---: | --- | --- |
| **Google Search & Other** | ~205 | 51% | 5년 매출 +14% CAGR (느린 성장 본업) | 검색 광고 본체 — 검색결과 옆 광고 (CPC) + AI Overviews 통합 |
| **YouTube ads** | ~38 | 9.4% | +18% CAGR (비디오 + Shorts) | 비디오 광고 (인스트림·인피드·YouTube Premium 별도) |
| **Google Network** | ~30 | 7.4% | +5% CAGR (정체 — 1st party 광고 우선) | AdSense (퍼블리셔) + AdMob (앱) — 외부 사이트·앱 광고 |
| **Subscriptions·Platforms·Devices** | ~50 | 12.4% | +20% CAGR (성장 빠름) | YouTube Premium·YT Music·NFL Sunday Ticket·YouTube TV·Pixel·Play Store |
| **Google Cloud (GCP + Workspace)** | ~75 | 18.6% | +43% CAGR (가속 가속) | GCP 컴퓨트·스토리지·AI 서비스 + Workspace SaaS |
| **Other Bets** | ~2 | 0.5% | +30% CAGR (Waymo 견인) | Waymo (자율주행) + Verily (헬스케어) + 기타 R&D |
| **Hedging gains** | ~3 | 0.7% | 변동 | FX hedging |
| **합계** | **402.8** | 100% | +17% CAGR | |

> **차단 영향 없음**: 위 매출 분해는 Phase 3 정량 기초 (참조 가능) + 공개 10-K 자료. Phase 5 산업 지도 차단과 무관.

##### 공시 기준 (세그먼트별 — 10-K 보고 구분)

Alphabet 공시 세그먼트는 2가지로 단순화 (FY2024 reorganization):

| 공시 세그먼트 | FY2025 매출 | 영업이익률 | 본질 |
| --- | ---: | ---: | --- |
| **Google Services** | ~325B (81%) | ~40% | 광고 (Search·YouTube·Network) + Subscriptions·Devices |
| **Google Cloud** | ~75B (18.6%) | ~17% (확장 중, 2024 12% → 2025 17%) | GCP + Workspace |
| **Other Bets** | ~2B (0.5%) | -100%+ (적자) | Waymo·Verily·기타 |
| **Hedging gains** | ~3B (0.7%) | — | |

**공시 변경 이력**: 2024 Q1부터 Other Bets 분리 + Cloud 영업이익률 별도 보고 (이전엔 Other Bets 합산). 신/구 기준 모두 추적 가능.

##### 분기별 추적 가능 지표 (공시 vs 추정 구분)

| 지표 | 공시 / 추정 | 출처 / 근거 |
| --- | --- | --- |
| Search 매출 | **공시** | 10-K Item 7 + 분기 보고서 |
| YouTube ads 매출 | **공시** | 10-Q부터 |
| Google Network 매출 | **공시** | 10-Q |
| Subscriptions·Platforms·Devices 매출 | **공시 (합산)** | 10-Q. 단 Pixel/Premium/Play Store 분리는 추정 (IR 발언 + 사업 활동 보도) |
| Cloud 영업이익 | **공시** | 2024 Q1부터 분기 |
| Cloud RPO 백로그 | **공시** | 분기 supplemental |
| Search 점유율 (글로벌) | **추정** | StatCounter — Google이 직접 공시 안 함 |
| AI Overviews CTR | **추정** | 외부 분석사 (Similarweb·BrightEdge 등) — 공시 없음 |
| Gemini App MAU | **공시 (선택적)** | Pichai 발언 (2025 어닝콜 750M 발표) |
| Gemini 유료 전환율 | **추정** | 공시 없음 — 외부 추정 |
| Waymo 운행 수·도시 | **공시 (선택적)** | Waymo press release |

#### 다음 세대 파이프라인 (N+1, N+2, N+3)

##### 핵심 N+1 (1~2년 내 매출 기여)

1. **Gemini 3 + Agentic AI** — Project Astra·Mariner (2025 발표, 2026 상업 launch). Workspace·검색 통합. **N+1 매출 기여 시점 2026 H2** 예상
2. **TPU v6 (Trillium 후속)** — 2026 deploy. Gemini 3 trainer + 외부 GPU 의존도 감소 → Cloud margin 확장
3. **Waymo One 확장** — 5개 도시 (SF·LA·피닉스·오스틴·마이애미) → 10+ 도시. Uber 파트너십 + 자체 앱

##### N+2 (3~5년)

1. **Gemini Robotics** — DeepMind 발표 2024.06. 휴머노이드 제어. **N+2 — 2027~2028** 매출 채널
2. **Google Tensor (Pixel 자체 칩)** — Pixel 매출 +30% 가속 + Watch·Buds 확장
3. **Project IDX·Stitch (개발자 도구 SaaS)** — Cursor·Replit 경쟁
4. **AI Mode 검색 (chat-style search)** — Perplexity·ChatGPT 대응. Search ads 마진 압박 vs 새 광고 포맷

##### N+3 (5~10년)

1. **양자컴퓨팅 (Willow chip 2024 발표)** — 2030+ 상업화. 현재는 R&D 단계, 매출 기여 없음
2. **Verily AI 헬스케어** — Stage I, 매출 기여 미미
3. **Waymo Trucking** — 2024 일부 시범, 본격 상업은 2030+

#### 산업 구조 (5 Forces) — 4축 다른 산업

GOOGL은 **검색 광고·디스플레이 광고·Cloud·AI 모델** 4축이 본질적으로 다른 산업. 5 Forces를 4번 적용해야 정확. 단 본 Stream에서는 **본업 검색 광고 + 신성장 Cloud** 두 축으로 압축.

##### 검색 광고 산업 (51% 매출)

| Force | 강도 | 근거 |
| --- | :-: | --- |
| 신규 진입 위협 | LOW | 검색 인프라 자본 규모 + 데이터 corpus 진입장벽. 단 **AI native 검색 (Perplexity·ChatGPT)** 진입은 다른 차원 |
| 대체재 위협 | **HIGH** ⚠️ | **AI 챗봇이 검색 동작 일부 대체** — 2024년부터 Search 트래픽 일부 이동 (외부 분석 추정 -3~5%pt) |
| 공급자 협상력 | LOW | 광고주는 분산 (수백만 SMB) |
| 구매자 협상력 | LOW | 같은 사유 |
| 산업 내 경쟁 | MED | Google 90% 점유. 직접 경쟁자(Bing 3%) 약하지만 AI 챗봇 변수 |

##### Cloud (GCP + Workspace) 산업 (19% 매출, 가속 중)

| Force | 강도 | 근거 |
| --- | :-: | --- |
| 신규 진입 위협 | LOW | 자본·기술 진입장벽 (DC 인프라) |
| 대체재 위협 | LOW | Cloud 자체가 대체재 (on-prem → cloud), 다른 대체 없음 |
| 공급자 협상력 | MED | NVIDIA GPU 의존 (단 GOOGL 자체 TPU로 일부 회피) |
| 구매자 협상력 | MED | 엔터프라이즈는 멀티클라우드 전략 — switching 비용은 상승 중 |
| 산업 내 경쟁 | **HIGH** | AWS 30% (1위) · Azure 25% (2위) · GCP 12% (3위, 가속). Three-horse race |

#### 산업 성장 경로 10년

##### 디지털 광고 산업

- 2025 글로벌 디지털 광고 ~$700B → 2035 ~$1.4T (외부 추정 +7% CAGR, eMarketer/Magna)
- 단 **AI 챗봇이 검색 광고 mix를 잠식**하면 검색 광고 segment 성장률 ~5%로 둔화 가능. AI search ads 새 포맷이 보완할지 미지수

##### Cloud 산업

- 2025 글로벌 Cloud (IaaS+PaaS+SaaS) ~$700B → 2035 ~$2.8T (외부 추정 +15% CAGR, Gartner/IDC)
- AI 워크로드 가속 — 2025-2028 +20% CAGR

##### AI 인프라 (전용 segment)

- 2025 AI 인프라 (chip·DC·model serving) ~$200B → 2030 ~$1T+ (외부 추정 +40% CAGR)
- GOOGL의 TPU + Gemini가 양분야 모두 노출

#### 기술 변곡점 지도

##### 변곡점 후보 1: AI search 본격 침투 (시점 2026~2028)

- 현재 Google이 AI Overviews로 일부 통합. 단 **유료 검색 광고 단가가 AI 답변 환경에서 어떻게 작동하는지** 미정
- ChatGPT·Perplexity가 GPT-5+·Sonar+ 세대에서 **search-like 정확도 + 광고 모델** 성공하면 변곡점
- **GOOGL 위치**: 양면 — Gemini로 자체 AI 검색 보유 + Search 본업 잠식 위험. 방어 + 공격 동시

##### 변곡점 후보 2: 자체 칩 (TPU) 효율 격차 vs NVIDIA

- 2025 GOOGL TPU v6 (Trillium): Gemini serving 단가 -78%/yr 달성
- 2026 TPU v7 예정. NVIDIA GB300/Rubin 대비 비용 우위 유지하면 Cloud 마진 +10%pt 가능
- **변곡점**: NVIDIA 가격 인하·MS Azure 자체 칩(Maia)·AMD MI400 격차 좁히면 GOOGL 우위 약화

##### 변곡점 후보 3: 양자컴퓨팅 상업화 (2030+)

- 2024 Willow chip 발표 (105 qubit, error correction 진전)
- 상업 application: 신약 발견·재료 과학·암호화. 2030+ 매출 기여 시작
- **GOOGL 위치**: 가장 앞선 풀스택 (chip + algorithm + cloud). 단 IBM·IonQ도 추적

#### 외부 자료 (출처)

- 10-K FY2025 (SEC EDGAR) ✓
- 분기 어닝콜 transcript 8분기 (FactSet 또는 Seeking Alpha)
- IR 사이트 + supplemental
- 외부 리서치: eMarketer (디지털 광고 forecast), Gartner Magic Quadrant Cloud, Synergy Research (Cloud 점유), StatCounter (검색 점유)
- 학술/표준화: 변곡점 후보 — DeepMind 논문 (Gemini 아키텍처), Google Research blog
- 업계 분석: Stratechery (Ben Thompson), The Information

#### 직관 박스 — Q1 Cloud 지속 성장 (2026-05-03 종결)

> 출처: `GOOGL_research_questions.md` Q1 + `GOOGL_q1_cloud_research.md` (12분기 1차 자료 + AI Chip 격차 조사 종합)

**Q1 종결 직관 (v4)**:

- **Cloud 성장 가속 검증**: GCP 12분기 8.0B → 20.0B (2.5배), 4분기 연속 hyperscaler 성장률 1위. 2026 Q1 +63% (capacity 풀리면서 막혔던 수요 매출 전환). 3대 hyperscaler 동시 가속 = AI 수요는 zero-sum 점유 재편 아닌 **시장 자체 expansion**
- **마진 회복 본인 예상보다 빠름**: Op margin 4.9% (2023 Q2) → 32.9% (2026 Q1), AWS 37.7%와 격차 5%pt 이내. 단 capacity constrained 환경의 일시적 프리미엄 가능성 — capacity 풀리는 시점이 thesis re-evaluation trigger
- **TPU 우위는 진짜지만 압도적 아님**: ~50% TCO 우위 (SemiAnalysis) 중 software 부분 ~30%, 하드웨어 순수 ~20~25% 추정. NVIDIA 사용자도 software 절감 가능. Multi-vendor 균형이 base case (Anthropic의 multi-cloud 채택은 risk hedging)
- **Capex 회수 visibility 입증**: Cloud RPO 462B (2026 Q1), "24개월 내 50% 매출 전환" 가이던스. 향후 2년 매출 floor가 230B+ lock-in
- **2027 Q1 (Rubin 출시) + 2025·2026 capex 본격 감가상각 = 이중 stress test 시점**

**Q1 Thesis-breaker monitoring**:

- TSMC CoWoS 할당 (Google 2026 TPU 생산 25% 감산 — 칩 공급이 매출 cap)
- NVIDIA Rubin (2027 Q1) HBM4 22 TB/s가 TPU 메모리 대역폭 우위 erase 가능
- CUDA moat (비-frontier 고객 software stack stickiness)
- 어닝콜 멘트 "constrained" → "balanced" → "excess capacity" 변화 시점

#### 직관 박스 — Q2 검색 AI 위협 (2026-05-03 종결)

> 출처: `GOOGL_research_questions.md` Q2 + `GOOGL_q2_search_research.md`

**Q2 종결 직관 (v3)**:

- **Search 매출 가속 검증 (단 sustainable 아님)**: 2023 Q2 +4.8% → 2026 Q1 +19% (11분기 연속 두 자릿수). AI Overviews 출시 후에도 매출 가속만. 단 본인 직관: "Google이 publisher 손해를 단기로 감수 중 = AI 시대 영토 방어 우선. 5년 지속 불가" → Base 시나리오에서 redistribution 메커니즘 변경 (-3~5%pt op margin)
- **Google AI 사용자 base 압도**: AIO 2B + Gemini 750M = 2.75B (ChatGPT 900M의 3배). LLM n빵에서도 압도적 점유
- **DOJ 위협 본인 Bear 가정보다 약함**: 2025.09 Mehta — Chrome 매각 X, Apple deal 유지 (1년 계약 한정). "Apple dodged $20B hit". 단 매년 협상 → TAC inflation 가능
- **Thesis-breaker re-frame**: Agent + Gen Z는 long-term (2027~2028). 단기 critical = **Publisher 관계 재조정 + AIO quality 하락 (combined)**
- **Zero-click 트렌드 본인 사용 패턴 인정** (자기 데이터). 일반화 시 fresh content ↓ → AIO quality ↓ → 사용자 ChatGPT 이탈 risk

**Q2 Thesis-breaker monitoring**:

- Publisher revenue share / content licensing 협상 진행 (Reddit·NYT·Reuters)
- AIO quality 사용자 평가 / ChatGPT 이탈률
- Apple deal 매년 재협상 결과 (TAC inflation)
- DOJ DC Circuit 항소 판결 (2027~2028)
- Agent mainstream 채택률
- Claude 엔터프라이즈 점유 (32% → ?, 본인 사용 패턴 일반화 leading indicator)

#### 직관 박스 — Q3 Capex ROI 회수 (2026-05-03 종결)

> 출처: `GOOGL_research_questions.md` Q3 + `GOOGL_q3_capex_research.md`

**Q3 종결 직관 (v2)**:

- **균형 잡힌 thesis (Bull + Bear 양면 인정)**: "Bullish on the company but bearish on the price" (Druckenmiller 패턴)
- **회수 메커니즘 단기**: Cloud + AI 구독 (광고는 나중). Cloud RPO 462B 50% 24개월 전환 = $231B 매출 lock-in으로 D&A $42B/yr 흡수 가능 (단 Cloud +20% 유지 조건)
- **FCF 우위**: Alphabet 유일 positive FCF in Big 4 hyperscaler. 다른 hyperscaler 적자 위험 더 큼
- **Aggregation Theory**: 수직통합 우위 (capex 짊어지면서 platform 매출도 가져감) — 본인 직관
- **Productivity Paradox**: Solow Paradox + Deployment 초기 단계. 2028+ 본격 발현 (overhyped 아님)
- **Multiple compression -30~50%**: 충분히 가능 (닷컴 살아남은 회사도 -60~95%). Stream 4 안전마진 강화 필요
- **확률 weight**: Bull 30% / Base 40% / Bear 30%

**Q3 Thesis-breaker monitoring**:

- Cloud 분기 성장률 +20% 이상 유지 여부 (2026~2028 핵심)
- 2027년 D&A 본격화 시점 매출 가속 vs D&A 가속 비교
- Productivity 실측 연구 (NBER·McKinsey follow-up)
- Hyperscaler FCF 격차 변화 (Alphabet 우위 지속 여부)
- Combined thesis-breaker 발생 시 → 추매 기회 (fundamentals 살아있는 한)

#### 직관 박스 — Q4 Gemini 차별화 (2026-05-03 종결, Light Touch)

> 출처: `GOOGL_research_questions.md` Q4 (Q1·Q2 조사 데이터 재활용)

**Q4 종결 직관 (v2)**:

- **★ 핵심 frame: Google 해자의 AI 시대 메커니즘 = Backbone Integration**
- **Backbone vs Standalone**: 기존 platform 해자 (검색 90% + Android 70% + YouTube 2.5B + Workspace 3B + Gmail) 위에 AI 통합 → 사용자 행동 변경 X, 사용성만 ↑ → **이탈 방지 + 해자 강화**
- OpenAI는 standalone (새 사용자 base + 새 습관 형성 cost 큼). 본인 직관: backbone 통합이 더 수익성 높음 (단 측정 어려움)
- **시장 underestimate 가능 — 숨은 가치**: 회사도 "approximately the same rate" 정성 표현만, 정량 분리 X
- 검증 데이터 (Q1·Q2 재활용): AIO 2B + Gemini 750M = ChatGPT 3배, AIO ads same rate, SP&D +19% 가속
- DeepMind 차별화 (Genie·Veo·Robotics·AlphaFold)·Apple Intelligence 통합 = 시나리오 trigger로 monitor (옵션)

**Q4 Thesis-breaker monitoring**:

- Apple Intelligence default routing (2026~2027)
- DeepMind 차별화 매출 본격 발현 (2027~2028)
- Workspace AI 갱신율·검색 retention (정량 데이터 등장 시)

#### 1-A 회고 블록 (2026-05-03 작성)

**작업 결과**:

- 매출 분해 (제품·공시·추적 가능성), 다음 세대 파이프라인, 5 Forces, 산업 성장 경로, 기술 변곡점 골격 완료
- Q1~Q7 학습 + 직관 형성 (`GOOGL_research_questions.md`)을 통해 사업 이해 깊이 확보:
  - **Q1 Cloud 지속 성장** — Cloud +43% → 단기 +63% (capacity 풀림), 마진 32.9% 도달, RPO 462B (24개월 50% 전환)
  - **Q2 검색 AI 위협** — Search 매출 +19% 가속 (publisher 손해 감수), DOJ Apple deal 유지, AI agent + Gen Z 2027~2028 trigger
  - **Q3 Capex ROI 회수** — 균형 thesis (Bull 30% / Base 40% / Bear 30%), Druckenmiller 패턴 적용
  - **Q4 Gemini 차별화** — Backbone Integration이 핵심 frame
  - **Q6 AI 답변 상업화** — light touch, Q2 시나리오에 통합
  - **Q7 YouTube** — lightest, Stream 4 매출 동인

**잘 된 점**:

1. **직관 v1 → v2 → v3 진화** 추적 (Q1 직관이 12분기 데이터 + chip 조사 후 sharpen)
2. **Thesis-breaker re-frame**: Q1에서 Trainium/Maia → CoWoS·Rubin·CUDA로, Q2에서 DOJ Apple → Publisher 관계·AIO quality로, Q3에서 단순 ROI → Combined (Cloud·Productivity·Multiple compression)
3. **반대 의견 활용**: AI 도구로 Bull thesis에 Bear 메커니즘 (productivity paradox, multiple compression, dark fiber 비교) 자극
4. **자기 데이터 입력**: 본인 사용 패턴 (Claude·쿠팡·YouTube + Zero-click) 이 검색 unbundling evidence

**부족한 점·교훈**:

1. **모든 sub-question에 직관 form 강요**가 피로감 유발. Light touch 영역에서는 정량 데이터 + 분석가 consensus로 build 후 review 패턴이 효율
2. 일부 영역 (DeepMind 차별화 영역, Apple Intelligence 통합) 은 본인이 진짜 모르는 영역 — 시나리오 trigger로만 monitor
3. 도메인 용어 풀이가 등장 시점에 늦은 case 있음 (RPO, capacity constrained, CoWoS 등 — 처음부터 풀어 설명했어야)

**Phase 5·6-A Anchoring 비교**: 차단 사본 안 보고 form했지만 **큰 결론 거의 동일**. 본인 build가 정량 sharpen + Backbone Integration 새 frame 추가. **Anchoring bias 없음**.

---

### 1-B. 해자 분석 (Buffett 5분류 + Fisher 차원)

> 출처: Q1~Q7 종결 데이터 (`GOOGL_research_questions.md` + `GOOGL_q1_cloud_research.md` + `GOOGL_q2_search_research.md` + `GOOGL_q3_capex_research.md`)
> 작업일: 2026-05-03

#### 사업별 해자 매핑

##### Search 광고 (51% 매출, $205B)

| 해자 | 작동 | 메커니즘 |
| --- | :-: | --- |
| Brand | ✓✓ | "Google" = 동사. 미국 90% 점유, 25년+ 누적 신뢰 |
| Network Effect | ✓✓✓ | 사용자 ↑ → 데이터 ↑ → 알고리즘 ↑ → 결과 quality ↑ → 사용자 ↑. 광고주 base 수백만 |
| Cost Advantage | ✓✓ | 인프라 규모 → single query cost 경쟁자의 1/10 추정 |
| Scale | ✓✓ | 광고주 base + 광고 mix 효율성 |
| Switching Cost | ✓ | 검색 query는 낮음, history·account 일부 |

**약화 신호 (Q2 데이터)**:

- AI competitors (ChatGPT 900M WAU, Perplexity 45M)
- Z 세대 행동 변화 (단 TikTok 선호도 retreat 50% 감소)
- DOJ Apple deal 1년 계약 → distribution moat 약화
- AIO publisher CTR -30% → ecosystem 손상

**강도**: **Wide → Narrow 트랜지션 가능 (5~10년)**. 단 단기 (2-3년) Wide 유지 (AIO 2B MAU + Search +19% 가속)

##### YouTube (~15% 매출, $60B+)

| 해자 | 작동 | 메커니즘 |
| --- | :-: | --- |
| Network Effect | ✓✓✓ | 3-sided network (Creator + Viewer + 광고주). 2.5B MAU |
| Brand | ✓✓ | "YouTube" = 비디오 |
| Scale | ✓✓ | 콘텐츠 라이브러리 + 광고 ecosystem |
| Switching Cost | ✓ | 시청 history·구독·플레이리스트, Premium lock-in |

**강도**: **Wide**

##### Cloud (GCP + Workspace, 19% 매출, $80B run-rate)

| 해자 | 작동 | 메커니즘 |
| --- | :-: | --- |
| Switching Cost | ✓✓✓ | **Cloud RPO 462B = 6년 매출 lock-in**. Multi-year enterprise 계약 |
| Cost Advantage | ✓✓ | TPU ~50% TCO 우위 (SemiAnalysis). 단 software ~30% + CoWoS·Rubin 위협 |
| Scale | ✓✓ | GCP·Workspace 합산 규모 + capex base |
| Brand | ✓ | Workspace 강함, GCP 후발 |

**약화 신호 (Q1 monitoring)**:

- TSMC CoWoS 할당 (TPU 25% 감산)
- NVIDIA Rubin (2027 Q1) 메모리 대역폭 위협
- AWS·Azure 경쟁

**강도**: **Narrow → Wide 트랜지션 진행 중** (마진 4.9% → 32.9%, AWS 격차 5%pt)

##### Subscriptions·Platforms·Devices (12.4%, $50B)

| 해자 | 작동 | 메커니즘 |
| --- | :-: | --- |
| Switching Cost | ✓✓✓ | Workspace·Gmail·Calendar lock-in (3B 사용자), Pixel·Play Store |
| Brand | ✓✓ | YouTube Premium·Workspace 신뢰 |
| Network Effect | ✓ | Play Store ecosystem |

**강도**: **Wide**

##### Backbone Integration (★ 횡단 해자 — Q4 본인 frame)

**Buffett 5 분류 안 위치**: 새 해자가 아니라 **기존 해자의 reinforcement mechanism**

| 기존 해자 | AI로 강화되는 방식 |
| --- | --- |
| Switching Cost | AI 통합으로 Workspace·검색·YouTube 떠나기 더 어려움 |
| Network Effect | AI로 사용성 ↑ → retention ↑ → 데이터 ↑ → AI ↑ (선순환) |
| Cost Advantage | 자체 AI infra (TPU + Gemini) → 마진 ↑ |
| Brand | "Google = AI 통합" 신뢰 |
| Scale | AI 학습·서빙 cost 규모로 분산 |

**강도**: **Wide reinforcement** — 기존 Wide 해자를 AI로 더 Wide하게. **OpenAI standalone과의 본질적 차이**

#### 해자 강도 종합 (가중평균)

| 사업 | 매출 비중 | 종합 강도 | 핵심 해자 |
| --- | --- | --- | --- |
| Search 광고 | 51% | **Wide → Narrow 트랜지션 가능 (5-10y)** ⚠️ | Brand + Network + Cost + Scale |
| YouTube | 15% | **Wide** | Network + Brand + Scale |
| Cloud | 19% | **Narrow → Wide 트랜지션 진행** | Switching Cost + Cost Advantage |
| SP&D | 12% | **Wide** | Switching Cost + Brand |
| Other Bets | 0.5% | **None** | (R&D 단계) |
| **GOOGL 전체** | 100% | **Wide (가중평균)** | 5분류 모두 |
| **Backbone Integration** | 횡단 | **Wide reinforcement** | 기존 해자 AI 강화 |

> 주: YouTube 15%는 ads + 구독 합산 기준 — YouTube 구독 매출은 SP&D 12%에도 포함되어 이중 계상 (해자 강도 판단용 비중이므로 결론에는 영향 없음).

#### 핵심 결론

1. **GOOGL 전체 해자 = Wide** — Buffett 5분류 모두 작동 (드문 case)
2. **Search 광고 (51% 비중) 약화 risk** — 5-10년 horizon Wide → Narrow 트랜지션 가능
3. **Cloud Narrow → Wide 트랜지션 가속** (Q1 데이터)
4. **Backbone Integration = GOOGL 해자의 AI 시대 differentiator** (Q4 본인 통찰)

**Druckenmiller 패턴 (Q3 frame)**: "Bullish on company (Wide moat) but bearish on price (compression risk)"

#### 1-B 회고 블록 (2026-05-03 작성)

**작업 결과**:

- Buffett 5분류 (Brand·Switching Cost·Network Effect·Cost Advantage·Scale) × GOOGL 5개 사업 매핑
- 사업별 강도 등급 (Wide·Narrow·None) + 가중평균
- **Backbone Integration** (Q4 본인 frame) 횡단 해자로 통합
- Phase 6-A Anchoring 차단 해제 비교 → **bias 없음** 확인

**핵심 결론**:

1. GOOGL 전체 해자 = **Wide** (5분류 모두 작동, 드문 case)
2. Search 광고 (51% 비중) Wide → Narrow 트랜지션 가능 (5-10y) — 본인 thesis Bear case의 근거
3. Cloud Narrow → Wide 트랜지션 가속 (Q1 데이터)
4. Backbone Integration = AI 시대 GOOGL 해자 differentiator (vs OpenAI standalone)

**Druckenmiller 패턴 (Q3 frame) 적용**: "Bullish on company (Wide moat) but bearish on price (compression risk)"

**Anchoring 비교**:

- Phase 6-A의 "수직통합 HIGH" = 본인 Backbone Integration의 본질 동일
- Phase 6-A의 "검색 네트워크 MODERATE (AI 흔들림)" = 본인 Search Wide → Narrow 트랜지션
- 본인이 더 sharp한 부분: **AI 시대 메커니즘 (Backbone Integration)** + **사업별 가중평균 정량화**
- Anchoring bias 없음

**다음 단계**:

- Stream 3 (위험·리스크) 진입 또는 Stream 4 (가격 평가) 직진
- 본인 thesis-breaker monitoring 항목들 → Stream 3 리스크 입력
- Q1·Q2·Q3·Q4 시나리오 → Stream 4 valuation 본격 모델링

---

## Stream 2: 경영진·조직 검증

> ⚠️ **차단 대상**: Phase 4 (경영진 프로필 + 1분 테스트 + 가이던스 성격). Stream 2 종료 후 비교.

### 2-0. 경영진 Baseline (사실 정리)

| Role | Name | 재임 | Background | 주식 보유 (대략) |
| --- | --- | --- | --- | --- |
| **CEO (Alphabet + Google)** | Sundar Pichai | 2015~ (Google), 2019~ (Alphabet) | Stanford MS, Wharton MBA, McKinsey, 2004 Google 입사 (Chrome 주도) | ~$250M (~0.02% 외부 보고) |
| **President & CIO** | Ruth Porat | 2024~ (직책 변경, 2015~2024 CFO) | Morgan Stanley CFO 출신, 재무 보수성 trademark | ~$50M |
| **CFO** | Anat Ashkenazi | 2024.07~ | Eli Lilly CFO (2021~2024), Lilly 약가·R&D capital allocation 평판 | 신규 — TBD |
| **DeepMind CEO** | Demis Hassabis | 2014~ (인수 후) | UCL PhD (인지신경), 노벨 화학상 2024 (AlphaFold), Google 보고체계상 SVP 급 | DeepMind 인수 시 vested |
| **YouTube CEO** | Neal Mohan | 2023~ (Susan Wojcicki 후임) | YouTube 17년, Chief Product Officer 출신 | — |
| **Cloud CEO** | Thomas Kurian | 2018~ | Oracle 22년 (Product 총괄), Cloud 영업이익률 2018→2025 흑전 견인 | — |
| **창업자** | Larry Page, Sergey Brin | 1998~ (창업), 2019 일상 운영 사임. 보드/voting B shares 보유 | Stanford CS PhD (중퇴) | 각 ~5%. **B shares 10x voting → 외부 통제력** |
| **전 CEO/Board** | Eric Schmidt | 2001~2011 (CEO), 2018 Board exit | Sun/Novell. 현재 Schmidt Futures, 일부 GOOGL 보유 | — |

**구조적 특징**:

- **Founder-controlled (Class B 10x voting)** — Page+Brin 합산 voting power 약 50%+. 외부 주주 의결권 약화. Berkshire 비슷한 founder-trust 모델. 단 일상 운영 미참여.
- **두터운 경영진 (depth) 매우 강함** — DeepMind/YouTube/Cloud/Search 4개 본체 모두 자체 leader 보유. CEO 부재 시 multi-leader 운영 가능.
- **CFO 교체 (2024)** — Porat → Ashkenazi. Porat의 자본 보수성 + Lilly의 약가 책정·capital allocation 평판이 이어질지 추적 (Stream 8 모니터링 항목).

### 2-bis. 1분 테스트 (정량)

`knowledge/경영진_지배구조.md` 1분 테스트.

| 항목 | GOOGL 상태 | PASS / WATCH / FAIL |
| --- | --- | --- |
| **CEO 자사주 보유** | Pichai ~$250M (자기자본 의미 있는 규모, 단 외부 보유 비중은 founders 중심) | **PASS** |
| **CEO 보상 구조** | 2022 $226M PSU package (3년 vest, 주가 연동). 2024년 base $2M + PSU. **장기 주가 연동 비중 90%+** | **PASS** |
| **가족·관계자 경영진** | 무. 비-친족 전문 경영진. founders는 운영 미참여 | **PASS** |
| **회계 일관성** | CFO/NI 5년 평균 1.30+ (보수적 회계). Big 4 EY auditor. restate 이력 없음 | **PASS** |
| **CFO/NI 안정성** | FY2020 1.31 / FY2021 1.16 / FY2022 1.36 / FY2023 1.27 / FY2024 1.18 / FY2025 ~1.20 — 안정 | **PASS** |
| **법적·규제 분쟁** | 개인 형사·내부자 거래 이력 없음. 단 회사 차원 **DOJ Search 독점 판결 (2024.08, 항소 중) + EU DMA 제재** — 회사 수준 규제 리스크 (1-ter에서 다룸) | **WATCH** (개인) / **WATCH** (법인) |

**1분 테스트 종합**: 경영진 개인 신뢰성은 **PASS**. 회사 차원 규제 리스크는 별도 Stream 3 시나리오 입력 (1-ter 추가 분석).

### 2-ter. 직원·내부 평가 + 결격사유 검증

#### 직원 만족도 (정량 신호)

| 지표 | GOOGL | 비교 기준 | 평가 |
| --- | --- | --- | --- |
| 글래스도어 평점 (5점, 2025) | 4.4 | 빅테크 평균 4.0 (Microsoft 4.4, Meta 4.3, Apple 4.1, Amazon 3.7) | **양호** (peer 평균보다 위) |
| CEO 승인율 (Pichai) | 86% (2025) | 70%+ 양호 | **PASS** |
| 블라인드 평점 (한국 GOOGL) | 4.0+ (한국 직원 비공식) | — | 양호 |
| 임직원 이직률 (5년) | 2022 12K layoff 후 회복 중. 2024-2025 이직률 ~13% (산업 평균 15%) | — | 양호 |
| 내부 승진 비율 | 신규 SVP 50%+ 내부 승진 (Mohan, Pichai 본인, Kurian-외부 등) | — | **두터운 경영진(#9) 보강 PASS** |
| 직원 리뷰 텍스트 (반복 키워드) | **긍정**: "smart colleagues", "great benefits", "ambitious projects". **부정**: "bureaucratic", "slow promotion", "political" | — | 형용사 §4에 누적 |

> **글래스도어 평점은 외부 데이터 (외부 사이트 인용)**. 본인 직접 검증 시 글래스도어 GOOGL 페이지 + 블라인드 검색 + 링크드인 People Insights로 1-2시간 작업.

#### 결격사유 검증 (5개 카테고리)

| # | 카테고리 | GOOGL 사례 | PASS / WATCH / FAIL |
| --- | --- | --- | --- |
| 1 | 노사 분쟁 | **2018 Google Walkout** (성희롱 대응 항의, 20K 직원 참여). 후속 정책 변경 — 강제 중재 폐지. 2022 12K layoff (전체 6%, peer 대비 낮음) | **WATCH** — 단발 이벤트, 후속 대응 적절 |
| 2 | CEO·임원 형사·민사 | 개인 형사·내부자 거래 이력 없음 | **PASS** |
| 3 | 차별·괴롭힘 | **2017 Damore memo 사건** (성차별 메모 작성자 해고 → EEOC 일부 화해). 2018 Andy Rubin 성희롱 → 퇴직 (Andy Rubin은 Android 창설자 출신, 사후 처리 비판) | **WATCH** — 단발, 정책 변경 |
| 4 | 회계 부정 | restate 이력 없음. SEC AAER 없음. Big 4 EY 무수정 의견 5년 연속 | **PASS** |
| 5 | 규제 제재 | **DOJ Search 독점 (2024.08 판결, 2025 remedy 단계, Chrome 매각 명령 가능성). EU DMA 제재 (2024-2025). DOJ Adtech 별도 소송 진행 중**. 단 회계·사기·형사 아니라 시장 지배력 자체 issue | **WATCH (큰)** — Stream 3 시나리오 입력 필수 |

**종합**:

- **개인 형사·회계 부정 = 깨끗 (PASS)**. Fisher #15 이해상충에서 경영진 개인 무결성 본체 충족
- **단 회사 차원 규제 리스크 = WATCH (큰)**. Search 독점·Adtech 분리·DMA가 Stream 3 Bear 시나리오 핵심 변수
- **노사·차별 = 단발 이벤트 + 정책 대응**, 패턴 아님 → 통과

> **결격사유 종합 판정**: **PASS with WATCH (규제)**. Stream 3 시나리오에 규제 변수 의무 반영. Stream 6 핵심 3조건 (#15) FAIL 트리거는 아님.

### 2-1. 1층위 통독 (주주서한 + 컨콜) — **DEFERRED** (2026-05-03)

> **종료 사유**: AI가 4분기 컨콜 Q&A verbatim 발췌 (`GOOGL_call_excerpts.md`)까지 작성. 본인이 직접 통독 시도했으나, 컨콜 언어의 short-term financial dance 성격 + 비교 baseline 부재로 형용사 추출이 효익 < 시간 비용. **GOOGL 정도의 가시성 높은 종목에 Fisher 형용사 정밀 추출은 thesis-changing 가능성 낮음** (Fisher 형용사 framework은 정보 비대칭 큰 종목에서 빛남). 이미 §2-bis·§2-ter·§2-5 정량 검증 종합 PASS.
>
> **재진입 trigger**: Stream 4 가치평가가 회색 영역 (확률가중 4-10%)으로 떨어지면, 그 시점에 1층위 통독 (10-K Item 1·7 + Pichai blog 분기 메시지)으로 thesis 재교정.
>
> **남겨진 자료**: `GOOGL_call_excerpts.md` (4분기 verbatim 발췌, 형용사 후보 10개 preliminary, 공언 8건 PRELIM 추적). 본인이 분산해서 읽고 싶을 때 참고 가능.

### 2-2. 2층위 교차검증 — 공언-결과 매핑 (preliminary 종료 — 1층위 통독 deferred)

> 4분기 컨콜 발췌 (`GOOGL_call_excerpts.md` §공언 추적)에서 8건 추적 + 본 §2-2의 5건 공언. 임계 ≥5건 충족.

| 경영진 공언 | 검증 자료 (2층위) | 일치 여부 |
| --- | --- | --- |
| "AI-first company" (2017~) | DeepMind+Google Brain 통합 (2023.04), TPU v6 (2025), Gemini 풀스택, R&D $50B+/yr | **PASS** (구조적 통합 일치) |
| "Cloud profitability path" (2019~) | Cloud Op Margin 2019 -29% → 2024 12% → 2025 17% → 2026 Q1 32.9% | **PASS (강)** |
| "Capital discipline" (Porat 2015~) | 자사주 매입 ~$300B 누적 + 2024 첫 정기 배당 + Wiz 2024 무산 (가격 신중) → 2025 재합의 | **PASS** |
| "Other Bets graduation" (2015~) | Waymo 단독 6개 도시 (2026 Q4 Miami 추가, 50만/주) | **PASS (가속)** |
| "AI Overviews monetization rate ~= 기존 검색" (2024~) | 외부 분석 — Search 트래픽 -3~5%pt 추정 (단 Search 매출은 +14~19% 가속) | **PRELIM PASS** (정밀 추적은 외부 3rd party — Similarweb·BrightEdge — 본인 직접 검증 시 1-2시간) |
| "Gemini serving cost -78% over 2025" (Q4 2025) | Q1 2026 추가 -30% since Gemini 3 — 일관 추세 | **PASS** |
| "tight supply environment 2026 내내" (Q3 2025) | Q1 2026: "compute constrained in near term" 재확인 | **PASS** (단 Q2 2025의 "2025 말 완화" 1회 후퇴) |
| "2026 = year agent experiences are used broadly" (Q2 2025) | Q1 2026: agentic flows launching, Universal Commerce Protocol | **TRACKING** (2026 결과 검증 필요) |

**공언-결과 매핑 종합**: 8건 중 6 PASS + 1 PRELIM PASS + 1 TRACKING + 1 후퇴 (솔직 인정). **Acceptance ≥5건 + 일치/불일치 판정 — 충족**.

### 2-3. Fisher 15 정성 10개 (preliminary)

> 1층위 통독 전 preliminary 판정. Stream 종료 시 갱신.

| # | 기준 | preliminary 판정 | 핵심 근거 |
| --- | --- | --- | --- |
| 2 | 신제품 개발 의지 | **PASS** | R&D $50B+/yr (FY2024), Stream 1-A N+1·N+2 파이프라인 12개+, 양자컴 Willow |
| 4 | 평균 이상의 영업 역량 | **PASS** | Google Services 영업이익률 ~40%, Cloud GTM 빠른 확장 (RPO 백로그 가속) |
| 7 | 노사관계 | **PASS-WATCH** | Glassdoor 4.4·CEO 86%. 단 2018 Walkout |
| 8 | 임원 간 관계 | **PRELIM PASS** | DeepMind+Google Brain 통합 (2023.04) 이후 큰 정치 이슈 보도 없음. 단 추적 필요 |
| 9 | 두터운 경영진 (depth) | **PASS (강)** | DeepMind/YouTube/Cloud/Search 모두 자체 leader, founder 부재에도 운영 |
| 12 | 장기적 시각 | **PASS** | Other Bets 10년+ 인내 (Waymo, Verily, Willow). Founders' 1998 Letter 장기 본질 |
| 13 | 대량 증자로 주주이익 훼손 | **PASS** | 5년 주식수 거의 일정 (자사주 매입이 SBC 흡수). 추가 증자 없음 |
| 14 | 나쁜 일에도 소통 | **PRELIM PASS** | 2024 Gemini Image 인종 묘사 사고 → Pichai 메모 "unacceptable" 인정. 2022 layoff 직접 메모. 단 5-10년 패턴 통독 필요 |
| 15 | 경영진 이해상충 | **PASS-WATCH** | Pichai PSU 90%+ 장기 주가 연동. 단 founder dual-class voting 50%+ → 외부 주주 통제 약화 (구조적). 사기·내부자 거래 없음 |
| 11 | 산업 특화 경쟁력 | **PASS (강)** | Stream 1 분석 — 검색 90% 점유 + Cloud 3위 가속 + AI 풀스택 (TPU+Gemini+DeepMind) |

**preliminary 종합**: 10개 중 **PASS 8개 + PASS-WATCH 2개 (#7, #15)**. 핵심 3조건 (#9, #14, #15) — #9 PASS (강), #14 PRELIM PASS, #15 PASS-WATCH. 1층위 통독 후 #14 갱신 필요.

### 2-4. 형용사 리스트 (preliminary)

> 1층위 통독 전 외부 자료·관측 기반 preliminary. 통독 후 갱신.

| 형용사 | preliminary 근거 | 일관성 (1층위/2층위) |
| --- | --- | --- |
| **장기적인 시각을 가진** | Other Bets 10년+, 양자컴 Willow 2030+ 투자, founders' Letter 장기 본질 | TBD |
| **자본 보수적인** | Porat CFO 2015-2024 — 자사주 매입+R&D 균형, 첫 정기 배당 2024까지 미지급, M&A 신중 (Wiz 2024 무산) | TBD |
| **두꺼운 경영진을 가진** | 4개 사업부 모두 자체 leader, founder 부재에도 운영 | TBD |
| **AI native하게 진화한** | DeepMind+Google Brain 통합 (2023), TPU 자체 개발, Gemini 풀스택 | TBD |
| **자기 본업을 잠식할 의지가 있는** | AI Overviews를 검색 광고 위에 배치 (cannibalization 수용). 단 광고 보완 형태로 진화 추적 | TBD |
| **관료적인 (부정)** | 직원 리뷰 반복 키워드 — 의사결정 속도 비판 (Stream 1 — Figma·Cursor 같은 전문 서비스에 일부 영역 잠식 위협 연계) | TBD |

**현재 preliminary 5+1 = 6개**. 1층위 통독 후 중복·삭제 + 추가 누적. 형용사 ≥ 5개 일관성 PASS 임계 충족 가능성 — 통독 작업으로 검증.

### 2-5. 자본배분 의사결정 (Buffett 트랙 보조 + Fisher #9·#13 검증용)

> Fisher 트랙이지만 #9 두터운 경영진(M&A 통합) + #13 주주이익 훼손(자사주·증자) 검증용으로 일부 분석.

| # | 의사결정 | 시점 | 규모 | 사후 ROI (5년+) | 평가 |
| --- | --- | --- | --- | --- | --- |
| 1 | YouTube 인수 | 2006 | $1.65B | 매출 ~$50B/yr (광고+구독, 2025) → ROI 거대. **자본배분 best case** | **EXCELLENT** |
| 2 | DeepMind 인수 | 2014 | $500M | Gemini 풀스택의 본거지. 노벨상(AlphaFold), AGI 비전. ROI 매출 환산 어렵지만 stock value 측면 거대 | **EXCELLENT** |
| 3 | Android 인수 | 2005 | $50M | Android 30억+ 사용자 → 검색 광고 default 채널. ROI 측정 불가능 거대 | **EXCELLENT** |
| 4 | Motorola Mobility | 2012 → 2014 매각 | $12.5B → $2.91B | Lenovo 매각. 특허 13K 흡수했으나 차이 약 $9B 손실. **명백한 실패** | **FAIL** |
| 5 | Nest 인수 | 2014 | $3.2B | Google Home/Pixel 통합으로 가치 일부 회수. 단 standalone 매출 미미 | **NEUTRAL** |
| 6 | Looker 인수 | 2019 | $2.6B | Cloud BI 통합. Cloud 매출 기여 직접 추적 어려움 | **NEUTRAL-PASS** |
| 7 | Wiz 합의 (재시도) | 2025.03 (2024 무산 후 재합의) | $32B | Cybersecurity Cloud 통합. 사후 ROI TBD | **TBD** |
| 8 | 2024 첫 정기 배당 도입 | 2024.04 | $0.20/sh quarterly = ~$10B/yr | Capital return 본격화 신호. 자본 보수적 culture 전환점 | **PASS (시그널)** |
| 9 | 자사주 매입 | 2018-2025 누적 | ~$300B+ | EPS 가속 + SBC dilution 흡수. 주식수 5년 거의 flat | **PASS** |
| 10 | CAPEX FY2025 | $75B+ (vs FY2024 ~$50B) | — | AI 인프라 (TPU·DC). ROI 검증은 Cloud 매출·margin 가속으로 측정 — 현재 RPO 백로그 가속 = preliminary positive | **WATCH (sized)** |

**자본배분 종합 평가**: 핵심 인수 (YouTube·Android·DeepMind) **EXCELLENT** + Motorola 실패 1건. 자사주 매입 + 신규 배당 = 주주환원 정착. **CAPEX $75B+ 의 ROI가 향후 2-3년 검증 핵심** (Stream 8 모니터링 1순위).

### 2-Acceptance Criteria 점검 (Stream 2 종료 — 2026-05-03)

| 항목 | 임계 | 종료 시점 상태 | 판정 |
| --- | --- | --- | --- |
| 자료 분량 | 주주서한 5-10년 + 컨콜 8-12분기 통독 | 컨콜 4분기 verbatim 발췌 (`GOOGL_call_excerpts.md`). 주주서한 직접 통독은 deferred | **PARTIAL** (관리 의도) |
| 형용사 일관성 | 5개 중 ≥ 4개가 ≥ 3년 + ≥ 4분기 반복 | preliminary 6개 (§2-4) + 컨콜 추출 10개 (`call_excerpts` §형용사) — 4분기 일관성 충족 후보 2개 ("솔직한", "공급 격차 사전 경고하는"), 3분기 일관성 1개 ("버티컬 풀스택 정체성") | **PARTIAL** (4분기 일관성만 검증, 5년 letter 미검증) |
| Fisher 15 정성 10개 PASS | 10개 중 ≥ 7개 PASS + 핵심 3조건 (#9·#14·#15) PASS | 8 PASS + 2 PASS-WATCH (#7·#15). 핵심 3조건 #9 PASS(강) / #14 PRELIM PASS / #15 PASS-WATCH | **PASS** (임계 충족) |
| 공언-결과 매핑 | 추적 공언 ≥5건 + 일치/불일치 판정 | **8건 추적, 6 PASS + 1 PRELIM PASS + 1 TRACKING** | **PASS** |
| 자본배분 ≥5건 분석 + ROI | Fisher 트랙 보조 | **10건 분석 완료**, EXCELLENT 3건 / FAIL 1건 / NEUTRAL 2건 / PASS 3건 / WATCH 1건 | **PASS (over-coverage)** |

**종합**: **Stream 2 PRELIM PASS** — 자료 분량 + 형용사 일관성 PARTIAL이지만 핵심 트리거 (Fisher 15 핵심 3조건 + 공언-결과 + 자본배분 + 1분 테스트 + 결격사유 5개) 모두 PASS. **Stream 6 핵심 3조건 FAIL 트리거 없음** → Stream 3 advance.

### 2-Anchoring 차단 비교 — Phase 4 (Stream 2 종료 후, 2026-05-03)

> 차단 사본 (`GOOGL_shallow_locked.md`) Phase 4 열람.

| 차원 | Shallow Phase 4 | Deep Stream 2 | 일치/불일치 | 분석 |
| --- | --- | --- | --- | --- |
| **CEO 자사주 보유** | ~$1.3B (0.03% 미만) | ~$250M (~0.02%) | **불일치** (5x 차이) | Shallow는 vested + unvested + ETF 합산 가능, Deep은 외부 직접 보고 (Form 4) 기준 가능. **본질에 영향 없음** (둘 다 PASS, 자기자본 의미 있는 규모). 정확 수치는 추후 Form 4 직접 확인 |
| **CFO 안정성** | Ashkenazi 1.8년차 — Shallow에서 "MARGINAL 단축 재직" 표시 | 신규 (2024.07~), Lilly 출신 capital allocation 평판 | 정보 일치, 평가 미세 차이 | Shallow는 "단축 재직 우려" 강조, Deep은 "Lilly 자본배분 평판"으로 보강. Stream 8 모니터링 항목으로 이미 박힘 |
| **위기 대응 트랙** | 2022 12K 정리해고, 2024 DOJ 패배 후 즉시 항소·remedies 협상 → 2025.09 Chrome 매각 회피 | 동일 사건 + 2018 Walkout, Damore memo, Andy Rubin 추가 분석 | **일치 + Deep 풍부** (결격사유 5개 카테고리 정렬) | Deep이 더 풍부 — 결격사유 framework이 추가 layer 제공 |
| **인상 문장** | "Gemini serving costs -78% over 2025" | 동일 인용 + Q1 2026 추가 -30%, 4분기 일관 형용사 (솔직·공급 격차 사전 경고) | **일치 + Deep 풍부** | Shallow의 인상 문장이 Deep 컨콜 발췌에서 패턴으로 확인 |
| **1분 테스트** | 정량 5개 (ROE/ROA/B-S/일회성/주주환원) — 모두 PASS | 정성 5개 (CEO 보유/보상/가족/회계 일관성/CFO-NI/법적 분쟁) — 모두 PASS, 단 회사 차원 규제 WATCH | **다른 framework + 둘 다 PASS** | Shallow의 정량 framework + Deep의 정성 framework은 보완. 상충 없음 |
| **가이던스 성격** | "재투자·파이프라인 중심, AI N+1 시장 확대" | 동일 + 4분기 컨콜 패턴 ("2026 CAPEX $175-185B", "compute constrained" 4분기 연속) | **일치 + Deep 정밀** | Deep이 4분기 컨콜로 Shallow 결론 정량 검증 |

**비교 종합**: **큰 불일치 없음**. CEO 보유 수치 차이는 본질 영향 없음 (둘 다 PASS 구간). Shallow의 결론을 Deep이 정량·정성 모두 보강. **Phase 4의 PASS with WATCH (CFO 단축 재직, 회사 차원 규제) 결론을 Deep이 그대로 재확인**. 트랙 재배정 트리거 없음 (Fisher 유지).

### 2-회고 블록 — Stream 2 (2026-05-03 종료)

#### 이번 분석에서 실제 필요했던 자료·질문·판단 기준

- **결격사유 5개 카테고리 (1-ter)**: GOOGL 같은 큰 회사는 단발 이벤트 (Walkout·Damore·Andy Rubin)와 회사 차원 규제 (DOJ·EU)를 분리해서 봐야 함. 패턴 vs 단발 구분이 핵심
- **자본배분 의사결정 사후 ROI 추적**: YouTube·Android·DeepMind 같은 수십 년 ROI 검증 가능한 인수 + Motorola 같은 명백한 실패 + CAPEX $175-185B 같은 sized bet의 분리 평가
- **컨콜 4분기 verbatim 발췌의 limitation**: 형용사 추출에 컨콜은 short-term financial dance 성격 → 정밀 형용사보다 패턴 형용사 (예: "솔직한") 추출에 그침
- **본인 의사결정 trigger 식별**: 형용사 정밀 추출보다 핵심 3조건 (#9·#14·#15) PASS + 결격사유 PASS 여부가 thesis-killing 여부 결정

#### 뼈대에서 부족했던 것

- **컨콜·서한 통독의 효익 vs 비용 가이던스 부재**: GOOGL 정도의 가시성 높은 종목에 1층위 통독이 thesis-changing 가능성이 낮은데, 뼈대는 일률적으로 "주주서한 5-10년 + 컨콜 8-12분기" 의무로 둠. **종목별 정보 비대칭 정도에 따라 1층위 통독 깊이를 가변**으로 두는 가이드라인 필요
- **형용사 추출의 비교 baseline**: 본인이 처음 접하는 회사에 형용사 추출 시 비교 baseline 부재로 "그냥 응 그렇구나" 상태. 다른 hyperscaler CEO (Nadella·Jassy) 컨콜 비교 또는 long-form interview (Lex Fridman·BG2) 권장이 뼈대에 없음
- **CEO 자사주 보유 수치 산출 기준 명시**: Form 4 (직접 보고) vs vested+unvested 합산 vs ETF 포함 — 산출 기준에 따라 5x 차이. 뼈대는 "CEO 자사주 보유" 한 줄

#### 뼈대에 없어도 됐던 것

- **공언-결과 매핑 5건 의무**: 본 종목엔 공언이 풍부 (8건 추적). 단 정보 비대칭 작은 종목엔 5건이 마지노선이 아니라 over-spec일 수 있음
- **Buffett 트랙 자본배분 5건 의무 — Fisher 트랙 보조**: 본 트랙 (Fisher)에서도 자본배분 분석이 #9·#13 검증에 직접 도움. **트랙 분리보다 통합 운영**이 본질에 가까움 (PR 1.5에서 부분 통합 됨)

#### Anchoring 차단 결과

- **차단 대상**: Phase 4 (경영진 프로필 + 1분 테스트 + 가이던스 성격)
- **차단 준수**: Stream 2 작업 중 Phase 4 미참조. 종료 후 비교 시 처음 열람
- **불일치 발견**: CEO 보유 수치 5x 차이 (산출 기준 차이로 추정, 본질 영향 없음). 그 외 큰 불일치 없음
- **Deep 풍부 vs Shallow 풍부**: Deep이 결격사유 framework·자본배분 10건·컨콜 4분기 verbatim·형용사 preliminary 추가로 풍부. Shallow의 정량 1분 테스트는 Deep에 없는 framework
- **트랙 재배정 트리거**: 없음 (Fisher 유지)

#### 다음 Deep Dive 또는 playbook 개정에 가져갈 포인트

1. **Stream 2 1층위 통독 의무 → 종목별 가변** (정보 비대칭 큰 종목에선 본체, 작은 종목엔 deferred 옵션)
2. **컨콜 발췌의 한계 + 비교 baseline 권장** (long-form interview·동급 CEO 컨콜 비교)
3. **CEO 자사주 보유 산출 기준 명시** (Form 4 직접 보고 우선)
4. **Stream 2 PRELIM PASS 조기 종료 옵션 = trigger 명시** (정량 검증 PASS + 핵심 3조건 PASS + 결격사유 PASS 시 advance 가능)

---

## Stream 3: 리스크·기회 통합 + 시나리오 명세

**작업일**: 2026-05-03
**출처**: Q1~Q7 종결 결과 + Stream 1-A·1-B + 본인 Driver Tree frame
**산출**: 리스크/기회 매트릭스 (확률 × 영향) + Driver Tree + 4-Period 시나리오 명세 → Stream 4 입력

### 3-1. Driver Tree (재무 결과 통합 frame)

> ★ 본인 통찰 (2026-05-03): 39개 risk·opp을 개별 추적이 아니라 **재무 결과 (EPS·매출·마진·FCF)** 로 통합. 요소들 간 상관관계 인지.

```text
                          EPS (분기 추적 ★)
                  ┌───────┴────────┐
                매출            영업이익 ÷ 주식수
        ┌───┬───┼────┬───┐         │
     Search Cloud SP&D YT Other     │
      51%   19% 12%  15% 0.5%       │
                                    │
   각 segment의 driver (39개 risk·opp이 leaf로 매핑)
```

#### Search 광고 (51%) Driver

| Driver | 영향 risk·opp | Verify |
| --- | --- | --- |
| 검색 query volume | R8·R12·O15 | Pichai 멘트만 |
| CTR (클릭률) | R12·O5·O12 | 외부 (BrightEdge) |
| CPC (단가) | O12·R11·O13 | 외부 추정 |
| Apple distribution | R9·R10·O14 | 보도 + 항소심 |
| AI agent 침투 | R8·O16 | 미공시 |
| → **결과: Search 매출 YoY%** | ✓ 분기 10-Q 직접 verify | |

#### Cloud (19%) Driver

| Driver | 영향 | Verify |
| --- | --- | --- |
| 신규 계약 (RPO 신규) | O2·O4 | 분기 RPO ✓ |
| RPO 전환률 | O2 | Pichai 멘트만 |
| Capacity 풀림 | R4·O4 | 멘트 |
| TPU 단가 우위 | O3·R1·R2·R3 | 외부 (SemiAnalysis) |
| → **결과: Cloud 매출 YoY% + Op Margin** | ✓ 분기 10-Q | |

#### SP&D (12%) + YouTube (15%) Driver

| Driver | 영향 | Verify |
| --- | --- | --- |
| 유료 구독자 수 | O7·O8 | 분기 update ✓ |
| ARPU | O8 (AI Pro/Ultra) | 추정 |
| TikTok 점유 (YouTube) | O15 | Sensor Tower |
| → **결과: SP&D + YouTube ads YoY%** | ✓ 분기 10-Q | |

#### 마진 Driver

| Driver | 영향 | 메커니즘 |
| --- | --- | --- |
| Cloud mix shift | O18 (Aggregation) | Cloud 비중 ↑ → 마진 ↑ |
| TPU 우위 | O3 | Cloud cost ↓ → 마진 ↑ |
| Capex 가속 | R5·R4 | 2027~2028 D&A 본격 → 마진 압박 |
| Productivity 발현 | O17·R7 | 자체 운영 효율 ↑ |
| Publisher cost | R11 | -3~5%pt op margin |

#### 핵심 Cascade (상관관계)

```text
TPU 우위 (O3) → Cloud cost ↓ → Cloud 마진 ↑ → 매출 mix shift → 영업이익률 ↑ → OCF ↑ → FCF ↑ → 자사주매입 → EPS ↑

Capex 가속 (Q3) → 2027 D&A 가속 → 영업이익률 압박 (-2~3%pt). 단 OCF엔 D&A 환원 → FCF 영향 작음

Search 매출 둔화 (R17) → AIO sponsored (O13) buffer → 안 일어나면 → Backbone Integration (O8) buffer → 둘 다 안 되면 → Combined thesis-breaker (R14)
```

### 3-2. 리스크 매트릭스 (19개, 5 카테고리)

| # | 항목 | 종류 | 확률 | 영향 | 시나리오 |
| --- | --- | --- | :-: | :-: | --- |
| R1 | TSMC CoWoS 할당 부족 (TPU 25% 감산) | 사업 | 4 | 3 | base/bear |
| R2 | NVIDIA Rubin (2027 Q1) HBM4 — TPU 메모리 우위 erase | 경쟁 | 3 | 4 | base/bear |
| R3 | CUDA moat — Midjourney defection 일반화 | 경쟁 | 3 | 3 | base/bear |
| R4 | Capacity constrained 풀리는 시점 → 마진 프리미엄 소멸 | 사업 | 3 | 4 | base/bear |
| R5 | 2027~2028 D&A 본격화 (useful-life extension exhausted) | 사업 | **5** | 3 | base/bear |
| R6 | Cloud 성장률 +20% 둔화 (Patel "no profits 2027") | 사업 | 3 | **5** | bear |
| R7 | Productivity Paradox 지속 (NBER 90% zero impact) | 매크로 | 3 | 4 | bear |
| R8 | AI agent + Gen Z 행동 변화 (combined, 2027~) | 사업 | 3 | 4 | bear |
| R9 | DOJ Apple deal 항소심 금지 가능성 | 매크로 (규제) | 2 | 3 | bear |
| R10 | DOJ DC Circuit Chrome·Android 분리 | 매크로 | 2 | 4 | bear |
| R11 | Publisher 관계 재조정 (revenue share·content licensing) | 사업 | 4 | 3 | base |
| R12 | AIO quality 하락 → 사용자 ChatGPT 이탈 | 사업 | 3 | 4 | bear |
| R13 | Multiple compression -30~50% (닷컴 살아남은 회사 패턴) | 밸류에이션 | 3 | 4 | base/bear |
| R14 | Combined thesis-breaker (Cloud 둔화 + Productivity 미실현 + Multiple compression) | 통합 | 2 | **5** | bear |
| R15 | Apple deal 1년 계약 → TAC inflation ($20B → $30B+) | 매크로 | 4 | 2 | base |
| R16 | SBC inflation ($5B → $6.5B+ in 12분기) | 사업 | **5** | 2 | base |
| R17 | Search 51% 약화 (5-10y Wide → Narrow 트랜지션) | 사업 | 3 | **5** | bear (long-term) |
| R18 | 경영진 가이던스 정확성 (Pichai·Ashkenazi) | 경영진 | 2 | 2 | base |
| R19 | 매크로 침체 (광고 매출 cyclical) | 매크로 | 2 | 3 | bear |

**Top 리스크 (확률×영향 ≥ 12)**: R5 (15) · R6 (15) · R17 (15) · R11 (12)

### 3-3. 기회 매트릭스 (20개)

| # | 항목 | 종류 | 확률 | 영향 | 시나리오 |
| --- | --- | --- | :-: | :-: | --- |
| O1 | AI 시장 expansion (3대 hyperscaler 동시 가속) | 사업 | **5** | 4 | bull/base |
| O2 | RPO 462B 50% 24개월 전환 ($231B 매출 lock-in) | 사업 | 4 | **5** | bull/base |
| O3 | TPU ~50% TCO 우위 (외부 모델사 채택) | 사업 | 3 | 4 | bull |
| O4 | Capacity 풀리며 매출 가속 (2026 Q1 +63%) | 사업 | 4 | 4 | bull/base |
| O5 | AIO 2B MAU + monetization "same rate" | 사업 | **5** | 4 | bull/base |
| O6 | Google 유일 positive FCF in Big 4 hyperscaler | 사업 | **5** | 4 | bull/base |
| O7 | AI 구독 매출 가속 (SP&D +19%, Gemini Enterprise +40% Q/Q) | 사업 | 4 | 3 | bull/base |
| O8 | **Backbone Integration** (기존 platform 해자 AI 강화) | 사업 (해자) | 4 | **5** | bull |
| O9 | Goldman 35% TPU 점유 시나리오 | 사업 | 2 | 4 | bull |
| O10 | DeepMind 차별화 매출 (Genie/Veo/Robotics) 5년 내 | 사업 | 2 | 4 | bull (long-term) |
| O11 | Apple Intelligence Gemini default 라우팅 | 사업 | 2 | 3 | bull |
| O12 | 광고 단가 ↑ (검색 인텐트 context 풍부) | 사업 | 4 | 3 | bull/base |
| O13 | AIO sponsored + AI Mode "Direct Offers" 매출 | 사업 | 3 | 3 | bull |
| O14 | Apple Services 자체 손실 → Google deal 유지 동기 | 사업 | 4 | 3 | base/bull |
| O15 | TikTok 선호도 retreat 50% (Gen Z) | 경쟁 | 3 | 2 | bull |
| O16 | AI Agent + Gen Z 영향 mainstream 2027~2028까지 미뤄짐 | 사업 | 4 | 3 | base |
| O17 | Productivity Paradox Solow 패턴 해소 (2028+ 본격) | 매크로 | 3 | **5** | bull (long-term) |
| O18 | Aggregation Theory 수직통합 우위 | 사업 | 4 | 4 | bull/base |
| O19 | 양자컴퓨팅 (Willow chip) 2030+ 매출 | 사업 | 1 | 3 | bull (very long-term) |
| O20 | Waymo 자율주행 매출 가속 | 사업 | 3 | 2 | base/bull |

**Top 기회 (확률×영향 ≥ 12)**: O1 (20) · O2 (20) · O5 (20) · O6 (20) · O8 (20) · O4 (16) · O18 (16) · O17 (15) · O14 (12)

→ Top 기회 5개 (≥20) > Top 리스크 4개 (≥12). 정량적 **기회 우세**, 단 R6·R17 영향 큼

### 3-4. 4-Period 시나리오 명세 (Frame + Qualitative Trigger)

> 수치 sanity check은 Stream 4-Main에서 본격 진행. Stream 3는 frame + 실현 조건 (qualitative trigger) 중심.

| Period | 역할 | Bull 실현 조건 | Base 실현 조건 | Bear 실현 조건 |
| --- | --- | --- | --- | --- |
| **단기 (1-2년)** | 분기 catalyst·모멘텀 | Cloud +50%/yr 유지, Search +15% (Backbone), AIO monetization 가속 | Cloud +30~35%, Search +12% (정상화), AIO 점진 | Cloud +25% 둔화, Search +5% (publisher cost 즉각) |
| **중기 (3-5년)** | Stream 4 DCF explicit period | RPO 60%+ 전환, Apple Intelligence Gemini default, capex 정상화 (Capex/Rev 33% → 28%) | RPO 50% 전환, Apple deal 유지 (1년 협상), 정상 capex 확장 | Cloud +20% 둔화 (Patel), Productivity 미실현 (NBER), Apple deal 위협 |
| **장기 (5-10년)** | Wide moat 트랜지션 + Productivity | Search Wide 유지 (Backbone 흡수), Productivity 본격 (Solow 해소), DeepMind 차별화 매출 발현 | Search Wide → Narrow 점진, Productivity 점진, 정상 사업 mix shift | Search Narrow 본격, AI agent + Gen Z mainstream, Multiple compression -30~50% |
| **Terminal (10y+)** | 영구 가정 (Aggregation·Backbone 지속) | Aggregation 수직통합 + Backbone 영구. Terminal g 5%+ | 정상 산업 평균. g 3% | 압박 (Search 감소, AI 답변 시대 광고 모델 변화). g 2% |

**확률 weight (본인)**: Bull 30% / Base 40% / Bear 30%

**핵심 frame**: Druckenmiller 패턴 — "Bullish on company (Wide moat + Top 기회 우세) but bearish on price (Multiple compression risk)". Bull/Base/Bear 모두 시나리오 변동 (thesis-breaker X), 단 R14 (Combined) 발현 시 매도 검토.

### 3-5. Monitoring Plan (Driver Tree 기반)

> ★ 본인 통찰 (2026-05-03): 39개 개별 추적이 아니라 재무 결과 통합 + Driver Tree 진단

#### Top 5 metric (분기 5분 — 정확 verify)

| # | 지표 | 어디서 (분기) | 시간 |
| --- | --- | --- | --- |
| 1 | Segment 매출 4개 YoY% (Search·Cloud·SP&D·YouTube) | 10-Q 1페이지 | 30초 |
| 2 | Cloud Op Margin | 10-Q segment table | 30초 |
| 3 | EPS (diluted) | 10-Q 1페이지 | 30초 |
| 4 | FCF (OCF - Capex) | 10-Q 2개 라인 | 1분 |
| 5 | Capex 가이던스 (다음 분기 + 연간) | 어닝콜 | 2분 |

→ **분기당 5분, IR PDF 1페이지로 끝**

#### Driver Tree 진단 (이상 신호 발생 시만)

분기 5개 중 이상 신호 (예: Cloud +63% → +35% 둔화) 발생 시 → Driver Tree 따라 진단 → 39개 risk·opp 매핑

#### 외부 보고 (연 1-2회)

- SemiAnalysis (CoWoS·TPU 분기 보고)
- NBER·McKinsey (Productivity 연간)
- Synergy (Cloud 점유 분기)

#### 자동화 옵션

- `/schedule` 분기 어닝콜 직후 (1·4·7·10월 말) Pichai 발언 + Top 5 metric 자동 추출
- 어닝콜 transcript 키워드 grep ("capacity"·"constrained"·"RPO"·"convert"·"agent"·"productivity")

#### Total Time

| 주기 | 시간 |
| --- | --- |
| 분기 IR Top 5 | 5분 × 4 = 20분/년 |
| Driver Tree 진단 (이상 시) | 30분 × 1-2 = 60분/년 |
| 외부 보고 | 30분 × 2 = 60분/년 |
| **합계** | **~2-3시간/년** |

### 3-6. Anchoring 차단 비교 (Phase 8)

#### Phase 8 1차 가설 vs Stream 3 결과

Phase 8 1차 가설:
> "Alphabet은 검색 광고 머니머신 + YouTube/Android 분포 + 자체 TPU 인프라 + DeepMind/Gemini 모델의 4축 풀스택 AI 플레이어로, 2026~2028년 동안 Cloud 부문이 매출 mix의 30%+로 확장하고 Gemini 기반 신규 제품군이 광고 외 수익 채널을 형성할 가능성. 단 매출 17% 복리는 광고 본업이 AI에 잠식당하지 않고 Cloud 가속이 capex ROI를 정당화한다는 두 축 모두에 의존하며, 현 가격은 이 시나리오를 대부분 가격에 반영. Pichai 체제 10년+의 캡 효율 개선 트랙(2020→2025 마진 23%→32%)이 이 경로를 뒷받침."

→ **Deep dive 결과로 검증·sharpen**:

- ✓ "4축 풀스택" = Stream 1-A·1-B 사업별 분석으로 검증 (Search·YouTube·Cloud·SP&D)
- ✓ "Cloud 30%+ 매출 mix" → 구체화: 매출 19% → 25~30% (5y Bull/Base)
- ✓ "Gemini 광고 외 수익 채널" → SP&D +19% 가속 + Backbone Integration frame으로 sharpen
- ✓ "두 축 의존" = Q1·Q2 thesis 핵심 (Cloud 가속 + Search 안 잠식). Stream 3 시나리오에 반영
- ✓ "현 가격은 시나리오 대부분 반영" = Multiple compression risk (R13) 인식과 일치
- ✓ "마진 23%→32% 트랙" = Stream 1-A 매출 마진 데이터로 검증

#### Phase 8 리스크·기회 톱3 vs Stream 3 매트릭스

**Phase 8 리스크 톱3** → Stream 3 매핑:

| Phase 8 리스크 | Stream 3 매핑 |
| --- | --- |
| 1. AI 검색 디스럽션 (ChatGPT, Perplexity) | R8 + R12 + R17 (3개로 세분화) |
| 2. DOJ 검색 remedies (2025.09 판결) | R9 + R10 (Q2 update 반영, 부분 약화) |
| 3. Capex ROI 미달 ($175-185B/yr 가이던스) | R5 + R6 (Q3 detail로 sharpen) |

**Phase 8 기회 톱3** → Stream 3 매핑:

| Phase 8 기회 | Stream 3 매핑 |
| --- | --- |
| 1. Cloud 가속 (RPO $240B +55% Q/Q) | O2 (RPO 462B로 update) + O4 + O6 + O18 |
| 2. Gemini App 750M MAU + 생성AI +400% | O5 + O7 + O8 (Backbone 신규 frame) |
| 3. Waymo + Other Bets 대형화 | O20 + O19 |

→ **모든 Phase 8 항목 Stream 3 매트릭스에 포함됨. 누락 없음**

#### Anchoring bias 평가

- 차단 사본 안 보고 build했지만 **Phase 8 모든 항목 매트릭스에 포함**
- Stream 3 build가 Phase 8을 **sharpen + extend**:
  - 39개 항목으로 세분화
  - Backbone Integration 새 frame
  - Combined thesis-breaker (R14) 새 통합
  - Multiple compression weight 30% (Druckenmiller 패턴)
  - Driver Tree 통합 frame
  - 4-Period 시나리오 명세
- **Anchoring bias 없음**

### 3-7. 회고 블록 (2026-05-03)

#### 작업 결과

- 39개 risk·opp 매트릭스 (R 19개 + O 20개) — 5 카테고리 전수 (사업·경쟁·경영진·매크로·밸류에이션)
- Driver Tree (재무 결과 통합 frame) — 본인 통찰
- 4-Period 시나리오 명세 (단기·중기·장기·terminal) — 본인 통찰 (5년 single timeline 한계)
- Monitoring Plan (Top 5 + Driver Tree 진단 + 외부 보고)
- Phase 8 anchoring 비교 — bias 없음 + sharpening

#### 잘 된 점

1. **본인 메타 통찰 통합**: (a) 39개 개별 추적 비현실 → Driver Tree 통합, (b) 5년 single timeline 한계 → 4-Period
2. **Druckenmiller 패턴 적용**: "Bullish on company, bearish on price". Multiple compression weight 30% 인정
3. **상관관계 매핑**: TPU·Cloud·Margin·FCF cascade. Search 둔화 → AIO buffer → Backbone buffer → Combined thesis-breaker

#### 부족한 점

1. **수치 sanity check 미진행**: Stream 4-Main에서 진행 예정 (현재 매트릭스 잠정)
2. **Productivity·CoWoS 등 외부 추정 의존성** 큼 — verify 한계 인정
3. **Phase 10 Anchoring 차단** (Stream 4 시나리오) 은 Stream 4 작업 후 비교

#### 다음 단계

- **Stream 4 (가격 평가) 본격 진입** — DCF + Fisher 기대수익률 + Multiple cross-check
- 4-Period 매트릭스에 수치 sanity check (P/E, Op Margin, Capex/Rev)
- Phase 10 Anchoring 비교

---

## Stream 4: 가격 평가 (2026-05-03 종결, 잠정 회색)

**작업일**: 2026-05-03 | **종가 (5/1/2026)**: $383.22 | **시가총액**: $4.69T

### 4-A. 종목 특성 진단

| 항목 | GOOGL | 해석 |
| --- | --- | --- |
| 자본 집약도 | Capex/Rev TTM 26%, 2026E ~33% | **자본집약 가속 중** |
| 무형자산 비중 | 광고·검색·SaaS 본업 | 무형자산 본체 |
| 이익 안정성 | Op Margin 26~32% (5y) | 안정 (사이클 평준화 X) |
| 자사주 매입 | 5y 주식수 CAGR -1.95%/yr | mild buyback |
| 산업 특성 | 광고 + Cloud + AI 인프라 hybrid | 플랫폼 + 자본집약 |
| 이익 부호 | EPS $12.91 (보고) / $10 (조정) | EPS 사용 가능 |

**도구 선택**:

- **1순위 (의무)**: DCF (OE 기반)
- **2순위 (보조)**: EPS × PER

**Caveat**:

- Q1 2026 NI에 $37B non-cash equity gain → P/E 계산 시 **조정 P/E ~38x** 사용 (보고 ~29.7x는 $37B equity gain으로 낮게 왜곡)
- OE 두 case 병행: (a) 보수 — Capex_total = Maintenance, OE TTM $38B / (b) 진보 — D&A = Maintenance, NOPAT $113B

### 4-B. 정량 산출

#### DCF (진보 OE = NOPAT, baseline)

가정: r = 10% (보수), 10y explicit + Terminal Gordon growth, Stream 3 4-Period 매트릭스 적용

| 시나리오 | 매출 CAGR (1-2y/3-5y/5-10y) | OM 평균 | Terminal g | IV (B) | per share |
| --- | --- | --- | --- | --- | --- |
| Bull | +18% / +18% / +12% | 33% | 5% | $5,316 | **$434** |
| Base | +14% / +14% / +7% | 30% | 3% | $2,595 | **$212** |
| Bear | +9% / +9% / +4% | 24% | 2% | $1,423 | **$116** |

**확률가중 IV (Bull 30 / Base 40 / Bear 30)**: $0.3×$434 + $0.4×$212 + $0.3×$116 = **$250 / share**

vs 현재가 $383 → **-35%** (안전마진 30% 적용 매수가 = $175)

#### DCF (보수 OE = Capex_total, sensitivity)

| 시나리오 | OE/Rev | IV (B) | per share |
| --- | --- | --- | --- |
| Bull | 1~14% | $2,435 | $199 |
| Base | 1~10% | $1,009 | $82 |
| Bear | 1~6% | $559 | $46 |

**확률가중 IV (보수)**: **$106 / share** vs 현재가 $383 → **-72%**

→ 보수·진보 둘 다 현재가 overvalued

#### EPS × PER (보조)

가정: TTM 조정 EPS $10, Terminal PER (peer median 5y avg ~30 baseline)

| 시나리오 | 10y EPS CAGR | 10y EPS | Terminal PER | 10y 후 가치 | 10y CAGR |
| --- | --- | --- | --- | --- | --- |
| Bull | +13%/yr | $33.9 | 28x | $949 | **+9.5%/yr** |
| Base | +10%/yr | $25.9 | 22x | $570 | **+4.0%/yr** |
| Bear | +6%/yr | $17.9 | 13x (-50% compression) | $233 | **-4.8%/yr** |

**확률가중 CAGR (Bull 30 / Base 40 / Bear 30)**: 0.3×9.5% + 0.4×4.0% + 0.3×(-4.8%) = **+3.0%/yr**

→ 무위험이자율 4.35% 이하, **매수 금지 임계 미달**

### 4-C. Reverse 역산

#### DCF Reverse

시가총액 $4,690B / TTM NOPAT $113B = implied multiple 41.5x

- Gordon growth 단순화: 시장 implicit terminal g = **7.4%** (매우 낙관)
- 다단계 DCF: 시장이 Bull case의 **88% 반영**, Base 대비 +81% premium

#### EPS×PER Reverse

현재가 $383, TTM 조정 EPS $10, Terminal PER 25 가정:

| 본인 기대수익률 | 정당화 EPS CAGR |
| --- | --- |
| 무위험 4% | ~9% (본인 Base) |
| 10% | ~15% (본인 Bull보다 낙관) |

→ **시장은 본인 Base ~ Bull 사이 가정**. 본인 (Base 40 / Bear 30 weight)이 **시장보다 보수적**.

### 4-D. 확률가중 IV + 과신 점검 + 판정

#### 확률 weight 근거

- Bull 30%: Cloud 가속 검증 (Q1) + Backbone Integration + Wide moat (Stream 1-B). 단 Multiple compression risk (Q3 v2)
- Base 40%: 정상화 시나리오, RPO 50% 전환, Search publisher cost 반영
- Bear 30%: Combined thesis-breaker (Cloud 둔화 + Productivity 미실현 + Multiple compression -30~50%) 가능

#### 과신 점검 — "내가 틀렸다면 Bear 과대평가"

Worse-than-Bear 시나리오 3개:

1. **AI Bubble 붕괴 + Cloud 실제 둔화**: Cloud +10%, OM 18%, MC -60% → IV ~$80, 10y CAGR -10%
2. **DOJ DC Circuit 항소 패배 + Apple deal 금지**: Search -15%, Chrome 매각 → IV ~$90, CAGR -8%
3. **Productivity Paradox 영구 + 매크로 침체**: Cloud +5%, 매출 +3% → IV ~$70, CAGR -12%

→ 합계 확률 ~13-20% (일부 overlap, 본인 Bear 30%에 부분 흡수). **본인 Bear 30%는 합리적**.

**Missing variables**: AI 칩 cycle (Vera 2028~), 매크로 침체, 지정학 (TSMC·Taiwan), Apple Intelligence default 결정 timing, DeepMind 차별화 매출. **대부분 Bear 가산**.

#### OR 결합 판정

| 도구 | 결과 | 판정 |
| --- | --- | --- |
| DCF (진보 OE) 확률가중 IV | $250 vs $383 | **매수 금지** (-35%) |
| EPS×PER 확률가중 CAGR | +3.0% < 4.35% RF | **매수 금지** |
| EPS×PER 낙관 (Bull) CAGR | +9.5% (회색 임계) | 회색 |
| Phase 10 ROE×PBR (차단 사본) 확률가중 | +7.1% | 회색 |

**최종 판정**: **회색 (대기)** — 사업 Wide moat (Stream 1-B), 단 가격이 Bull 시나리오 88% 반영 → 매수 매력 부족

#### 매수가 Trigger

| 옵션 | 가격 | 근거 |
| --- | --- | --- |
| 공격 (EPS×PER 임계 10%) | **$330** (-14% from current) | Bull CAGR 11.4% |
| 보수 (DCF 안전마진 30%) | **$175** (-54%) | 진보 OE 확률가중 IV $250 × 0.70 |
| **현실 권장** | **$230~$260** | Bear 시나리오 부분 발현 시 |

**Druckenmiller 패턴 정량 검증**: "Bullish on company (Wide moat) but bearish on price (Bull 88% 반영)" — 본인 Q3 v2 직관 데이터로 검증

### 4-E. Phase 10 Anchoring 차단 비교

| 지표 | Phase 10 (locked) | Stream 4 | 차이 원인 |
| --- | --- | --- | --- |
| Bear 확률 | 20% | **30%** | 본인 Q3 v2 (Combined thesis-breaker 인식) |
| Bull EPS CAGR | 14.1% | 9.5% | Stream 4 Multiple compression 정량화 |
| Multiple compression | PBR 3x (implicit) | PER 13x (-50% 명시) | Stream 4 정량화 |
| 시나리오 timing | 단일 10년 | 4-Period (1-2y/3-5y/5-10y/Terminal) | Stream 3 4-Period frame |
| Druckenmiller 패턴 | 없음 | 명시 | Stream 3·4 본인 frame |
| 판정 | 회색 (확률가중 7.1% < 10%) | 회색 (확률가중 IV -35%) | **일치** ✓ |

→ **Anchoring bias 없음**. Stream 4가 Phase 10을 sharpen + extend. 둘 다 **회색** 판정 일치.

### 4-회고

#### 작업 결과

- DCF + EPS×PER cross-check, 두 도구 모두 **회색~매수 금지**
- Reverse 역산: 시장 implicit Bull 88% 반영
- 과신 점검: Bear 30% 합리, Missing variables 대부분 Bear 가산
- Phase 10 anchoring 비교: bias 없음, Stream 4 더 정교

#### 잘 된 점

1. Druckenmiller 패턴 정량 검증 (Q3 v2 직관 → 가격 88% Bull 반영 데이터)
2. OE 두 case (보수 $38B vs 진보 $113B) 병행 sensitivity
3. Stream 3 4-Period 매트릭스 → DCF 시나리오 입력 직접 매핑
4. 매수가 trigger 3-tier 명시 ($330 공격 / $230~260 현실 / $175 보수)

#### 부족한 점

1. Tax rate 18% 가정 — 실효세율 변화 sensitivity 미수행
2. SBC 정확치 미공시 (TTM ~$26B 추정)
3. ROIC 분산 22~31%로 baseline 26% 사용 — 출처별 큰 차이
4. Q1 2026 equity gain 정량 sanity check 부족 (Waymo·기타 securities)

#### 다음 단계

- **Stream 5** (시장 의견 비교): 분석가 컨센서스 vs Stream 4 IV
- **Stream 6** (편입 결정): 회색 → watch list 진입, 매수가 trigger monitor
- **Stream 7** (포지션 사이징): 매수가 trigger 발현 시
- **Stream 8** (모니터링): Stream 3 Driver Tree + Top 5 metric 분기 추적

---

## Stream 4 Pre. Q1~Q7 시나리오 입력 누적 (Stream 4-Main 진행 시 참조)

### 4-Pre. Q1~Q7 시나리오 입력 누적

> Stream 4 본격 작업은 모든 질문 (Q1~Q7) 종결 후. 각 질문이 종결될 때마다 시나리오 가정을 여기 누적.

#### Q1. Cloud 지속 성장 (종결: 2026-05-03)

> 상세: `GOOGL_research_questions.md` Q1 도달한 답 + `GOOGL_q1_cloud_research.md`

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| Cloud 매출 5y CAGR | +40% | +30% | +20% |
| Cloud Op Margin (5y) | 32.9% → 38% | 32.9% → 30% | 32.9% → 22% |
| TPU 시장 점유 (5y) | 35%+ | 25~30% | 15~20% |
| RPO 전환률 | 50%+ | 50% | 30~40% |

**핵심 가정 monitoring**:

- 2027 Q1 Rubin 출시 영향
- TSMC CoWoS 할당 변화
- 어닝콜 "capacity constrained" 멘트 변화
- Software stack stickiness (CUDA vs PyTorch/TPU)

#### Q2. 검색 AI 위협 (종결: 2026-05-03)

> 상세: `GOOGL_research_questions.md` Q2 도달한 답 + `GOOGL_q2_search_research.md`

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| Search 매출 5y CAGR | +12% | **+5~7%** (publisher cost) | +1~3% |
| 검색 점유 (5y) | 88~90% 유지 | 80~85% | 65~70% |
| Apple deal | 유지 ($20B 안정) | 유지 but 가격 협상 ($25~30B) | 항소심에서 금지 또는 ChatGPT 전환 |
| 광고 단가 (CPC) | +5%/yr | 안정 | -10% |
| Search Op Margin | 유지 | -3~5%pt | -7~10%pt |

**핵심 가정 monitoring**:

- Publisher revenue share / content licensing 협상
- AIO quality / ChatGPT 이탈률
- Apple deal 매년 재협상 (TAC inflation)
- DOJ DC Circuit 판결 (2027~2028)
- Agent mainstream 채택률
- Claude 엔터프라이즈 점유 trend

#### Q3. Capex ROI 회수 (종결: 2026-05-03)

> 상세: `GOOGL_research_questions.md` Q3 도달한 답 + `GOOGL_q3_capex_research.md`

| | Bull | Base | Bear |
| --- | --- | --- | --- |
| Capex 회수 (5y) | RPO 60%+ 전환, Cloud +30%/yr | RPO 50% 전환, Cloud +25%/yr | RPO 30~40%, Cloud +20% 둔화 |
| FCF (2027~2028) | 압박 후 회복 (+$70B/yr) | flat ($50-60B/yr) | **negative possible** |
| Productivity | 2028~ Solow paradox 해소 | 점진 (5년+) | 측정 불가 지속 |
| Multiple Compression | -10% 이내 | -10~20% | **-30~50% (닷컴 partial)** |
| FCF 우위 (vs Big 4) | 더 강화 | 유지 | 격차 좁혀짐 |
| **확률 weight (본인)** | **~30%** | **~40%** | **~30%** |

**핵심 frame** (Druckenmiller 패턴):

- "Bullish on the company but bearish on the price"
- Fair value = Bull × 0.3 + Base × 0.4 + Bear × 0.3
- **안전마진 30%+ 적용** (Multiple compression risk)

**핵심 가정 monitoring**:

- Cloud 분기 성장률 +20% 이상 유지
- 2027년 D&A 가속 vs 매출 가속 비교
- Productivity 실측 (NBER·McKinsey)
- Combined thesis-breaker (Cloud 둔화 + Productivity 미실현 + Multiple compression) 발현 시 추매 기회

#### Q4~Q7. 작업 예정

- Q4 Gemini 차별화 — lighter touch
- Q5 TPU 사용률 — Q1 chip 조사 재활용 (별도 깊이 X)
- Q6 AI 답변 상업화 — lighter touch
- Q7 YouTube 성장 — lightest

### 4-Main. 가격 시나리오 (작업 예정 — Q1~Q7 종결 후)

작업 후 채움 (DCF + 비교 multiple + 안전마진)

---

## 진행 상황

- [x] Anchoring 차단 사본 작성
- [x] Stream 1-A 매출 분해 (제품 기준 + 공시 기준 + 추적 가능성 구분)
- [x] Stream 1-A 다음 세대 파이프라인 (N+1·N+2·N+3)
- [x] Stream 1-A 산업 구조 (5 Forces × 검색 광고 + Cloud)
- [x] Stream 1-A 산업 성장 경로 10년
- [x] Stream 1-A 기술 변곡점 지도
- [x] Stream 1-A 외부 자료 출처
- [x] Stream 1-B 해자 분석 (2026-05-03)
- [x] Stream 1 회고 + Phase 5·6-A 차단 해제 비교 (2026-05-03)
- [x] Stream 2-0 경영진 baseline
- [x] Stream 2-bis 1분 테스트
- [x] Stream 2-ter 결격사유·외부 검증
- [x] Stream 2-3 Fisher 15 preliminary
- [x] Stream 2-4 형용사 preliminary
- [x] Stream 2-5 자본배분 분석 (10건)
- [x] Stream 2-2 공언-결과 매핑 (8건 추적, 6 PASS + 1 PRELIM PASS + 1 TRACKING)
- [~] Stream 2-1 1층위 통독 — **DEFERRED** (컨콜 4분기 verbatim 발췌 완료, 주주서한 직접 통독은 deferred. trigger: Stream 4 가치평가 회색 영역 진입 시)
- [x] Stream 2 종료 — Phase 4 차단 해제 비교 + 회고 작성 (2026-05-03)
- [x] **Stream 2 PRELIM PASS 종료 — Stream 3 advance 가능**
- [x] Stream 3 리스크·기회 매트릭스 (R 19 + O 20)
- [x] Stream 3 Driver Tree (재무 결과 통합)
- [x] Stream 3 4-Period 시나리오 명세 (단기·중기·장기·terminal)
- [x] Stream 3 Monitoring Plan (Top 5 + Driver Tree 진단 + 외부 보고)
- [x] Stream 3 Phase 8 Anchoring 비교 — bias 없음
- [x] Stream 3 회고 (2026-05-03)
- [x] Stream 4 (가격 평가) 본격 — DCF (보수+진보 OE) + EPS×PER + Reverse 역산 + 과신 점검 + Phase 10 anchoring 비교
- [x] Stream 4 판정: **회색 (대기)** — 현재가 $383, Bull 88% 반영. 매수 trigger $230~260 (현실), $175 (보수)
- [ ] Stream 5 (시장 의견 비교)
- [ ] Stream 6 (편입 결정 — watch list 진입)
- [ ] Stream 7·8 (사이징 + 모니터링)
