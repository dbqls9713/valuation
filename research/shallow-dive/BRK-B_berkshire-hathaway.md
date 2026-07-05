---
ticker: BRK-B
shallow_date: 2026-05-04
track_verdict: buffett
confidence: medium
deep_eligible: true
scenario_pv_weighted: 255    # 10-A Operating EPS×PER 확률가중 PV (USD)
scenario_cagr_bull: 0.140    # 10-B Bull CAGR
scenario_cagr_weighted: 0.088  # 10-B 확률가중 CAGR
scenario_sotp: 895           # 10-C SOTP (USD bn)
---

# Berkshire Hathaway Class B (BRK-B) Shallow Dive — 2026-05-04

기준일: 2026-05-04 | 데이터: SEC EDGAR companyfacts (CIK 0001067983) + Berkshire 2025 Annual Release | 주가: $473.01 (2026-05-03)

> **사용자 요청**: 위대함은 이미 알고 있음. **시나리오 + Valuation 집중**. Phase 1-8 압축, **Phase 9-10 정밀**.

---

## Executive Summary

| 항목 | 결론 |
| --- | --- |
| 트랙 | **Buffett형** (4축 중 3축 명확, 1축 Mixed) |
| 확신도 | **중간** (Greg Abel 승계 — 2026.1 시작 — 의 자본배분 능력 검증되지 않음) |
| 10-A (Operating EPS×PER) 확률가중 PV | $255 vs 현재 $473 (−46%) |
| 10-B (ROE×PBR) 확률가중 CAGR | 8.8% (목표 10% 미달) |
| 10-C (SOTP) | $895B vs 시총 $1,020B (−12%) |
| Deep 진입 결정 | **YES, 그러나 Parking Lot도 합리적**. 강한 매수 신호 없음, fair price 부근. |
| 매수금지 trigger | 미발동 (Bear CAGR 4.7% > 4%, 간신히 통과) |

**한 문단 결론**: BRK는 세 가지 valuation 방법(Operating DCF / ROE×PBR / SOTP)이 모두 "현재가는 fair에서 약간 비쌈" 구간을 가리키며 의미있는 margin of safety가 없다. **Bull 시나리오에서만 두 자릿수 CAGR**이 나오고, 그 Bull은 Greg Abel가 (a) 캐시 $348B를 효율적으로 deploy하고 (b) 보험 underwriting 우월성을 유지하는 두 조건이 동시에 충족되어야 한다. Buffett 트랙 Deep에서 (1) Abel 자본배분 track record (BHE 28년) 정밀 평가, (2) Float leverage의 향후 비용, (3) 운영 자회사 SOTP 정밀화가 핵심.

---

## 1. 유니버스 하한선 (Phase 1) — PASS

| 항목 | 조건 | 판정 | 근거 |
| --- | --- | --- | --- |
| 상장 기간 | ≥ 5년 | PASS | A주 1980~, B주 1996~ (30년+) |
| 컨퍼런스콜 | 정기 + Q&A | PASS | 분기 release + 연례주총 5시간 Q&A 전사본 공개 |
| 현 경영진 재직 | ≥ 3년 | PASS | Greg Abel 부회장 (Insurance ex) 2018~, CEO 2026.1~. **신규 CEO 4개월차 — flag.** |
| 임원 이직률 | DART/EDGAR 추적 | PASS | Form 4 정상 |
| 업계 평판 접근 | | PASS | 산업·금융업 표준 |

**flag**: CEO 신규(Abel) — 임원 재직 ≥ 3년 조건은 부회장 직책 기준으로는 PASS이지만, **CEO로서의 자본배분 record는 0**. Phase 9 D축 + Phase 10 시나리오 가정에 직접 반영.

---

## 2. 2분 테스트 + 사업모델 (Phase 2)

**2분 테스트**: BRK는 **(a) 보험 underwriting + float leverage**, **(b) 운영 자회사 (BNSF 철도 / BHE 에너지 / GEICO + 재보험 / 제조·유통)**, **(c) 거대 주식 포트폴리오 (Apple·AXP·BAC·KO·CVX 등 $274B)** + **(d) 캐시·T-bill $348B**의 4-stack 복합체. 위대함의 본질은 "Float을 부채 아닌 free leverage로 활용 + 잉여 현금을 IV 대비 할인된 자산에 배분"하는 **자본배분 머신**이라는 점.

| 항목 | 내용 |
| --- | --- |
| 제품/서비스 | 보험 (P&C 87.1% combined) / 운송 (BNSF) / 에너지 (BHE) / 제조·소매 / 투자 |
| 주요 고객 | 보험가입자(개인·기업) / 화주 / 전력 utility 고객 / 일반 소비자 |
| 수익 모델 | Float yield + Underwriting profit + 운영 자회사 owner's earnings + 투자 dividend·realized gains |
| 고정비 vs 변동비 | BNSF·BHE 고정비 매우 높음 (자본집약). 보험은 변동(claims)이 큼 |
| 규모의 경제 | 작동 강함 (재보험 capacity, 철도 network, 에너지 grid scale) |

---

## 3. 정량 기초 5년 (Phase 3) — Phase 10 입력

**SEC companyfacts 기반** (FY2021~FY2025, $B):

| 지표 | 2021 | 2022 | 2023 | 2024 | 2025 | 5y CAGR |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 매출 (Revenues) | 276.2 | 302.0 | 364.5 | 371.4 | 371.4 | 7.7% |
| 순이익 (Reported NI, mark-to-market 포함) | 89.9 | −22.8 | 96.2 | 89.0 | 67.0 | n/a (변동성↑) |
| **Operating Earnings (Buffett 정의)** | 27.6¹ | 30.8¹ | 37.4¹ | 47.4 | 44.5 | 12.7% |
| CFO | 39.4 | 37.4 | 49.2 | 30.6 | 46.0 | 3.1% |
| CAPEX | 13.3 | 15.5 | 19.4 | 19.0 | 20.9 | 12.0% |
| FCF (CFO−CAPEX) | 26.1 | 21.9 | 29.8 | 11.6 | 25.1 | n/a |
| 자기자본 | 506.2 | 473.4 | 561.3 | 649.4 | 717.4 | 9.1%² |
| 총자산 | 959.4 | 948.5 | 1,070.0 | 1,153.9 | 1,222.2 | 6.3% |
| BV per share ($) | 234³ | 219³ | 260³ | 301 | 332.55 | 9.2%² |
| **Insurance Float** | 147 | 164 | 169 | 171 | 176 | 4.6% |
| Cash + T-bills | 144 | 129 | 168 | 334 | 348 | 24.7%! |

¹ BRK 연차보고서 (Buffett 서한). ² 6년 환산. ³ 추정 (BV/share 환산).

**5년 추세 한 줄**: Operating earnings 견고 성장 (~13% CAGR) + BV per share 9% CAGR + **캐시 폭발적 누적 ($144B → $348B, 4년에 +$204B)** = "deploy할 곳을 못 찾는 자본"의 시그널. 2025 보험 underwriting 약화(combined 87.1%, 5y avg 90.7% 대비 양호하지만 2024 대비 -19%) + AAPL 매도로 cash 추가 증가.

**Phase 9 Track Gate 입력 추출**:

| 입력 | 값 |
| --- | --- |
| 5년 매출 CAGR | 7.7% |
| 5년 ROE 평균 (NI mark-to-market 포함) | 12-15% (변동성 큼) |
| 5년 ROE (Operating earnings 기준) | 6-8% (자기자본 대비) |
| 재투자율 = (Capex+R&D)/CFO | (20.9+0)/46 ≈ **45%** (그러나 R&D 사실상 0이므로 운영 자회사 capex만; buyback·M&A 별도) |
| 5년 주식수 변화 | −5.4% (2021 2.28B → 2025 2.16B), 평균 −1.1%/yr (2024-2025 buyback 둔화) |
| 캐시/총자산 비율 | 28.5% (2025) — **자본배분 정체 시그널** |

---

## 4. 경영진 기본 프로필 (Phase 4)

| 항목 | 내용 |
| --- | --- |
| CEO | **Greg Abel** (2026.1~, 부회장 Insurance ex 2018~, BHE 28년) |
| 전 CEO/Chairman | **Warren Buffett** (1965~2025) — 2025.5 사임 발표, 2025.12 공식 사임. 92세. |
| CFO | Marc Hamburg (1992~) — long-tenure |
| 후계 | Vice Chair Insurance: **Ajit Jain** (재보험 전설) — 73세, 점진적 isolation 가능성 |
| Investment | Todd Combs (GEICO CEO) + Ted Weschler — 2~3년 내 portfolio 의사결정 본격 인계 |
| Buffett 보유 | 약 14% (B주 등가). 사후 자선재단으로 점진적 처분 (10년에 걸쳐). **공급 압박 risk.** |
| 위기 대응 record | Abel — 2008 BHE 인수 후 28년 무사고 자본배분, 신재생 투자 leadership |

### 1분 신뢰성 테스트

| # | 항목 | 판정 | 근거 |
| --- | --- | --- | --- |
| 1 | ROE 10%+ | PASS (mixed) | Operating ROE 7-9%, Reported ROE 변동성 큼 |
| 2 | ROA 7%+ | FAIL | $1.22T 자산 대비 7% = $85B NI 필요. Operating earnings $45B로는 ROA ~3.7% (보험사 특성) |
| 3 | 의심 항목 | PASS | 무형자산 비중 낮음, goodwill 보수적 평가 |
| 4 | 일회성 비용 | PASS | mark-to-market 외 매우 깔끔 |
| 5 | 주주환원 | MIXED | 배당 0%, buyback 둔화 (2024-2025 거의 정지) → 캐시 누적의 그림자 |

**가이던스 성격 (Phase 9 D축)**: BRK는 가이던스 미제공. Buffett 서한은 **"IV 대비 할인 시 buyback / 보험 underwriting 규율 / 자회사 owner-orientation"** 톤 = 명백한 **현금환원·마진안정 = Buffett형**. Abel 시대의 가이던스 톤은 아직 미관찰. 첫 주주서한(2026.2) 이미 발표 — Abel 명의. Buffett 톤 계승.

---

## 5. 산업 지도 (Phase 5)

| 항목 | 내용 |
| --- | --- |
| 경쟁사 | (Conglomerate 자체는 unique). 부문별: Insurance — Chubb·Travelers·Allstate / Rail — UNP·CSX / Energy — Duke·NextEra / Manufacturing — 다양 |
| 시장 집중도 | 부문별 3-5사 oligopoly. P&C 보험 top 5 = 50%. Class I 철도 5사 = 100% |
| 산업 성장률 (CAGR) | P&C 보험 3-5% / 철도 2-4% / 전력 utility 3% / 다양 |
| 산업 내 위치 | 1등 (재보험·utility 중 BHE) 또는 2-3등. 절대 다양화. |
| 해자 후보 | 규모 (보험), Network (철도), Cost (utility), 자본 접근성 (전체) |

**산업 성숙도**: 매출 CAGR 7.7% > 산업 가중평균 3-4% → 명백히 산업 상회하지만, 이는 **인수 효과** 포함. Organic growth만 보면 2-3% (성숙 산업의 자연 성장률). → Buffett형 신호.

---

## 6. 질적 체크리스트 (Phase 6) — 압축

### 6-A. 버핏 해자 (요약)

| 조건 | 판정 | 근거 |
| --- | --- | --- |
| 1. 소비자 욕망 | YES (부문별) | 보험 필수재, 전력 필수재, 철도 화물 필수재 |
| 2. 대체재 부재 | YES (부문별) | 단, BRK 자체는 ETF·index로 대체 가능 (소액주주 관점) |
| 3. 가격 규제 | MIXED | 보험·utility는 일부 규제. P&C는 자유롭지만 utility는 PUC 규제 |

**See's 테스트**: ROIC 안정 ~10-12% < See's 60% 기준 (자본집약 사업이라 본질적 mismatch). **CAPEX/매출 5.6% (BNSF·BHE 위주). OE > NI**: Operating earnings $45B vs Reported NI $67B (mark-to-market 효과로 NI > OE인 해, 반대인 해 혼재).

**해자 등급**: Brand HIGH (GEICO·BNSF·Buffett 자체) / Network HIGH (BNSF·재보험) / Switching MODERATE (보험) / Cost HIGH (BHE scale·세제 활용) / 수직통합 HIGH (보험-투자-운영 stack).

### 6-B. Fisher 정량 5개 (요약)

| # | 기준 | 판정 |
| --- | --- | --- |
| 1 | 매출 성장 잠재력 | MIXED (산업 상회하지만 인수 의존) |
| 3 | R&D 효율 | N/A (R&D 거의 0) |
| 5 | 충분한 이익률 | PASS (Operating margin 12% — diversified 평균 상회) |
| 6 | 마진 유지 | PASS (보험 combined ratio 5y avg 90.7%) |
| 10 | 회계 관리 | PASS (Buffett 회계 보수 평판) |

→ 정량 5개 PASS 3 + MIXED 1 + N/A 1. Buffett 트랙 적합.

### 6-C. 프리미엄 9요인 (요약)

| # | 요인 | 점수 |
| --- | --- | --- |
| 1 | 반복구매 | 5 (보험 갱신·전력) |
| 2 | 확장성 | 3 (자본집약, M&A 의존) |
| 3 | 생산성 | 3 |
| 4 | 원가절감 | 4 (BHE·BNSF) |
| 5 | 락인 | 4 (보험·utility) |
| 6 | 예측 가능성 | 3 (보험 cycle, mark-to-market 변동) |
| 7 | 비순환성 | 3 (보험 cycle 영향) |
| 8 | 산업 고성장 | 1 (성숙 산업) |
| 9 | 가격결정력 | 3 (보험 P&C cycle, utility 규제) |

**합계**: 29/45 (mid-tier 프리미엄 — Buffett형 typical).

### 6-D. Lynch 6분류

→ **대형 우량주 (Stalwart)**. 시총 $1.02T, 안정 성장. PEG: PER (Operating) ~22.9 / 성장률 6-9% = 2.5~3.8 (Lynch 기준 **불리** ≥ 2). 하지만 Stalwart의 PEG는 다른 기준 적용.

---

## 7. 밸류에이션 현재 위치 (Phase 7)

| 지표 | 값 (현재) | 5년 평균 | 5년 레인지 | 해석 |
| --- | --- | --- | --- | --- |
| PER (Reported) | $473×2.16B / $67B = 15.2x | n/a (변동) | 9-50x | Reported는 노이즈 |
| **PER (Operating)** | $473×2.16B / $44.5B = **22.9x** | ~21x | 17-26x | 평균 약간 상회 |
| **PBR** | **1.42x** | 1.40x | 1.20-1.55x | 평균 부근 |
| FCF Yield | $25B / $1,020B = 2.5% | 2.5% | 1.5-3.5% | 낮음 |
| 주가 5년 추이 | 현재 $473, high $542, low $289 (5y) | | | 5년 +60% |

### PBR 공식 역산

`PBR = ((1+ROE)/(1+r))^N`

ROE = 9% (Operating, 자기자본 기준), PBR = 1.42, r = 10%:

- `1.42 = (1.09/1.10)^N` → 1.09/1.10 = 0.991. 0.991^N = 1.42 ⇒ N = ln(1.42)/ln(0.991) = **−39년**(음수, 무의미)

ROE = 11% (낙관, mark-to-market 평균 포함), r = 10%:

- `(1.11/1.10)^N = 1.42` → 1.0091^N = 1.42 ⇒ N = ln(1.42)/ln(1.0091) = **39년**

→ **시장은 BRK가 향후 39년간 ROE 11% 유지를 가정**. r=10%이면 ROE-r = 1%p의 thin margin 39년 = 1.42x. **시장이 매우 너그럽거나, ROE 가정이 너무 낮은 것**. Operating ROE 12-13% 가정이라면 N=12년 정도로 합리화 가능.

---

## 8. 1차 가설 + 리스크·기회 톱3 (Phase 8) — Phase 10 입력

### 1차 가설

> BRK는 **보험 float ($176B) + 운영 자회사 cash flow + 거대 투자 포트폴리오 + 역대 최대 캐시 ($348B)**의 4-stack 복합체로, **Greg Abel 시대의 자본배분이 Buffett의 절반만 따라가도** 8-10% CAGR이 가능해 보인다. 다만 **현재가 $473 (PBR 1.42x, Operating PER 22.9x)는 historical 평균에서 위쪽**이며 의미있는 margin of safety는 없다. 시장은 "Abel = Buffett continuity"를 이미 가격에 반영한 것으로 보인다.

### 리스크 톱3

| 순위 | 리스크 | 발생 시 영향 | 관찰 지표 |
| --- | --- | --- | --- |
| 1 | **Abel 자본배분 실패** — $348B 캐시를 IV 대비 비싼 자산에 deploy | ROE 11% → 7%로 영구 하락, PBR 1.42 → 1.1로 리레이팅 | Abel 1-2년 acquisition track record, buyback 활성도, Annual letter 톤 |
| 2 | **Buffett 사후 supply pressure** — Berkshire 재단 매년 ~$5-7B B주 매도 (10년 분할) | 주가 -5~10% 압박 (수요·공급 mismatch) | 재단 13D 공시, Float-adjusted 거래량 |
| 3 | **보험 cycle deterioration + AAPL 의존도** — AAPL이 portfolio 22.6% (집중 risk) | 포트폴리오 -10% = 시총 -$30B 영향 | AAPL P/E·매출 추이, P&C combined ratio (87.1% 추세) |

### 기회 톱3

| 순위 | 기회 | 실현 시 영향 | 관찰 지표 |
| --- | --- | --- | --- |
| 1 | **대형 인수** — Cash $348B로 $50B+ 가치 인수 (Buffett "elephant gun") | Operating earnings +$3-5B/yr, 캐시 productivity 회복 | M&A 공시, Cash 잔고 추이 |
| 2 | **Float yield 상승** — 5% T-bill 환경 지속 시 $348B × 5% = $17B/yr risk-free | Investment income +$5B/yr (현 baseline 대비) | Fed funds rate, BRK 분기 investment income |
| 3 | **BHE/BNSF 회복** — 재생에너지 capex 회수 + 화물 volume 회복 | Segment operating earnings +20% | BHE segment EPS, BNSF carloads |

---

## 9. ⭐ Track Assignment Gate (Phase 9)

**배정 질문**: BRK는 "해자가 이미 완성되어 현금 회수 중인가?" YES — 보험·BNSF·BHE 모두 mature franchise.

### 4축 판정

| 축 | 관측값 | 판정 |
| --- | --- | --- |
| **A. ROIC 추세** | Operating ROE 7-9% (자기자본 기준), 5년 안정 (변동성은 mark-to-market). 최근 추세 안정~소폭 하락 (자기자본 폭발 증가 vs operating earnings 정체). | **Buffett형** ✓ |
| **B. 재투자율** | (Capex+R&D)/CFO = 45% (R&D 0). 그러나 본질적 재투자는 **buyback (-1.1%/yr 5y, 2024-2025 거의 정지) + M&A**. 캐시 누적 $204B in 4년 = "deploy 정체". 배당 0%. | **Buffett형 (강한)** ✓ — buyback과 M&A 모두 둔화는 "회수 단계 도달" 시그널 |
| **C. 매출 CAGR** | 기업 7.7% (5y) vs 산업 가중평균 3-4%. 인수 효과 제외 시 organic 2-3%. | **Buffett형** ✓ (산업 근접) |
| **D. 가이던스 성격** | Buffett 서한 일관: IV 대비 할인 시 buyback, 보험 규율, owner-orientation. Abel 첫 서한(2026.2)도 동일 톤 계승. | **Buffett형** ✓ |

### 배정

- **배정**: **Buffett형** (4축 모두 Buffett, 단 A·B에 약간의 모호성)
- **확신도**: **중간** (배정 자체는 4/4지만, **Abel 시대 record 부재**가 valuation 가정 신뢰도를 떨어뜨림 → Phase 10 결과 신뢰구간 넓음)
- **Deep 프로토콜**: `playbook/DEEP_DIVE.md` (8 Stream 통합)
- **재배정 플래그**: 만약 Abel가 캐시 deployment를 **R&D-like 재투자 (예: AI infra + 에너지 transition + 신규 산업 인수)**로 강하게 frame하면 → Fisher형 재배정 가능. 현재까지는 그런 시그널 없음.

---

## 10. ⭐⭐ 간이 3시나리오 Valuation (Phase 10) — Track 분기 = Buffett형

> **사용자 요청 집중 영역**. 정상은 10-A만 (Buffett 배정), 그러나 **BRK 특성상 EPS×PER만으로는 부적합** (NI 변동성 + 캐시 누적 미반영). 따라서:
>
> - **10-A**: Operating EPS × PER (mark-to-market 제외, BRK 정의 사용)
> - **10-B**: ROE × PBR (book compounding) — **BRK에 가장 적합**, Buffett 본인 buyback 의사결정 framework
> - **10-C**: SOTP 검증 (4-stack 분리 평가) — sanity check
>
> 세 결과의 **divergence 자체가 정보**.

### 10-0. 시나리오 가정 매트릭스 (공통)

| 변수 | Bull (P=20%) | Base (P=55%) | Bear (P=25%) |
| --- | ---: | ---: | ---: |
| 매출 CAGR (10년) | 8% | 5% | 3% |
| Operating Earnings CAGR | 9% | 6% | 3% |
| 유지 ROE (operating, %BV) | 12% | 9% | 7% |
| 주식수 변화 (annual) | −1.0% | −0.3% | 0% |
| 터미널 PER (operating) | 20x | 17x | 13x |
| 터미널 PBR | 1.7x | 1.4x | 1.15x |

**확률 배분 근거**: 시장은 이미 Base 부근에 가격 매김 ($473 ≈ Base의 PV 부근). Bull이 20%인 이유: Abel 검증 + 큰 인수 동시 발생 확률 낮음. Bear 25%: 캐시 misallocation + 보험 cycle 동시 악화는 base case보다 약간 낮은 확률이지만, **과거 50년 Buffett 천재성에 의존했던 spread**가 vanish하는 경로이므로 무시 못 함.

### 10-A. Risk·Opportunity → 시나리오 매핑

| 항목 | Bull 전제 | Base 전제 | Bear 전제 |
| --- | --- | --- | --- |
| Risk #1 (Abel 배분 실패) | 해소 — 1-2년 내 큰 deal 성공 | 완만 — 점진적 deploy | 실현 — cash $400B 누적, 5%만 yield |
| Risk #2 (재단 supply 압박) | 해소 — 시장 완충 (수요 강함) | 완만 — 5% 디스카운트 | 실현 — 10% 디스카운트, PBR 리레이팅 |
| Risk #3 (보험 + AAPL 집중) | 해소 — combined 88%, AAPL 안정 | 완만 — combined 92%, AAPL flat | 실현 — combined 96%, AAPL -30% |
| Opp #1 (큰 인수) | 실현 — $50B 인수 +$5B earnings | 부분 — $20B 누적 인수 +$2B | 미실현 |
| Opp #2 (Float yield) | 실현 — 5% 유지 | 부분 — 4% 평균 | 미실현 — 2% (금리 인하) |
| Opp #3 (BHE/BNSF 회복) | 실현 — segment +20% | 부분 — +10% | 미실현 — 부진 |

### 10-A. Buffett DCF (Operating EPS × PER)

**BRK 조정**: NI 대신 **Operating Earnings** 사용 (Buffett 정의: net earnings ex investment gains/losses). 이는 보험 underwriting + investment income + 운영 자회사 owner's earnings의 합.

#### 계산

```text
2025 Operating Earnings 기준 = $44.5B (확정)
10년 후 Operating Earnings = $44.5B × (1 + g)^10
10년 후 주식수 = 2.16B × (1 + Δshares)^10
10년 후 Operating EPS = Earnings / Shares
10년 후 가격 = EPS × Terminal PER
PV @ r=10% = 가격 / 1.10^10 = 가격 / 2.594
```

| 시나리오 | 2035 Op.Earn ($B) | 2035 Shares (B) | 2035 Op.EPS | 2035 가격 | PV ($) | 현재가 대비 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Bull | 105.4 | 1.95 | $54.0 | $1,081 | **$417** | −12% |
| Base | 79.7 | 2.10 | $38.0 | $646 | **$249** | −47% |
| Bear | 59.8 | 2.16 | $27.7 | $360 | **$139** | −71% |

**확률가중 PV** = 0.20×$417 + 0.55×$249 + 0.25×$139 = $83.4 + $137.0 + $34.8 = **$255**
**현재가 대비**: $255 / $473 = 0.539 → **−46% (significantly overvalued by Operating DCF)**

### 10-B. Fisher-style ROE×PBR (book compounding) — BRK에 적합

**근거**: BRK는 retain earnings 100% (배당 0%) + buyback 둔화 → **earnings의 BV 누적 효과가 매우 강함**. Buffett 본인이 IV 추정 시 BV/share 변화를 핵심 지표로 사용.

**공식**: 배당 0이므로 단순화: `r = (1+ROE) × (Terminal_PBR / Current_PBR)^(1/N) − 1` ≈ ROE + PBR drift 조정

#### 계산 (10-B)

```text
2025 BV/share = $332.55 (확정)
2035 BV/share = $332.55 × (1 + ROE)^10
2035 가격 = BV/share × Terminal PBR
CAGR = (2035가격 / 현재가 $473)^(1/10) − 1
```

| 시나리오 | 2035 BV/share | Terminal PBR | 2035 가격 | CAGR (10년) |
| --- | ---: | ---: | ---: | ---: |
| Bull | $1,033 | 1.7x | $1,756 | **14.0%** |
| Base | $787 | 1.4x | $1,102 | **8.8%** |
| Bear | $654 | 1.15x | $752 | **4.74%** |

**확률가중 CAGR** = 0.20×14.0% + 0.55×8.8% + 0.25×4.74% = 2.80% + 4.84% + 1.19% = **8.83%**

**판정** (Phase 10-C trigger):

- Bull CAGR 14.0% ≥ 10% → **매수 가능 후보** ✓
- Bear CAGR 4.74% > 4% → **매수금지 trigger 미발동** (간신히)
- Base CAGR 8.8% < 10% → 합리적 가격, 명백한 alpha 없음

### 10-C. Sum-of-the-Parts (BRK 특화 보조)

**기준일 2025-12-31 기준 ($B)**:

| 자산 | 평가 | 값 ($B) |
| --- | --- | ---: |
| 주식 포트폴리오 (mark-to-market) | 13F 공시 | 274 |
| 캐시 + T-bills | 분기보고서 | 348 |
| 운영 자회사 (BNSF + BHE + Insurance ex-investment + Manufacturing/Service/Retail) | $44.5B Op.Earn − $15B(investment income) = $29.5B × 14x multiple | 413 |
| **소계** | | **1,035** |
| (−) 장기부채 (BHE+BNSF subsidiary debt 등) | balance sheet | −80 |
| (−) Float liability 보수적 차감 (현금가 30% — float은 free leverage이지만 일부 future claim) | $176B × 0.30 | −53 |
| **Net SOTP** | | **$902B** |

| 시나리오 | 운영자회사 multiple | Equity portfolio drift | Cash deploy | Net SOTP ($B) | 시총 대비 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bull | 16x ($472B) | +5% ($288B) | $50B 인수 +$5B earnings (5y) | 1,030 | +1% |
| Base | 14x ($413B) | flat ($274B) | gradual ($20B/yr) | 902 | −12% |
| Bear | 11x ($325B) | −15% ($233B) | not deployed (idle) | 773 | −24% |

¹ Bull Net $1,030B는 구성요소 합에 cash deploy($50B 인수 +$5B earnings)의 가치 반영을 포함한 값.

**SOTP 확률가중**: 0.20×1,030 + 0.55×902 + 0.25×773 = 206 + 496 + 193 = **$895B**
**시총 대비**: $895B / $1,020B = 0.877 → **−12%** (소폭 overvalued, 사실상 fair)

### 10-D. 세 valuation 결과 종합 비교

| Method | Bull | Base | Bear | 확률가중 (vs $473) |
| --- | --- | --- | --- | --- |
| **10-A Operating EPS×PER** | $417 (−12%) | $249 (−47%) | $139 (−71%) | **$255 / −46%** |
| **10-B ROE×PBR (CAGR)** | 14.0% | 8.8% | 4.74% | **8.83%** |
| **10-C SOTP (시총 vs IV)** | +1% | −12% | −24% | **−12%** |

**Divergence 해석** (이게 BRK 분석의 핵심):

1. **10-A vs 10-B 차이**: EPS×PER은 BV에 누적되는 cash·investment portfolio의 future earnings power를 underestimate. ROE×PBR은 누적 효과를 자연 반영. **BRK 구조상 10-B가 더 정확.**
2. **10-B vs 10-C 차이**: 10-B Base 8.8% CAGR은 미래 BV 증식 가정. 10-C SOTP −12%는 현재 자산 가치 stack. 두 결과는 **"현재가 fair, future return은 약간 미달"**로 일관.
3. **10-A의 −46%는 "보수적 신호"로만 사용**: NI 기반 DCF가 BRK의 retain-and-compound 모델에 부적합하므로, 결정 기준에서 weight 낮춤.

**최종 valuation 신호**:

- 정당가 **$880-$920B** 범위 (시총 vs −10~−15% 부근)
- Buffett의 "fair value × margin of safety 0.7" 기준 적용 시 매수가 ~$280-310 (현재 −35~−40%)
- 현재가 $473은 **"Hold or Wait, not Buy"** 구간

### 10-E. Trigger 신호 종합

| Trigger | 결과 |
| --- | --- |
| 10-A Bear PV > 현재가 | NO ($139 < $473) — 극단 저가 신호 아님 |
| 현재가 > 10-A Bull PV | YES ($473 > $417 by 14%) — 약한 과신 경고 |
| 10-B Bull CAGR ≥ 10% | YES (14.0%) — 매수 가능 후보 |
| 10-B Bull CAGR < 4% | NO — 매수금지 trigger **미발동** |
| 10-B Base CAGR > 10% | NO (8.8%) — 명확한 매수 신호 아님 |
| 10-A·10-B divergence | YES, 크다 — Phase 11 재검토 |

---

## 11. Deep 진입 결정 + 확신도 (Phase 11)

| 결과 | **Deep Dive 진입** (조건부) |
| --- | --- |
| 트랙 | Buffett형 |
| 확신도 | 중간 |
| 진입 조건 충족 | 유니버스 PASS ✓ / 2분 명료 ✓ / 정량 5개 PASS 3+ ✓ / Track Gate 확신도 중간 ✓ / 가격 축 장애 없음 (Bull CAGR 14% > 4%) ✓ |
| 진입 망설임 요인 | (a) 현재가가 fair에서 약간 비쌈 (의미있는 MoS 없음) (b) Abel 자본배분 record 부재 |

**대안 결정**: **Parking Lot** — Greg Abel 6-12개월 자본배분 행보 (첫 큰 deal 또는 buyback 재가속) 관찰 후 Deep 본격화. 현재 시점 Deep은 Stream A-D만 (사업·산업·과거 자본배분) 진행하고, Stream E (가격 평가)는 Abel 행보 관찰 후 정밀화.

### 과신 점검 (Overconfidence Check)

> "왜 비싼가? 왜 이 트랙인가?"

**왜 시장이 BRK를 PBR 1.42x에 사는가?**:

- 가설 1: Abel = Buffett continuity (시장의 너그러움)
- 가설 2: $348B 캐시의 optionality value (deployable)
- 가설 3: Float $176B의 "permanent free leverage" 가치 인정
- 가설 4: 집중도 낮은 portfolio가 risk-off 환경에서 quality flight 수혜

→ **가설 1·2는 검증 필요 (Deep)**. 가설 3·4는 합리적이지만 이미 historical avg PBR 1.40에 반영.

**Bear IV $139 < 현재가 / 2.5** → "내가 핵심 리스크를 놓치고 있다"는 아니지만, **EPS×PER 모델 자체가 BRK에 부적합**한 게 더 유력.

**Track Gate 4축**: 4/4 Buffett 명확. Direction 확실. 단 Abel uncertainty가 confidence를 저하.

### 다음 행동

1. **Deep Stream A-D (사업·산업·과거 자본배분)** 진행 가능 — Abel BHE 28년 record 정밀 분석.
2. **Stream E (가격 평가) 보류** — Abel 첫 큰 deal 또는 buyback 활성화 관찰 후 (6-12개월).
3. **Monitoring 트리거**:
   - PBR < 1.25x 재진입 (역사적 buyback zone): Active buy 검토
   - PBR > 1.6x: Sell 검토
   - Abel 첫 $30B+ acquisition: 시나리오 가정 재계산
   - AAPL portfolio 비중 < 15%: portfolio rebalance 분석

---

## 부록. Shallow에서 Deep으로 넘기는 미해결 질문

1. **Greg Abel의 자본배분 ROI** — BHE 28년 ROIC 정밀 계산 (Shallow에서는 미수행)
2. **Float의 "true cost"** — 보험 underwriting cost ratio 추세 (combined ratio 87.1% 지속 가능?)
3. **AAPL 의존도 해소 시나리오** — Buffett가 2024 매도 가속한 의도 (밸류에이션 vs 집중 risk vs 세제)
4. **재단 supply 압박 정밀 모델링** — 매년 $5-7B 매도가 PBR에 미치는 영향 계량화
5. **운영 자회사 multiple 14x 적정성 검증** — Peer 분석 (Diversified Holdings: BAM, Markel, Loews 등) 필요

---

## 변경 이력

- **2026-05-04 v1**: 신규 작성. 사용자 요청에 따라 Phase 1-8 압축 + Phase 9-10에 분석 무게중심. BRK 특수성 반영하여 Phase 10에 10-C SOTP 추가 (playbook 표준 외 보조). Buffett 사임·Greg Abel 승계라는 timing-critical context 반영.
