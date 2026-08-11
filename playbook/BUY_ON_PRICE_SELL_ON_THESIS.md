# Buy on Price, Sell on Thesis — 매수·매도 전략 Framework

**작성일**: 2026-05-03 (v2: 2026-05-03 — 안전마진 정의 명확화 + 경주산 소시지 + Wonderful 정량 정의 + 확증편향 추가 / **v3: 2026-08-11 — 래칫 룰 + 비중 상한 2단계(소프트·하드) + 자금 경쟁 우선순위 + 점검 주기**)
**목적**: 모든 종목에 적용 가능한 일반 매수·매도 의사결정 framework. 8-Stream playbook (`DEEP_DIVE.md`, `SHALLOW_DIVE.md`)의 산출물을 어떻게 매매 결정으로 연결할지 정의
**근간**: Buffett (Wide moat + 안전마진), Munger (ROIC 사후수익률 결정), Druckenmiller (사업 ≠ 가격), Fisher (15 정성 + 장기 hold), Graham (Mr. Market is moody)

---

## 0. 핵심 원칙 5개

### 1. 사업 Quality First, Price Second

- **Wide moat → Narrow → None** 분류가 모든 결정의 base
- Wonderful + Excellent만 매수 후보 (Good 이하 매수 X)
- Wide moat만 long-term hold + DCA 가능

### 2. Buy on Price, Sell on Thesis

> 매수 trigger = 가격 (IV 대비 안전마진 도달 시)
> 매도 trigger = thesis-breaker (해자·사업 본질 깨질 때)

- **가격만으로 매도 X** (Stream 4 playbook 명시)
- Buffett: "Our favorite holding period is forever"

### 3. 확률가중 IV + 안전마진

**안전마진 정확한 정의** (시장가와 무관):

```text
안전마진 = (본인 IV - 매수가) / 본인 IV
매수가 trigger = 본인 IV × (1 - 안전마진)
```

**확률가중 IV (E[IV])**:

```text
E[IV] = P_bull × IV_bull + P_base × IV_base + P_bear × IV_bear
```

**핵심**: Bull/Base/Bear **확률 weight 자체가 본인 IV 가정의 불확실성**을 capture. **conviction별 별도 안전마진 차등 안 함** (double-counting 회피).

**안전마진 size — 종목 Quality + 해자 단계 기반**:

| 종목 등급 + 해자 | 안전마진 |
| --- | --- |
| Wonderful + Established | **15-20%** |
| Wonderful + Emergent | 25-35% |
| Excellent + Established | **25-30%** |
| Excellent + Emergent | 35-45% |
| Good 이하 | 매수 X |

### 4. 포지션 비중 분산 (Risk Management)

비중 상한은 **두 가지 다른 일**을 한다. 하나의 숫자로 묶으면 모순이 나므로 2단계로 나눈다.

| Conviction | 소프트 상한 (초과 시 **신규 매수 중단**) | 하드 상한 (초과 시 **강제 trim**) |
| --- | --- | --- |
| High (deep dive 통과 + Wide moat) | **50%** | **65%** |
| Medium (shallow dive 통과) | 15% | 25% |
| Low (관심 watch) | 5% | 10% |

- **소프트 초과**: 더 사지 않는다. **팔지는 않는다.** 가격 상승으로 인한 비중 증가(drift)를 매도로 되돌리면 그건 §7-1 "가격으로 매도"에 해당한다.
- **하드 초과**: 소프트 상한까지 축소. 사업과 무관한 순수 risk management.

> **2026-08-11 개정.** 종전 High 25-30% 단일 상한은 실무에서 지켜지지 않았고(본인 ADBE 60%), 초과분을 강제 매도하라는 규칙은 "안 판다"는 결론과 정면 충돌했다. 집중 투자를 택한 이상 상한을 현실화하되, 감시 의무(§6 "점검 주기")를 대가로 붙인다.

**자금 경쟁 시 우선순위** — 현금은 유한하고 기존 종목 추매와 신규 종목 매수가 경쟁한다.

1. **안전마진이 큰 쪽을 먼저 산다.** 프레임워크가 모든 가격을 IV 대비 %로 재므로 비교 가능하다.
2. **신규 종목은 deep dive 완료 전까지 Medium 소프트 상한(15%)을 넘지 않는다.** 확신이 검증되기 전에 자금을 몰지 않기 위함.

### 5. 확증편향 점검 (Confirmation Bias) ⭐

> "내가 이 종목을 사고 싶어서 근거 찾고 있는가?"

본인 thesis 지지 정보만 찾고 반대 정보 무시하는 패턴. 모든 단계에서 self-check 필수.

**실용 룰**:

- Deep Dive 종결 시 "내 thesis가 틀린다면 무엇이 바뀌어야 하나?" 명시 (Stream 3 과신 점검)
- Stream 4-D 과신 점검 ("Bear가 과대평가되었다면") 반드시 수행
- Bear 확률 weight ≥ 25% (default 30%) — 0%로 잡으면 confirmation bias
- "사고 싶어서 근거 찾고 있다" self-realization 시 → 매수 보류, framework 처음부터 재평가

---

## 1. Pre-Stage: 경주산 소시지 Screening (정성 6개)

| 약자 | 의미 | 8-Stream 매핑 | 점검 항목 |
| --- | --- | --- | --- |
| **경** | 경제적 해자 (Moat) | Stream 1-B | Buffett 5분류 중 작동하는 해자, Wide/Narrow 등급 |
| **주** | 주주 친화성 | Stream 2-5 | 자사주매입·배당·M&A 자본배분 track record |
| **산** | 산업 성장성 (TAM CAGR 7%+) | Stream 1-A | 향후 5-10년 시장 성장률, 산업 성숙도 |
| **소** | 소비자 만족도 + 브랜드 파워 | Stream 1-A (Brand 해자) | NPS, 재구매율, 브랜드 재인 |
| **C** | CEO 역량 | Stream 2-0·2-bis | 재임 5년+, 가이던스 정확성, capital allocation 능력 |
| **G** | 직원 만족도 | Stream 2-ter | Glassdoor 4.0+, 이직률, 채용 매력도 |

**판정**:

- **6개 모두 PASS** → Deep Dive 진입 가치 ★
- 5개 PASS + 1개 PRELIM → Shallow Dive 후 Deep
- 4개 이하 PASS → **매수 안 함**

---

## 2. Stage 1: 종목 Quality 분류

### 정량 기준 (5년 평균)

| 등급 | ROIC | Op Margin | 재투자 효율 | TAM CAGR | Moat |
| --- | --- | --- | --- | --- | --- |
| **Wonderful** | **20%+** | 25%+ | 매출 성장 ≥ ROIC × Retention | 7%+ | Wide (5분류 중 4-5개) |
| **Excellent** | 15-20% | 18-25% | 매출 성장 ≈ ROIC × Retention | 5-7% | Wide (3-4개) |
| Good | 10-15% | 12-18% | 매출 성장 < ROIC × Retention | 3-5% | Narrow |
| Average 이하 | < 10% | < 12% | 정체 | < 3% | None |

### 정성 기준 (필수 충족 항목)

**Wonderful** 5개 중 4-5개 충족:

1. **Pricing power** — COGS 인플레 통과 가능 (예: KO 매년 +5% 가격)
2. **Long runway** — 시장 TAM 5-10년+ 성장 가능
3. **Network effect or Switching cost** — 사용자 lock-in 강함
4. **Capital-light or capex 효율** — KO style (1% capex/매출) OR ROIC 25%+ 유지하며 capex 가능
5. **Predictability** — 5년 후 사업 모델 거의 동일

**Excellent**: 3-4개
**Good**: 1-2개 (본인 투자 대상 X)

### 본인 투자 기준

- **Wonderful + Excellent만 매수 후보**
- **Good 이하 매수 안 함**

### Wonderful 진짜 예시

- **Visa·Mastercard**: ROIC 30%+ × Network Effect × 가격 결정력
- **Costco**: ROIC 20%+ × Membership lock-in × 가격 우위
- **Microsoft (post-Nadella)**: ROIC 25%+ × Switching cost × Cloud 가속
- **Adobe**: ROIC 25%+ × Switching cost × subscription model
- **ASML**: ROIC 20%+ × 기술 독점 (EUV 리소그래피)

---

## 3. Stage 2: 해자 단계 분류 (Established vs Emergent)

### 정의

| 단계 | 정의 | 예시 | 안전마진 추가 |
| --- | --- | --- | --- |
| **Established** | 이미 Wide moat 형성, 5-10년+ 지속 입증 | KO·V·MSFT (post-2010)·AAPL (post-2010)·검색 (GOOGL) | **+0%** (기준) |
| **Emergent** | 해자 형성 중, 미실현 | TI 1950s·MOTO 1960s·초기 NVIDIA·Cloud (GOOGL) | **+10-15%pt** |

### Buffett vs Fisher Dichotomy

- **Buffett**: Established 위주 (See's, KO, AXP) — predictable, lower risk
- **Fisher**: Emergent 가능 (TI, MOTO 1950s) — higher growth, higher risk

### Hybrid 종목 처리

본인 GOOGL = Established 본업 (검색 51%) + Emergent (Cloud 19%)
→ 매출 비중 가중평균. Established 비중 큼 → Established 안전마진 (+0%)

---

## 4. Stage 3: 8-Stream Deep Dive (Conviction Build)

기존 `playbook/DEEP_DIVE.md` 참조. 핵심 단계:

1. **Stream 1-A·1-B**: 사업·해자 (경주산 소시지의 경·산·소 검증)
2. **Stream 2**: 경영진 (C·G 검증)
3. **Stream 3**: 리스크·기회 + 4-Period 시나리오 명세 + Driver Tree
4. **Stream 4**: IV 산출 + Reverse 역산 + 과신 점검 + OR 결합 판정
5. **Stream 5**: 시장 의견 비교 (Stream 6-A 가설 누락 점검)
6. **Stream 6**: 편입 결정 (4갈래)
7. **Stream 7**: 포지션 사이징
8. **Stream 8**: 모니터링 (thesis-breaker 추적)

---

## 5. Stage 4: IV 산출 + 안전마진

### IV 산출 도구 (종목 특성별)

| 종목 유형 | 1순위 도구 | 2순위 |
| --- | --- | --- |
| 소비재·소프트웨어·플랫폼 (안정 EPS) | EPS × PER | DCF (OE) |
| 자본집약 + 자사주 매입 적은 | DCF (OE) | EPS × PER |
| 자사주 매입 heavy (5y -5%+) | DCF (OE/Share) | EPS × PER 참고 |
| 금융주 (은행·보험) | ROE × PBR | DCF |
| 부동산·REIT | NAV (자산 가치) | FFO/AFFO |
| 자본집약 산업 (반도체·중공업) | EV/EBITDA | ROIC × EV/IC |
| 무수익·재투자 heavy (고성장 SaaS) | P/S 또는 Rule of 40 | DCF (미래 OE) |
| 사이클러컬 (에너지·철강) | EV/EBITDA + Mid-cycle EPS | DCF |

→ **두 도구 cross-check 필수** (한 도구 약점을 다른 도구가 제동)

### 시나리오 + 확률 Weight

Stream 3 4-Period 매트릭스 (단기·중기·장기·terminal) 적용:

- Bull / Base / Bear 시나리오
- 확률 weight default: **Bull 30 / Base 40 / Bear 30**
- 본인 conviction 강하면 Bull 35-40, Bear 25-30
- 본인 conviction 약하면 Bull 20-25, Bear 35-40
- **Bear ≥ 25% 필수** (확증편향 방지)

### Reverse 역산 (시장 implicit 가정 추론)

현재가 → 정당화 가정 추론 → 본인 가정 비교:

- 시장 < 본인 IV → **Time Horizon Arbitrage sweet spot** (공격 매수)
- 시장 ≈ 본인 IV → 대기 또는 partial DCA
- 시장 > 본인 IV → **대기**, 매수 안 함

---

## 6. Stage 5: Tier 분산 매수

### 5-Tier 결정 Matrix

| 액션 | 가격 (IV 대비) | Fundamentals | Reserve 행동 |
| --- | --- | --- | --- |
| **강력매수** | < Bear IV (또는 E[IV] × 50% 이하) | Wide moat 살아있음, thesis-breaker X | Reserve **50%+** |
| **매수 (강)** | E[IV] × 0.5 ~ 0.7 (안전마진 30-50%) | 강함 | Reserve **30-40%** |
| **매수 (보통)** | E[IV] × 0.7 ~ 0.9 (안전마진 10-30%) | 강함 | Reserve **20-30%** |
| **대기** | E[IV] × 0.9 ~ Bull IV | 강함 | 보유 유지, 적립 X |
| **일부매도 (Trim)** | > Bull IV + 비중 과대 (limit 초과) **OR** thesis 부분 약화 | 일부 약화 | trim 5-10%pt |
| **전량매도** | (가격 무관) | thesis-breaker 본격 | 전량 매도 후 재평가 |

### 각 Tier 풀어 설명

#### 강력매수 — "Panic 추매" zone

- 시장 panic, multiple compression -50%+
- 가격이 Bear IV 근접
- fundamentals 살아있어야 함 (필수)
- 닷컴급 (MSFT -66% in 2002, AAPL -82%)
- 발생 빈도: 10년에 1-2번

#### 매수 (강) — "Druckenmiller panic 추매"

- Multiple compression 본격
- Bear scenario 부분 발현
- "Be greedy when fearful" (Buffett)

#### 매수 (보통) — "현실 trigger"

- Multiple compression 시작
- 시장 sentiment 약화
- 가장 자주 도달 (3-5년 1번)

#### 대기 — "회색"

- 가격이 E[IV] 근처 또는 위
- 사업 강함
- 신규 매수 기회 X, 매도도 X
- 적립 멈춤 또는 감액

#### 일부매도 (Trim) — 신중

3가지 trigger 중 하나라도:

| Trigger | Trim |
| --- | --- |
| (a) 가격 > Bull IV 큰 폭 | 5-10%pt |
| (b) 비중 limit 초과 | limit으로 축소 |
| (c) thesis 부분 약화 | 5-10%pt |

**가격만으로 trim 안 함**. 비중 또는 thesis 신호 필요.

#### 전량매도 — 매우 신중

조건 (모두 충족):

- thesis-breaker 본격 발현 (단순 신호 X)
- 해자 (Wide → Narrow) 명확히 약화
- 사업 본질 변화 (paradigm shift)

Buffett·Druckenmiller도 거의 안 함. 1년에 1-2번 미만.

### 래칫 룰 (Ratchet) — 추격매수 차단 ⭐

Tier 밴드는 "얼마에 사는가"만 정할 뿐, **같은 밴드 안에서 가격이 오를 때 또 사는 것**을 막지 못한다. 실제 실패는 대부분 거기서 난다.

> **직전 매수가보다 비싸면 추가 매수하지 않는다.**
>
> **예외:** 직전 매수 이후 **분기 실적이 새로 발표되었고, 그것으로 IV를 재산출**했을 때만.

- 예외 조건은 반드시 **외부 사건 + 문서화된 노동**이어야 한다. "아직 저렴하다", "더 오르기 전에" 같은 자기 진술은 필터가 아니다 — 언제든 지어낼 수 있기 때문.
- 실적은 분기 1회이므로, **매수 창은 분기당 최대 1번 열린다.**
- 실효 매수 트리거는 세 조건의 **교집합**이다: 안전마진 밴드 ∩ 소프트 상한 여력 ∩ 래칫선. 가장 빡센 것이 지배한다.

> **도입 근거 (2026-08-11).** 본인 ADBE 실측: 4/13 $225 매수 → 3일 뒤 4/16 $247(+10%)에 더 큰 금액 매수. 그 사이 새 정보는 없었다. 실탄 52%가 $225~247에 소진되어 6월 $198~209 구간에서 6주밖에 사지 못했다. 4/16 건($2,965)을 6/18 $198에 썼다면 평단 $224 → $210, 수익률 +21.9% → +30.0%.
>
> 실제 이력 백테스트에서 이 룰의 비용은 **-$60 (-0.5%)로 사실상 0**이었다. 반면 밴드별 예산제·코어위성 등 더 복잡한 규칙은 모두 실제보다 나빴다. **규칙은 늘릴수록 나빠진다 — 이 한 줄만 남긴다.**

### 점검 주기

| 구분 | 주기 | 내용 |
| --- | --- | --- |
| **정기** | 분기 실적 후 (연 4회) | 모니터링 문서 갱신 + IV 재산출. 래칫 예외가 열리는 유일한 시점 |
| **비정기** | 가격 알림만 | ① 래칫선 하향 돌파 → 매수 검토 ② Bull IV 상향 돌파 → 매도 검토 |

**평소에는 보지 않는다.** 매일 보는 것의 위험은 계산 착오가 아니라 **행동 유발**이다. 화면을 자주 볼수록 손이 나가고, 결국 래칫의 예외를 찾게 된다. §7-8(Over-Trading)과 같은 취지.

---

## 7. Common Pitfalls (피해야 할 패턴)

### 7-1. 가격으로 매도

- 가격만 빠진다고 매도 → 본인 thesis 무시
- "오를 때 매수, 떨어질 때 매도" = 손실 패턴
- **정답**: thesis-breaker 발현 시만 매도

### 7-2. Anchoring (정착)

- 본인 평균 단가에 anchor (예: "$300에 샀으니까 $300 이하면 매수")
- **정답**: 평균 단가 무관, **IV 대비 가격**으로 판단

### 7-3. Sunk Cost Fallacy

- "이미 많이 떨어졌으니까 안 팔아"
- **정답**: thesis 검증 → 깨졌으면 매도, 살아있으면 추매

### 7-4. Overconfidence (과신)

- Bull case만 보고 매수
- **정답**: Bear scenario 25%+ weight 필수, Stream 4-D 과신 점검

### 7-5. **Confirmation Bias (확증편향)** ⭐

- 본인이 사고 싶은 종목 근거만 찾기
- Bear scenario·반대 의견 무시
- **정답**: 모든 단계에서 self-check, "이 종목을 사고 싶어서 분석하나?" 자문

### 7-6. Position Concentration

- 한 종목에 50%+ 비중
- **정답**: max 25-30% (high conviction even)

### 7-7. Reserve 부족

- 평소 cash 0% → panic 시 매수 못 함
- **정답**: reserve 20-30% 항상 보존

### 7-8. Over-Trading

- 매분기 매매
- **정답**: trigger 도달 시만 매수, thesis-breaker 시만 매도. 1년에 거래 5-10번 미만
- 매일 시세를 보는 것 자체가 원인. §6 "점검 주기" — 정기는 분기 1회, 그 외엔 가격 알림만

---

## 8. Decision Flowchart — 5-Stage 매수 Pipeline

```text
Stage 0: 경주산 소시지 6개 PASS?
        └─ No → 매수 X
        └─ Yes (5+ PASS) ↓

Stage 1: ROIC 15%+ × Wide moat?
        └─ No (Good 이하) → 매수 X
        └─ Yes ↓

Stage 1b: Wonderful or Excellent?
         └─ Wonderful → 안전마진 15-20%
         └─ Excellent → 안전마진 25-30%

Stage 2: Established or Emergent?
        └─ Established → 안전마진 그대로
        └─ Emergent → +10-15%pt 추가

Stage 3: 8-Stream Deep Dive 통과 + 확증편향 점검?
        └─ No → 매수 X 또는 watch only
        └─ Yes ↓

Stage 4: IV 산출 (시나리오 + 확률 weight)
        └─ E[IV] = Σ P × IV
        └─ Bear weight ≥ 25%

Stage 5: 매수가 Trigger 도달 (E[IV] × (1-안전마진))?
        └─ No → 대기 (Tier 1·2·3 분산 alert)
        └─ Yes ↓

Stage 5b: 소프트 상한 미달?
         └─ No (초과) → 신규 매수 중단 (매도는 X)
         └─ Yes ↓

Stage 5c: 래칫 통과? (직전 매수가보다 싼가?)
         └─ No → 매수 X
              └─ 단, 직전 매수 후 분기 실적 발표 + IV 재산출 완료 → 통과
         └─ Yes → 매수 (Tier 분산)
```

### 매도 결정 (별도 — 가격 무관)

```text
1. Thesis-breaker 본격 발현?
   └─ Yes → 전량 매도 재평가
   └─ No ↓

2. 비중 limit 초과?
   └─ Yes → trim
   └─ No ↓

3. 가격 > Bull IV 큰 폭 + 다른 더 좋은 기회?
   └─ Yes → trim 5-10%pt
   └─ No ↓

4. 보유 유지
```

---

## 9. 8-Stream Playbook과의 연결

| Stream | Framework 적용 |
| --- | --- |
| Stream 1-A·1-B (사업·해자) | Wonderful/Excellent/Good 분류 + Established/Emergent |
| Stream 2 (경영진) | 경주산 소시지 C·G 검증 + Conviction 등급 |
| Stream 3 (리스크·기회) | thesis-breaker 정의 + 4-Period 시나리오 + Driver Tree |
| Stream 4 (가격 평가) | E[IV] 산출 + 안전마진 + Reverse 역산 + 과신 점검 |
| Stream 5 (시장 의견) | Reverse 역산 비교 (시장 vs 본인) + 가설 누락 점검 |
| Stream 6 (편입 결정) | 5-Tier 결정 + 비중 limit |
| Stream 7 (사이징) | 비중 limit + 분산 매수 (Tier) |
| Stream 8 (모니터링) | thesis-breaker 추적 (매도 trigger) |

---

## 10. 도메인 용어

- **IV (Intrinsic Value, 내재가치)**: 사업 fundamentals 기반 적정 가치
- **E[IV] (Expected IV, 기대 내재가치)**: 확률가중 IV = Σ (P × IV)
- **Margin of Safety (안전마진)**: (IV - 매수가) / IV. **시장가 무관, IV 대비 할인율**
- **Wonderful·Excellent·Good 등급**: ROIC + Moat + 정성 기준 분류
- **Established vs Emergent**: 해자 형성 단계
- **Wide/Narrow/None Moat**: Morningstar/Pat Dorsey 분류 (해자 강도)
- **Reverse 역산**: 현재가 → 정당화 가정 추론
- **Time Horizon Arbitrage**: 시장보다 longer horizon → alpha 출처
- **Multiple Compression**: P/E·P/S 등 valuation multiple 축소
- **DCA (Dollar Cost Averaging)**: 정기 적립 매수 (평균 단가)
- **Drawdown**: 고점 대비 하락폭
- **Concentration Risk**: 단일 종목 비중 위험
- **Thesis-breaker**: 사업 본질 깨지는 시나리오 (매도 trigger)
- **래칫 (Ratchet)**: 직전 매수가보다 비싸면 추가 매수 금지. 톱니바퀴가 한 방향으로만 돌듯 매수 기준가가 내려가기만 하는 구조
- **소프트 상한 / 하드 상한**: 소프트 = 초과 시 신규 매수만 중단. 하드 = 초과 시 강제 trim
- **Drift**: 매매 없이 가격 변동만으로 비중이 바뀌는 것. Drift로 인한 소프트 초과는 매도 사유가 아님
- **Confirmation Bias (확증편향)**: 본인 thesis 지지 정보만 찾는 편향
- **NOPAT (Net Operating Profit After Tax)**: 영업이익 × (1 - 세율)
- **OE (Owner Earnings, Buffett)**: NI + D&A − Maintenance Capex (또는 OCF − SBC − Capex)
- **TAM (Total Addressable Market)**: 잠재 시장 규모
- **Pricing Power**: 매년 가격 인상 가능한 능력
- **NPS (Net Promoter Score)**: 고객 만족도 지표

---

## 11. 마이그레이션 출처

- Buffett 트랙: `playbook/buffett/DEEP_DIVE.md` Section 6 (가설 + 시나리오 valuation)
- Fisher 트랙: `playbook/fisher/DEEP_DIVE.md` Stream C (10년 기대수익률)
- Munger ROIC 멘트 출처: Berkshire 1996 연례총회 + Poor Charlie's Almanack
- Druckenmiller 패턴: 본인 GOOGL Q3 v2 직관 (Stream 3 회고)
- 경주산 소시지: 한국 가치투자 커뮤니티 framework (출처 미상, 본인 도입)
- Stream 4 통합: `playbook/deep-dive/04_valuation.md`
- Stream 3 시나리오 명세: `playbook/deep-dive/03_risk_opportunity.md`
- 본인 학습 케이스: `research/fisher/deep-dive/GOOGL_alphabet.md` Stream 3·4 (2026-05-03)

### 비공개 / 출처 불확실 항목

다음은 framework에서 specific %로 표기했지만 출처 미공개 — 본인 conviction 기반 사용:

- Munger BYD·Costco 안전마진 % — Berkshire 13F만 공개, 정확한 IV·안전마진 비공개
- Wonderful 안전마진 15-20% — Munger·Fisher 정신 일반화, 정확한 % 본인 판단
- Bear weight 25% 최소 — 확증편향 방지 룰, 본인 framework 결정
