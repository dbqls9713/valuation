# Stream 5: 시장 의견 수집 (Stream 4와 병렬)

**입력**: 외부 자료 — 애널리스트 콘센서스, Seeking Alpha, 어닝콜 Q&A, 컨센서스 forecast, 동급 피어 분석
**산출**: Bull case 핵심 논점 ≥ 3개 + Bear case 핵심 논점 ≥ 3개 + 동의/반대 매핑
**선행**: Stream 3 권장 (시나리오 명세 후 비교 가능)
**병렬**: Stream 4와 동시 진행
**작업 시간**: 반나절

## 5-근간 철학

자체 분석만 누적하면 confirmation bias에 빠진다. 시장의 반대 의견을 명시적으로 마주해야 한다.

핵심 원칙 (Buffett DEEP_DIVE Section 4 원전):

- **"반대하는 지점 = 투자 가설의 원천"** — 우리가 동의 안 하는 시장 논점이 있어야, 우리 가설이 시장과 어디서 다른지 명시될 수 있음. 동의만 하는 가설은 시장과 차별화 없음
- **"시장이 틀렸다는 결론이 쉽게 나오면, 내가 모르는 것이 있을 가능성이 높다"** — 시장 가격은 다수 분석가의 합의. 우리만 다른 결론이 나오면 missing variable 의심해야 함
- **확신 표현을 낮추면 결론이 더 강해진다** — "확실히 싸다"보다 "싸 보인다"가 정직하고 신뢰 가능

단 시장 의견을 Stream 4 가치평가의 입력으로 사용하지 않는다 — Stream 5의 역할은 **수집만**. Cross-check (IV vs 시장가 괴리 설명, 우리 가설 누락 점검)는 Stream 6에서 수행.

이 분리의 이유: Stream 4의 IV 산출이 외부 시장 의견에 anchored되는 것을 방지. 우리 가치평가가 self-contained하게 진행되어야 사후 cross-check가 의미 있어진다.

> **Note**: Fisher 본인은 "전문가·증권사 일반 리포트는 정보원으로서 가치 0"이라고 했지만, 이 Stream의 목적은 정보 수집이 아니라 **반대 의견 마주하기 (confirmation bias 제동)**. 출처 신뢰도가 낮아도 "어떤 논거를 펴는지" 자체가 가치.

## 5-뼈대 작업

### 1. Bull case 수집

- 핵심 논점 ≥ 3개
- 출처 ≥ 2 (애널리스트 보고서·SA·어닝콜 buy-side 질문 등)
- 각 논점의 가정·증거 메모

### 2. Bear case 수집

- 핵심 논점 ≥ 3개
- 출처 ≥ 2 (short report·skeptical analyst·SA bear thesis 등)
- 각 논점의 가정·증거 메모

### 3. 동의/반대 매핑

| 논점 | 분류 (Bull/Bear) | 출처 | 우리 판단 (동의/반대/중립) | 근거 1줄 |
| --- | --- | --- | --- | --- |
| | | | | |

## 5-Acceptance Criteria

PR 2에서 보강 예정. 초안:

- [ ] Bull case 핵심 논점 ≥ 3개 (출처 ≥ 2)
- [ ] Bear case 핵심 논점 ≥ 3개 (출처 ≥ 2)
- [ ] 각 논점에 동의/반대/중립 매핑 + 근거 1줄

**PASS**: 3개 모두 충족.

**주의**: Stream 5의 산출물은 Stream 6의 입력이지만, Stream 4의 IV 산출에는 영향 없음 (병렬 진행). 우리 가치평가가 외부 시장 의견에 anchored되는 것을 방지.

## 5-Anchoring 차단

차단 대상 없음 (시장 의견은 외부 자료라 Shallow 산출과 별개).

단 **Stream 5 작업이 Stream 4 작업에 anchored되지 않도록** 주의: Stream 4 진행 중 Stream 5의 시장 의견을 미리 보지 않음. 두 Stream의 산출이 각자 self-contained하게 끝난 후 Stream 6에서 비교.

## 5-트랙별 분기

| 단계 | Buffett 트랙 출처 | Fisher 트랙 출처 |
| --- | --- | --- |
| Bull case | 애널리스트 콘센서스, sell-side strong buy 보고서 | 동급 풀스택 피어, 산업 thought leader |
| Bear case | sell-side downgrade, short report (Citron 등) | skeptical 동종 분석가, 대체 기술 옹호자 |

수집 출처만 트랙별로 다르고, 산출 형식·매핑 절차는 공통.

## 5-회고 블록

PR 2에서 표준화.

---

## 원천 마이그레이션

- **Buffett 트랙**: `playbook/buffett/DEEP_DIVE.md` Section 4 (시장 내러티브 vs 우리 분석) — 단 **수집 부분만**. Cross-check는 Stream 6으로 분리
- **Fisher 트랙**: **신설** (기존 Fisher DEEP_DIVE에 시장 내러티브 단계 부재 — confirmation bias 결손 보강)
- **이전과의 차이**: Buffett Section 4가 "수집 + 동의/반대 + cross-check"의 통합이었으나, **수집 = Stream 5, cross-check = Stream 6**으로 분리. Stream 4와의 병렬화로 anchoring 방지 강화
