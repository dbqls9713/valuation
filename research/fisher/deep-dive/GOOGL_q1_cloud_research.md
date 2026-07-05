# GOOGL Q1 — Cloud 지속적 성장 가능성 (조사 raw 데이터 archive)

**연결**: `GOOGL_research_questions.md` Q1 도달한 답·시나리오의 근거 자료
**조사 범위**: 12분기 Alphabet 1차 자료 (2023 Q2 ~ 2026 Q1) + AI Chip 격차 (TPU·Trainium·Maia·NVIDIA)
**조사일**: 2026-05-03

---

## 1. Cloud 매출·마진·Capex·RPO 12분기 추이 (Alphabet 1차 자료)

| 분기 | GCP 매출 (B) | YoY% | Op Margin | Alphabet 분기 Capex (B) | Cloud Backlog/RPO (B) |
| --- | --- | --- | --- | --- | --- |
| 2023 Q2 | 8.0 | +28% | 4.9% | ~6.9 (derived) | (분리 미공시) |
| 2023 Q3 | 8.4 | +22% | 3.2% | 8.06 | 64.9 (Alphabet 전체) |
| 2023 Q4 | 9.2 | +26% | 9.4% | ~11.0 (derived) | (10-K) |
| 2024 Q1 | 9.6 | +28% | 9.4% | 12.0 | (release 미공시) |
| 2024 Q2 | 10.4 | +29% | 11.3% | 13.2 | 78.8 (Alphabet) |
| 2024 Q3 | 11.4 | **+35%** | **17.1%** | 13.06 | 86.8 (Alphabet) |
| 2024 Q4 | 12.0 | +30% | 17.5% | 14.28 | ~93 (FY24 10-K) |
| 2025 Q1 | 12.3 | +28% | 17.8% | ~17.2 | (release 미공시) |
| 2025 Q2 | 13.6 | +32% | 20.7% | 22.4 | **106 (Cloud-only, 신규 공시)** / 108.2 (Alphabet) |
| 2025 Q3 | 15.2 | +34% | 23.7% | 24.0 | **155 (Cloud-only)** |
| 2025 Q4 | 17.7 | **+48%** | **30.0%** | 27.9 | **240 (Cloud-only)** / 242.8 (Alphabet) |
| **2026 Q1** | **20.0** | **+63%** | **32.9%** | **35.7** | **462 (Cloud-only)** / 467.6 (Alphabet) |

### 경영진 멘트 (Pichai · Ashkenazi · Porat 어닝콜 핵심)

- 2024 Q4·2025 Q1: "Capacity constrained" — 수요 > 공급 (GPU·데이터센터·전력 부족). 매출 둔화는 공급 제약 때문, 진짜 수요는 더 큼
- 2025 Q2 이후: "Capacity 점진 확보, 그동안 막혔던 수요가 매출로 전환 중"
- 2026 Q1: Cloud backlog (RPO) **462B**, "**just over 50% converts to revenue within 24 months**" (24개월 내 50% 매출 전환)
- Capex 가이던스: 2026년 180~190B (분기마다 상향), 2027년 "significantly increase"
- Capex 구성: 60% 서버·칩, 40% 데이터센터·네트워크
- 78% Gemini 단가 절감 멘트 (Pichai Q4 2025): "we were able to lower Gemini serving unit costs by 78% over 2025 through model optimizations, efficiency and utilization improvements" — **하드웨어만 아니라 모델 최적화·효율·활용률 종합 (blended)**

---

## 2. AWS·Azure 비교 (12분기)

### 2026 Q1 hyperscaler 스냅샷

| 사업자 | Q1 2026 매출 (B) | YoY% | Op Margin |
| --- | --- | --- | --- |
| AWS | 37.6 | +28% | 37.7% |
| Microsoft Intelligent Cloud | 34.7 | +30% | (Azure 별도 미공시) |
| Microsoft Azure (growth only) | — | +40% | — |
| **GCP** | **20.0** | **+63%** | **32.9%** |

### 12분기 핵심 추이

- 2024 Q3에 GCP (+35%) > AWS (+19%) **첫 성장률 추월**
- 그 후 4분기 연속 GCP 성장률 1위 유지
- GCP가 Azure 추월: 2025 Q4 (+48% vs +39%), 2026 Q1 (+63% vs +40%)
- AWS: 2023 Q2 +12% → 2024 Q1~Q4 +17~19% plateau → 2025 H2 재가속 → 2026 Q1 +28%
- Azure (services growth%): 2023~2024 +29~33% → 2025 H2 +34~40%
- **3대 hyperscaler 모두 2025 H2부터 동시 재가속 = AI 수요가 시장 expansion (zero-sum 점유 재편 아님)**

---

## 3. AI Chip 격차 (TPU·Trainium·Maia vs NVIDIA)

### 핵심 칩 비교 (2026-05-03 기준)

| 칩 | 출시 | Peak FP8 (PF) | HBM (GB) | 대역폭 (TB/s) | 외부 접근 |
| --- | --- | --- | --- | --- | --- |
| NVIDIA H100 | 2022 | ~2 | 80 | 3.35 | 모든 cloud |
| NVIDIA B200 (Blackwell) | 2024-25 | ~4.5 | 192 | 8 | 모든 cloud |
| NVIDIA GB300 NVL72 | 2025 | ~5/chip | 288 | 8 | 모든 cloud |
| **NVIDIA Rubin** | **2027 Q1** | **35 (FP4 train)** | **288 HBM4** | **22** ⭐ | 모든 cloud |
| Google TPU v6e (Trillium) | 2024 | ~0.93 BF16 | 32 | 1.6 | GCP only |
| **Google TPU v7 (Ironwood)** | **2025 Nov** | **4.6** | **192 HBM3e** | **7.37** | **GCP + 일부 neocloud** |
| AWS Trainium2 | 2024 Dec | ~0.65 (est.) | 96 | 2.9 | AWS only |
| **AWS Trainium3** | **2025 Dec** | **2.52** | **144 HBM3e** | **4.9** | **AWS only** |
| Microsoft Maia 100 | 2024 | ~1.6 (est.) | 64 | 1.8 | Azure only |
| **Microsoft Maia 200** | **2026 Q1** | **5 FP8 / 10 FP4** | **216 HBM3e** | (미공시) | **Azure only, inference 위주** |

### 비용·성능 비교 (SemiAnalysis 독립 분석, 2025 Nov)

- **TPU v7 vs GB300 NVL72**: ~50% TCO 우위 (rack-scale, kernel 튜닝 후 effective FLOPs 기준)
- Anthropic 같은 워크로드에서 **per-effective PF ~52% TCO 우위**
- 전력 효율: TPU v7 5.42 TF/W vs GB300 3.57 TF/W (약 1.5배 효율)
- TPU v7 chip 0.85 kW vs GB300 1.4 kW
- **Trainium3 (AWS 마케팅)**: "GPU 대비 50% 저렴" — 벤더 주장, 독립 검증 안 됨, MLPerf 미제출
- **Maia 200**: 가격 데이터 미공시, MLPerf 제출 zero, inference 외 평가 불가

### 외부 채택 (thesis 검증의 핵심 신호)

| 고객 | Primary 칩 | 신호 |
| --- | --- | --- |
| **Anthropic** | Trainium2 >1M + TPU v7 1M + NVIDIA 일부 | 의도적 multi-cloud. AWS·Google 둘 다 "primary" 주장 |
| Apple | Trainium2 (평가) + TPU + NVIDIA | 멀티 |
| Meta | NVIDIA + 자체 MTIA + 2026 TPU rental 협상 보고 | NVIDIA 위주 |
| OpenAI | NVIDIA + Maia 200 (GPT-5.2 inference) | NVIDIA 의존 |
| xAI | NVIDIA only (Colossus 200K H100) | NVIDIA only |
| Cohere · Salesforce · SSI · Safe Superintelligence | TPU | TPU 친화 |
| Databricks · Adobe · Poolside · Qualcomm | Trainium2 | Trainium 친화 |
| **Midjourney** | **TPU (2024) → NVIDIA로 회귀 (2026 V8) ⚠️** | **defection 신호** — TPU software stack 한계 (PyTorch 호환성, iteration 속도) |

### Midjourney defection 분석

- 2024년 Midjourney가 TPU 채택해서 monthly $2M → $700K (65% 절감) 자랑했음
- **2026년 초 V8 모델에서 NVIDIA GPU + PyTorch로 회귀**
- 이유: TPU software stack의 한계 (특히 PyTorch 호환성). 학습·실험 속도가 느림
- **함의**: TPU는 frontier lab에는 좋지만, **빠른 iteration이 필요한 startup·중규모 회사에는 software 비용이 cost 우위를 상쇄**

### 제조 공급 병목 ⭐ (가장 큰 발견)

**TSMC CoWoS 2026 할당** (Morgan Stanley/TrendForce):

| 회사 | 할당 (wafer) | 비중 |
| --- | --- | --- |
| **NVIDIA** | **595K** | **60%** |
| Broadcom (Google TPU + Meta + OpenAI ASIC) | 150K | 15% |
| (그중) Google TPU | ~90K | ~9% |
| 기타 | 250K | 25% |

**충격적 발견**:

- **Google이 NVIDIA에 CoWoS 할당 경쟁에서 밀림**
- **Google 2026년 TPU 생산 목표 4M → 3M units로 25% 감산** (CoWoS 부족)
- HBM3e: SK하이닉스가 NVIDIA에 sold out, 가격 +20% YoY 상승
- **TPU가 더 좋아도 만들 수 없음**. capex 폭증해도 칩 공급이 못 따라옴

### 외부 분석가 5년 전망 (consensus)

| 분석가 | 5년 전망 |
| --- | --- |
| SemiAnalysis (Dylan Patel) | TPU v7 = "900-lb gorilla". cost 우위 진짜. 단 software stack + CoWoS 제약 |
| Stratechery (Ben Thompson) | TPU 우위 진짜 (10년 전부터). 단 TSMC 할당이 binding constraint |
| Counterpoint Research | 2028년까지 custom ASIC > GPU 출하량 |
| **Goldman Sachs** | **TPU 시장점유 35% 도달 가능 (2026 Q4, aggressive case)** |
| NVIDIA bulls (S&P 등) | NVIDIA 80% 점유 2030년까지. CUDA + 6M 개발자 ecosystem |
| Epoch AI | Google = 세계 AI compute 25% 통제 (5M H100-equivalents 중 4M이 TPU) |

---

## 4. 도메인 용어 모음

- **RPO (Remaining Performance Obligations, 잔여수주)** = 계약은 했으나 아직 매출 인식 안 된 잔여 계약 금액. 미래 매출 가시성 지표
- **Op Margin (영업이익률)** = 영업이익 ÷ 매출. Capex는 직접 안 들어가고, 감가상각비로 5~7년에 걸쳐 분산되어 영업비용에 들어감
- **Capacity Constrained** = 수요는 있는데 공급 (데이터센터·GPU·TPU·전력) 부족으로 매출을 더 받고 싶어도 못 받는 상태. 가격 결정력 ↑, 마진 ↑ 환경. 반대로 capacity가 풀리면 가격 결정력 ↓ + 경쟁 심화 → 마진 압박 가능
- **Capex 자본화·감가상각** = 데이터센터·칩 100B 지어도 그 해 100B 비용이 아니라 자산화 후 매년 ~14B 감가상각으로 분산. 2025·2026 capex의 본격 마진 압박은 2027~2028년 발현 가능
- **ASP (Average Selling Price, 평균 단가)** = AI 서비스 (Vertex AI·Gemini API)가 일반 compute보다 ASP 높음 → 마진 확장 동인
- **HBM (High Bandwidth Memory)** = AI 칩에 필수인 초고속 메모리. SK하이닉스·삼성·마이크론 생산. 세대: HBM3 → HBM3e → HBM4
- **CoWoS (Chip-on-Wafer-on-Substrate)** = TSMC의 첨단 칩 패키징 기술. AI 칩의 chip + HBM 결합에 필수. **이게 현재 산업 전체 병목**
- **TF/W (TeraFLOPS per Watt)** = 와트당 연산 능력. 데이터센터 전력 비용 직결
- **ASIC vs GPU** = ASIC은 특정 용도 (TPU·Trainium·Maia)으로 설계된 맞춤 칩, GPU는 범용 (NVIDIA). ASIC은 효율 ↑, GPU는 유연성 ↑
- **TCO (Total Cost of Ownership, 총소유비용)** = 칩 + 전력 + 냉각 + 활용률 종합. 데이터센터 5~7년 운영 기준
- **MLPerf** = AI 칩 표준 벤치마크 (mlcommons.org)
- **CUDA** = NVIDIA의 GPU 소프트웨어 개발 플랫폼. 6M+ 개발자 + 19년 ecosystem = NVIDIA 해자

---

## 5. 1차 자료 출처

### Alphabet

- Q1 2026 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204426000043/googexhibit991q12026.htm>
- Q4 2025 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204426000012/googexhibit991q42025.htm>
- 2025 10-K: <https://s206.q4cdn.com/479360582/files/doc_financials/2025/q4/GOOG-10-K-2025.pdf>
- Q3 2025 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204425000087/googexhibit991q32025.htm>
- Q2 2025 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204425000056/googexhibit991q22025.htm>
- Q4 2024 release: <https://www.sec.gov/Archives/edgar/data/1652044/000165204425000010/googexhibit991q42024.htm>
- (Q1~Q3 2023, Q1~Q4 2024 release·10-Q는 SEC EDGAR 동일 패턴)
- Pichai Q4 2025 78% 멘트: <https://blog.google/company-news/inside-google/message-ceo/alphabet-earnings-q4-2025/>

### AWS

- Q1 2026 release: <https://ir.aboutamazon.com/news-release/news-release-details/2026/Amazon-com-Announces-First-Quarter-Results/default.aspx>
- Q4 2025 10-K release: <https://www.sec.gov/Archives/edgar/data/1018724/000101872426000002/amzn-20251231xex991.htm>

### Microsoft

- FY26 Q3 IC: <https://www.microsoft.com/en-us/Investor/earnings/FY-2026-Q3/intelligent-cloud-performance>

### Chip 분석 (외부)

- TPU v7 Ironwood 공식 docs: <https://docs.cloud.google.com/tpu/docs/tpu7x>
- Google blog Ironwood 발표: <https://blog.google/products/google-cloud/ironwood-tpu-age-of-inference/>
- SemiAnalysis TPU v7 분석: <https://newsletter.semianalysis.com/p/tpuv7-google-takes-a-swing-at-the>
- SemiAnalysis Trainium3 분석: <https://newsletter.semianalysis.com/p/aws-trainium3-deep-dive-a-potential>
- AWS Trn3 GA: <https://aws.amazon.com/about-aws/whats-new/2025/12/amazon-ec2-trn3-ultraservers/>
- Maia 200 발표: <https://blogs.microsoft.com/blog/2026/01/26/maia-200-the-ai-accelerator-built-for-inference/>
- NVIDIA Rubin: <https://www.tomshardware.com/pc-components/gpus/nvidia-confirms-blackwell-ultra-and-vera-rubin-gpus-are-on-track-for-2025-and-2026-post-rubin-gpus-in-the-works>
- Anthropic Google TPU 확장 (2025.10): <https://www.anthropic.com/news/expanding-our-use-of-google-cloud-tpus-and-services>
- Anthropic Google + Broadcom 5GW (2026.04): <https://www.anthropic.com/news/google-broadcom-partnership-compute>
- Anthropic + AWS 5GW: <https://www.anthropic.com/news/anthropic-amazon-compute>
- Stratechery Google·NVIDIA·OpenAI: <https://stratechery.com/2025/google-nvidia-and-openai/>
- Epoch AI chip owners: <https://epoch.ai/blog/introducing-the-ai-chip-owners-explorer>
- TrendForce HBM3e 가격: <https://www.trendforce.com/news/2025/12/24/news-samsung-sk-hynix-reportedly-plan-20-hbm3e-price-hike-for-2026-as-nvidia-h200-asic-demand-rises/>
- Digitimes TSMC CoWoS NVIDIA dominance: <https://www.digitimes.com/news/a20251210PD218/tsmc-cowos-capacity-nvidia-equipment.html>
- 36kr 2026 CoWoS 할당: <https://eu.36kr.com/en/p/3580962946874242>
- MLCommons Training v5.1: <https://mlcommons.org/2025/11/training-v5-1-results/>

---

## 6. 데이터 gap / caveats

- GCP 세그먼트별 RPO는 별도 공시 안 됨 (10-Q는 Alphabet 전체, "primarily Google Cloud" 라벨)
- 2025 Q1 Alphabet capex (~17.2B)는 derived (10-Q 직접 인용 미확인)
- 2024 Q1 GCP OI는 press release 반올림 ($0.9B); precise 10-Q segment table 미인용
- AWS op margin은 OI/Revenue 계산값 (별도 공시 X)
- Microsoft Azure dollar revenue·op margin 미공시 (services growth%만 공시)
- TPU v7 process node N3 추정 (공식 미확인)
- Trainium3 chip-level FP8 PF 일부 (est.)
- TPU 실제 2025 출하량 미공시 (3M-unit 2026 target은 미디어 보고)
- Anthropic 학습/inference TPU·Trainium 정확 비율 미공시 (양 파트너 모두 "primary" 주장)
- TPU v7 vs GB300/B300 독립 MLPerf head-to-head 미제출
- Trainium3 MLPerf 미제출
- Maia 200 독립 벤치마크 zero
- 텍스트 인용된 SemiAnalysis ~52% TCO advantage는 분석가 추정, 감사 안 됨
