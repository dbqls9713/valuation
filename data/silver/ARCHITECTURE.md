# Architecture Documentation

## 설계 원칙

### 1. Single Responsibility Principle
각 클래스와 모듈은 하나의 명확한 책임을 가짐:
- `Pipeline`: ETL 흐름 관리
- `Extractor`: 데이터 추출
- `Transformer`: 데이터 변환
- `Validator`: 데이터 검증
- `Writer`: 데이터 저장

### 2. Open/Closed Principle
- 확장에는 열려있고 수정에는 닫혀있음
- 새로운 소스 추가 시 기존 코드 수정 불필요
- `Pipeline` 추상 클래스 상속으로 구현

### 3. Dependency Inversion
- 추상화에 의존, 구현에 의존하지 않음
- `Pipeline`, `Validator` 등은 인터페이스
- 구체적 구현은 `sources/` 하위에 위치

### 4. Separation of Concerns
- 비즈니스 로직과 인프라 분리
- 데이터 변환 로직과 I/O 분리
- 설정과 코드 분리

## 클래스 다이어그램

```
┌─────────────────────┐
│     Pipeline        │
│   (Abstract Base)   │
├─────────────────────┤
│ + extract()         │
│ + transform()       │
│ + validate()        │
│ + load()            │
│ + run()             │
└──────────┬──────────┘
           │
           ├─────────────────────┐
           │                     │
  ┌────────▼─────────┐  ┌───────▼──────────┐
  │  SECPipeline     │  │ StooqPipeline    │
  ├──────────────────┤  ├──────────────────┤
  │ - extractor      │  │ - validator      │
  │ - transformer    │  │ - writer         │
  │ - metrics_builder│  │                  │
  │ - validator      │  │                  │
  │ - writer         │  │                  │
  └──────────────────┘  └──────────────────┘
```

## 데이터 흐름

```
Bronze Layer (Raw)
      │
      ▼
  Extractor ──────────────> DataFrame
      │                         │
      ▼                         │
  Transformer ◄─────────────────┘
      │
      ▼
  Validator
      │
      ▼
  Writer
      │
      ▼
Silver Layer (Parquet + Metadata)
```

## 디렉토리 구조 설명

### `core/`
재사용 가능한 추상 클래스들:
- **pipeline.py**: ETL Pipeline 기본 클래스
- **dataset.py**: 스키마 검증 기능
- **validator.py**: Validator 인터페이스

### `config/`
설정과 스키마 정의:
- **metric_specs.py**: SEC 메트릭 정의
- **schemas.py**: 데이터셋 스키마 (타입, nullable, PK 등)

### `shared/`
여러 소스에서 공통으로 사용하는 유틸리티:
- **transforms.py**: TTM 계산, Fiscal Year 계산 등
- **io.py**: Parquet 읽기/쓰기, 메타데이터 관리
- **validators.py**: 공통 검증 로직

### `sources/`
데이터 소스별 구현:
- **sec/**: SEC 데이터 처리
  - `extractors.py`: companyfacts JSON → DataFrame
  - `transforms.py`: Dedup, YTD→Quarterly, Metrics builder
  - `pipeline.py`: SEC ETL 파이프라인
- **stooq/**: Stooq 가격 데이터 처리
  - `pipeline.py`: Stooq ETL 파이프라인

## 확장 가이드

### 새로운 데이터 소스 추가

1. **디렉토리 생성**
```bash
mkdir -p data/new_silver/sources/newsource
```

2. **Pipeline 구현**
```python
# sources/newsource/pipeline.py
from data.silver.core.pipeline import Pipeline

class NewSourcePipeline(Pipeline):
    def extract(self):
        # Bronze 데이터 로드
        pass

    def transform(self):
        # 데이터 변환
        pass

    def validate(self):
        # 검증
        pass

    def load(self):
        # Parquet 저장
        pass
```

3. **build.py에 등록**
```python
if sources is None or 'newsource' in sources:
    pipelines['newsource'] = NewSourcePipeline(context)
```

### 새로운 메트릭 추가

```python
# config/metric_specs.py
METRIC_SPECS = {
    'NEW_METRIC': {
        'namespace': 'us-gaap',
        'tags': ['TagName1', 'TagName2'],
        'unit': 'USD',
        'is_ytd': True,  # YTD 값인지
        'abs': False,    # 절대값 취할지
    },
}
```

### 커스텀 Transformer 추가

```python
# sources/sec/transforms.py
class CustomTransformer:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # 변환 로직
        return df
```

## 테스트 전략

### Unit Tests
각 컴포넌트 독립 테스트:
```python
def test_extractor():
    extractor = SECCompanyFactsExtractor()
    df = extractor.extract_companies(path, submissions)
    assert len(df) > 0
    assert 'ticker' in df.columns
```

### Integration Tests
전체 파이프라인 테스트:
```python
def test_sec_pipeline():
    context = PipelineContext(bronze_dir, silver_dir)
    pipeline = SECPipeline(context)
    result = pipeline.run()
    assert result.success
    assert 'companies' in result.datasets
```

### Data Quality Tests
출력 데이터 검증:
```python
def test_data_quality():
    df = pd.read_parquet('output.parquet')
    assert df['filed'].ge(df['end']).all()
    assert not df.duplicated(subset=primary_key).any()
```

## 성능 최적화

### 병렬 처리
```python
from concurrent.futures import ProcessPoolExecutor

def process_company(cik_file):
    return extractor.extract_facts(cik_file)

with ProcessPoolExecutor() as executor:
    results = list(executor.map(process_company, cik_files))
```

### 증분 빌드
```python
def get_modified_files(bronze_dir, last_build_time):
    return [f for f in bronze_dir.glob('*.json')
            if f.stat().st_mtime > last_build_time]
```

### 메모리 최적화
```python
# 청크 단위 처리
for chunk in pd.read_csv(file, chunksize=10000):
    process(chunk)
```

## 모니터링

### 로깅 레벨
```python
import logging

# DEBUG: 상세 디버깅 정보
logger.debug('Processing file: %s', filename)

# INFO: 일반 진행 상황
logger.info('✓ Pipeline completed')

# WARNING: 주의 필요
logger.warning('Missing data for %s', ticker)

# ERROR: 에러 발생
logger.error('Failed to process: %s', error)
```

### 메트릭 수집
```python
metrics = {
    'records_processed': len(df),
    'errors': len(errors),
    'duration_seconds': elapsed,
    'memory_mb': memory_usage,
}
```

## 에러 핸들링

### 파일 레벨 에러 격리
```python
for file in files:
    try:
        process(file)
    except Exception as e:
        errors.append(f'{file}: {e}')
        continue  # 다음 파일 계속 처리
```

### 검증 에러 수집
```python
def validate(self, df):
    errors = []
    if condition1:
        errors.append('Error 1')
    if condition2:
        errors.append('Error 2')
    return ValidationResult(is_valid=len(errors)==0, errors=errors)
```

## 베스트 프랙티스

1. **명확한 네이밍**: 클래스/함수 이름으로 역할 명확히
2. **작은 함수**: 각 함수는 하나의 일만 수행
3. **타입 힌트**: 모든 함수에 타입 힌트 추가
4. **문서화**: Docstring으로 의도 명확히
5. **에러 핸들링**: 예상 가능한 에러 모두 처리
6. **로깅**: 중요한 단계마다 로깅
7. **테스트**: 모든 public 함수 테스트 작성

## 검증 (Validation)

### validate.py
New Silver 레이어 출력을 검증하는 스크립트:

```bash
# 기본 실행
python -m data.silver.validate

# 커스텀 디렉토리
python -m data.silver.validate --silver-dir data/silver_out

# 수동 fixture 포함
python -m data.silver.validate --with-manual
```

### 검증 항목

1. **Schema & Types**: 필수 컬럼 존재 여부
2. **Key Uniqueness**: Primary key 중복 검사
3. **filed >= end**: 파일링 날짜가 기간 종료일보다 이후인지 확인
4. **YTD Identity**: YTD 값이 분기별 값의 누적과 일치하는지 확인
5. **TTM Correctness**: TTM(Trailing 12 Months)이 4분기 합계와 일치하는지
6. **CAPEX Convention**: CAPEX 값이 양수(abs)인지 확인
7. **Manual Fixture** (optional): 수동으로 검증한 fixture 데이터와 비교

### 검증 결과 예시

```
======================================================================
=== New Silver Validation Summary ===
======================================================================
✓ OK   facts_unique_period
✓ OK   metrics_unique_period
✓ OK   facts_filed_ge_end
✓ OK   metrics_filed_ge_end
✗ FAIL ytd_identity
       16/571 rows fail YTD identity (tol=1e-06)
✓ OK   ttm_check
✓ OK   capex_abs
✓ OK   prices_unique_symbol_date
✓ OK   prices_positive_close
======================================================================
Results: 8/9 checks passed
======================================================================
```

### 검증 로직 커스터마이징

```python
# 허용 오차 조정
python -m data.silver.validate --tol 1e-3

# CAPEX epsilon 조정
python -m data.silver.validate --capex-eps 1e-6
```
