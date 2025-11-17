# DataHub + Airflow Docker Compose 설정 가이드

## 📋 구성 요소

- **DataHub GMS**: 메타데이터 저장소 (포트 8080)
- **DataHub Frontend**: UI (포트 3000)
- **DataHub Exporter**: Prometheus 메트릭 수집 (포트 9105)
- **Airflow Scheduler**: DAG 실행 스케줄링
- **Airflow Webserver**: UI (포트 8888)
- **PostgreSQL (Airflow)**: Airflow 메타데이터 DB

## 🚀 빠른 시작

### 1. 디렉토리 구조 생성

```bash
mkdir -p airflow/dags airflow/logs airflow/plugins
```

### 2. 파일 배치

```bash
# 현재 디렉토리에 다음 파일 배치:
docker-compose.yml          # Docker Compose 설정
datahub_exporter.py         # DataHub Exporter 스크립트
datahub_metadata_update.csv # 메타데이터 업데이트 CSV

# Airflow DAG 배치:
airflow/dags/datahub_dag.py # Airflow DAG 파일
```

### 3. 컨테이너 시작

```bash
# 모든 서비스 시작
docker-compose up -d

# 로그 확인
docker-compose logs -f

# 특정 서비스 로그만 보기
docker-compose logs -f airflow-scheduler
docker-compose logs -f datahub-gms
```

### 4. 서비스 접근

- **DataHub UI**: http://localhost:3000
  - 기본 계정: datahub / datahub

- **Airflow UI**: http://localhost:8888
  - 기본 계정: admin / admin

- **Prometheus Metrics**: http://localhost:9105/metrics

- **DataHub GMS API**: http://localhost:8080/api/graphql

## 📝 메타데이터 업데이트 사용 방법

### 1. CSV 파일 준비

`datahub_metadata_update.csv` 파일을 편집하여 업데이트할 메타데이터 추가:

```csv
type,urn,field_path,description,tag_urn,glossary_term_urn
dataset,urn:li:dataset:(urn:li:dataPlatform:oracle,PDSM.MY_TABLE,PROD),,"테이블 설명",,
column,urn:li:dataset:(urn:li:dataPlatform:oracle,PDSM.MY_TABLE,PROD),COL_NAME,"컬럼 설명",,
tag,urn:li:dataset:(urn:li:dataPlatform:oracle,PDSM.MY_TABLE,PROD),,"",urn:li:tag:important,
```

### 2. URN 구조 이해

#### Dataset URN
```
urn:li:dataset:(urn:li:dataPlatform:<platform>,<database>.<schema>.<table>,<env>)
```

예시:
- Oracle: `urn:li:dataset:(urn:li:dataPlatform:oracle,PDSM.EMPLOYEES,PROD)`
- PostgreSQL: `urn:li:dataset:(urn:li:dataPlatform:postgres,cdc_pdsm.public.customers,PROD)`
- MySQL: `urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.users,PROD)`

#### Tag URN
```
urn:li:tag:<tag_name>
```

예시:
- `urn:li:tag:pii`
- `urn:li:tag:critical`
- `urn:li:tag:financial`

#### Glossary Term URN
```
urn:li:glossaryTerm:<term_name>
```

예시:
- `urn:li:glossaryTerm:PRIMARY_KEY`
- `urn:li:glossaryTerm:EMPLOYEE_DATA`

### 3. DataHub에서 URN 확인

1. DataHub UI (http://localhost:3000) 접속
2. 테이블/컬럼 검색
3. URL에서 URN 확인 또는 상세 정보에서 복사

### 4. Airflow DAG 실행

#### 방법 1: Airflow UI에서 수동 트리거
1. Airflow UI (http://localhost:8888) 접속
2. DAG 목록에서 `datahub_metadata_bulk_update` 찾기
3. 우측 "Trigger DAG" 버튼 클릭
4. 로그 확인

#### 방법 2: CLI에서 트리거
```bash
docker exec airflow-scheduler airflow dags trigger datahub_metadata_bulk_update
```

### 5. 실행 결과 확인

Airflow UI에서 DAG 실행 로그:
```
Row 1: Processing dataset - urn:li:dataset:...
✓ Updated dataset: urn:li:dataset:...
Row 2: Processing column - urn:li:dataset:...
✓ Updated field: EMP_ID
========================================
Processing complete!
  Success: 10
  Failed: 0
========================================
```

## 🔐 인증 설정 (선택사항)

DataHub에 인증이 활성화된 경우:

### 1. Personal Access Token (PAT) 생성

1. DataHub UI > Settings > Access Tokens
2. "Generate Personal Access Token" 클릭
3. 토큰 복사

### 2. docker-compose.yml에 토큰 설정

```yaml
airflow-scheduler:
  environment:
    DATAHUB_AUTH_TOKEN: 'your_token_here'

airflow-webserver:
  environment:
    DATAHUB_AUTH_TOKEN: 'your_token_here'
```

### 3. 재시작

```bash
docker-compose restart airflow-scheduler airflow-webserver
```

## ⚠️ 주의사항

1. **CSV 파일 형식**: 반드시 UTF-8 인코딩 사용
2. **특수문자 처리**: 설명에 쌍따옴표 포함 시 이스케이프 처리
3. **URN 정확성**: URN이 잘못되면 업데이트 실패
4. **권한 확인**: 토큰 사용자가 메타데이터 편집 권한 필요
5. **연결 확인**: DAG 실행 전 DataHub GMS 접근 가능 확인

## 🔍 문제 해결

### DAG 실행 실패

```bash
# Airflow 로그 확인
docker-compose logs airflow-scheduler

# DAG 구문 확인
docker exec airflow-webserver airflow dags list
docker exec airflow-webserver airflow dags validate
```

### DataHub 연결 불가

```bash
# GMS 헬스 체크
curl http://localhost:8080/health

# GMS 로그 확인
docker-compose logs datahub-gms
```

### 메타데이터 업데이트 불가

```bash
# 토큰 검증
# Airflow DAG 로그에서 "Using Bearer token authentication" 확인

# URN 정확성 확인
# DataHub UI에서 URN 복사하여 CSV에 붙여넣기
```

## 📊 Prometheus 메트릭 확인

DataHub Exporter가 수집한 메트릭:

```bash
# 메트릭 목록 확인
curl http://localhost:9105/metrics | grep datahub

# 특정 메트릭 조회
curl http://localhost:9105/metrics | grep datahub_schema_table_desc_ratio
```

## 🛑 서비스 중지

```bash
# 모든 서비스 중지
docker-compose down

# 데이터도 삭제 (주의!)
docker-compose down -v
```

## 📚 추가 리소스

- DataHub 문서: https://datahubproject.io/docs
- Airflow 문서: https://airflow.apache.org/docs
- GraphQL 쿼리: http://localhost:8080/api/graphql (GraphQL IDE)

## 💡 팁

- CSV 파일에 많은 행이 있으면 처리 시간이 오래 걸릴 수 있습니다
- 배치 업데이트는 스케줄 설정으로 자동화 가능합니다
- DAG 로그에서 성공/실패 내역을 상세히 확인할 수 있습니다
