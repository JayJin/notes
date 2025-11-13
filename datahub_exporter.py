import os
import time
import logging
import base64
import requests
from prometheus_client import start_http_server, Gauge, Counter

# 로깅 설정
log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 환경 변수 설정
DATAHUB_GMS_HOST = os.getenv('DATAHUB_GMS_HOST', 'datahub-gms')
DATAHUB_GMS_PORT = os.getenv('DATAHUB_GMS_PORT', '8080')
GMS_URL = f"http://{DATAHUB_GMS_HOST}:{DATAHUB_GMS_PORT}"

SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '60'))
MAX_RETRIES = 3
RETRY_DELAY = 5

# Prometheus Metrics
scrape_errors = Counter('datahub_scrape_errors_total', 'Total scrape errors')

# 전체 메트릭
total_datasets = Gauge('datahub_datasets_total', '전체 데이터셋 수')
total_tags = Gauge('datahub_tags_total', '전체 태그 수')
total_glossary_terms = Gauge('datahub_glossary_terms_total', '전체 용어집 수')
total_domains = Gauge('datahub_domains_total', '전체 도메인 수')

# 스키마별 테이블 메트릭
schema_table_count = Gauge('datahub_schema_table_count', '스키마별 테이블 수', ['schema', 'platform'])
schema_table_with_desc = Gauge('datahub_schema_table_with_desc', '스키마별 설명이 있는 테이블 수', ['schema', 'platform'])
schema_table_without_desc = Gauge('datahub_schema_table_without_desc', '스키마별 설명이 없는 테이블 수', ['schema', 'platform'])
schema_table_desc_ratio = Gauge('datahub_schema_table_desc_ratio', '스키마별 테이블 설명 등록율(%)', ['schema', 'platform'])

# 스키마별 컬럼 메트릭
schema_column_count = Gauge('datahub_schema_column_count', '스키마별 전체 컬럼 수', ['schema', 'platform'])
schema_column_with_desc = Gauge('datahub_schema_column_with_desc', '스키마별 설명이 있는 컬럼 수', ['schema', 'platform'])
schema_column_without_desc = Gauge('datahub_schema_column_without_desc', '스키마별 설명이 없는 컬럼 수', ['schema', 'platform'])
schema_column_desc_ratio = Gauge('datahub_schema_column_desc_ratio', '스키마별 컬럼 설명 등록율(%)', ['schema', 'platform'])

# 전체 Owner/Tag 메트릭
datasets_with_owner = Gauge('datahub_datasets_with_owner', '소유자가 있는 데이터셋')
datasets_without_owner = Gauge('datahub_datasets_without_owner', '소유자가 없는 데이터셋')
datasets_with_tags = Gauge('datahub_datasets_with_tags', '태그가 있는 데이터셋')
datasets_without_tags = Gauge('datahub_datasets_without_tags', '태그가 없는 데이터셋')

# 수집 상태 메트릭
last_scrape_success = Gauge('datahub_last_scrape_success', '마지막 수집 성공 여부 (1=성공, 0=실패)')
last_scrape_duration_seconds = Gauge('datahub_last_scrape_duration_seconds', '마지막 수집 소요 시간(초)')

def _auth_header():
    """인증 헤더 생성"""
    client_id = os.getenv('DATAHUB_CLIENT_ID')
    client_secret = os.getenv('DATAHUB_CLIENT_SECRET')
    
    headers = {"Content-Type": "application/json"}
    
    if client_id and client_secret:
        token = f"{client_id}:{client_secret}"
        b64_token = base64.b64encode(token.encode()).decode()
        headers["Authorization"] = f"Basic {b64_token}"
    
    return headers

def _post(query: str, variables: dict = None):
    """GraphQL 쿼리 실행"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            
            resp = requests.post(
                f"{GMS_URL}/api/graphql",
                json=payload,
                headers=_auth_header(),
                timeout=120
            )
            
            if resp.status_code == 500:
                log.error(f"500 Server Error - Response: {resp.text[:500]}")
                scrape_errors.inc()
                return None
            
            resp.raise_for_status()
            result = resp.json()
            
            if "errors" in result:
                log.error(f"GraphQL errors: {result['errors']}")
                scrape_errors.inc()
                return None
            
            return result.get("data")
        
        except Exception as e:
            log.error(f"Request error (attempt {attempt}/{MAX_RETRIES}): {e}")
            scrape_errors.inc()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    
    return None

def get_datasets_by_platform_and_schema(platform, schema_name=None):
    """
    특정 플랫폼 및 스키마의 모든 데이터셋과 스키마 메타데이터 조회
    Oracle/PostgreSQL: database.schema.table 형식
    """
    datasets = []
    start = 0
    count = 100
    
    log.info(f"Fetching datasets for platform={platform}, schema={schema_name}...")
    
    while True:
        # 스키마 이름으로 필터링
        if schema_name:
            # database.schema.table 형식에서 schema 부분 매칭
            query_text = f'platform:{platform}'
        else:
            query_text = f'platform:{platform}'
        
        query = """
        query searchDatasets($input: SearchInput!) {
          search(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                ... on Dataset {
                  urn
                  name
                  platform {
                    name
                  }
                  properties {
                    description
                    qualifiedName
                  }
                  editableProperties {
                    description
                  }
                  schemaMetadata {
                    fields {
                      fieldPath
                      description
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "type": "DATASET",
                "query": query_text,
                "start": start,
                "count": count
            }
        }
        
        payload = _post(query, variables)
        
        if not payload or 'search' not in payload:
            break
        
        results = payload['search']['searchResults']
        for result in results:
            if 'entity' in result:
                entity = result['entity']
                
                # 스키마 이름 필터링
                if schema_name:
                    name = entity.get('name', '')
                    qualified_name = entity.get('properties', {}).get('qualifiedName', '')
                    
                    # database.schema.table 형식에서 스키마 추출 및 매칭
                    # 예: "MYDB.SCHEMA1.TABLE1" 에서 "SCHEMA1" 추출
                    name_parts = name.split('.')
                    qname_parts = qualified_name.split('.') if qualified_name else []
                    
                    schema_match = False
                    if len(name_parts) >= 2 and name_parts[-2].upper() == schema_name.upper():
                        schema_match = True
                    elif len(qname_parts) >= 2 and qname_parts[-2].upper() == schema_name.upper():
                        schema_match = True
                    
                    if schema_match:
                        datasets.append(entity)
                else:
                    datasets.append(entity)
        
        total = payload['search']['total']
        start += count
        
        if start >= total:
            break
        
        time.sleep(0.1)  # API 부하 방지
    
    log.info(f"  Found {len(datasets)} datasets for {platform}/{schema_name}")
    return datasets

def analyze_schema_metrics(platform, schema_name):
    """
    특정 스키마의 테이블 및 컬럼 메트릭 분석
    """
    log.info(f"Analyzing metrics for platform={platform}, schema={schema_name}...")
    
    datasets = get_datasets_by_platform_and_schema(platform, schema_name)
    
    if not datasets:
        log.warning(f"No datasets found for {platform}/{schema_name}")
        # 메트릭을 0으로 설정
        schema_table_count.labels(schema=schema_name, platform=platform).set(0)
        schema_table_with_desc.labels(schema=schema_name, platform=platform).set(0)
        schema_table_without_desc.labels(schema=schema_name, platform=platform).set(0)
        schema_table_desc_ratio.labels(schema=schema_name, platform=platform).set(0)
        schema_column_count.labels(schema=schema_name, platform=platform).set(0)
        schema_column_with_desc.labels(schema=schema_name, platform=platform).set(0)
        schema_column_without_desc.labels(schema=schema_name, platform=platform).set(0)
        schema_column_desc_ratio.labels(schema=schema_name, platform=platform).set(0)
        return
    
    # 첫 번째 데이터셋 로깅 (디버깅용)
    if datasets:
        log.info(f"  Sample dataset name: {datasets[0].get('name')}")
    
    table_count = len(datasets)
    table_with_desc = 0
    table_without_desc = 0
    
    total_columns = 0
    columns_with_desc = 0
    columns_without_desc = 0
    
    for dataset in datasets:
        # 테이블 설명 확인
        has_table_desc = False
        
        # properties.description 또는 editableProperties.description 확인
        if dataset.get('properties') and dataset['properties'].get('description'):
            desc = dataset['properties']['description'].strip()
            if desc:
                has_table_desc = True
        
        if not has_table_desc and dataset.get('editableProperties') and dataset['editableProperties'].get('description'):
            desc = dataset['editableProperties']['description'].strip()
            if desc:
                has_table_desc = True
        
        if has_table_desc:
            table_with_desc += 1
        else:
            table_without_desc += 1
        
        # 컬럼 설명 확인
        schema_metadata = dataset.get('schemaMetadata')
        if schema_metadata and schema_metadata.get('fields'):
            for field in schema_metadata['fields']:
                total_columns += 1
                field_desc = field.get('description', '').strip()
                if field_desc:
                    columns_with_desc += 1
                else:
                    columns_without_desc += 1
    
    # 메트릭 설정
    schema_table_count.labels(schema=schema_name, platform=platform).set(table_count)
    schema_table_with_desc.labels(schema=schema_name, platform=platform).set(table_with_desc)
    schema_table_without_desc.labels(schema=schema_name, platform=platform).set(table_without_desc)
    
    table_desc_ratio = (table_with_desc / table_count * 100) if table_count > 0 else 0
    schema_table_desc_ratio.labels(schema=schema_name, platform=platform).set(table_desc_ratio)
    
    schema_column_count.labels(schema=schema_name, platform=platform).set(total_columns)
    schema_column_with_desc.labels(schema=schema_name, platform=platform).set(columns_with_desc)
    schema_column_without_desc.labels(schema=schema_name, platform=platform).set(columns_without_desc)
    
    column_desc_ratio = (columns_with_desc / total_columns * 100) if total_columns > 0 else 0
    schema_column_desc_ratio.labels(schema=schema_name, platform=platform).set(column_desc_ratio)
    
    log.info(f"  [{schema_name}] Tables: {table_count}, with desc: {table_with_desc} ({table_desc_ratio:.1f}%)")
    log.info(f"  [{schema_name}] Columns: {total_columns}, with desc: {columns_with_desc} ({column_desc_ratio:.1f}%)")

def get_entity_count_simple(entity_type: str):
    """단순 엔티티 수 조회"""
    query = f"""
    query {{
      search(
        input: {{
          type: {entity_type}
          query: "*"
          start: 0
          count: 1
        }}
      ) {{
        total
      }}
    }}
    """
    
    payload = _post(query)
    return payload['search']['total'] if payload and 'search' in payload else 0

def list_sample_datasets(platform, limit=5):
    """
    특정 플랫폼의 샘플 데이터셋 이름 출력 (스키마 이름 확인용)
    """
    log.info(f"Listing sample datasets for platform={platform}...")
    
    query = """
    query searchDatasets($input: SearchInput!) {
      search(input: $input) {
        searchResults {
          entity {
            ... on Dataset {
              name
              properties {
                qualifiedName
              }
            }
          }
        }
      }
    }
    """
    
    variables = {
        "input": {
            "type": "DATASET",
            "query": f"platform:{platform}",
            "start": 0,
            "count": limit
        }
    }
    
    payload = _post(query, variables)
    
    if payload and 'search' in payload:
        for idx, result in enumerate(payload['search']['searchResults'], 1):
            entity = result['entity']
            name = entity.get('name', '')
            qualified_name = entity.get('properties', {}).get('qualifiedName', '')
            log.info(f"  {idx}. Name: {name}")
            if qualified_name:
                log.info(f"     QualifiedName: {qualified_name}")
            
            # 스키마 추출 시도
            parts = name.split('.')
            if len(parts) >= 2:
                log.info(f"     -> Detected Schema: {parts[-2]}")

def collect_metrics():
    """메트릭 수집"""
    start_time = time.time()
    log.info("=" * 70)
    log.info("Starting metric collection...")
    log.info("=" * 70)
    
    try:
        # 전체 데이터셋 수
        total = get_entity_count_simple('DATASET')
        total_datasets.set(total)
        log.info(f"✅ Total datasets: {total}")
        
        # 샘플 데이터셋 출력 (스키마 확인용 - 첫 실행 시)
        # list_sample_datasets('oracle', 5)
        # list_sample_datasets('postgres', 5)
        
        # Oracle 3개 스키마 분석 (실제 스키마 이름으로 변경 필요)
        analyze_schema_metrics('oracle', 'SCHEMA1')
        analyze_schema_metrics('oracle', 'SCHEMA2')
        analyze_schema_metrics('oracle', 'SCHEMA3')
        
        # PostgreSQL 1개 스키마 분석 (실제 스키마 이름으로 변경 필요)
        analyze_schema_metrics('postgres', 'public')
        
        # 기타 전체 메트릭
        total_tags.set(get_entity_count_simple('TAG'))
        total_glossary_terms.set(get_entity_count_simple('GLOSSARY_TERM'))
        total_domains.set(get_entity_count_simple('DOMAIN'))
        
        log.info(f"✅ Total tags: {total_tags._value.get()}")
        log.info(f"✅ Total glossary terms: {total_glossary_terms._value.get()}")
        log.info(f"✅ Total domains: {total_domains._value.get()}")
        
        duration = time.time() - start_time
        last_scrape_duration_seconds.set(duration)
        last_scrape_success.set(1)
        log.info(f"✅ Collection completed in {duration:.2f}s")
        log.info("=" * 70)
        
    except Exception as e:
        log.error(f"❌ Error during collection: {e}", exc_info=True)
        last_scrape_success.set(0)
        scrape_errors.inc()

if __name__ == "__main__":
    log.info("=" * 70)
    log.info("DataHub Prometheus Exporter (Schema-based Version)")
    log.info("=" * 70)
    log.info(f"GMS URL: {GMS_URL}")
    log.info(f"Scrape interval: {SCRAPE_INTERVAL}s")
    log.info(f"Metrics port: 9105")
    log.info("=" * 70)
    
    start_http_server(9105)
    log.info("✅ Metrics server started at http://localhost:9105/metrics")
    log.info("")
    
    # 초기 메트릭 설정
    total_datasets.set(0)
    last_scrape_success.set(0)
    
    try:
        collect_metrics()
    except Exception as e:
        log.error(f"Initial collection error: {e}", exc_info=True)
    
    while True:
        try:
            time.sleep(SCRAPE_INTERVAL)
            collect_metrics()
        except KeyboardInterrupt:
            log.info("\n👋 Shutting down...")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
