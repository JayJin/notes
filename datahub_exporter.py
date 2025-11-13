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

# DB별 테이블 메트릭
db_table_count = Gauge('datahub_db_table_count', 'DB별 테이블 수', ['database', 'platform'])
db_table_with_desc = Gauge('datahub_db_table_with_desc', 'DB별 설명이 있는 테이블 수', ['database', 'platform'])
db_table_without_desc = Gauge('datahub_db_table_without_desc', 'DB별 설명이 없는 테이블 수', ['database', 'platform'])
db_table_desc_ratio = Gauge('datahub_db_table_desc_ratio', 'DB별 테이블 설명 등록율', ['database', 'platform'])

# DB별 컬럼 메트릭
db_column_count = Gauge('datahub_db_column_count', 'DB별 전체 컬럼 수', ['database', 'platform'])
db_column_with_desc = Gauge('datahub_db_column_with_desc', 'DB별 설명이 있는 컬럼 수', ['database', 'platform'])
db_column_without_desc = Gauge('datahub_db_column_without_desc', 'DB별 설명이 없는 컬럼 수', ['database', 'platform'])
db_column_desc_ratio = Gauge('datahub_db_column_desc_ratio', 'DB별 컬럼 설명 등록율', ['database', 'platform'])

# 기타 메트릭
datasets_with_owner = Gauge('datahub_datasets_with_owner', '소유자가 있는 데이터셋')
datasets_without_owner = Gauge('datahub_datasets_without_owner', '소유자가 없는 데이터셋')
datasets_with_tags = Gauge('datahub_datasets_with_tags', '태그가 있는 데이터셋')
datasets_without_tags = Gauge('datahub_datasets_without_tags', '태그가 없는 데이터셋')

last_scrape_success = Gauge('datahub_last_scrape_success', '마지막 수집 성공 여부')
last_scrape_duration_seconds = Gauge('datahub_last_scrape_duration_seconds', '마지막 수집 소요 시간')

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
                timeout=60
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

def get_datasets_by_platform_and_db(platform, db_name=None):
    """
    특정 플랫폼 및 데이터베이스의 모든 데이터셋과 스키마 메타데이터 조회
    """
    datasets = []
    start = 0
    count = 100
    
    while True:
        # 플랫폼과 데이터베이스 이름으로 필터링
        if db_name:
            query_text = f'platform:{platform} AND name:{db_name}.*'
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
                datasets.append(result['entity'])
        
        total = payload['search']['total']
        start += count
        
        if start >= total:
            break
        
        time.sleep(0.1)  # API 부하 방지
    
    return datasets

def analyze_db_metrics(platform, db_name=None):
    """
    특정 DB의 테이블 및 컬럼 메트릭 분석
    """
    log.info(f"Analyzing metrics for platform={platform}, db={db_name}...")
    
    datasets = get_datasets_by_platform_and_db(platform, db_name)
    
    if not datasets:
        log.warning(f"No datasets found for {platform}/{db_name}")
        return
    
    table_count = len(datasets)
    table_with_desc = 0
    table_without_desc = 0
    
    total_columns = 0
    columns_with_desc = 0
    columns_without_desc = 0
    
    for dataset in datasets:
        # 테이블 설명 확인
        has_table_desc = False
        
        if dataset.get('properties') and dataset['properties'].get('description'):
            has_table_desc = True
        elif dataset.get('editableProperties') and dataset['editableProperties'].get('description'):
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
                if field.get('description'):
                    columns_with_desc += 1
                else:
                    columns_without_desc += 1
    
    # 메트릭 설정
    label_db = db_name if db_name else platform
    
    db_table_count.labels(database=label_db, platform=platform).set(table_count)
    db_table_with_desc.labels(database=label_db, platform=platform).set(table_with_desc)
    db_table_without_desc.labels(database=label_db, platform=platform).set(table_without_desc)
    
    table_desc_ratio = (table_with_desc / table_count * 100) if table_count > 0 else 0
    db_table_desc_ratio.labels(database=label_db, platform=platform).set(table_desc_ratio)
    
    db_column_count.labels(database=label_db, platform=platform).set(total_columns)
    db_column_with_desc.labels(database=label_db, platform=platform).set(columns_with_desc)
    db_column_without_desc.labels(database=label_db, platform=platform).set(columns_without_desc)
    
    column_desc_ratio = (columns_with_desc / total_columns * 100) if total_columns > 0 else 0
    db_column_desc_ratio.labels(database=label_db, platform=platform).set(column_desc_ratio)
    
    log.info(f"  [{label_db}] Tables: {table_count}, with desc: {table_with_desc} ({table_desc_ratio:.1f}%)")
    log.info(f"  [{label_db}] Columns: {total_columns}, with desc: {columns_with_desc} ({column_desc_ratio:.1f}%)")

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
        
        # Oracle 3개 DB 분석
        analyze_db_metrics('oracle', 'db1')
        analyze_db_metrics('oracle', 'db2')
        analyze_db_metrics('oracle', 'db3')
        
        # PostgreSQL 1개 DB 분석
        analyze_db_metrics('postgres', 'main_db')
        
        # 기타 전체 메트릭
        total_tags.set(get_entity_count_simple('TAG'))
        total_glossary_terms.set(get_entity_count_simple('GLOSSARY_TERM'))
        total_domains.set(get_entity_count_simple('DOMAIN'))
        
        duration = time.time() - start_time
        last_scrape_duration_seconds.set(duration)
        last_scrape_success.set(1)
        log.info(f"✅ Collection completed in {duration:.2f}s")
        
    except Exception as e:
        log.error(f"❌ Error during collection: {e}", exc_info=True)
        last_scrape_success.set(0)
        scrape_errors.inc()

if __name__ == "__main__":
    log.info("=" * 70)
    log.info("DataHub Prometheus Exporter (DB-specific Version)")
    log.info("=" * 70)
    log.info(f"GMS URL: {GMS_URL}")
    log.info(f"Scrape interval: {SCRAPE_INTERVAL}s")
    log.info(f"Metrics port: 9105")
    log.info("=" * 70)
    
    start_http_server(9105)
    log.info("✅ Metrics server started at http://localhost:9105/metrics")
    
    try:
        collect_metrics()
    except Exception as e:
        log.error(f"Initial collection error: {e}")
    
    while True:
        try:
            time.sleep(SCRAPE_INTERVAL)
            collect_metrics()
        except KeyboardInterrupt:
            log.info("\n👋 Shutting down...")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
