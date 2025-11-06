import os
import time
import logging
import base64
import requests
from prometheus_client import start_http_server, Gauge, Counter, Info

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

PAGE_SIZE = 500
MAX_RETRIES = 3
RETRY_DELAY = 5
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '60'))

# ========================================
# Prometheus Metrics 정의
# ========================================

# 에러 카운터
scrape_errors = Counter('datahub_scrape_errors_total', 'Total scrape errors')

# === 데이터셋 관련 메트릭 ===
total_datasets = Gauge('datahub_datasets_total', '전체 데이터셋 수')
datasets_by_platform = Gauge('datahub_datasets_by_platform', '플랫폼별 데이터셋 수', ['platform'])
datasets_by_domain = Gauge('datahub_datasets_by_domain', '도메인별 데이터셋 수', ['domain'])
datasets_by_env = Gauge('datahub_datasets_by_env', '환경별 데이터셋 수 (PROD/DEV/QA)', ['environment'])

# === 메타데이터 품질 지표 ===
datasets_with_owner = Gauge('datahub_datasets_with_owner', '소유자가 있는 데이터셋')
datasets_without_owner = Gauge('datahub_datasets_without_owner', '소유자가 없는 데이터셋')
datasets_with_description = Gauge('datahub_datasets_with_description', '설명이 있는 데이터셋')
datasets_without_description = Gauge('datahub_datasets_without_description', '설명이 없는 데이터셋')
datasets_with_tags = Gauge('datahub_datasets_with_tags', '태그가 있는 데이터셋')
datasets_without_tags = Gauge('datahub_datasets_without_tags', '태그가 없는 데이터셋')
datasets_with_glossary_terms = Gauge('datahub_datasets_with_glossary_terms', '용어집이 있는 데이터셋')
datasets_without_glossary_terms = Gauge('datahub_datasets_without_glossary_terms', '용어집이 없는 데이터셋')
datasets_with_schema = Gauge('datahub_datasets_with_schema', '스키마가 있는 데이터셋')
datasets_without_schema = Gauge('datahub_datasets_without_schema', '스키마가 없는 데이터셋')

# === 컬럼 레벨 메트릭 ===
total_columns = Gauge('datahub_columns_total', '전체 컬럼 수')
columns_with_description = Gauge('datahub_columns_with_description', '설명이 있는 컬럼 수')
columns_without_description = Gauge('datahub_columns_without_description', '설명이 없는 컬럼 수')

# === 태그 및 용어집 메트릭 ===
total_tags = Gauge('datahub_tags_total', '전체 태그 수')
total_glossary_terms = Gauge('datahub_glossary_terms_total', '전체 용어집 수')
total_domains = Gauge('datahub_domains_total', '전체 도메인 수')
datasets_by_tag = Gauge('datahub_datasets_by_tag', '태그별 데이터셋 수', ['tag'])

# === 최근 활동 메트릭 ===
datasets_updated_last_day = Gauge('datahub_datasets_updated_last_day', '최근 1일 내 업데이트된 데이터셋')
datasets_updated_last_week = Gauge('datahub_datasets_updated_last_week', '최근 7일 내 업데이트된 데이터셋')
datasets_updated_last_month = Gauge('datahub_datasets_updated_last_month', '최근 30일 내 업데이트된 데이터셋')

# === 데이터 계보(Lineage) 메트릭 ===
datasets_with_upstream = Gauge('datahub_datasets_with_upstream', '상위 의존성이 있는 데이터셋')
datasets_with_downstream = Gauge('datahub_datasets_with_downstream', '하위 의존성이 있는 데이터셋')

# === 품질 완성도 지표 ===
metadata_completeness_ratio = Gauge('datahub_metadata_completeness_ratio', '메타데이터 완성도 비율 (0~1)')
documentation_coverage_ratio = Gauge('datahub_documentation_coverage_ratio', '문서화 커버리지 (설명+용어집)')
governance_coverage_ratio = Gauge('datahub_governance_coverage_ratio', '거버넌스 커버리지 (소유자+태그+도메인)')

# === 사용자 및 그룹 메트릭 ===
total_users = Gauge('datahub_users_total', '전체 사용자 수')
total_groups = Gauge('datahub_groups_total', '전체 그룹 수')

# === 기타 엔티티 메트릭 ===
total_dashboards = Gauge('datahub_dashboards_total', '전체 대시보드 수')
total_charts = Gauge('datahub_charts_total', '전체 차트 수')
total_data_jobs = Gauge('datahub_data_jobs_total', '전체 데이터 작업 수')
total_data_flows = Gauge('datahub_data_flows_total', '전체 데이터 플로우 수')
total_ml_models = Gauge('datahub_ml_models_total', '전체 ML 모델 수')
total_ml_features = Gauge('datahub_ml_features_total', '전체 ML 피처 수')

# === 시스템 상태 ===
last_scrape_success = Gauge('datahub_last_scrape_success', '마지막 수집 성공 여부 (1=성공, 0=실패)')
last_scrape_duration_seconds = Gauge('datahub_last_scrape_duration_seconds', '마지막 수집 소요 시간 (초)')


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
                timeout=30
            )
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


def get_entity_count(entity_type: str, filters: list = None):
    """엔티티 수 조회 (범용)"""
    filter_clause = ""
    if filters:
        filter_str = ", ".join([
            f'{{ field: "{f["field"]}", values: {f["values"]} }}'
            for f in filters
        ])
        filter_clause = f", filters: [{filter_str}]"
    
    query = f"""
    query {{
      search(
        input: {{
          type: {entity_type}
          query: "*"
          start: 0
          count: 1
          {filter_clause}
        }}
      ) {{
        total
      }}
    }}
    """
    
    payload = _post(query)
    
    if payload and 'search' in payload:
        return payload['search']['total']
    
    return 0


def get_datasets_by_platform():
    """플랫폼별 데이터셋 수"""
    # 주요 플랫폼 목록
    platforms = [
        'postgres', 'mysql', 'oracle', 'mssql', 'mongodb',
        'snowflake', 'bigquery', 'redshift', 'databricks',
        'hive', 'spark', 'kafka', 's3', 'hdfs', 'glue'
    ]
    
    log.info("Fetching datasets by platform...")
    for platform in platforms:
        count = get_entity_count('DATASET', [
            {"field": "platform", "values": [f'"{platform}"']}
        ])
        if count > 0:
            datasets_by_platform.labels(platform=platform).set(count)
            log.info(f"  Platform {platform}: {count}")


def get_datasets_by_environment():
    """환경별 데이터셋 수 (PROD, DEV, QA 등)"""
    environments = ['PROD', 'DEV', 'QA', 'STAGING', 'UAT']
    
    log.info("Fetching datasets by environment...")
    for env in environments:
        count = get_entity_count('DATASET', [
            {"field": "origin", "values": [f'"{env}"']}
        ])
        if count > 0:
            datasets_by_env.labels(environment=env).set(count)
            log.info(f"  Environment {env}: {count}")


def get_top_tags():
    """상위 태그별 데이터셋 수"""
    # 모든 태그 조회
    query = """
    query {
      search(
        input: {
          type: TAG
          query: "*"
          start: 0
          count: 50
        }
      ) {
        searchResults {
          entity {
            ... on Tag {
              name
              urn
            }
          }
        }
      }
    }
    """
    
    log.info("Fetching top tags...")
    payload = _post(query)
    
    if payload and 'search' in payload:
        tags = payload['search']['searchResults']
        
        for tag_result in tags[:20]:  # 상위 20개만
            tag = tag_result['entity']
            tag_name = tag['name']
            tag_urn = tag['urn']
            
            # 해당 태그를 가진 데이터셋 수
            count = get_entity_count('DATASET', [
                {"field": "tags", "values": [f'"{tag_urn}"']}
            ])
            
            if count > 0:
                datasets_by_tag.labels(tag=tag_name).set(count)
                log.info(f"  Tag {tag_name}: {count} datasets")


def get_column_statistics():
    """컬럼 레벨 통계"""
    log.info("Fetching column-level statistics...")
    
    # 스키마가 있는 데이터셋들을 샘플링하여 컬럼 통계 계산
    query = """
    query {
      search(
        input: {
          type: DATASET
          query: "*"
          start: 0
          count: 100
        }
      ) {
        searchResults {
          entity {
            ... on Dataset {
              urn
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
    
    payload = _post(query)
    
    if payload and 'search' in payload:
        results = payload['search']['searchResults']
        
        total_cols = 0
        cols_with_desc = 0
        
        for result in results:
            dataset = result['entity']
            schema = dataset.get('schemaMetadata')
            
            if schema and 'fields' in schema:
                fields = schema['fields']
                total_cols += len(fields)
                
                for field in fields:
                    if field.get('description'):
                        cols_with_desc += 1
        
        total_columns.set(total_cols)
        columns_with_description.set(cols_with_desc)
        columns_without_description.set(total_cols - cols_with_desc)
        
        log.info(f"  Total columns: {total_cols}")
        log.info(f"  Columns with description: {cols_with_desc}")


def get_recent_updates():
    """최근 업데이트 통계"""
    log.info("Fetching recent update statistics...")
    
    # 최근 1일
    count_1d = get_entity_count('DATASET', [
        {"field": "lastModified", "values": ['"now-1d"']}
    ])
    datasets_updated_last_day.set(count_1d)
    log.info(f"  Updated in last 1 day: {count_1d}")
    
    # 최근 7일
    count_7d = get_entity_count('DATASET', [
        {"field": "lastModified", "values": ['"now-7d"']}
    ])
    datasets_updated_last_week.set(count_7d)
    log.info(f"  Updated in last 7 days: {count_7d}")
    
    # 최근 30일
    count_30d = get_entity_count('DATASET', [
        {"field": "lastModified", "values": ['"now-30d"']}
    ])
    datasets_updated_last_month.set(count_30d)
    log.info(f"  Updated in last 30 days: {count_30d}")


def collect_metrics():
    """메트릭 수집"""
    start_time = time.time()
    
    log.info("=" * 70)
    log.info("Starting comprehensive metric collection...")
    log.info("=" * 70)
    
    try:
        # === 1. 기본 데이터셋 메트릭 ===
        log.info("\n[1/12] Collecting basic dataset metrics...")
        total = get_entity_count('DATASET')
        total_datasets.set(total)
        log.info(f"✅ Total datasets: {total}")
        
        # === 2. 메타데이터 품질 지표 ===
        log.info("\n[2/12] Collecting metadata quality metrics...")
        
        without_owner = get_entity_count('DATASET', [
            {"field": "hasOwners", "values": ['"false"']}
        ])
        datasets_without_owner.set(without_owner)
        datasets_with_owner.set(max(0, total - without_owner))
        log.info(f"  Datasets without owner: {without_owner}")
        
        without_desc = get_entity_count('DATASET', [
            {"field": "hasDescription", "values": ['"false"']}
        ])
        datasets_without_description.set(without_desc)
        datasets_with_description.set(max(0, total - without_desc))
        log.info(f"  Datasets without description: {without_desc}")
        
        without_tags = get_entity_count('DATASET', [
            {"field": "hasTags", "values": ['"false"']}
        ])
        datasets_without_tags.set(without_tags)
        datasets_with_tags.set(max(0, total - without_tags))
        log.info(f"  Datasets without tags: {without_tags}")
        
        without_terms = get_entity_count('DATASET', [
            {"field": "hasGlossaryTerms", "values": ['"false"']}
        ])
        datasets_without_glossary_terms.set(without_terms)
        datasets_with_glossary_terms.set(max(0, total - without_terms))
        log.info(f"  Datasets without glossary terms: {without_terms}")
        
        # === 3. 플랫폼별 통계 ===
        log.info("\n[3/12] Collecting platform statistics...")
        get_datasets_by_platform()
        
        # === 4. 환경별 통계 ===
        log.info("\n[4/12] Collecting environment statistics...")
        get_datasets_by_environment()
        
        # === 5. 태그 통계 ===
        log.info("\n[5/12] Collecting tag statistics...")
        tag_count = get_entity_count('TAG')
        total_tags.set(tag_count)
        log.info(f"  Total tags: {tag_count}")
        get_top_tags()
        
        # === 6. 용어집 통계 ===
        log.info("\n[6/12] Collecting glossary statistics...")
        term_count = get_entity_count('GLOSSARY_TERM')
        total_glossary_terms.set(term_count)
        log.info(f"  Total glossary terms: {term_count}")
        
        # === 7. 도메인 통계 ===
        log.info("\n[7/12] Collecting domain statistics...")
        domain_count = get_entity_count('DOMAIN')
        total_domains.set(domain_count)
        log.info(f"  Total domains: {domain_count}")
        
        # === 8. 컬럼 레벨 통계 ===
        log.info("\n[8/12] Collecting column-level statistics...")
        get_column_statistics()
        
        # === 9. 최근 업데이트 통계 ===
        log.info("\n[9/12] Collecting recent update statistics...")
        get_recent_updates()
        
        # === 10. 기타 엔티티 통계 ===
        log.info("\n[10/12] Collecting other entity statistics...")
        
        dashboard_count = get_entity_count('DASHBOARD')
        total_dashboards.set(dashboard_count)
        log.info(f"  Dashboards: {dashboard_count}")
        
        chart_count = get_entity_count('CHART')
        total_charts.set(chart_count)
        log.info(f"  Charts: {chart_count}")
        
        job_count = get_entity_count('DATA_JOB')
        total_data_jobs.set(job_count)
        log.info(f"  Data jobs: {job_count}")
        
        flow_count = get_entity_count('DATA_FLOW')
        total_data_flows.set(flow_count)
        log.info(f"  Data flows: {flow_count}")
        
        # === 11. 사용자 및 그룹 ===
        log.info("\n[11/12] Collecting user and group statistics...")
        
        user_count = get_entity_count('CORP_USER')
        total_users.set(user_count)
        log.info(f"  Users: {user_count}")
        
        group_count = get_entity_count('CORP_GROUP')
        total_groups.set(group_count)
        log.info(f"  Groups: {group_count}")
        
        # === 12. 완성도 지표 계산 ===
        log.info("\n[12/12] Calculating quality scores...")
        
        if total > 0:
            # 메타데이터 완성도 (소유자 + 설명 + 태그 모두 있는 비율)
            max_incomplete = max(without_owner, without_desc, without_tags)
            completeness = (total - max_incomplete) / total
            metadata_completeness_ratio.set(completeness)
            log.info(f"  Metadata completeness: {completeness:.2%}")
            
            # 문서화 커버리지 (설명 + 용어집)
            max_undocumented = max(without_desc, without_terms)
            doc_coverage = (total - max_undocumented) / total
            documentation_coverage_ratio.set(doc_coverage)
            log.info(f"  Documentation coverage: {doc_coverage:.2%}")
            
            # 거버넌스 커버리지 (소유자 + 태그)
            max_ungoverned = max(without_owner, without_tags)
            gov_coverage = (total - max_ungoverned) / total
            governance_coverage_ratio.set(gov_coverage)
            log.info(f"  Governance coverage: {gov_coverage:.2%}")
        
        # 수집 완료
        duration = time.time() - start_time
        last_scrape_duration_seconds.set(duration)
        last_scrape_success.set(1)
        
        log.info("=" * 70)
        log.info(f"✅ Metric collection completed in {duration:.2f} seconds")
        log.info("=" * 70)
        
    except Exception as e:
        log.error(f"❌ Error during metric collection: {e}", exc_info=True)
        last_scrape_success.set(0)
        scrape_errors.inc()


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("DataHub Comprehensive Prometheus Exporter")
    log.info("=" * 70)
    log.info(f"GMS URL: {GMS_URL}")
    log.info(f"Scrape interval: {SCRAPE_INTERVAL} seconds")
    log.info(f"Metrics port: 9105")
    log.info("=" * 70)
    
    # Prometheus 서버 시작
    start_http_server(9105)
    log.info("✅ Prometheus metrics server started")
    log.info(f"   Access metrics at: http://localhost:9105/metrics")
    
    # 초기 메트릭 설정
    log.info("Setting initial metric values...")
    total_datasets.set(0)
    last_scrape_success.set(0)
    
    # 첫 수집 즉시 실행
    log.info("\nRunning initial collection...")
    try:
        collect_metrics()
    except Exception as e:
        log.error(f"Error in initial collection: {e}")
    
    # 주기적 수집
    while True:
        try:
            time.sleep(SCRAPE_INTERVAL)
            collect_metrics()
        except KeyboardInterrupt:
            log.info("\n👋 Shutting down gracefully...")
            break
        except Exception as e:
            log.error(f"Error in main loop: {e}", exc_info=True)
