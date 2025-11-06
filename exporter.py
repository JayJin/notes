import os
import time
import logging
import base64
import requests
import json
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

PAGE_SIZE = 500
MAX_RETRIES = 3
RETRY_DELAY = 5
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '60'))

# Prometheus Metrics
scrape_errors = Counter('datahub_scrape_errors_total', 'Total scrape errors')

total_datasets = Gauge('datahub_datasets_total', '전체 데이터셋 수')
datasets_by_platform = Gauge('datahub_datasets_by_platform', '플랫폼별 데이터셋 수', ['platform'])

datasets_with_owner = Gauge('datahub_datasets_with_owner', '소유자가 있는 데이터셋')
datasets_without_owner = Gauge('datahub_datasets_without_owner', '소유자가 없는 데이터셋')
datasets_with_description = Gauge('datahub_datasets_with_description', '설명이 있는 데이터셋')
datasets_without_description = Gauge('datahub_datasets_without_description', '설명이 없는 데이터셋')
datasets_with_tags = Gauge('datahub_datasets_with_tags', '태그가 있는 데이터셋')
datasets_without_tags = Gauge('datahub_datasets_without_tags', '태그가 없는 데이터셋')

total_tags = Gauge('datahub_tags_total', '전체 태그 수')
total_glossary_terms = Gauge('datahub_glossary_terms_total', '전체 용어집 수')
total_domains = Gauge('datahub_domains_total', '전체 도메인 수')

datasets_updated_last_week = Gauge('datahub_datasets_updated_last_week', '최근 7일 내 업데이트')

total_dashboards = Gauge('datahub_dashboards_total', '전체 대시보드 수')
total_charts = Gauge('datahub_charts_total', '전체 차트 수')
total_data_jobs = Gauge('datahub_data_jobs_total', '전체 데이터 작업 수')
total_users = Gauge('datahub_users_total', '전체 사용자 수')
total_groups = Gauge('datahub_groups_total', '전체 그룹 수')

metadata_completeness_ratio = Gauge('datahub_metadata_completeness_ratio', '메타데이터 완성도')
documentation_coverage_ratio = Gauge('datahub_documentation_coverage_ratio', '문서화 커버리지')

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
            
            log.debug(f"Sending query: {query[:200]}...")
            
            resp = requests.post(
                f"{GMS_URL}/api/graphql",
                json=payload,
                headers=_auth_header(),
                timeout=30
            )
            
            # 500 에러 시 응답 내용 출력
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
        
        except requests.exceptions.HTTPError as e:
            log.error(f"HTTP error (attempt {attempt}/{MAX_RETRIES}): {e}")
            log.error(f"Response: {resp.text[:500] if 'resp' in locals() else 'No response'}")
            scrape_errors.inc()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        
        except Exception as e:
            log.error(f"Request error (attempt {attempt}/{MAX_RETRIES}): {e}")
            scrape_errors.inc()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    
    return None


def get_entity_count_simple(entity_type: str):
    """✅ 단순 엔티티 수 조회 (필터 없음)"""
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
    
    if payload and 'search' in payload:
        return payload['search']['total']
    
    return 0


def get_datasets_without_field(field_name: str):
    """✅ 특정 필드가 없는 데이터셋 수"""
    # DataHub GraphQL에서 지원하는 필터 필드
    valid_filters = {
        'owner': 'hasOwners',
        'description': 'hasDescription', 
        'tags': 'hasTags',
        'glossary': 'hasGlossaryTerms'
    }
    
    filter_field = valid_filters.get(field_name)
    
    if not filter_field:
        log.warning(f"Unknown field: {field_name}")
        return 0
    
    query = f"""
    query {{
      search(
        input: {{
          type: DATASET
          query: "*"
          filters: [
            {{
              field: "{filter_field}"
              values: ["false"]
            }}
          ]
          start: 0
          count: 1
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
    """✅ 플랫폼별 데이터셋 수 (간단한 방식)"""
    platforms = ['postgres', 'mysql', 'snowflake', 'bigquery', 'kafka']
    
    log.info("Fetching datasets by platform...")
    
    for platform in platforms:
        query = f"""
        query {{
          search(
            input: {{
              type: DATASET
              query: "platform:{platform}"
              start: 0
              count: 1
            }}
          ) {{
            total
          }}
        }}
        """
        
        payload = _post(query)
        
        if payload and 'search' in payload:
            count = payload['search']['total']
            if count > 0:
                datasets_by_platform.labels(platform=platform).set(count)
                log.info(f"  Platform {platform}: {count}")


def collect_metrics():
    """메트릭 수집 (안정화된 버전)"""
    start_time = time.time()
    
    log.info("=" * 70)
    log.info("Starting metric collection...")
    log.info("=" * 70)
    
    try:
        # === 1. 전체 데이터셋 수 ===
        log.info("\n[1/10] Total datasets...")
        total = get_entity_count_simple('DATASET')
        total_datasets.set(total)
        log.info(f"✅ Total datasets: {total}")
        
        # === 2. 메타데이터 품질 지표 ===
        log.info("\n[2/10] Metadata quality metrics...")
        
        without_owner = get_datasets_without_field('owner')
        datasets_without_owner.set(without_owner)
        datasets_with_owner.set(max(0, total - without_owner))
        log.info(f"  Without owner: {without_owner}")
        
        without_desc = get_datasets_without_field('description')
        datasets_without_description.set(without_desc)
        datasets_with_description.set(max(0, total - without_desc))
        log.info(f"  Without description: {without_desc}")
        
        without_tags = get_datasets_without_field('tags')
        datasets_without_tags.set(without_tags)
        datasets_with_tags.set(max(0, total - without_tags))
        log.info(f"  Without tags: {without_tags}")
        
        # === 3. 플랫폼별 통계 ===
        log.info("\n[3/10] Platform statistics...")
        get_datasets_by_platform()
        
        # === 4. 태그 수 ===
        log.info("\n[4/10] Tag count...")
        tag_count = get_entity_count_simple('TAG')
        total_tags.set(tag_count)
        log.info(f"  Total tags: {tag_count}")
        
        # === 5. 용어집 수 ===
        log.info("\n[5/10] Glossary term count...")
        term_count = get_entity_count_simple('GLOSSARY_TERM')
        total_glossary_terms.set(term_count)
        log.info(f"  Total glossary terms: {term_count}")
        
        # === 6. 도메인 수 ===
        log.info("\n[6/10] Domain count...")
        domain_count = get_entity_count_simple('DOMAIN')
        total_domains.set(domain_count)
        log.info(f"  Total domains: {domain_count}")
        
        # === 7. 대시보드 & 차트 ===
        log.info("\n[7/10] Dashboard and chart counts...")
        dashboard_count = get_entity_count_simple('DASHBOARD')
        total_dashboards.set(dashboard_count)
        log.info(f"  Dashboards: {dashboard_count}")
        
        chart_count = get_entity_count_simple('CHART')
        total_charts.set(chart_count)
        log.info(f"  Charts: {chart_count}")
        
        # === 8. 데이터 작업 ===
        log.info("\n[8/10] Data job count...")
        job_count = get_entity_count_simple('DATA_JOB')
        total_data_jobs.set(job_count)
        log.info(f"  Data jobs: {job_count}")
        
        # === 9. 사용자 & 그룹 ===
        log.info("\n[9/10] User and group counts...")
        user_count = get_entity_count_simple('CORP_USER')
        total_users.set(user_count)
        log.info(f"  Users: {user_count}")
        
        group_count = get_entity_count_simple('CORP_GROUP')
        total_groups.set(group_count)
        log.info(f"  Groups: {group_count}")
        
        # === 10. 완성도 지표 ===
        log.info("\n[10/10] Calculating quality scores...")
        
        if total > 0:
            # 메타데이터 완성도
            max_incomplete = max(without_owner, without_desc, without_tags)
            completeness = (total - max_incomplete) / total
            metadata_completeness_ratio.set(completeness)
            log.info(f"  Metadata completeness: {completeness:.2%}")
            
            # 문서화 커버리지
            doc_coverage = (total - without_desc) / total
            documentation_coverage_ratio.set(doc_coverage)
            log.info(f"  Documentation coverage: {doc_coverage:.2%}")
        
        # 수집 완료
        duration = time.time() - start_time
        last_scrape_duration_seconds.set(duration)
        last_scrape_success.set(1)
        
        log.info("=" * 70)
        log.info(f"✅ Collection completed in {duration:.2f}s")
        log.info("=" * 70)
        
    except Exception as e:
        log.error(f"❌ Error: {e}", exc_info=True)
        last_scrape_success.set(0)
        scrape_errors.inc()


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("DataHub Prometheus Exporter (Stable Version)")
    log.info("=" * 70)
    log.info(f"GMS URL: {GMS_URL}")
    log.info(f"Scrape interval: {SCRAPE_INTERVAL}s")
    log.info(f"Metrics port: 9105")
    log.info("=" * 70)
    
    # Prometheus 서버 시작
    start_http_server(9105)
    log.info("✅ Metrics server started")
    log.info(f"   http://localhost:9105/metrics")
    
    # 초기값 설정
    total_datasets.set(0)
    last_scrape_success.set(0)
    
    # 첫 수집
    log.info("\nRunning initial collection...")
    try:
        collect_metrics()
    except Exception as e:
        log.error(f"Initial collection error: {e}")
    
    # 주기적 수집
    while True:
        try:
            time.sleep(SCRAPE_INTERVAL)
            collect_metrics()
        except KeyboardInterrupt:
            log.info("\n👋 Shutting down...")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
