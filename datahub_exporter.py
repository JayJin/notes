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
datasets_with_glossary = Gauge('datahub_datasets_with_glossary', '용어집 연결이 있는 데이터셋')
datasets_without_glossary = Gauge('datahub_datasets_without_glossary', '용어집 연결이 없는 데이터셋')

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
            
            resp = requests.post(
                f"{GMS_URL}/api/graphql",
                json=payload,
                headers=_auth_header(),
                timeout=30
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
        
        except requests.exceptions.HTTPError as e:
            log.error(f"HTTP error (attempt {attempt}/{MAX_RETRIES}): {e}")
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


def get_datasets_with_empty_or_missing_field(field_name: str):
    """
    ✅ 필드가 없거나 (hasX=false) 값이 비어있는 데이터셋 수
    """
    field_map = {
        'description': {
            'hasField': 'hasDescription',
            'emptyQuery': 'editableProperties.description:""'
        },
        'owner': {
            'hasField': 'hasOwners',
            'emptyQuery': 'ownership.owners.owner.urn:""'
        },
        'tags': {
            'hasField': 'hasTags',
            'emptyQuery': 'globalTags.tags.tag.urn:""'
        },
        'glossary': {
            'hasField': 'hasGlossaryTerms',
            'emptyQuery': 'glossaryTerms.terms.urn:""'
        }
    }

    if field_name not in field_map:
        log.warning(f"Unknown field: {field_name}")
        return 0

    log.info(f"Checking datasets with empty or missing {field_name}...")

    has_field = field_map[field_name]['hasField']
    empty_query_value = field_map[field_name]['emptyQuery']

    # 필드가 없는 경우
    missing_query = f"""
    query {{
      search(
        input: {{
          type: DATASET
          filters: [{{ field: "{has_field}", values: ["false"] }}]
          start: 0
          count: 1
        }}
      ) {{
        total
      }}
    }}
    """

    # 필드는 있지만 값이 비어있는 경우
    empty_query = f"""
    query {{
      search(
        input: {{
          type: DATASET
          query: "{empty_query_value}"
          start: 0
          count: 1
        }}
      ) {{
        total
      }}
    }}
    """

    missing_payload = _post(missing_query)
    empty_payload = _post(empty_query)

    missing_count = missing_payload['search']['total'] if missing_payload and 'search' in missing_payload else 0
    empty_count = empty_payload['search']['total'] if empty_payload and 'search' in empty_payload else 0

    total_empty = missing_count + empty_count
    log.info(f"  Missing or empty {field_name}: {total_empty} (missing={missing_count}, empty={empty_count})")

    return total_empty


def get_datasets_by_platform():
    """플랫폼별 데이터셋 수"""
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
    """메트릭 수집"""
    start_time = time.time()
    log.info("=" * 70)
    log.info("Starting metric collection...")
    log.info("=" * 70)
    
    try:
        total = get_entity_count_simple('DATASET')
        total_datasets.set(total)
        log.info(f"✅ Total datasets: {total}")

        without_owner = get_datasets_with_empty_or_missing_field('owner')
        datasets_without_owner.set(without_owner)
        datasets_with_owner.set(max(0, total - without_owner))

        without_desc = get_datasets_with_empty_or_missing_field('description')
        datasets_without_description.set(without_desc)
        datasets_with_description.set(max(0, total - without_desc))

        without_tags = get_datasets_with_empty_or_missing_field('tags')
        datasets_without_tags.set(without_tags)
        datasets_with_tags.set(max(0, total - without_tags))

        without_glossary = get_datasets_with_empty_or_missing_field('glossary')
        datasets_without_glossary.set(without_glossary)
        datasets_with_glossary.set(max(0, total - without_glossary))

        get_datasets_by_platform()

        total_tags.set(get_entity_count_simple('TAG'))
        total_glossary_terms.set(get_entity_count_simple('GLOSSARY_TERM'))
        total_domains.set(get_entity_count_simple('DOMAIN'))
        total_dashboards.set(get_entity_count_simple('DASHBOARD'))
        total_charts.set(get_entity_count_simple('CHART'))
        total_data_jobs.set(get_entity_count_simple('DATA_JOB'))
        total_users.set(get_entity_count_simple('CORP_USER'))
        total_groups.set(get_entity_count_simple('CORP_GROUP'))

        if total > 0:
            completeness = (total - max(without_owner, without_desc, without_tags, without_glossary)) / total
            metadata_completeness_ratio.set(completeness)
            documentation_coverage_ratio.set((total - without_desc) / total)

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
    log.info("DataHub Prometheus Exporter (Extended Version)")
    log.info("=" * 70)
    log.info(f"GMS URL: {GMS_URL}")
    log.info(f"Scrape interval: {SCRAPE_INTERVAL}s")
    log.info(f"Metrics port: 9105")
    log.info("=" * 70)
    
    start_http_server(9105)
    log.info("✅ Metrics server started at http://localhost:9105/metrics")

    total_datasets.set(0)
    last_scrape_success.set(0)

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
