import os
import time
import logging
import base64
import requests
from prometheus_client import start_http_server, Gauge, Counter

# 로깅 설정 (더 상세하게)
log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,  # ✅ DEBUG 레벨로 변경
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

# ✅ 에러 카운터 추가
scrape_errors = Counter('datahub_scrape_errors_total', 'Total number of scrape errors')

# Prometheus Metrics 정의
total_datasets = Gauge('datahub_dataset_total', '전체 데이터셋 수')
desc_filled_total = Gauge('datahub_dataset_with_description', '설명이 있는 데이터셋 수')
owner_filled_total = Gauge('datahub_dataset_with_owner', '소유자가 있는 데이터셋 수')
tag_filled_total = Gauge('datahub_dataset_with_tags', '태그가 있는 데이터셋 수')
datasets_without_owner = Gauge('datahub_dataset_without_owner', '소유자가 없는 데이터셋 수')
datasets_without_description = Gauge('datahub_dataset_without_description', '설명이 없는 데이터셋 수')
datasets_without_tags = Gauge('datahub_dataset_without_tags', '태그가 없는 데이터셋 수')
metadata_completeness_ratio = Gauge('datahub_metadata_completeness_ratio', '메타데이터 완성도 비율')
last_scrape_success = Gauge('datahub_last_scrape_success', '마지막 수집 성공 여부 (1=성공, 0=실패)')


def _auth_header():
    """인증 헤더 생성 (Basic Auth)"""
    client_id = os.getenv('DATAHUB_CLIENT_ID')
    client_secret = os.getenv('DATAHUB_CLIENT_SECRET')
    
    headers = {"Content-Type": "application/json"}
    
    if client_id and client_secret:
        token = f"{client_id}:{client_secret}"
        b64_token = base64.b64encode(token.encode()).decode()
        headers["Authorization"] = f"Basic {b64_token}"
        log.debug("Using authentication")
    else:
        log.debug("No authentication configured (this is OK for default DataHub setup)")
    
    return headers


def _post(query: str, variables: dict = None):
    """GraphQL 쿼리 실행"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            
            log.debug(f"Sending GraphQL request to {GMS_URL}/api/graphql")
            log.debug(f"Query: {query[:200]}...")  # 쿼리 일부만 로깅
            
            resp = requests.post(
                f"{GMS_URL}/api/graphql",
                json=payload,
                headers=_auth_header(),
                timeout=30
            )
            
            log.debug(f"Response status code: {resp.status_code}")
            
            resp.raise_for_status()
            
            result = resp.json()
            
            # GraphQL 에러 체크
            if "errors" in result:
                log.error(f"GraphQL errors: {result['errors']}")
                scrape_errors.inc()
                return None
            
            log.debug(f"Successfully received response")
            return result.get("data")
        
        except requests.exceptions.ConnectionError as e:
            log.error(f"Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
            log.error(f"Cannot connect to {GMS_URL} - check if datahub-gms is running and network is correct")
            scrape_errors.inc()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        
        except requests.exceptions.Timeout as e:
            log.warning(f"Request timeout (attempt {attempt}/{MAX_RETRIES}): {e}")
            scrape_errors.inc()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        
        except requests.exceptions.HTTPError as e:
            log.error(f"HTTP error (attempt {attempt}/{MAX_RETRIES}): {e}")
            log.error(f"Response content: {resp.text[:500]}")
            scrape_errors.inc()
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        
        except Exception as e:
            log.error(f"Unexpected error (attempt {attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            scrape_errors.inc()
            return None
    
    log.error(f"Failed after {MAX_RETRIES} retries")
    return None


def test_connection():
    """DataHub GMS 연결 테스트"""
    log.info("Testing connection to DataHub GMS...")
    
    try:
        # 간단한 health check
        resp = requests.get(f"{GMS_URL}/health", timeout=10)
        log.info(f"Health check response: {resp.status_code}")
        
        if resp.status_code == 200:
            log.info("✅ DataHub GMS is reachable")
            return True
        else:
            log.warning(f"⚠️ DataHub GMS returned status {resp.status_code}")
            return False
    
    except requests.exceptions.ConnectionError as e:
        log.error(f"❌ Cannot connect to DataHub GMS at {GMS_URL}")
        log.error(f"Error: {e}")
        log.error("Check: 1) Is datahub-gms container running? 2) Are containers on same network?")
        return False
    
    except Exception as e:
        log.error(f"❌ Unexpected error during connection test: {e}")
        return False


def get_dataset_count():
    """전체 데이터셋 수만 가져오기"""
    query = """
    query {
      search(
        input: {
          type: DATASET
          query: "*"
          start: 0
          count: 1
        }
      ) {
        total
      }
    }
    """
    
    log.info("Fetching total dataset count...")
    payload = _post(query)
    
    if payload and 'search' in payload:
        count = payload['search']['total']
        log.info(f"✅ Total datasets: {count}")
        return count
    else:
        log.error("❌ Failed to fetch dataset count")
        return 0


def get_datasets_without_owners():
    """소유자가 없는 데이터셋 수"""
    query = """
    query {
      search(
        input: {
          type: DATASET
          query: "*"
          filters: [
            {
              field: "hasOwners"
              values: ["false"]
            }
          ]
          start: 0
          count: 1
        }
      ) {
        total
      }
    }
    """
    
    log.info("Fetching datasets without owners...")
    payload = _post(query)
    
    if payload and 'search' in payload:
        count = payload['search']['total']
        log.info(f"✅ Datasets without owners: {count}")
        return count
    else:
        log.error("❌ Failed to fetch datasets without owners")
        return 0


def get_datasets_without_description():
    """설명이 없는 데이터셋 수"""
    query = """
    query {
      search(
        input: {
          type: DATASET
          query: "*"
          filters: [
            {
              field: "hasDescription"
              values: ["false"]
            }
          ]
          start: 0
          count: 1
        }
      ) {
        total
      }
    }
    """
    
    log.info("Fetching datasets without description...")
    payload = _post(query)
    
    if payload and 'search' in payload:
        count = payload['search']['total']
        log.info(f"✅ Datasets without description: {count}")
        return count
    else:
        log.error("❌ Failed to fetch datasets without description")
        return 0


def get_datasets_without_tags():
    """태그가 없는 데이터셋 수"""
    query = """
    query {
      search(
        input: {
          type: DATASET
          query: "*"
          filters: [
            {
              field: "hasTags"
              values: ["false"]
            }
          ]
          start: 0
          count: 1
        }
      ) {
        total
      }
    }
    """
    
    log.info("Fetching datasets without tags...")
    payload = _post(query)
    
    if payload and 'search' in payload:
        count = payload['search']['total']
        log.info(f"✅ Datasets without tags: {count}")
        return count
    else:
        log.error("❌ Failed to fetch datasets without tags")
        return 0


def collect_metrics():
    """메트릭 수집 및 업데이트"""
    log.info("=" * 60)
    log.info("Starting metric collection...")
    log.info("=" * 60)
    
    try:
        # 1. 전체 데이터셋 수
        total = get_dataset_count()
        total_datasets.set(total)
        
        # 2. 소유자 없는 데이터셋
        without_owner = get_datasets_without_owners()
        datasets_without_owner.set(without_owner)
        owner_filled_total.set(max(0, total - without_owner))
        
        # 3. 설명 없는 데이터셋
        without_desc = get_datasets_without_description()
        datasets_without_description.set(without_desc)
        desc_filled_total.set(max(0, total - without_desc))
        
        # 4. 태그 없는 데이터셋
        without_tags = get_datasets_without_tags()
        datasets_without_tags.set(without_tags)
        tag_filled_total.set(max(0, total - without_tags))
        
        # 5. 메타데이터 완성도 계산
        if total > 0:
            max_incomplete = max(without_owner, without_desc, without_tags)
            completeness = (total - max_incomplete) / total
            metadata_completeness_ratio.set(completeness)
            log.info(f"📊 Metadata completeness ratio: {completeness:.2%}")
        
        last_scrape_success.set(1)
        log.info("=" * 60)
        log.info("✅ Metric collection completed successfully")
        log.info("=" * 60)
        
    except Exception as e:
        log.error(f"❌ Error during metric collection: {e}", exc_info=True)
        last_scrape_success.set(0)
        scrape_errors.inc()


if __name__ == "__main__":
    log.info("=" * 60)
    log.info("DataHub Prometheus Exporter Starting...")
    log.info("=" * 60)
    log.info(f"GMS URL: {GMS_URL}")
    log.info(f"Scrape interval: {SCRAPE_INTERVAL} seconds")
    log.info(f"Metrics port: 9105")
    log.info("=" * 60)
    
    # 연결 테스트
    if not test_connection():
        log.error("Cannot connect to DataHub GMS. Exiting...")
        exit(1)
    
    # Prometheus HTTP 서버 시작
    start_http_server(9105)
    log.info("✅ Prometheus metrics server started on port 9105")
    log.info(f"   Metrics available at http://localhost:9105/metrics")
    
    # 초기 메트릭 값 설정
    total_datasets.set(0)
    desc_filled_total.set(0)
    owner_filled_total.set(0)
    tag_filled_total.set(0)
    datasets_without_owner.set(0)
    datasets_without_description.set(0)
    datasets_without_tags.set(0)
    metadata_completeness_ratio.set(0)
    last_scrape_success.set(0)
    
    log.info("✅ Initial metrics set")
    
    # 즉시 첫 수집 실행
    log.info("Running initial metric collection...")
    try:
        collect_metrics()
    except Exception as e:
        log.error(f"Error in initial collection: {e}", exc_info=True)
    
    # 주기적으로 메트릭 수집
    while True:
        try:
            log.info(f"Sleeping for {SCRAPE_INTERVAL} seconds...")
            time.sleep(SCRAPE_INTERVAL)
            collect_metrics()
        except KeyboardInterrupt:
            log.info("Received interrupt signal. Shutting down...")
            break
        except Exception as e:
            log.error(f"Error in main loop: {e}", exc_info=True)
    log.info("DataHub Prometheus Exporter stopped.")