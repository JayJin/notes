import os
import time
import logging
import base64
import requests
from prometheus_client import start_http_server, Gauge

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

# Prometheus Metrics 정의
total_datasets = Gauge('datahub_dataset_total', '전체 데이터셋 수')
desc_filled_total = Gauge('datahub_dataset_with_description', '설명이 있는 데이터셋 수')
owner_filled_total = Gauge('datahub_dataset_with_owner', '소유자가 있는 데이터셋 수')
tag_filled_total = Gauge('datahub_dataset_with_tags', '태그가 있는 데이터셋 수')
datasets_without_owner = Gauge('datahub_dataset_without_owner', '소유자가 없는 데이터셋 수')
datasets_without_description = Gauge('datahub_dataset_without_description', '설명이 없는 데이터셋 수')
datasets_without_tags = Gauge('datahub_dataset_without_tags', '태그가 없는 데이터셋 수')
metadata_completeness_ratio = Gauge('datahub_metadata_completeness_ratio', '메타데이터 완성도 비율')


def _auth_header():
    """인증 헤더 생성 (Basic Auth)"""
    client_id = os.getenv('DATAHUB_CLIENT_ID')
    client_secret = os.getenv('DATAHUB_CLIENT_SECRET')
    
    headers = {"Content-Type": "application/json"}
    
    # 인증 정보가 있는 경우에만 추가
    if client_id and client_secret:
        token = f"{client_id}:{client_secret}"
        b64_token = base64.b64encode(token.encode()).decode()
        headers["Authorization"] = f"Basic {b64_token}"
        log.info("Using authentication")
    else:
        log.info("No authentication configured")
    
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
            
            # GraphQL 에러 체크
            if "errors" in result:
                log.error(f"GraphQL errors: {result['errors']}")
                return None
            
            return result.get("data")
        
        except requests.exceptions.Timeout:
            log.warning(f"Request timeout (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            log.error(f"Request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            return None
    
    log.error(f"Failed after {MAX_RETRIES} retries")
    return None


def get_all_datasets():
    """모든 데이터셋 정보 가져오기 (페이징)"""
    start = 0
    all_entities = []
    
    while True:
        query = f"""
        query {{
          search(
            input: {{
              type: DATASET
              query: "*"
              start: {start}
              count: {PAGE_SIZE}
            }}
          ) {{
            total
            searchResults {{
              entity {{
                ... on Dataset {{
                  urn
                  name
                  platform {{
                    name
                  }}
                  properties {{
                    description
                  }}
                  ownership {{
                    owners {{
                      owner {{
                        ... on CorpUser {{
                          username
                        }}
                      }}
                    }}
                  }}
                  globalTags {{
                    tags {{
                      tag {{
                        name
                      }}
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        payload = _post(query)
        
        if not payload or 'search' not in payload:
            log.error("Failed to fetch datasets")
            break
        
        search_result = payload['search']
        total = search_result['total']
        results = search_result['searchResults']
        
        log.info(f"Fetched {len(results)} datasets (offset {start}, total {total})")
        
        all_entities.extend(results)
        
        # 더 이상 가져올 데이터가 없으면 종료
        if start + len(results) >= total:
            break
        
        start += PAGE_SIZE
    
    return all_entities


def get_dataset_count():
    """전체 데이터셋 수만 가져오기 (빠른 조회)"""
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
    
    payload = _post(query)
    
    if payload and 'search' in payload:
        return payload['search']['total']
    
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
    
    payload = _post(query)
    
    if payload and 'search' in payload:
        return payload['search']['total']
    
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
    
    payload = _post(query)
    
    if payload and 'search' in payload:
        return payload['search']['total']
    
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
    
    payload = _post(query)
    
    if payload and 'search' in payload:
        return payload['search']['total']
    
    return 0


def collect_metrics():
    """메트릭 수집 및 업데이트"""
    log.info("Starting metric collection...")
    
    try:
        # 1. 전체 데이터셋 수 (빠른 조회)
        total = get_dataset_count()
        total_datasets.set(total)
        log.info(f"Total datasets: {total}")
        
        # 2. 소유자 없는 데이터셋
        without_owner = get_datasets_without_owners()
        datasets_without_owner.set(without_owner)
        owner_filled_total.set(total - without_owner)
        log.info(f"Datasets without owner: {without_owner}")
        
        # 3. 설명 없는 데이터셋
        without_desc = get_datasets_without_description()
        datasets_without_description.set(without_desc)
        desc_filled_total.set(total - without_desc)
        log.info(f"Datasets without description: {without_desc}")
        
        # 4. 태그 없는 데이터셋
        without_tags = get_datasets_without_tags()
        datasets_without_tags.set(without_tags)
        tag_filled_total.set(total - without_tags)
        log.info(f"Datasets without tags: {without_tags}")
        
        # 5. 메타데이터 완성도 계산
        # (소유자 + 설명 + 태그 모두 있는 데이터셋 비율)
        if total > 0:
            # 가장 많이 누락된 항목을 기준으로 불완전한 데이터셋 추정
            max_incomplete = max(without_owner, without_desc, without_tags)
            completeness = (total - max_incomplete) / total
            metadata_completeness_ratio.set(completeness)
            log.info(f"Metadata completeness ratio: {completeness:.2%}")
        
        log.info("Metric collection completed successfully")
        
    except Exception as e:
        log.error(f"Error during metric collection: {e}")


if __name__ == "__main__":
    log.info(f"Starting DataHub Prometheus Exporter")
    log.info(f"GMS URL: {GMS_URL}")
    log.info(f"Scrape interval: {SCRAPE_INTERVAL} seconds")
    log.info(f"Starting Prometheus server on port 9105")
    
    # Prometheus HTTP 서버 시작
    start_http_server(9105)
    
    log.info("Exporter started successfully")
    
    # 주기적으로 메트릭 수집
    while True:
        try:
            collect_metrics()
        except Exception as e:
            log.error(f"Error in main loop: {e}")
        
        log.info(f"Sleeping for {SCRAPE_INTERVAL} seconds...")
        time.sleep(SCRAPE_INTERVAL)
