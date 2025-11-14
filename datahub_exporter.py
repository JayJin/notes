import os
import time
import logging
import requests
from prometheus_client import start_http_server, Gauge

# --------------------------------------------------------------
# logging
# --------------------------------------------------------------
log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --------------------------------------------------------------
# env
# --------------------------------------------------------------
DATAHUB_GMS_HOST = os.getenv('DATAHUB_GMS_HOST', 'datahub-gms')
DATAHUB_GMS_PORT = os.getenv('DATAHUB_GMS_PORT', '8080')
GMS_URL = f"http://{DATAHUB_GMS_HOST}:{DATAHUB_GMS_PORT}"
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '60'))

# --------------------------------------------------------------
# Prometheus metrics 정의
# --------------------------------------------------------------
schema_table_count = Gauge('datahub_schema_table_count', 'Total tables per schema', ['schema', 'platform'])
schema_table_with_desc = Gauge('datahub_schema_table_with_desc', 'Tables with description', ['schema', 'platform'])
schema_table_without_desc = Gauge('datahub_schema_table_without_desc', 'Tables without description', ['schema', 'platform'])
schema_table_desc_ratio = Gauge('datahub_schema_table_desc_ratio', 'Table description ratio (%)', ['schema', 'platform'])

schema_column_count = Gauge('datahub_schema_column_count', 'Columns per schema', ['schema', 'platform'])
schema_column_with_desc = Gauge('datahub_schema_column_with_desc', 'Columns with description', ['schema', 'platform'])
schema_column_without_desc = Gauge('datahub_schema_column_without_desc', 'Columns without description', ['schema', 'platform'])
schema_column_desc_ratio = Gauge('datahub_schema_column_desc_ratio', 'Column description ratio (%)', ['schema', 'platform'])

schema_tables_with_owner = Gauge('datahub_schema_table_with_owner', 'Tables with owner', ['schema', 'platform'])
schema_tables_without_owner = Gauge('datahub_schema_table_without_owner', 'Tables without owner', ['schema', 'platform'])

schema_tables_with_tag = Gauge('datahub_schema_table_with_tag', 'Tables with tags', ['schema', 'platform'])
schema_tables_without_tag = Gauge('datahub_schema_table_without_tag', 'Tables without tags', ['schema', 'platform'])

# --------------------------------------------------------------
# DataHub API
# --------------------------------------------------------------

def get_all_schemas():
    """
    DataHub에서 ingestion된 모든 dataset의 platform/schema 목록을 자동으로 추출한다.
    반환 형태: [(platform, schema_name), ...]
    """
    log.info("Fetching all schemas from DataHub...")

    query = """
    query listDatasets {
      listUrns(input: { type: DATASET, start: 0, count: 99999 })
    }
    """

    try:
        resp = requests.post(f"{GMS_URL}/api/graphql", json={"query": query}, timeout=30)
        
        if resp.status_code != 200:
            log.error(f"Failed to list datasets: {resp.status_code} - {resp.text}")
            return []

        # None 체크 추가
        response_json = resp.json()
        if response_json is None:
            log.error("API returned None response")
            return []
        
        data = response_json.get('data')
        if data is None:
            log.error("API response has no 'data' field")
            return []
            
        urns = data.get('listUrns', [])
        if urns is None:
            urns = []
            
        log.info(f"Retrieved {len(urns)} dataset URNs")

        results = set()

        for urn in urns:
            # 예시: urn:li:dataset:(urn:li:dataPlatform:oracle,HR.EMPLOYEES,PROD)
            try:
                if not urn or not isinstance(urn, str):
                    continue
                    
                if "(" not in urn or ")" not in urn:
                    continue
                    
                inside = urn.split("(")[1].split(")")[0]
                parts = inside.split(",")
                
                if len(parts) < 2:
                    continue
                    
                platform_part = parts[0]
                name_part = parts[1]

                platform = platform_part.replace("urn:li:dataPlatform:", "")
                
                # Oracle/Postgres에서는 보통 name_part = "SCHEMA.TABLE"
                if "." in name_part:
                    schema = name_part.split(".")[0]
                    results.add((platform, schema))
                    
            except Exception as e:
                log.debug(f"Failed to parse URN {urn}: {e}")
                continue

        log.info(f"Detected {len(results)} schemas: {sorted(results)}")
        return list(results)
        
    except requests.exceptions.RequestException as e:
        log.error(f"Network error fetching schemas: {e}")
        return []
    except Exception as e:
        log.error(f"Error fetching schemas: {e}", exc_info=True)
        return []


def get_datasets_by_platform_and_schema(platform, schema_name):
    """
    특정 platform/schema의 모든 Dataset metadata 전체 조회
    schema 필터는 지원되지 않으므로 platform으로만 필터링 후 client-side에서 schema로 필터링
    """
    log.info(f"Fetching datasets for platform={platform}, schema={schema_name}")
    
    query = """
    query search($input: SearchInput!) {
      search(input: $input) {
        total
        entities {
          urn
          ... on Dataset {
            properties { 
              description
              name
            }
            editableProperties { description }
            ownership { 
              owners { 
                owner { 
                  urn 
                } 
              } 
            }
            tags { 
              tags { 
                tag { 
                  urn 
                } 
              } 
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
    """

    # DataHub GraphQL API는 values를 배열로 요구함
    variables = {
        "input": {
            "type": "DATASET",
            "query": "*",
            "filters": [
                {
                    "field": "platform",
                    "values": [f"urn:li:dataPlatform:{platform}"]
                }
            ],
            "start": 0,
            "count": 10000
        }
    }

    try:
        url = f"{GMS_URL}/api/graphql"
        resp = requests.post(url, json={"query": query, "variables": variables}, timeout=30)

        if resp.status_code != 200:
            log.error(f"Failed dataset search for {platform}: {resp.status_code} - {resp.text[:500]}")
            return []

        # None 체크 추가
        response_json = resp.json()
        if response_json is None:
            log.error(f"API returned None response for {platform}/{schema_name}")
            return []
        
        data = response_json.get("data")
        if data is None:
            log.error(f"API response has no 'data' field for {platform}/{schema_name}")
            log.error(f"Full response: {response_json}")
            return []
            
        search_result = data.get("search")
        if search_result is None:
            log.error(f"API response has no 'search' field for {platform}/{schema_name}")
            return []
            
        total = search_result.get("total", 0)
        all_entities = search_result.get("entities")
        
        if all_entities is None:
            all_entities = []
        
        log.info(f"Platform {platform}: Retrieved {len(all_entities)} datasets (total: {total})")

        # Client-side 필터링: schema로 필터
        filtered = []
        for entity in all_entities:
            if entity is None:
                continue
                
            urn = entity.get("urn", "")
            if not urn:
                continue
            
            # URN 형식: urn:li:dataset:(urn:li:dataPlatform:oracle,SCHEMA.TABLE,PROD)
            try:
                if "(" not in urn or ")" not in urn:
                    continue
                    
                inside = urn.split("(")[1].split(")")[0]
                parts = inside.split(",")
                
                if len(parts) < 2:
                    continue
                    
                dataset_name = parts[1]  # "SCHEMA.TABLE"
                
                # Schema 이름이 일치하는지 확인
                if "." in dataset_name:
                    dataset_schema = dataset_name.split(".")[0]
                    if dataset_schema.upper() == schema_name.upper():
                        filtered.append(entity)
                        
            except (IndexError, AttributeError) as e:
                log.debug(f"Failed to parse URN {urn}: {e}")
                continue

        log.info(f"Filtered to {len(filtered)} datasets for schema={schema_name}")
        return filtered
        
    except requests.exceptions.RequestException as e:
        log.error(f"Network error querying datasets: {e}")
        return []
    except Exception as e:
        log.error(f"Error querying datasets for {platform}/{schema_name}: {e}", exc_info=True)
        return []


def analyze_schema_metrics(platform, schema_name):
    """특정 스키마의 테이블 및 컬럼 메트릭 분석"""
    log.info(f"Analyzing metrics for platform={platform}, schema={schema_name}...")
    
    try:
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
            schema_tables_with_owner.labels(schema=schema_name, platform=platform).set(0)
            schema_tables_without_owner.labels(schema=schema_name, platform=platform).set(0)
            schema_tables_with_tag.labels(schema=schema_name, platform=platform).set(0)
            schema_tables_without_tag.labels(schema=schema_name, platform=platform).set(0)
            return

        # 첫 번째 데이터셋 로깅 (디버깅용)
        if datasets:
            first_dataset = datasets[0]
            log.info(f"Sample dataset URN: {first_dataset.get('urn')}")

        table_count = len(datasets)
        table_with_desc = 0
        table_without_desc = 0
        
        tables_with_owner = 0
        tables_without_owner = 0
        
        tables_with_tag = 0
        tables_without_tag = 0
        
        total_columns = 0
        columns_with_desc = 0
        columns_without_desc = 0

        for dataset in datasets:
            if dataset is None:
                continue
                
            # 테이블 설명 확인
            has_table_desc = False

            # properties.description 또는 editableProperties.description 확인
            properties = dataset.get('properties')
            if properties and properties.get('description'):
                desc = properties['description'].strip()
                if desc:
                    has_table_desc = True

            editable_props = dataset.get('editableProperties')
            if not has_table_desc and editable_props and editable_props.get('description'):
                desc = editable_props['description'].strip()
                if desc:
                    has_table_desc = True

            if has_table_desc:
                table_with_desc += 1
            else:
                table_without_desc += 1

            # Owner 확인
            has_owner = False
            ownership = dataset.get('ownership')
            if ownership:
                owners = ownership.get('owners')
                if owners and isinstance(owners, list) and len(owners) > 0:
                    has_owner = True
            
            if has_owner:
                tables_with_owner += 1
            else:
                tables_without_owner += 1

            # Tag 확인
            has_tag = False
            tags_data = dataset.get('tags')
            if tags_data:
                tags = tags_data.get('tags')
                if tags and isinstance(tags, list) and len(tags) > 0:
                    has_tag = True
            
            if has_tag:
                tables_with_tag += 1
            else:
                tables_without_tag += 1

            # 컬럼 설명 확인
            schema_metadata = dataset.get('schemaMetadata')
            if schema_metadata:
                fields = schema_metadata.get('fields')
                if fields and isinstance(fields, list):
                    for field in fields:
                        if field is None:
                            continue
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

        schema_tables_with_owner.labels(schema=schema_name, platform=platform).set(tables_with_owner)
        schema_tables_without_owner.labels(schema=schema_name, platform=platform).set(tables_without_owner)

        schema_tables_with_tag.labels(schema=schema_name, platform=platform).set(tables_with_tag)
        schema_tables_without_tag.labels(schema=schema_name, platform=platform).set(tables_without_tag)

        log.info(f"[{schema_name}] Tables: {table_count}, with desc: {table_with_desc} ({table_desc_ratio:.1f}%)")
        log.info(f"[{schema_name}] Columns: {total_columns}, with desc: {columns_with_desc} ({column_desc_ratio:.1f}%)")
        log.info(f"[{schema_name}] Owners: {tables_with_owner}/{table_count}, Tags: {tables_with_tag}/{table_count}")
        
    except Exception as e:
        log.error(f"Error analyzing metrics for {platform}/{schema_name}: {e}", exc_info=True)


# --------------------------------------------------------------
# Main Loop
# --------------------------------------------------------------
def main():
    log.info("Starting DataHub exporter...")
    start_http_server(8000)
    log.info("Prometheus metrics server started on port 8000")

    while True:
        try:
            log.info("=" * 60)
            log.info("Starting new scrape cycle...")
            
            # 1. 사용 가능한 플랫폼/스키마 자동 탐색
            schemas = get_all_schemas()

            if not schemas:
                log.warning("No schemas detected. Will retry in next cycle.")
            else:
                # 2. 각 플랫폼/스키마별 메트릭 분석 실행
                for platform, schema_name in schemas:
                    try:
                        analyze_schema_metrics(platform, schema_name)
                    except Exception as e:
                        log.error(f"Failed to analyze {platform}/{schema_name}: {e}", exc_info=True)

            log.info(f"Scrape cycle completed. Sleeping for {SCRAPE_INTERVAL} seconds...")

        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)

        time.sleep(SCRAPE_INTERVAL)


# --------------------------------------------------------------
# Entry Point
# --------------------------------------------------------------
if __name__ == "__main__":
    main()
