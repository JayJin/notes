import os
import time
import logging
import base64
import requests
from prometheus_client import start_http_server, Gauge, Counter

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

scrape_errors = Counter('datahub_scrape_errors_total', 'Total scrape errors')

# --------------------------------------------------------------
# DataHub API (이미 작성된 함수들이 여기 들어옴)
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

    resp = requests.post(f"{GMS_URL}/api/graphql", json={"query": query})
    
    if resp.status_code != 200:
        log.error(f"Failed to list datasets: {resp.text}")
        scrape_errors.inc()
        return []

    urns = resp.json().get('data', {}).get('listUrns', [])

    results = set()

    for urn in urns:
        # 예시: urn:li:dataset:(urn:li:dataPlatform:oracle,HR.EMPLOYEES,PROD)
        try:
            inside = urn.split("(")[1].split(")")[0]
            platform_part, name_part, _ = inside.split(",")

            platform = platform_part.replace("urn:li:dataPlatform:", "")
            
            # Oracle/Postgres에서는 보통 name_part = "SCHEMA.TABLE"
            if "." in name_part:
                schema = name_part.split(".")[0]
                results.add((platform, schema))
        except Exception:
            continue

    log.info(f"Detected {len(results)} schemas: {results}")
    return list(results)


# def get_datasets_by_platform_and_schema(platform, schema):
def get_datasets_by_platform_and_schema(platform, schema_name):
    """
    특정 platform/schema의 모든 Dataset metadata 전체 조회
    """
    query = """
    query search($input: SearchInput!) {
      search(input: $input) {
        entities {
          urn
          ... on Dataset {
            properties { description }
            editableProperties { description }
            ownership { owners { owner { urn } } }
            tags { tags { tag { urn } } }
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

    variables = {
        "input": {
            "type": "DATASET",
            "query": f"*",
            "filters": [
                {"field": "platform", "value": platform},
                {"field": "schema", "value": schema_name}
            ],
            "start": 0,
            "count": 5000
        }
    }

    url = f"{GMS_URL}/api/graphql"
    resp = requests.post(url, json={"query": query, "variables": variables})

    if resp.status_code != 200:
        log.error(f"Failed dataset search: {resp.text}")
        scrape_errors.inc()
        return []

    return resp.json().get("data", {}).get("search", {}).get("entities", [])


def analyze_schema_metrics(platform, schema_name):
    """ 특정 스키마의 테이블 및 컬럼 메트릭 분석 """ 
    log.info(f"Analyzing metrics for platform={platform}, schema={schema_name}...") 
    datasets = get_datasets_by_platform_and_schema(platform, schema_name) 
    if not datasets: 
        log.warning(f"No datasets found for {platform}/{schema_name}") # 메트릭을 0으로 설정 
        schema_table_count.labels(schema=schema_name, platform=platform).set(0) 
        schema_table_with_desc.labels(schema=schema_name, platform=platform).set(0) 
        schema_table_without_desc.labels(schema=schema_name, platform=platform).set(0) 
        schema_table_desc_ratio.labels(schema=schema_name, platform=platform).set(0) 
        schema_column_count.labels(schema=schema_name, platform=platform).set(0) 
        schema_column_with_desc.labels(schema=schema_name, platform=platform).set(0) 
        schema_column_without_desc.labels(schema=schema_name, platform=platform).set(0) 
        schema_column_desc_ratio.labels(schema=schema_name, platform=platform).set(0) 
        return # 첫 번째 데이터셋 로깅 (디버깅용) 
    
    if datasets: 
        log.info(f" Sample dataset name: {datasets[0].get('name')}") 
        
    table_count = len(datasets) 
    table_with_desc = 0 
    table_without_desc = 0 
    total_columns = 0 
    columns_with_desc = 0 
    columns_without_desc = 0 
    for dataset in datasets: # 테이블 설명 확인 
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
            table_without_desc += 1 # 컬럼 설명 확인 
            
        schema_metadata = dataset.get('schemaMetadata') 
        if schema_metadata and schema_metadata.get('fields'): 
            for field in schema_metadata['fields']: 
                total_columns += 1 
                field_desc = field.get('description', '').strip() 
                if field_desc: 
                    columns_with_desc += 1 
                else: 
                    columns_without_desc += 1 # 메트릭 설정 
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
     
    log.info(f" [{schema_name}] Tables: {table_count}, with desc: {table_with_desc} ({table_desc_ratio:.1f}%)") 
    log.info(f" [{schema_name}] Columns: {total_columns}, with desc: {columns_with_desc} ({column_desc_ratio:.1f}%)")   
    

# --------------------------------------------------------------
# Main Loop
# --------------------------------------------------------------
def main():
    log.info("Starting DataHub exporter...")
    start_http_server(8000)

    while True:
        try:
            # 1. 사용 가능한 플랫폼/스키마 자동 탐색
            schemas = get_all_schemas()  # 이 함수는 앞선 코드에서 이미 만들어 둠

            # 2. 각 플랫폼/스키마별 메트릭 분석 실행
            for platform, schema_name in schemas:
                analyze_schema_metrics(platform, schema_name)

        except Exception as e:
            log.exception("Scrape failed")
            scrape_errors.inc()

        time.sleep(SCRAPE_INTERVAL)


# --------------------------------------------------------------
# Entry Point
# --------------------------------------------------------------
if __name__ == "__main__":
    main()
