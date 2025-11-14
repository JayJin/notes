import os
import time
import requests
from prometheus_client import Gauge, start_http_server

DATAHUB_URL = os.getenv("DATAHUB_URL")
DATAHUB_TOKEN = os.getenv("DATAHUB_TOKEN")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {DATAHUB_TOKEN}"
}

# -----------------------------
# GraphQL Helper
# -----------------------------
def gql(query, variables=None):
    payload = {"query": query, "variables": variables}
    res = requests.post(DATAHUB_URL, json=payload, headers=HEADERS)
    res.raise_for_status()
    return res.json()["data"]

# -----------------------------
# Prometheus Metrics
# -----------------------------
table_count_metric = Gauge("datahub_schema_table_count", "Table count", ["platform", "schema"])
col_count_metric = Gauge("datahub_schema_column_count", "Column count", ["platform", "schema"])
table_desc_metric = Gauge("datahub_schema_table_with_desc", "Tables with description", ["platform", "schema"])
col_desc_metric = Gauge("datahub_schema_column_with_desc", "Columns with description", ["platform", "schema"])

# -----------------------------
# Browse Query (Schema 탐색)
# -----------------------------
LIST_SCHEMAS_QUERY = """
query listSchemas($path: String!) {
  browse(path: $path) {
    groups {
      name
      path
    }
  }
}
"""

# -----------------------------
# Browse Query (Table 탐색)
# -----------------------------
LIST_TABLES_QUERY = """
query listTables($path: String!) {
  browse(path: $path) {
    entities {
      urn
      name
    }
  }
}
"""

# -----------------------------
# Dataset 상세 조회
# -----------------------------
DATASET_DETAIL_QUERY = """
query datasetDetails($urn: String!) {
  dataset(urn: $urn) {
    properties { description }
    editableProperties { description }
    schemaMetadata {
      fields {
        fieldPath
        description
      }
    }
  }
}
"""

# -----------------------------
# Schema 분석 함수
# -----------------------------
def analyze_platform(platform, platform_path):
    print(f"\n🔍 Processing platform: {platform}")

    # 1) 스키마 자동 조회
    schemas = gql(LIST_SCHEMAS_QUERY, {"path": platform_path})["browse"]["groups"]

    for schema in schemas:
        schema_name = schema["name"]
        schema_path = schema["path"]

        print(f"  - Schema: {schema_name}")

        # 2) 해당 스키마의 테이블 자동 조회
        tables = gql(LIST_TABLES_QUERY, {"path": schema_path})["browse"]["entities"]

        table_count = len(tables)
        col_count = 0
        table_desc_count = 0
        col_desc_count = 0

        # 3) 각 테이블의 메타데이터 조회
        for t in tables:
            urn = t["urn"]

            detail = gql(DATASET_DETAIL_QUERY, {"urn": urn})["dataset"]
            if not detail:
                continue

            # 테이블 description
            table_desc = (
                (detail.get("properties") or {}).get("description")
                or (detail.get("editableProperties") or {}).get("description")
            )

            if table_desc:
                table_desc_count += 1

            # 컬럼 description
            schema_meta = detail.get("schemaMetadata")
            if schema_meta and schema_meta.get("fields"):
                fields = schema_meta["fields"]
                col_count += len(fields)
                col_desc_count += sum(1 for f in fields if f.get("description"))

        # Prometheus Metric 업데이트
        table_count_metric.labels(platform=platform, schema=schema_name).set(table_count)
        col_count_metric.labels(platform=platform, schema=schema_name).set(col_count)
        table_desc_metric.labels(platform=platform, schema=schema_name).set(table_desc_count)
        col_desc_metric.labels(platform=platform, schema=schema_name).set(col_desc_count)

        print(f"    Tables={table_count}, Cols={col_count}, T_desc={table_desc_count}, C_desc={col_desc_count}")

# -----------------------------
# Main Loop
# -----------------------------
if __name__ == "__main__":
    start_http_server(8000)
    print("Exporter running at :8000/metrics")

    while True:
        # Oracle
        analyze_platform("oracle", "/platform/oracle")

        # PostgreSQL
        analyze_platform("postgres", "/platform/postgres")

        time.sleep(300)
