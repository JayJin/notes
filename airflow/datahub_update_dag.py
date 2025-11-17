from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import requests
import logging

log = logging.getLogger(__name__)
DATAHUB_GMS_URL = "http://datahub-gms:8080"

def update_descriptions_from_csv():
    """CSV에서 메타데이터 업데이트"""
    csv_file = "/tmp/update_descriptions.csv"
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_type = row['type'].lower()
            urn = row['urn']
            description = row['description']
            
            if row_type == 'dataset':
                query = f"""
                mutation {{
                  updateDataset(
                    urn: "{urn}"
                    input: {{
                      editableProperties: {{
                        description: "{description.replace('"', '\\"')}"
                      }}
                    }}
                  ) {{ urn }}
                }}
                """
            else:
                field_path = row['field_path']
                query = f"""
                mutation {{
                  updateDataset(
                    urn: "{urn}"
                    input: {{
                      editableSchemaMetadata: {{
                        editableSchemaFieldInfo: [
                          {{
                            fieldPath: "{field_path}"
                            description: "{description.replace('"', '\\"')}"
                          }}
                        ]
                      }}
                    }}
                  ) {{ urn }}
                }}
                """
            
            resp = requests.post(
                f"{DATAHUB_GMS_URL}/api/graphql",
                json={"query": query}
            )
            log.info(f"Updated {urn}: {resp.status_code}")

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    'datahub_bulk_update_descriptions',
    default_args=default_args,
    schedule_interval=None,  # 수동 트리거
    catchup=False,
) as dag:
    
    update_task = PythonOperator(
        task_id='update_descriptions',
        python_callable=update_descriptions_from_csv,
    )
    
    update_task
