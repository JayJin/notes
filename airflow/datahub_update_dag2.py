
import os
import csv
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import sys

# 표준출력으로도 로그 출력 (Airflow가 캡처하도록)
logger = logging.getLogger(__name__)

# 강제 로그 출력
def log_to_console(msg):
    print(msg)
    sys.stdout.flush()
    logger.info(msg)

DATAHUB_GMS_URL = os.getenv('DATAHUB_GMS_URL', 'http://datahub-gms:8080')
CSV_FILE_PATH = '/tmp/datahub_metadata_update.csv'

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'datahub_metadata_bulk_update',
    default_args=default_args,
    description='DataHub 메타데이터 업데이트',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['datahub'],
)

def update_dataset_description(urn, description):
    """테이블 설명 업데이트"""
    log_to_console(f"[UPDATE_DATASET_START] URN={urn}, DESC={description}")
    
    try:
        payload = {
            "query": "mutation updateDataset($urn: String!, $description: String!) { updateDataset(urn: $urn, input: { editableProperties: { description: $description } }) { urn } }",
            "variables": {
                "urn": urn,
                "description": description
            }
        }
        
        log_to_console(f"[PAYLOAD] {payload}")
        
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        log_to_console(f"[RESPONSE_STATUS] {resp.status_code}")
        log_to_console(f"[RESPONSE_BODY] {resp.text[:500]}")
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" not in result:
                log_to_console(f"[SUCCESS] Updated dataset")
                return True
            log_to_console(f"[GRAPHQL_ERROR] {result['errors']}")
            return False
        log_to_console(f"[HTTP_ERROR] {resp.status_code}")
        return False
    except Exception as e:
        log_to_console(f"[EXCEPTION] {str(e)}")
        import traceback
        log_to_console(traceback.format_exc())
        return False

def update_field_description(dataset_urn, field_path, description):
    """컬럼 설명 업데이트"""
    log_to_console(f"[UPDATE_FIELD_START] URN={dataset_urn}, FIELD={field_path}, DESC={description}")
    
    try:
        payload = {
            "query": "mutation updateDescription($urn: String!, $subResource: String!, $description: String!) { updateDescription(input: { description: $description, resourceUrn: $urn, subResource: $subResource, subResourceType: DATASET_FIELD }) }",
            "variables": {
                "urn": dataset_urn,
                "subResource": field_path,
                "description": description
            }
        }
        
        log_to_console(f"[PAYLOAD] {payload}")
        
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        log_to_console(f"[RESPONSE_STATUS] {resp.status_code}")
        log_to_console(f"[RESPONSE_BODY] {resp.text[:500]}")
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" not in result:
                log_to_console(f"[SUCCESS] Updated field")
                return True
            log_to_console(f"[GRAPHQL_ERROR] {result['errors']}")
            return False
        log_to_console(f"[HTTP_ERROR] {resp.status_code}")
        return False
    except Exception as e:
        log_to_console(f"[EXCEPTION] {str(e)}")
        import traceback
        log_to_console(traceback.format_exc())
        return False

def validate_connection():
    """DataHub 연결 테스트"""
    log_to_console(f"[VALIDATE_START] {DATAHUB_GMS_URL}")
    try:
        resp = requests.get(f"{DATAHUB_GMS_URL}/health", timeout=10)
        log_to_console(f"[VALIDATE_STATUS] {resp.status_code}")
        if resp.status_code == 200:
            log_to_console("[VALIDATE_SUCCESS] Connected")
            return True
    except Exception as e:
        log_to_console(f"[VALIDATE_FAILED] {str(e)}")
    return False

def process_csv_file():
    """CSV 파일 처리"""
    log_to_console(f"[CSV_START] {CSV_FILE_PATH}")
    
    if not os.path.exists(CSV_FILE_PATH):
        log_to_console(f"[CSV_NOT_FOUND] {CSV_FILE_PATH}")
        return
    
    success = 0
    fail = 0
    
    try:
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, 1):
                row_type = row.get('type', '').strip().lower()
                urn = row.get('urn', '').strip()
                field_path = row.get('field_path', '').strip()
                description = row.get('description', '').strip()
                
                log_to_console(f"[ROW_{idx}] type={row_type}, urn={urn[:50]}, field={field_path}, desc={description[:50]}")
                
                if not urn:
                    log_to_console(f"[ROW_{idx}_SKIP] No URN")
                    fail += 1
                    continue
                
                if row_type == 'dataset' and description:
                    if update_dataset_description(urn, description):
                        success += 1
                        log_to_console(f"[ROW_{idx}_SUCCESS]")
                    else:
                        fail += 1
                        log_to_console(f"[ROW_{idx}_FAILED]")
                
                elif row_type == 'column' and field_path and description:
                    if update_field_description(urn, field_path, description):
                        success += 1
                        log_to_console(f"[ROW_{idx}_SUCCESS]")
                    else:
                        fail += 1
                        log_to_console(f"[ROW_{idx}_FAILED]")
                else:
                    log_to_console(f"[ROW_{idx}_SKIP] Insufficient data")
                    fail += 1
        
        log_to_console(f"[CSV_COMPLETE] Success={success}, Failed={fail}")
    except Exception as e:
        log_to_console(f"[CSV_ERROR] {str(e)}")
        import traceback
        log_to_console(traceback.format_exc())

task_validate = PythonOperator(
    task_id='validate_datahub_connection',
    python_callable=validate_connection,
    dag=dag,
)

task_process = PythonOperator(
    task_id='process_csv_file',
    python_callable=process_csv_file,
    dag=dag,
)
