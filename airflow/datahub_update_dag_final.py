import os
import csv
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

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
    """테이블 설명 업데이트 (GraphQL Variables 사용)"""
    query = """
    mutation updateDataset($urn: String!, $description: String!) {
      updateDataset(
        urn: $urn
        input: {
          editableProperties: {
            description: $description
          }
        }
      ) {
        urn
      }
    }
    """
    
    try:
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json={
                "query": query,
                "variables": {
                    "urn": urn,
                    "description": description
                }
            },
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" in result:
                logger.error(f"GraphQL error: {result['errors']}")
                return False
            logger.info(f"✓ Updated dataset: {urn[:60]}")
            return True
        else:
            logger.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Error: {e}")
        return False


def update_field_description(dataset_urn, field_path, description):
    """컬럼 설명 업데이트 (GraphQL Variables 사용)"""
    query = """
    mutation updateDescription($urn: String!, $subResource: String!, $description: String!) {
      updateDescription(
        input: {
          description: $description
          resourceUrn: $urn
          subResource: $subResource
          subResourceType: DATASET_FIELD
        }
      )
    }
    """
    
    try:
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json={
                "query": query,
                "variables": {
                    "urn": dataset_urn,
                    "subResource": field_path,
                    "description": description
                }
            },
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" in result:
                logger.error(f"GraphQL error: {result['errors']}")
                return False
            logger.info(f"✓ Updated field: {field_path}")
            return True
        else:
            logger.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Error: {e}")
        return False


def validate_connection():
    """DataHub 연결 테스트"""
    logger.info(f"Testing: {DATAHUB_GMS_URL}")
    try:
        resp = requests.get(f"{DATAHUB_GMS_URL}/health", timeout=10)
        if resp.status_code == 200:
            logger.info("✓ Connected to DataHub")
            return True
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return False


def process_csv_file():
    """CSV 파일 처리"""
    if not os.path.exists(CSV_FILE_PATH):
        logger.error(f"CSV not found: {CSV_FILE_PATH}")
        return
    
    logger.info(f"Reading: {CSV_FILE_PATH}")
    success_count = 0
    fail_count = 0
    
    try:
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for idx, row in enumerate(reader, 1):
                row_type = row.get('type', '').strip().lower()
                urn = row.get('urn', '').strip()
                field_path = row.get('field_path', '').strip()
                description = row.get('description', '').strip()
                
                if not urn:
                    logger.warning(f"Row {idx}: Missing URN")
                    continue
                
                logger.info(f"Row {idx}: {row_type} - {urn[:50]}")
                
                if row_type == 'dataset' and description:
                    if update_dataset_description(urn, description):
                        success_count += 1
                    else:
                        fail_count += 1
                
                elif row_type == 'column' and field_path and description:
                    if update_field_description(urn, field_path, description):
                        success_count += 1
                    else:
                        fail_count += 1
                else:
                    logger.warning(f"Row {idx}: Insufficient data")
                    fail_count += 1
        
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Complete! Success: {success_count}, Failed: {fail_count}")
        logger.info("=" * 60)
    
    except Exception as e:
        logger.error(f"Error: {e}")


# Tasks
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

task_validate >> task_process