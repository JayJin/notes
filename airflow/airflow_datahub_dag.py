"""
DataHub 메타데이터 대량 업데이트 DAG
CSV 파일에서 테이블/컬럼 설명, 태그, 용어집 정보를 읽어 DataHub에 반영
"""

import os
import csv
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import task
import logging

logger = logging.getLogger(__name__)

# Airflow 설정
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'datahub_metadata_bulk_update',
    default_args=default_args,
    description='DataHub 메타데이터 대량 업데이트 DAG',
    schedule_interval=None,  # 수동 트리거
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['datahub', 'metadata'],
)

# DataHub 연결 정보
DATAHUB_GMS_URL = os.getenv('DATAHUB_GMS_URL', 'http://datahub-gms:8080')
DATAHUB_AUTH_TOKEN = os.getenv('DATAHUB_AUTH_TOKEN', '')  # 필요시 PAT 토큰 설정

# CSV 파일 경로
CSV_FILE_PATH = '/tmp/datahub_metadata_update.csv'


def get_auth_headers():
    """
    DataHub 인증 헤더 생성
    토큰이 있으면 Bearer token 사용, 없으면 빈 헤더
    """
    headers = {
        'Content-Type': 'application/json',
    }
    
    if DATAHUB_AUTH_TOKEN:
        headers['Authorization'] = f'Bearer {DATAHUB_AUTH_TOKEN}'
        logger.info("Using Bearer token authentication")
    else:
        logger.warning("No authentication token provided. Using unauthenticated requests.")
        logger.warning("If DataHub requires authentication, set DATAHUB_AUTH_TOKEN environment variable.")
    
    return headers


def update_dataset_description(urn, description):
    """
    Dataset(테이블) 설명 업데이트
    GraphQL Mutation 사용
    """
    query = """
    mutation updateDataset {
      updateDataset(
        urn: "%s"
        input: {
          editableProperties: {
            description: "%s"
          }
        }
      ) {
        urn
      }
    }
    """ % (urn, description.replace('"', '\\"').replace('\n', '\\n'))
    
    try:
        headers = get_auth_headers()
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json={"query": query},
            headers=headers,
            timeout=30
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" in result:
                logger.error(f"GraphQL error for {urn}: {result['errors']}")
                return False
            else:
                logger.info(f"✓ Updated dataset: {urn[:60]}...")
                return True
        else:
            logger.error(f"HTTP {resp.status_code} for {urn}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Error updating {urn}: {e}")
        return False


def update_field_description(dataset_urn, field_path, description):
    """
    필드(컬럼) 설명 업데이트
    GraphQL Mutation 사용
    """
    query = """
    mutation updateDescription {
      updateDescription(
        input: {
          description: "%s"
          resourceUrn: "%s"
          subResource: "%s"
          subResourceType: DATASET_FIELD
        }
      )
    }
    """ % (description.replace('"', '\\"').replace('\n', '\\n'), dataset_urn, field_path)
    
    try:
        headers = get_auth_headers()
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json={"query": query},
            headers=headers,
            timeout=30
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" in result:
                logger.error(f"GraphQL error for {dataset_urn}.{field_path}: {result['errors']}")
                return False
            else:
                logger.info(f"✓ Updated field: {field_path}")
                return True
        else:
            logger.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Error updating {dataset_urn}.{field_path}: {e}")
        return False


def add_tag_to_dataset(dataset_urn, tag_urn):
    """
    Dataset에 태그 추가
    """
    query = """
    mutation addTag {
      addTag(
        input: {
          resourceUrn: "%s"
          tagUrn: "%s"
        }
      )
    }
    """ % (dataset_urn, tag_urn)
    
    try:
        headers = get_auth_headers()
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json={"query": query},
            headers=headers,
            timeout=30
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" in result:
                logger.error(f"GraphQL error adding tag: {result['errors']}")
                return False
            else:
                logger.info(f"✓ Added tag {tag_urn[:40]}... to dataset")
                return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error adding tag: {e}")
        return False


def add_glossary_term_to_dataset(dataset_urn, glossary_term_urn):
    """
    Dataset에 Glossary Term 연결
    """
    query = """
    mutation addGlossaryTerm {
      addTermToEntity(
        input: {
          urns: ["%s"]
          termUrn: "%s"
        }
      )
    }
    """ % (dataset_urn, glossary_term_urn)
    
    try:
        headers = get_auth_headers()
        resp = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            json={"query": query},
            headers=headers,
            timeout=30
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if "errors" in result:
                logger.error(f"GraphQL error adding glossary term: {result['errors']}")
                return False
            else:
                logger.info(f"✓ Added glossary term to dataset")
                return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error adding glossary term: {e}")
        return False


def process_csv_file():
    """
    CSV 파일 읽고 메타데이터 업데이트
    
    CSV 형식:
    type,urn,field_path,description,tag_urn,glossary_term_urn
    dataset,urn:li:dataset:(...),,"테이블 설명",,
    column,urn:li:dataset:(...),COLUMN_NAME,"컬럼 설명",,
    tag,urn:li:dataset:(...),,"",urn:li:tag:important,
    glossary,urn:li:dataset:(...),,"",,"urn:li:glossaryTerm:..."
    """
    
    if not os.path.exists(CSV_FILE_PATH):
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
        logger.info("Please mount or upload update CSV file to: /tmp/datahub_metadata_update.csv")
        return
    
    logger.info(f"Reading CSV file: {CSV_FILE_PATH}")
    
    success_count = 0
    fail_count = 0
    
    try:
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for idx, row in enumerate(reader, 1):
                try:
                    row_type = row.get('type', '').strip().lower()
                    urn = row.get('urn', '').strip()
                    field_path = row.get('field_path', '').strip()
                    description = row.get('description', '').strip()
                    tag_urn = row.get('tag_urn', '').strip()
                    glossary_term_urn = row.get('glossary_term_urn', '').strip()
                    
                    if not urn:
                        logger.warning(f"Row {idx}: Missing urn, skipping")
                        continue
                    
                    logger.info(f"Row {idx}: Processing {row_type} - {urn[:50]}...")
                    
                    if row_type == 'dataset' and description:
                        # 테이블 설명 업데이트
                        if update_dataset_description(urn, description):
                            success_count += 1
                        else:
                            fail_count += 1
                    
                    elif row_type == 'column' and field_path and description:
                        # 컬럼 설명 업데이트
                        if update_field_description(urn, field_path, description):
                            success_count += 1
                        else:
                            fail_count += 1
                    
                    elif row_type == 'tag' and tag_urn:
                        # 태그 추가
                        if add_tag_to_dataset(urn, tag_urn):
                            success_count += 1
                        else:
                            fail_count += 1
                    
                    elif row_type == 'glossary' and glossary_term_urn:
                        # 용어집 연결
                        if add_glossary_term_to_dataset(urn, glossary_term_urn):
                            success_count += 1
                        else:
                            fail_count += 1
                    else:
                        logger.warning(f"Row {idx}: Insufficient data or unknown type")
                        fail_count += 1
                
                except Exception as e:
                    logger.error(f"Row {idx}: Error processing - {e}")
                    fail_count += 1
        
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Processing complete!")
        logger.info(f"  Success: {success_count}")
        logger.info(f"  Failed: {fail_count}")
        logger.info("=" * 60)
        
    except FileNotFoundError:
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")


def validate_datahub_connection():
    """
    DataHub 연결 테스트
    """
    logger.info(f"Testing DataHub GMS connection: {DATAHUB_GMS_URL}")
    
    try:
        headers = get_auth_headers()
        resp = requests.get(
            f"{DATAHUB_GMS_URL}/api/graphql",
            headers=headers,
            timeout=10
        )
        
        if resp.status_code in [200, 400]:  # 400은 empty query 때문
            logger.info("✓ DataHub GMS is accessible")
            return True
        else:
            logger.error(f"✗ DataHub GMS returned status code: {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"✗ Failed to connect to DataHub: {e}")
        return False


# DAG Tasks

task_validate = PythonOperator(
    task_id='validate_datahub_connection',
    python_callable=validate_datahub_connection,
    dag=dag,
)

task_process_csv = PythonOperator(
    task_id='process_csv_file',
    python_callable=process_csv_file,
    dag=dag,
)

# Task 의존성
task_validate >> task_process_csv
