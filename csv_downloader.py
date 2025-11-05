import requests
import json
import csv
from typing import List, Dict

# DataHub 설정
DATAHUB_GMS_URL = "http://localhost:8080"
DATAHUB_TOKEN = "your-access-token"  # 필요시

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {DATAHUB_TOKEN}"  # 인증 활성화시
}

def fetch_all_datasets():
    """scrollAcrossEntities를 사용하여 모든 데이터셋 조회"""
    all_datasets = []
    scroll_id = None
    
    query = """
    query scrollDatasets($scrollId: String) {
      scrollAcrossEntities(
        input: {
          types: [DATASET],
          query: "*",
          scrollId: $scrollId,
          count: 100
        }
      ) {
        nextScrollId
        count
        searchResults {
          entity {
            ... on Dataset {
              urn
              name
              platform {
                name
              }
              properties {
                name
                description
                customProperties {
                  key
                  value
                }
              }
              schemaMetadata {
                fields {
                  fieldPath
                  description
                  type
                  globalTags {
                    tags {
                      tag {
                        name
                      }
                    }
                  }
                  glossaryTerms {
                    terms {
                      term {
                        name
                      }
                    }
                  }
                }
              }
              globalTags {
                tags {
                  tag {
                    name
                  }
                }
              }
              glossaryTerms {
                terms {
                  term {
                    name
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    
    while True:
        variables = {"scrollId": scroll_id} if scroll_id else {}
        response = requests.post(
            f"{DATAHUB_GMS_URL}/api/graphql",
            headers=headers,
            json={"query": query, "variables": variables}
        )
        
        data = response.json()
        results = data['data']['scrollAcrossEntities']['searchResults']
        all_datasets.extend(results)
        
        scroll_id = data['data']['scrollAcrossEntities'].get('nextScrollId')
        if not scroll_id:
            break
    
    return all_datasets

def export_to_csv(datasets: List[Dict], filename: str):
    """메타데이터를 CSV UTF-8로 저장"""
    rows = []
    
    for result in datasets:
        dataset = result['entity']
        urn = dataset['urn']
        platform = dataset['platform']['name']
        name = dataset.get('properties', {}).get('name', '')
        description = dataset.get('properties', {}).get('description', '')
        
        # Dataset 레벨 태그와 용어
        dataset_tags = '|'.join([
            tag['tag']['name'] 
            for tag in dataset.get('globalTags', {}).get('tags', [])
        ])
        dataset_terms = '|'.join([
            term['term']['name'] 
            for term in dataset.get('glossaryTerms', {}).get('terms', [])
        ])
        
        # 스키마 필드가 없는 경우 데이터셋 레벨 정보만 저장
        if not dataset.get('schemaMetadata', {}).get('fields'):
            rows.append({
                'resource': urn,
                'subresource': '',
                'platform': platform,
                'name': name,
                'description': description,
                'tags': dataset_tags,
                'glossary_terms': dataset_terms
            })
        else:
            # 각 컬럼별로 행 생성
            for field in dataset.get('schemaMetadata', {}).get('fields', []):
                field_path = field['fieldPath']
                field_desc = field.get('description', '')
                field_type = field.get('type', '')
                
                field_tags = '|'.join([
                    tag['tag']['name'] 
                    for tag in field.get('globalTags', {}).get('tags', [])
                ])
                field_terms = '|'.join([
                    term['term']['name'] 
                    for term in field.get('glossaryTerms', {}).get('terms', [])
                ])
                
                rows.append({
                    'resource': urn,
                    'subresource': field_path,
                    'platform': platform,
                    'name': name,
                    'description': field_desc or description,
                    'tags': field_tags or dataset_tags,
                    'glossary_terms': field_terms or dataset_terms,
                    'field_type': field_type
                })
    
    # CSV 저장
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    
    print(f"총 {len(rows)}개 행이 {filename}에 저장되었습니다.")

# 실행
if __name__ == "__main__":
    datasets = fetch_all_datasets()
    export_to_csv(datasets, 'datahub_metadata_export.csv')
