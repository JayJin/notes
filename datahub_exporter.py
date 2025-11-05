from prometheus_client import start_http_server, Gauge
import time, requests

GMS_URL = "http://datahub-gms:8080"
METRICS_PORT = 9105  # Prometheus가 이 포트를 scrape

# Prometheus metrics 정의
owner_rate = Gauge('datahub_owner_registration_rate', 'Owner 등록율 (%)')
desc_rate = Gauge('datahub_description_filled_rate', 'DB 설명정보 등록율 (%)')
sensitive_ratio = Gauge('datahub_sensitive_data_ratio', '민감 데이터 비율 (%)')

def get_datasets():
    url = f"{GMS_URL}/api/graphql"
    query = {
        "query": """
        {
          search(input: {type: DATASET, query: "*", start: 0, count: 500}) {
            entities {
              urn
              ownership { owners { owner { urn } } }
              editableProperties { description }
              tags { tags { tag { urn } } }
            }
          }
        }
        """
    }
    r = requests.post(url, json=query)
    return r.json()["data"]["search"]["entities"]

def collect_metrics():
    datasets = get_datasets()
    total = len(datasets)
    if total == 0: return

    desc_filled = sum(1 for d in datasets if d.get("editableProperties", {}).get("description"))
    owner_filled = sum(1 for d in datasets if d.get("ownership", {}).get("owners"))
    sensitive_tagged = sum(
        1 for d in datasets
        if any("tag-secret" in t["tag"]["urn"] for t in d.get("tags", {}).get("tags", []))
    )

    desc_rate.set(desc_filled / total * 100)
    owner_rate.set(owner_filled / total * 100)
    sensitive_ratio.set(sensitive_tagged / total * 100)

if __name__ == "__main__":
    start_http_server(METRICS_PORT)
    while True:
        collect_metrics()
        time.sleep(300)  # 5분마다 갱신
