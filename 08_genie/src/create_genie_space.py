# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - Genie Space 배포 (REST API)
# MAGIC
# MAGIC Genie Space는 DAB 리소스 타입이 없지만,
# MAGIC REST API를 호출하는 노트북을 DAB Job으로 배포하여 자동화할 수 있습니다.

# COMMAND ----------

import json
import requests

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
base_url = f"https://{host}/api/2.0"

CATALOG = "ebay_anomaly_detection_catalog"
SCHEMA = "predictive_maintenance"
WAREHOUSE_ID = "86ec93a98d884cde"
GENIE_TITLE = "PM - 예측 정비 장비 상태 조회"

print(f"워크스페이스: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 테이블 코멘트 추가 (Genie 자연어 이해도 향상)

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA}.equipment_master IS
    '장비 마스터: equipment_id(장비ID), equipment_name(장비명), line(생산라인), criticality(중요도: 핵심/중요/보조), operating_hours(총운전시간h)'
""")
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA}.sensor_readings IS
    '센서측정: equipment_id(장비ID), timestamp(측정시각), vibration_rms(진동RMS-클수록위험), temperature_c(온도도씨), pressure_bar(압력bar), current_amp(전류A)'
""")
print("테이블 코멘트 추가 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 기존 동일 이름 Genie Space 확인

# COMMAND ----------

existing_space_id = None
resp = requests.get(f"{base_url}/genie/spaces", headers=headers)
if resp.status_code == 200:
    for space in resp.json().get("spaces", []):
        if space.get("title") == GENIE_TITLE:
            existing_space_id = space["space_id"]
            print(f"기존 Genie Space 발견: {existing_space_id}")
            break

if not existing_space_id:
    print("기존 Space 없음 - 새로 생성합니다")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Genie Space 생성 또는 업데이트

# COMMAND ----------

if existing_space_id:
    # 기존 Space 삭제 후 재생성 (멱등성)
    requests.delete(f"{base_url}/genie/spaces/{existing_space_id}", headers=headers)
    print(f"기존 Space {existing_space_id} 삭제")

# Genie Space 생성
payload = {
    "serialized_space": json.dumps({"version": 2}),
    "warehouse_id": WAREHOUSE_ID,
    "title": GENIE_TITLE,
    "description": "공장 설비 센서 데이터를 자연어로 질의합니다. 장비 상태, 진동, 온도, 운전시간 등을 한국어로 질문하세요.",
    "table_identifiers": [
        f"{CATALOG}.{SCHEMA}.equipment_master",
        f"{CATALOG}.{SCHEMA}.sensor_readings"
    ]
}

resp = requests.post(f"{base_url}/genie/spaces", headers=headers, json=payload)
result = resp.json()

if resp.status_code == 200 and result.get("space_id"):
    space_id = result["space_id"]
    genie_url = f"https://{host}/genie/rooms/{space_id}"
    print(f"Genie Space 생성 성공!")
    print(f"  Space ID: {space_id}")
    print(f"  URL: {genie_url}")
    print(f"  제목: {GENIE_TITLE}")
    dbutils.notebook.exit(json.dumps({"status": "success", "space_id": space_id, "url": genie_url}))
else:
    print(f"생성 실패: {resp.status_code}")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    dbutils.notebook.exit(json.dumps({"status": "failed", "error": str(result)}))
