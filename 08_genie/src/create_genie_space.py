# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - Genie Space 배포 (Metric View 연동)
# MAGIC
# MAGIC Metric View를 Genie Space의 데이터 소스로 연결하여
# MAGIC 자연어로 장비 가동률, OEE, 건강 지표를 질의할 수 있도록 합니다.

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
GENIE_TITLE = "PM - 예측 정비 Metric View 분석"

print(f"워크스페이스: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Metric View 및 원본 테이블에 코멘트 추가

# COMMAND ----------

# 원본 테이블 코멘트 (Genie가 컨텍스트 파악용)
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA}.equipment_master IS
    '장비 마스터 데이터. equipment_id(장비ID: EQ-001~EQ-008), equipment_name(장비명: CNC선반,프레스기,컨베이어,로봇암,펌프,압축기,냉각기,보일러), line(생산라인), criticality(중요도: 핵심/중요/보조), operating_hours(총운전시간h)'
""")
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA}.sensor_readings IS
    '센서 측정 데이터. equipment_id(장비ID), timestamp(측정시각), vibration_rms(진동RMS-정상:4.5이하,주의:4.5~7,위험:7~10,긴급:10초과), temperature_c(온도-정상:60이하,주의:60~75,위험:75~90,긴급:90초과), pressure_bar(압력bar), current_amp(전류A)'
""")
print("원본 테이블 코멘트 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 기존 동일 이름 Genie Space 삭제

# COMMAND ----------

resp = requests.get(f"{base_url}/genie/spaces", headers=headers)
if resp.status_code == 200:
    for space in resp.json().get("spaces", []):
        if space.get("title") == GENIE_TITLE:
            requests.delete(f"{base_url}/genie/spaces/{space['space_id']}", headers=headers)
            print(f"기존 Space 삭제: {space['space_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Genie Space 생성 (Metric View + 원본 테이블 연동)

# COMMAND ----------

# Genie 데이터 소스: Metric View 3개 + 장비 마스터 + 센서 원시 데이터
# ⚠️ identifier 기준 알파벳순 정렬 필수 (Genie API 요구사항)
tables = sorted([
    f"{CATALOG}.{SCHEMA}.equipment_master",             # 장비 마스터 (JOIN용)
    f"{CATALOG}.{SCHEMA}.mv_equipment_availability",    # 가동률 메트릭
    f"{CATALOG}.{SCHEMA}.mv_equipment_health",          # 건강 종합 메트릭
    f"{CATALOG}.{SCHEMA}.mv_oee",                       # OEE 메트릭
    f"{CATALOG}.{SCHEMA}.sensor_readings",              # 원시 센서 데이터
])

serialized_space = json.dumps({
    "version": 2,
    "data_sources": {
        "tables": [{"identifier": t} for t in tables]
    }
})

payload = {
    "serialized_space": serialized_space,
    "warehouse_id": WAREHOUSE_ID,
    "title": GENIE_TITLE,
    "description": """예측 정비 Metric View 기반 분석 Genie Space입니다.

다음과 같은 질문을 자연어로 할 수 있습니다:

[가동률 관련]
- "장비별 가동률을 보여줘"
- "가동률이 가장 낮은 장비는?"
- "EQ-002의 일별 가동률 추이는?"

[OEE 관련]
- "장비별 OEE를 비교해줘"
- "OEE가 60% 미만인 장비는?"
- "가동률, 성능률, 품질률을 각각 보여줘"

[건강 지표 관련]
- "이상 징후 비율이 높은 장비 Top 3는?"
- "평균 진동이 위험 수준(7.0 이상)인 장비는?"
- "장비별 최대 온도를 비교해줘"

[종합]
- "핵심 등급 장비들의 OEE와 이상률을 보여줘"
- "운전시간 대비 이상률이 높은 장비는?"
"""
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
    print(f"  연결된 Metric View: mv_equipment_availability, mv_equipment_health, mv_oee")
    dbutils.notebook.exit(json.dumps({"status": "success", "space_id": space_id, "url": genie_url}))
else:
    print(f"생성 실패: {resp.status_code}")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    dbutils.notebook.exit(json.dumps({"status": "failed", "error": str(result)}))
