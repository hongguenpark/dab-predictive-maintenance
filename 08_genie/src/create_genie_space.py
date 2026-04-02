# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - Genie Space 배포 (REST API + SQL)
# MAGIC
# MAGIC Genie Space는 DAB 리소스 타입이 아직 없지만,
# MAGIC 노트북에서 REST API를 호출하여 DAB Job으로 자동 배포할 수 있습니다.

# COMMAND ----------

import json
import requests

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
base_url = f"https://{host}/api/2.0"

GENIE_SPACE_TITLE = "PM - 예측 정비 장비 상태 조회"
WAREHOUSE_ID = "6898e1e8ebe84201"

print(f"워크스페이스: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 테이블 코멘트 추가 (Genie 자연어 이해도 향상)

# COMMAND ----------

# 테이블/컬럼에 설명 추가 - Genie가 자연어 질의를 SQL로 변환할 때 참조
spark.sql("""
    COMMENT ON TABLE hg_demos.predictive_maintenance.equipment_master IS
    '장비 마스터: equipment_id(장비ID), equipment_name(장비명), line(생산라인), criticality(중요도: 핵심/중요/보조), operating_hours(총운전시간)'
""")
spark.sql("""
    COMMENT ON TABLE hg_demos.predictive_maintenance.sensor_readings IS
    '센서측정: equipment_id(장비ID), timestamp(측정시각), vibration_rms(진동RMS-클수록위험), temperature_c(온도℃), pressure_bar(압력bar), current_amp(전류A)'
""")
print("✅ 테이블 코멘트 추가 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie 호환 요약 뷰 생성

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW hg_demos.predictive_maintenance.genie_equipment_status
    COMMENT '장비별 현재 상태 요약 뷰 - Genie 자연어 질의용. 장비ID, 장비명, 생산라인, 중요도, 운전시간, 평균진동, 평균온도, 평균압력, 상태(정상/주의/위험/긴급), 측정횟수'
    AS
    SELECT
        e.equipment_id AS `장비ID`,
        e.equipment_name AS `장비명`,
        e.line AS `생산라인`,
        e.criticality AS `중요도`,
        e.operating_hours AS `운전시간`,
        ROUND(AVG(s.vibration_rms), 2) AS `평균진동RMS`,
        ROUND(AVG(s.temperature_c), 1) AS `평균온도`,
        ROUND(AVG(s.pressure_bar), 1) AS `평균압력`,
        CASE
            WHEN AVG(s.vibration_rms) > 10.0 OR AVG(s.temperature_c) > 90.0 THEN '긴급'
            WHEN AVG(s.vibration_rms) > 7.0 OR AVG(s.temperature_c) > 75.0 THEN '위험'
            WHEN AVG(s.vibration_rms) > 4.5 OR AVG(s.temperature_c) > 60.0 THEN '주의'
            ELSE '정상'
        END AS `상태`,
        COUNT(*) AS `측정횟수`
    FROM hg_demos.predictive_maintenance.equipment_master e
    JOIN hg_demos.predictive_maintenance.sensor_readings s
        ON e.equipment_id = s.equipment_id
    GROUP BY ALL
""")
print("✅ genie_equipment_status 뷰 생성 완료")
display(spark.table("hg_demos.predictive_maintenance.genie_equipment_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Genie Space REST API 생성

# COMMAND ----------

# 기존 동일 이름 Space 확인
existing_space_id = None
try:
    resp = requests.get(f"{base_url}/genie/spaces", headers=headers)
    if resp.status_code == 200:
        for space in resp.json().get("spaces", []):
            if space.get("title") == GENIE_SPACE_TITLE:
                existing_space_id = space["space_id"]
                print(f"기존 Space 발견: {existing_space_id}")
                break
except Exception as e:
    print(f"기존 Space 조회 실패 (무시): {e}")

# COMMAND ----------

# Genie Space 생성 시도 (여러 API 형식 시도)
space_id = None
genie_url = None

payload = {
    "title": GENIE_SPACE_TITLE,
    "description": "공장 설비 센서 데이터를 자연어로 질의하는 Genie Space (예측 정비 시나리오)",
    "warehouse_id": WAREHOUSE_ID,
    "table_identifiers": [
        "hg_demos.predictive_maintenance.equipment_master",
        "hg_demos.predictive_maintenance.sensor_readings",
        "hg_demos.predictive_maintenance.genie_equipment_status"
    ],
    "instructions": "이 Genie Space는 공장 예측 정비 데이터를 분석합니다. 장비 상태 기준: 정상(진동≤4.5,온도≤60), 주의(진동>4.5 or 온도>60), 위험(진동>7 or 온도>75), 긴급(진동>10 or 온도>90). 항상 한국어로 답변하세요.",
    "curated_questions": [
        {"question": "위험 상태인 장비 목록을 보여줘"},
        {"question": "EQ-002 프레스기의 최근 평균 진동과 온도는?"},
        {"question": "운전시간이 10000시간 이상인 장비는?"},
        {"question": "장비별 평균 진동 RMS를 비교해줘"},
        {"question": "핵심 등급 장비들의 현재 상태는?"}
    ]
}

# API v1 시도
for endpoint in ["/genie/spaces", "/genie/spaces/create"]:
    try:
        resp = requests.post(f"{base_url}{endpoint}", headers=headers, json=payload)
        print(f"API {endpoint}: status={resp.status_code}")
        if resp.status_code == 200:
            result = resp.json()
            space_id = result.get("space_id")
            genie_url = f"https://{host}/genie/rooms/{space_id}"
            print(f"✅ Genie Space 생성 성공! Space ID: {space_id}")
            break
        else:
            print(f"   응답: {resp.text[:300]}")
    except Exception as e:
        print(f"   에러: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 결과 요약

# COMMAND ----------

if space_id:
    print(f"✅ Genie Space 배포 완료!")
    print(f"   Space ID: {space_id}")
    print(f"   URL: {genie_url}")
    print(f"   제목: {GENIE_SPACE_TITLE}")
    dbutils.notebook.exit(json.dumps({"space_id": space_id, "url": genie_url, "status": "success"}))
else:
    print("⚠️ REST API로 Genie Space 직접 생성은 실패했지만,")
    print("   다음 대안이 성공적으로 배포되었습니다:")
    print(f"   1. 테이블 코멘트 추가 완료 (Genie 자연어 이해도 향상)")
    print(f"   2. genie_equipment_status 뷰 생성 완료")
    print(f"   3. Databricks UI에서 위 테이블들을 선택하여 Genie Space 수동 생성 가능")
    print(f"\n   Genie Space 수동 생성 경로:")
    print(f"   Databricks > AI/BI > Genie > New Space")
    print(f"   테이블: hg_demos.predictive_maintenance.equipment_master,")
    print(f"           hg_demos.predictive_maintenance.sensor_readings,")
    print(f"           hg_demos.predictive_maintenance.genie_equipment_status")
    dbutils.notebook.exit(json.dumps({
        "status": "partial_success",
        "message": "REST API 생성 실패, Genie 호환 뷰 및 테이블 코멘트 배포 완료",
        "view": "hg_demos.predictive_maintenance.genie_equipment_status"
    }))
