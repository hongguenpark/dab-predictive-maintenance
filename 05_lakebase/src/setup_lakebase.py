# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - Lakebase 데이터베이스 배포 (REST API)
# MAGIC
# MAGIC Lakebase(Databricks 관리형 PostgreSQL)는 DAB 리소스 타입이 아직 없지만,
# MAGIC 노트북에서 REST API를 호출하여 DAB Job으로 자동 배포할 수 있습니다.
# MAGIC
# MAGIC ### 예측 정비에서 Lakebase 활용
# MAGIC - 정비 작업 주문(Work Order) CRUD 관리
# MAGIC - 장비 정비 이력 저장
# MAGIC - 실시간 대시보드 백엔드 (저지연 OLTP)

# COMMAND ----------

import json
import requests
import time

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
base_url = f"https://{host}/api/2.0"

CATALOG_NAME = "ebay_anomaly_detection_catalog"
SCHEMA_NAME = "predictive_maintenance"
DB_NAME = "pm_lakebase_db"

print(f"워크스페이스: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lakebase 데이터베이스 확인/생성

# COMMAND ----------

# 기존 Lakebase DB 확인
existing_db = None
try:
    resp = requests.get(
        f"{base_url}/lakebase/databases",
        headers=headers
    )
    print(f"Lakebase 목록 조회: status={resp.status_code}")
    if resp.status_code == 200:
        dbs = resp.json().get("databases", [])
        for db in dbs:
            if db.get("name") == DB_NAME:
                existing_db = db
                print(f"기존 DB 발견: {db.get('name')} (state: {db.get('state')})")
                break
        if not existing_db:
            print(f"기존 '{DB_NAME}' DB 없음 - 새로 생성합니다")
    else:
        print(f"응답: {resp.text[:300]}")
except Exception as e:
    print(f"조회 실패: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lakebase 데이터베이스 생성

# COMMAND ----------

if not existing_db:
    # Lakebase DB 생성
    create_payload = {
        "name": DB_NAME,
        "catalog_name": CATALOG_NAME,
        "schema_name": SCHEMA_NAME,
    }

    resp = requests.post(
        f"{base_url}/lakebase/databases",
        headers=headers,
        json=create_payload
    )
    print(f"Lakebase 생성 요청: status={resp.status_code}")
    result = resp.json()
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if resp.status_code in [200, 201]:
        # 프로비저닝 대기
        db_id = result.get("database_id") or result.get("id") or DB_NAME
        print(f"\nDB 프로비저닝 대기 중... (id: {db_id})")

        for i in range(30):
            time.sleep(10)
            check_resp = requests.get(
                f"{base_url}/lakebase/databases/{db_id}",
                headers=headers
            )
            if check_resp.status_code == 200:
                state = check_resp.json().get("state", "UNKNOWN")
                print(f"  [{i*10}s] state: {state}")
                if state in ["RUNNING", "ACTIVE", "AVAILABLE"]:
                    existing_db = check_resp.json()
                    print(f"✅ Lakebase DB 프로비저닝 완료!")
                    break
                elif state in ["FAILED", "ERROR"]:
                    print(f"❌ 프로비저닝 실패: {check_resp.json()}")
                    break
            else:
                print(f"  [{i*10}s] 상태 확인 실패: {check_resp.status_code}")
    else:
        print(f"❌ 생성 실패: {result}")
else:
    print(f"기존 DB 사용: {existing_db.get('name')} (state: {existing_db.get('state')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 정비 관련 테이블 생성 (SQL)

# COMMAND ----------

# Lakebase API가 사용 불가한 경우, Delta 테이블로 정비 OLTP 데이터 구조 생성
# (Lakebase가 활성화된 워크스페이스에서는 PostgreSQL 테이블로 생성됨)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.work_orders (
        work_order_id STRING COMMENT '정비 작업 주문 ID',
        equipment_id STRING COMMENT '장비 ID',
        order_type STRING COMMENT '주문 유형 (예방정비/긴급정비/점검)',
        priority STRING COMMENT '우선순위 (긴급/높음/보통/낮음)',
        status STRING COMMENT '상태 (대기/진행중/완료/취소)',
        description STRING COMMENT '작업 설명',
        assigned_to STRING COMMENT '담당 기술자',
        created_at TIMESTAMP COMMENT '생성 시각',
        scheduled_date DATE COMMENT '예정일',
        completed_at TIMESTAMP COMMENT '완료 시각',
        cost_estimate DOUBLE COMMENT '예상 비용 (원)',
        actual_cost DOUBLE COMMENT '실제 비용 (원)'
    )
    COMMENT '정비 작업 주문 테이블 - OLTP 워크로드 (Lakebase 대상)'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.maintenance_history (
        history_id STRING COMMENT '이력 ID',
        equipment_id STRING COMMENT '장비 ID',
        maintenance_type STRING COMMENT '정비 유형',
        work_order_id STRING COMMENT '관련 작업 주문 ID',
        performed_by STRING COMMENT '수행 기술자',
        performed_at TIMESTAMP COMMENT '수행 시각',
        duration_hours DOUBLE COMMENT '소요 시간',
        parts_replaced STRING COMMENT '교체 부품',
        notes STRING COMMENT '비고',
        next_maintenance_date DATE COMMENT '다음 정비 예정일'
    )
    COMMENT '장비 정비 이력 테이블 - Lakebase 대상'
""")

print("✅ 정비 관리 테이블 생성 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 샘플 데이터 삽입

# COMMAND ----------

from datetime import datetime, date, timedelta
import random

work_orders = [
    ("WO-001", "EQ-001", "예방정비", "보통", "완료", "CNC 선반 스핀들 베어링 교체", "김정비", datetime(2024, 3, 1, 9, 0), date(2024, 3, 5), datetime(2024, 3, 5, 14, 30), 500000, 480000),
    ("WO-002", "EQ-002", "긴급정비", "긴급", "완료", "프레스기 유압 실린더 누유 수리", "박기술", datetime(2024, 3, 10, 8, 0), date(2024, 3, 10), datetime(2024, 3, 11, 16, 0), 1200000, 1350000),
    ("WO-003", "EQ-004", "점검", "보통", "완료", "로봇암 정기 캘리브레이션", "이엔지", datetime(2024, 3, 15, 10, 0), date(2024, 3, 20), datetime(2024, 3, 20, 12, 0), 200000, 180000),
    ("WO-004", "EQ-005", "예방정비", "낮음", "대기", "펌프 필터 교체 예정", "김정비", datetime(2024, 4, 1, 9, 0), date(2024, 4, 10), None, 100000, None),
    ("WO-005", "EQ-008", "긴급정비", "긴급", "진행중", "보일러 안전밸브 이상 점검", "박기술", datetime(2024, 4, 2, 7, 0), date(2024, 4, 2), None, 800000, None),
]

schema = "work_order_id STRING, equipment_id STRING, order_type STRING, priority STRING, status STRING, description STRING, assigned_to STRING, created_at TIMESTAMP, scheduled_date DATE, completed_at TIMESTAMP, cost_estimate DOUBLE, actual_cost DOUBLE"
df_wo = spark.createDataFrame(work_orders, schema)
df_wo.write.mode("overwrite").insertInto(f"{CATALOG_NAME}.{SCHEMA_NAME}.work_orders")

history = [
    ("MH-001", "EQ-001", "베어링 교체", "WO-001", "김정비", datetime(2024, 3, 5, 14, 30), 5.5, "스핀들 베어링 2ea", "정상 완료", date(2024, 6, 5)),
    ("MH-002", "EQ-002", "유압 수리", "WO-002", "박기술", datetime(2024, 3, 11, 16, 0), 16.0, "유압 실린더 씰, 호스", "부품 수급 지연으로 1일 추가 소요", date(2024, 6, 11)),
    ("MH-003", "EQ-004", "캘리브레이션", "WO-003", "이엔지", datetime(2024, 3, 20, 12, 0), 2.0, "없음", "정밀도 0.01mm 이내 확인", date(2024, 6, 20)),
]

schema2 = "history_id STRING, equipment_id STRING, maintenance_type STRING, work_order_id STRING, performed_by STRING, performed_at TIMESTAMP, duration_hours DOUBLE, parts_replaced STRING, notes STRING, next_maintenance_date DATE"
df_hist = spark.createDataFrame(history, schema2)
df_hist.write.mode("overwrite").insertInto(f"{CATALOG_NAME}.{SCHEMA_NAME}.maintenance_history")

print(f"✅ 정비 작업주문 {len(work_orders)}건, 정비이력 {len(history)}건 삽입 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 결과 확인

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.work_orders ORDER BY created_at DESC"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.maintenance_history ORDER BY performed_at DESC"))

# COMMAND ----------

lakebase_status = "Lakebase REST API 호출 완료" if existing_db else "Delta 테이블로 대체 구현 (Lakebase 미활성화 시)"
print(f"\n=== Lakebase 배포 결과 ===")
print(f"  DB 이름: {DB_NAME}")
print(f"  상태: {lakebase_status}")
print(f"  테이블: work_orders, maintenance_history")

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "db_name": DB_NAME,
    "tables": ["work_orders", "maintenance_history"],
    "lakebase_api_available": existing_db is not None
}))
