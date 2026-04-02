# Databricks notebook source
# MAGIC %md
# MAGIC # 센서 데이터 수집
# MAGIC 일일 배치로 센서 데이터를 수집하고 Delta 테이블에 적재합니다.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

# 오늘 날짜 기준 센서 데이터 시뮬레이션
today = datetime.now().date()
equipment_ids = ["EQ-001","EQ-002","EQ-003","EQ-004","EQ-005","EQ-006","EQ-007","EQ-008"]

records = []
for eq_id in equipment_ids:
    for hour in range(24):
        ts = datetime.combine(today, datetime.min.time()) + timedelta(hours=hour)
        records.append((
            eq_id, ts,
            round(3.0 + random.gauss(0, 1.5), 2),
            round(55 + random.gauss(0, 8), 1),
            round(95 + random.gauss(0, 5), 1),
            round(12 + random.gauss(0, 2), 2)
        ))

schema = "equipment_id STRING, timestamp TIMESTAMP, vibration_rms DOUBLE, temperature_c DOUBLE, pressure_bar DOUBLE, current_amp DOUBLE"
df_new = spark.createDataFrame(records, schema)
df_new.write.mode("append").saveAsTable("hg_demos.predictive_maintenance.sensor_readings")
print(f"✅ {len(records)}건 센서 데이터 적재 완료 ({today})")
