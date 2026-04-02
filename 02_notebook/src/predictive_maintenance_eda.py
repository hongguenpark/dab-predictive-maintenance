# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 (Predictive Maintenance) - 탐색적 데이터 분석
# MAGIC
# MAGIC 이 노트북은 설비 센서 데이터를 생성하고 탐색적 데이터 분석(EDA)을 수행합니다.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 센서 데이터 생성

# COMMAND ----------

# 장비 마스터 데이터
equipment_data = [
    ("EQ-001", "CNC 선반", "가공라인A", "핵심", 8500),
    ("EQ-002", "프레스기", "프레스라인B", "핵심", 12000),
    ("EQ-003", "컨베이어", "조립라인C", "중요", 5000),
    ("EQ-004", "로봇암", "용접라인D", "핵심", 3000),
    ("EQ-005", "펌프", "유틸리티E", "보조", 15000),
    ("EQ-006", "압축기", "유틸리티E", "중요", 9000),
    ("EQ-007", "냉각기", "가공라인A", "중요", 7000),
    ("EQ-008", "보일러", "유틸리티E", "핵심", 11000),
]

equipment_schema = StructType([
    StructField("equipment_id", StringType()),
    StructField("equipment_name", StringType()),
    StructField("line", StringType()),
    StructField("criticality", StringType()),
    StructField("operating_hours", IntegerType()),
])

df_equipment = spark.createDataFrame(equipment_data, equipment_schema)
df_equipment.write.mode("overwrite").saveAsTable("hg_demos.predictive_maintenance.equipment_master")
display(df_equipment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 시계열 센서 데이터 생성

# COMMAND ----------

from datetime import datetime, timedelta

sensor_records = []
base_time = datetime(2024, 1, 1)

for eq in equipment_data:
    eq_id = eq[0]
    hours = eq[4]
    degradation = hours / 15000.0  # 운전시간에 비례한 열화

    for i in range(1000):
        ts = base_time + timedelta(hours=i)
        vibration = 2.0 + degradation * 5.0 + random.gauss(0, 0.5)
        temperature = 40 + degradation * 30 + random.gauss(0, 2)
        pressure = 100 - degradation * 20 + random.gauss(0, 3)
        current = 10 + degradation * 5 + random.gauss(0, 0.8)

        sensor_records.append((
            eq_id, ts, round(vibration, 2), round(temperature, 1),
            round(pressure, 1), round(current, 2)
        ))

sensor_schema = StructType([
    StructField("equipment_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("vibration_rms", DoubleType()),
    StructField("temperature_c", DoubleType()),
    StructField("pressure_bar", DoubleType()),
    StructField("current_amp", DoubleType()),
])

df_sensors = spark.createDataFrame(sensor_records, sensor_schema)
df_sensors.write.mode("overwrite").saveAsTable("hg_demos.predictive_maintenance.sensor_readings")

print(f"센서 데이터 {df_sensors.count():,}건 생성 완료")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 기본 통계 분석

# COMMAND ----------

df_stats = df_sensors.groupBy("equipment_id").agg(
    F.avg("vibration_rms").alias("avg_vibration"),
    F.max("vibration_rms").alias("max_vibration"),
    F.avg("temperature_c").alias("avg_temperature"),
    F.max("temperature_c").alias("max_temperature"),
    F.stddev("vibration_rms").alias("std_vibration"),
    F.count("*").alias("reading_count")
)
display(df_stats.orderBy("equipment_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 이상 징후 탐지

# COMMAND ----------

df_anomalies = df_sensors.filter(
    (F.col("vibration_rms") > 7.0) | (F.col("temperature_c") > 75.0)
).withColumn(
    "anomaly_type",
    F.when((F.col("vibration_rms") > 7.0) & (F.col("temperature_c") > 75.0), "진동+온도")
     .when(F.col("vibration_rms") > 7.0, "고진동")
     .otherwise("고온")
)

display(df_anomalies.groupBy("equipment_id", "anomaly_type").count().orderBy("equipment_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 장비별 상태 요약

# COMMAND ----------

# UC Function 없이 PySpark로 상태 요약
df_summary = df_sensors.join(df_equipment, "equipment_id").groupBy(
    "equipment_id", "equipment_name", "criticality", "operating_hours"
).agg(
    F.round(F.avg("vibration_rms"), 2).alias("avg_vibration"),
    F.round(F.avg("temperature_c"), 1).alias("avg_temp"),
).withColumn("status",
    F.when((F.col("avg_vibration") > 10.0) | (F.col("avg_temp") > 90.0), "긴급")
     .when((F.col("avg_vibration") > 7.0) | (F.col("avg_temp") > 75.0), "위험")
     .when((F.col("avg_vibration") > 4.5) | (F.col("avg_temp") > 60.0), "주의")
     .otherwise("정상")
).orderBy("avg_vibration", ascending=False)

display(df_summary)
print(f"✅ 분석 완료: {df_summary.count()}개 장비 상태 요약")
