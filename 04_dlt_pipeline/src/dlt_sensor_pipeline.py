# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - DLT (Delta Live Tables) 파이프라인
# MAGIC
# MAGIC Medallion 아키텍처로 센서 데이터를 Bronze → Silver → Gold 레이어로 처리합니다.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - 원시 센서 데이터

# COMMAND ----------

@dlt.table(
    name="sensor_raw_bronze",
    comment="원시 센서 데이터 (Bronze Layer) - IoT 게이트웨이에서 수집된 가공 전 데이터",
    table_properties={"quality": "bronze"}
)
def sensor_raw_bronze():
    return (
        spark.table("ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source", F.lit("iot_gateway"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - 정제된 센서 데이터

# COMMAND ----------

@dlt.table(
    name="sensor_cleaned_silver",
    comment="정제된 센서 데이터 (Silver Layer) - 품질 검증 및 이상치 태깅 완료"
)
@dlt.expect_or_drop("유효한_진동값", "vibration_rms > 0 AND vibration_rms < 50")
@dlt.expect_or_drop("유효한_온도값", "temperature_c > -20 AND temperature_c < 200")
@dlt.expect("유효한_압력값", "pressure_bar > 0")
def sensor_cleaned_silver():
    return (
        dlt.read("sensor_raw_bronze")
        .withColumn("vibration_status",
            F.when(F.col("vibration_rms") > 10.0, "긴급")
             .when(F.col("vibration_rms") > 7.0, "위험")
             .when(F.col("vibration_rms") > 4.5, "주의")
             .otherwise("정상"))
        .withColumn("temperature_status",
            F.when(F.col("temperature_c") > 90.0, "긴급")
             .when(F.col("temperature_c") > 75.0, "위험")
             .when(F.col("temperature_c") > 60.0, "주의")
             .otherwise("정상"))
        .withColumn("date", F.to_date("timestamp"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - 장비 건강 지표

# COMMAND ----------

@dlt.table(
    name="equipment_health_gold",
    comment="장비 건강 지표 요약 (Gold Layer) - 장비별 일일 상태 집계"
)
def equipment_health_gold():
    return (
        dlt.read("sensor_cleaned_silver")
        .groupBy("equipment_id", "date")
        .agg(
            F.avg("vibration_rms").alias("avg_vibration"),
            F.max("vibration_rms").alias("max_vibration"),
            F.avg("temperature_c").alias("avg_temperature"),
            F.max("temperature_c").alias("max_temperature"),
            F.avg("pressure_bar").alias("avg_pressure"),
            F.stddev("vibration_rms").alias("vibration_std"),
            F.count("*").alias("reading_count"),
            F.sum(F.when(F.col("vibration_status") == "긴급", 1).otherwise(0)).alias("critical_count"),
            F.sum(F.when(F.col("vibration_status") == "위험", 1).otherwise(0)).alias("warning_count")
        )
    )

# COMMAND ----------

@dlt.table(
    name="maintenance_recommendations_gold",
    comment="정비 추천 목록 (Gold Layer) - 즉시 정비가 필요한 장비 목록"
)
def maintenance_recommendations_gold():
    df_health = dlt.read("equipment_health_gold")
    return (
        df_health.filter(
            (F.col("max_vibration") > 7.0) |
            (F.col("max_temperature") > 75.0) |
            (F.col("critical_count") > 0)
        )
        .withColumn("urgency",
            F.when(F.col("critical_count") > 5, "즉시 정비")
             .when(F.col("warning_count") > 10, "금주 내 정비")
             .otherwise("다음 정기 정비 시 확인"))
        .withColumn("recommendation_date", F.current_date())
    )
