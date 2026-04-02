# Databricks notebook source
# MAGIC %md
# MAGIC # 장비 건강 상태 분석
# MAGIC 최근 24시간 센서 데이터를 분석하여 장비 상태를 업데이트합니다.

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("hg_demos.predictive_maintenance.sensor_readings")

# 최근 24시간 데이터만 필터링
df_recent = df.filter(F.col("timestamp") >= F.date_sub(F.current_timestamp(), 1))

# 장비별 건강 지표 집계
df_health = df_recent.groupBy("equipment_id").agg(
    F.avg("vibration_rms").alias("avg_vibration"),
    F.max("vibration_rms").alias("max_vibration"),
    F.avg("temperature_c").alias("avg_temperature"),
    F.max("temperature_c").alias("max_temperature"),
    F.avg("pressure_bar").alias("avg_pressure"),
    F.stddev("vibration_rms").alias("vibration_volatility"),
    F.count("*").alias("reading_count"),
    F.current_timestamp().alias("analysis_timestamp")
)

df_health.write.mode("overwrite").saveAsTable("hg_demos.predictive_maintenance.equipment_health_daily")
display(df_health)
