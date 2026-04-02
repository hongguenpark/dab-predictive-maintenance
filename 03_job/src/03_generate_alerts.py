# Databricks notebook source
# MAGIC %md
# MAGIC # 정비 알림 생성
# MAGIC 장비 상태 분석 결과를 기반으로 정비 알림을 생성합니다.

# COMMAND ----------

from pyspark.sql import functions as F

df_health = spark.table("ebay_anomaly_detection_catalog.predictive_maintenance.equipment_health_daily")
df_equip = spark.table("ebay_anomaly_detection_catalog.predictive_maintenance.equipment_master")

df_alerts = df_health.join(df_equip, "equipment_id").withColumn(
    "status",
    F.expr("ebay_anomaly_detection_catalog.predictive_maintenance.assess_vibration_status(avg_vibration, avg_temperature)")
).withColumn(
    "rul_hours",
    F.expr("ebay_anomaly_detection_catalog.predictive_maintenance.estimate_rul(operating_hours, avg_vibration, avg_temperature)")
).filter(
    F.col("status").isin("위험", "긴급")
).withColumn(
    "alert_message",
    F.concat(
        F.lit("⚠️ ["), F.col("status"), F.lit("] "),
        F.col("equipment_name"), F.lit(" ("), F.col("equipment_id"), F.lit(") - "),
        F.lit("진동: "), F.round(F.col("avg_vibration"), 1), F.lit(" RMS, "),
        F.lit("온도: "), F.round(F.col("avg_temperature"), 1), F.lit("°C, "),
        F.lit("잔여수명: "), F.round(F.col("rul_hours"), 0), F.lit("시간")
    )
).withColumn("created_at", F.current_timestamp())

df_alerts.select(
    "equipment_id", "equipment_name", "status", "alert_message",
    "avg_vibration", "avg_temperature", "rul_hours", "created_at"
).write.mode("append").saveAsTable("ebay_anomaly_detection_catalog.predictive_maintenance.maintenance_alerts")

print(f"🔔 {df_alerts.count()}건의 정비 알림 생성 완료")
display(df_alerts.select("alert_message"))
