-- Databricks notebook source
-- 예측 정비(Predictive Maintenance) - Metric View 정의
-- CREATE VIEW ... WITH METRICS 문법으로 Metric View를 생성합니다.

-- COMMAND ----------

-- 0. 기존 일반 뷰 삭제 (Metric View로 재생성하기 위해)
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_availability;
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_mtbf;
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee;
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_health;

-- COMMAND ----------

-- 1. 장비 가동률 메트릭 뷰
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_availability
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "장비별 가동률 메트릭 - 정상 가동 비율을 일별로 집계"
  source: ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
  filter: vibration_rms IS NOT NULL AND temperature_c IS NOT NULL
  dimensions:
    - name: equipment_id
      expr: equipment_id
      comment: "장비 ID"
    - name: metric_date
      expr: DATE(timestamp)
      comment: "측정 날짜"
  measures:
    - name: total_readings
      expr: COUNT(1)
      comment: "총 센서 측정 횟수"
    - name: normal_readings
      expr: COUNT(1) FILTER (WHERE vibration_rms <= 7.0 AND temperature_c <= 75.0)
      comment: "정상 범위 측정 횟수 (진동 ≤ 7.0 AND 온도 ≤ 75℃)"
    - name: availability_pct
      expr: ROUND(COUNT(1) FILTER (WHERE vibration_rms <= 7.0 AND temperature_c <= 75.0) * 100.0 / COUNT(1), 1)
      comment: "가동률 (%) - 정상 측정 비율"
$$;

-- COMMAND ----------

-- 2. 장비 건강 종합 메트릭 뷰 (MTBF 포함)
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_health
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "장비 건강 종합 메트릭 - 진동, 온도, 이상 징후 집계"
  source: ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
  dimensions:
    - name: equipment_id
      expr: equipment_id
      comment: "장비 ID"
  measures:
    - name: avg_vibration
      expr: ROUND(AVG(vibration_rms), 2)
      comment: "평균 진동 (RMS)"
    - name: max_vibration
      expr: ROUND(MAX(vibration_rms), 2)
      comment: "최대 진동 (RMS)"
    - name: avg_temperature
      expr: ROUND(AVG(temperature_c), 1)
      comment: "평균 온도 (℃)"
    - name: max_temperature
      expr: ROUND(MAX(temperature_c), 1)
      comment: "최대 온도 (℃)"
    - name: avg_pressure
      expr: ROUND(AVG(pressure_bar), 1)
      comment: "평균 압력 (bar)"
    - name: anomaly_count
      expr: COUNT(1) FILTER (WHERE vibration_rms > 7.0 OR temperature_c > 75.0)
      comment: "이상 징후 건수 (진동 > 7 OR 온도 > 75)"
    - name: total_readings
      expr: COUNT(1)
      comment: "총 측정 건수"
    - name: anomaly_rate_pct
      expr: ROUND(COUNT(1) FILTER (WHERE vibration_rms > 7.0 OR temperature_c > 75.0) * 100.0 / COUNT(1), 2)
      comment: "이상 징후 비율 (%)"
$$;

-- COMMAND ----------

-- 3. OEE (Overall Equipment Effectiveness) 메트릭 뷰
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "OEE(종합설비효율) 메트릭 - 가동률 x 성능 x 품질"
  source: ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
  dimensions:
    - name: equipment_id
      expr: equipment_id
      comment: "장비 ID"
  measures:
    - name: availability_pct
      expr: ROUND(COUNT(1) FILTER (WHERE vibration_rms <= 7.0 AND temperature_c <= 75.0) * 100.0 / COUNT(1), 1)
      comment: "가동률 (%) - 정상 범위 가동 비율"
    - name: performance_pct
      expr: ROUND(COUNT(1) FILTER (WHERE vibration_rms <= 4.5 AND temperature_c <= 60.0) * 100.0 / COUNT(1), 1)
      comment: "성능률 (%) - 최적 범위 가동 비율"
    - name: quality_pct
      expr: ROUND(COUNT(1) FILTER (WHERE vibration_rms > 0 AND temperature_c > 0 AND pressure_bar > 50) * 100.0 / COUNT(1), 1)
      comment: "품질률 (%) - 유효 데이터 비율"
    - name: oee_pct
      expr: >
        ROUND(
          (COUNT(1) FILTER (WHERE vibration_rms <= 7.0 AND temperature_c <= 75.0) * 100.0 / COUNT(1)) *
          (COUNT(1) FILTER (WHERE vibration_rms <= 4.5 AND temperature_c <= 60.0) * 100.0 / COUNT(1)) *
          (COUNT(1) FILTER (WHERE vibration_rms > 0 AND temperature_c > 0 AND pressure_bar > 50) * 100.0 / COUNT(1))
          / 10000, 1)
      comment: "OEE (%) = 가동률 × 성능률 × 품질률"
$$;

-- COMMAND ----------

-- 검증: Metric View 조회 (measure 컬럼은 MEASURE() 함수로 감싸야 함)
SELECT
  equipment_id,
  MEASURE(availability_pct) as availability_pct,
  MEASURE(performance_pct) as performance_pct,
  MEASURE(quality_pct) as quality_pct,
  MEASURE(oee_pct) as oee_pct
FROM ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee
GROUP BY equipment_id
ORDER BY oee_pct;
