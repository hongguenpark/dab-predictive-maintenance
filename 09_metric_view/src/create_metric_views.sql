-- Databricks notebook source
-- 예측 정비(Predictive Maintenance) - Metric View 정의
-- Metric View는 DAB에서 직접 리소스로 선언 불가하지만, SQL notebook을 통해 배포 가능

-- COMMAND ----------

-- 1. 장비 가동률 메트릭
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_availability AS
SELECT
  equipment_id,
  DATE(timestamp) as metric_date,
  COUNT(*) as total_readings,
  SUM(CASE WHEN vibration_rms <= 7.0 AND temperature_c <= 75.0 THEN 1 ELSE 0 END) as normal_readings,
  ROUND(
    SUM(CASE WHEN vibration_rms <= 7.0 AND temperature_c <= 75.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) as availability_pct
FROM ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
GROUP BY equipment_id, DATE(timestamp);

-- COMMAND ----------

-- 2. 평균 고장 간격 (MTBF) 메트릭
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_mtbf AS
SELECT
  equipment_id,
  COUNT(DISTINCT CASE WHEN vibration_rms > 7.0 OR temperature_c > 75.0 THEN DATE(timestamp) END) as failure_days,
  DATEDIFF(MAX(timestamp), MIN(timestamp)) as total_days,
  ROUND(
    DATEDIFF(MAX(timestamp), MIN(timestamp)) * 1.0 /
    GREATEST(COUNT(DISTINCT CASE WHEN vibration_rms > 7.0 OR temperature_c > 75.0 THEN DATE(timestamp) END), 1),
    1
  ) as mtbf_days
FROM ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
GROUP BY equipment_id;

-- COMMAND ----------

-- 3. OEE (Overall Equipment Effectiveness) 근사 메트릭
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee AS
SELECT
  e.equipment_id,
  e.equipment_name,
  e.criticality,
  avail.availability_pct,
  -- Performance: 정상 범위 내 가동 비율
  ROUND(
    SUM(CASE WHEN s.vibration_rms <= 4.5 AND s.temperature_c <= 60.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) as performance_pct,
  -- Quality: 이상 없는 데이터 비율
  ROUND(
    SUM(CASE WHEN s.vibration_rms > 0 AND s.temperature_c > 0 AND s.pressure_bar > 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) as quality_pct,
  -- OEE = 가동률 × 성능 × 품질
  ROUND(
    avail.availability_pct *
    (SUM(CASE WHEN s.vibration_rms <= 4.5 AND s.temperature_c <= 60.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) *
    (SUM(CASE WHEN s.vibration_rms > 0 AND s.temperature_c > 0 AND s.pressure_bar > 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
    / 10000, 1
  ) as oee_pct
FROM ebay_anomaly_detection_catalog.predictive_maintenance.equipment_master e
JOIN ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings s ON e.equipment_id = s.equipment_id
JOIN (
  SELECT equipment_id, AVG(availability_pct) as availability_pct
  FROM ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_availability
  GROUP BY equipment_id
) avail ON e.equipment_id = avail.equipment_id
GROUP BY e.equipment_id, e.equipment_name, e.criticality, avail.availability_pct;

-- COMMAND ----------

-- 검증 쿼리
SELECT * FROM ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee ORDER BY oee_pct ASC;
