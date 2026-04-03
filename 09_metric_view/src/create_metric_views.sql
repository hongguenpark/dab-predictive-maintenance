-- Databricks notebook source
-- 예측 정비(Predictive Maintenance) - Metric View 정의
-- Genie Space에서 자연어로 조회할 수 있는 Metric View를 생성합니다.

-- COMMAND ----------

-- 0. 기존 뷰 삭제 (Metric View로 재생성)
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_availability;
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_health;
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee;
DROP VIEW IF EXISTS ebay_anomaly_detection_catalog.predictive_maintenance.mv_mtbf;

-- COMMAND ----------

-- 1. 장비 가동률 메트릭
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_availability
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: >
    장비별 일별 가동률 메트릭.
    가동률은 전체 센서 측정 중 진동과 온도가 정상 범위인 비율입니다.
    정상 기준: 진동 RMS <= 7.0 AND 온도 <= 75도.
    Genie에서 '장비별 가동률', '가동률이 낮은 장비' 등으로 질의하세요.
  source: ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
  dimensions:
    - name: equipment_id
      expr: equipment_id
      comment: "장비 ID (EQ-001 ~ EQ-008)"
    - name: measurement_date
      expr: DATE(timestamp)
      comment: "센서 측정 날짜"
  measures:
    - name: total_readings
      expr: COUNT(1)
      comment: "총 센서 측정 횟수"
    - name: normal_readings
      expr: COUNT(1) FILTER (WHERE vibration_rms <= 7.0 AND temperature_c <= 75.0)
      comment: "정상 범위 측정 횟수"
    - name: abnormal_readings
      expr: COUNT(1) FILTER (WHERE vibration_rms > 7.0 OR temperature_c > 75.0)
      comment: "이상 범위 측정 횟수 (진동 > 7 OR 온도 > 75)"
    - name: availability_pct
      expr: ROUND(COUNT(1) FILTER (WHERE vibration_rms <= 7.0 AND temperature_c <= 75.0) * 100.0 / COUNT(1), 1)
      comment: "가동률 (%) - 높을수록 좋음, 90% 이상이 목표"
$$;

-- COMMAND ----------

-- 2. 장비 건강 종합 메트릭
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_health
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: >
    장비별 센서 건강 지표 종합 메트릭.
    진동, 온도, 압력, 전류의 평균/최대값과 이상 징후 비율을 집계합니다.
    장비 상태 기준: 정상(진동<=4.5,온도<=60), 주의(진동>4.5 or 온도>60),
    위험(진동>7 or 온도>75), 긴급(진동>10 or 온도>90).
    Genie에서 '장비 건강 상태', '이상 비율이 높은 장비', '진동이 가장 큰 장비' 등으로 질의하세요.
  source: ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
  dimensions:
    - name: equipment_id
      expr: equipment_id
      comment: "장비 ID (EQ-001 ~ EQ-008)"
  measures:
    - name: avg_vibration
      expr: ROUND(AVG(vibration_rms), 2)
      comment: "평균 진동 RMS - 정상 기준 4.5 이하, 위험 기준 7.0 초과"
    - name: max_vibration
      expr: ROUND(MAX(vibration_rms), 2)
      comment: "최대 진동 RMS"
    - name: avg_temperature
      expr: ROUND(AVG(temperature_c), 1)
      comment: "평균 온도 (℃) - 정상 기준 60도 이하, 위험 기준 75도 초과"
    - name: max_temperature
      expr: ROUND(MAX(temperature_c), 1)
      comment: "최대 온도 (℃)"
    - name: avg_pressure
      expr: ROUND(AVG(pressure_bar), 1)
      comment: "평균 압력 (bar)"
    - name: avg_current
      expr: ROUND(AVG(current_amp), 2)
      comment: "평균 전류 (A)"
    - name: total_readings
      expr: COUNT(1)
      comment: "총 측정 건수"
    - name: critical_count
      expr: COUNT(1) FILTER (WHERE vibration_rms > 10.0 OR temperature_c > 90.0)
      comment: "긴급 상태 건수 (진동>10 OR 온도>90)"
    - name: warning_count
      expr: COUNT(1) FILTER (WHERE vibration_rms > 7.0 OR temperature_c > 75.0)
      comment: "위험 이상 건수 (진동>7 OR 온도>75)"
    - name: anomaly_rate_pct
      expr: ROUND(COUNT(1) FILTER (WHERE vibration_rms > 7.0 OR temperature_c > 75.0) * 100.0 / COUNT(1), 2)
      comment: "이상 징후 비율 (%) - 낮을수록 좋음"
$$;

-- COMMAND ----------

-- 3. OEE (종합설비효율) 메트릭
CREATE OR REPLACE VIEW ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: >
    OEE(Overall Equipment Effectiveness, 종합설비효율) 메트릭.
    OEE = 가동률 × 성능률 × 품질률.
    가동률: 정상 가동 비율 (진동<=7, 온도<=75).
    성능률: 최적 성능 비율 (진동<=4.5, 온도<=60).
    품질률: 유효 데이터 비율 (압력>50).
    세계적 수준 OEE는 85% 이상, 일반적으로 60% 이상이 목표입니다.
    Genie에서 'OEE', '종합설비효율', '설비효율이 낮은 장비' 등으로 질의하세요.
  source: ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings
  dimensions:
    - name: equipment_id
      expr: equipment_id
      comment: "장비 ID (EQ-001 ~ EQ-008)"
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
      comment: "OEE (%) = 가동률 × 성능률 × 품질률 / 10000"
$$;

-- COMMAND ----------

-- 검증: OEE Metric View 조회
SELECT
  equipment_id,
  MEASURE(availability_pct) as availability_pct,
  MEASURE(performance_pct) as performance_pct,
  MEASURE(quality_pct) as quality_pct,
  MEASURE(oee_pct) as oee_pct
FROM ebay_anomaly_detection_catalog.predictive_maintenance.mv_oee
GROUP BY equipment_id
ORDER BY oee_pct;

-- COMMAND ----------

-- 검증: 장비 건강 Metric View 조회
SELECT
  equipment_id,
  MEASURE(avg_vibration) as avg_vibration,
  MEASURE(avg_temperature) as avg_temperature,
  MEASURE(anomaly_rate_pct) as anomaly_rate_pct,
  MEASURE(warning_count) as warning_count
FROM ebay_anomaly_detection_catalog.predictive_maintenance.mv_equipment_health
GROUP BY equipment_id
ORDER BY anomaly_rate_pct DESC;
