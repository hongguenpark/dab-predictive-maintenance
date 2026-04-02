-- Databricks notebook source
-- 예측 정비(Predictive Maintenance) - Unity Catalog 함수

-- 1. 진동 데이터 기반 장비 상태 판별 함수
CREATE OR REPLACE FUNCTION hg_demos.predictive_maintenance.assess_vibration_status(
  vibration_rms DOUBLE,
  temperature DOUBLE
)
RETURNS STRING
LANGUAGE SQL
COMMENT '진동 RMS와 온도를 기반으로 장비 상태를 판별합니다 (정상/주의/위험/긴급)'
RETURN
  CASE
    WHEN vibration_rms > 10.0 OR temperature > 90.0 THEN '긴급'
    WHEN vibration_rms > 7.0 OR temperature > 75.0 THEN '위험'
    WHEN vibration_rms > 4.5 OR temperature > 60.0 THEN '주의'
    ELSE '정상'
  END;

-- 2. 잔여 수명(RUL) 추정 함수
CREATE OR REPLACE FUNCTION hg_demos.predictive_maintenance.estimate_rul(
  operating_hours DOUBLE,
  avg_vibration DOUBLE,
  avg_temperature DOUBLE,
  max_operating_hours DOUBLE DEFAULT 10000.0
)
RETURNS DOUBLE
LANGUAGE SQL
COMMENT '장비의 잔여 유효 수명(Remaining Useful Life)을 시간 단위로 추정합니다'
RETURN
  GREATEST(
    0,
    max_operating_hours - operating_hours
    - (avg_vibration * 50)
    - (GREATEST(avg_temperature - 50, 0) * 30)
  );

-- 3. 정비 우선순위 점수 계산 함수
CREATE OR REPLACE FUNCTION hg_demos.predictive_maintenance.maintenance_priority_score(
  rul_hours DOUBLE,
  equipment_criticality STRING,
  last_maintenance_days INT
)
RETURNS DOUBLE
LANGUAGE SQL
COMMENT '정비 우선순위 점수를 0~100 사이로 계산합니다 (높을수록 긴급)'
RETURN
  LEAST(100, GREATEST(0,
    (1 - rul_hours / 10000.0) * 40 +
    CASE equipment_criticality
      WHEN '핵심' THEN 30
      WHEN '중요' THEN 20
      WHEN '보조' THEN 10
      ELSE 5
    END +
    LEAST(last_maintenance_days / 10.0, 30)
  ));

-- 테스트 실행
SELECT
  hg_demos.predictive_maintenance.assess_vibration_status(8.5, 72.0) as status,
  hg_demos.predictive_maintenance.estimate_rul(5000, 6.0, 65.0, 10000) as rul_hours,
  hg_demos.predictive_maintenance.maintenance_priority_score(3000, '핵심', 45) as priority;
