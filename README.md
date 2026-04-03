# Databricks Asset Bundles (DAB) 기능별 배포 테스트

**예측 정비 (Predictive Maintenance) 시나리오** 기반으로 13개 Databricks 기능의 DAB 배포 가능 여부를 테스트한 프로젝트입니다.

## 테스트 결과 요약

### DAB 직접 배포 가능 (7개)

YAML 선언만으로 `databricks bundle deploy`로 자동 배포되는 리소스입니다.

| # | 기능 | DAB 리소스 타입 | 배포 결과 | 비고 |
|---|------|---------------|----------|------|
| 1 | **Notebook** | `jobs` (notebook_task) | SUCCESS | DAB sync로 워크스페이스 자동 업로드 |
| 2 | **Job (Workflow)** | `jobs` | SUCCESS | 멀티태스크, 스케줄, 알림 설정 포함 |
| 3 | **DLT Pipeline (Lakeflow)** | `pipelines` | 배포 완료 | Medallion 아키텍처, Serverless, DLT Expectations |
| 4 | **Databricks App** | `apps` | 배포 완료 | FastAPI 대시보드, SQL Warehouse 바인딩 |
| 5 | **AI/BI Dashboard** | `dashboards` | 배포 완료 | Lakeview JSON, 2페이지 12위젯 |
| 6 | **ML Custom Model** | `experiments` + `registered_models` | SUCCESS | MLflow 실험 추적 + UC 모델 레지스트리 |
| 7 | **Model Serving** | `model_serving_endpoints` | 배포 완료 | Scale-to-zero, AI Gateway inference table |

### DAB Job 경유 배포 (3개)

DAB 리소스 타입은 없지만, 노트북에서 SQL 또는 REST API를 실행하는 Job으로 배포합니다.

| # | 기능 | 배포 방법 | 배포 결과 | 비고 |
|---|------|----------|----------|------|
| 8 | **Unity Catalog Function** | SQL notebook (`CREATE FUNCTION`) | SUCCESS | 3개 함수: assess_vibration_status, estimate_rul, maintenance_priority_score |
| 9 | **Metric View** | SQL notebook (`CREATE VIEW WITH METRICS`) | SUCCESS | 3개 Metric View: 가동률, 건강종합, OEE |
| 10 | **Genie** | Python notebook (REST API) | SUCCESS | `serialized_space.data_sources.tables[].identifier`로 테이블 연결 |

### DAB 미지원 - UI 전용 (3개)

DAB 리소스 타입이 없고 REST API로도 배포 불가하여 Databricks UI에서만 생성/구성 가능합니다.

| # | 기능 | 미지원 사유 |
|---|------|-----------|
| 11 | **Lakebase** | DAB 리소스 타입 없음. UI에서만 PostgreSQL 인스턴스 프로비저닝 가능 |
| 12 | **AgentBricks Knowledge Assistant** | UI에서만 구성 가능. Vector Search 인덱스 연결, Foundation Model 선택 등 UI 작업 필요 |
| 13 | **AgentBricks Supervisor Agent** | UI에서만 구성 가능. Sub-Agent 연결, 시스템 프롬프트 등 UI 작업 필요 |

## 시나리오: 예측 정비 (Predictive Maintenance)

공장 8대 핵심 설비의 IoT 센서 데이터를 수집/분석하여 장비 상태를 모니터링하고, ML 모델로 잔여 수명(RUL)을 예측합니다.

```
IoT 센서 → DLT Pipeline (Bronze→Silver→Gold) → 일일 분석 Job → ML 모델 → AI/BI Dashboard
센서 데이터 → Metric View (가동률, OEE, 건강지표) → Genie (자연어 질의)
```

### Genie + Metric View 연동

Genie Space에서 Metric View를 데이터 소스로 활용하여 자연어로 장비 상태를 질의합니다.

**Metric View (3개)**:

| Metric View | 설명 | Dimensions | 주요 Measures |
|------------|------|-----------|--------------|
| `mv_equipment_availability` | 장비별 일별 가동률 | equipment_id, measurement_date | availability_pct, normal_readings, abnormal_readings |
| `mv_equipment_health` | 장비 건강 종합 | equipment_id | avg_vibration, avg_temperature, anomaly_rate_pct, warning_count |
| `mv_oee` | OEE 종합설비효율 | equipment_id | availability_pct, performance_pct, quality_pct, oee_pct |

**Genie에서 할 수 있는 질문 예시**:
- "장비별 가동률을 보여줘"
- "OEE가 60% 미만인 장비는?"
- "이상 징후 비율이 높은 장비 Top 3는?"
- "핵심 등급 장비들의 OEE와 이상률을 보여줘"

### 장비 목록

| 장비 ID | 장비명 | 중요도 |
|--------|--------|-------|
| EQ-001 | CNC 선반 | 핵심 |
| EQ-002 | 프레스기 | 핵심 |
| EQ-003 | 컨베이어 | 중요 |
| EQ-004 | 로봇암 | 핵심 |
| EQ-005 | 펌프 | 보조 |
| EQ-006 | 압축기 | 중요 |
| EQ-007 | 냉각기 | 중요 |
| EQ-008 | 보일러 | 핵심 |

### UC Functions

- `assess_vibration_status(vibration_rms, temperature)` - 장비 상태 판별 (정상/주의/위험/긴급)
- `estimate_rul(operating_hours, avg_vibration, avg_temperature)` - 잔여 수명 추정
- `maintenance_priority_score(rul_hours, criticality, last_maintenance_days)` - 정비 우선순위 점수

## 프로젝트 구조

```
dab-predictive-maintenance/
├── databricks.yml                    # 메인 번들 설정
├── index.html                        # 테스트 결과 HTML 보고서
├── 00_report_app/                    # HTML 보고서 Databricks App
├── 01_unity_catalog_function/        # UC Function (SQL notebook -> Job)
├── 02_notebook/                      # 탐색적 데이터 분석 노트북
├── 03_job/                           # 멀티태스크 일일 분석 Job
├── 04_dlt_pipeline/                  # DLT Medallion 파이프라인 (Serverless)
├── 05_lakebase/                      # Lakebase 참고 코드 (DAB 미지원)
├── 06_databricks_app/                # FastAPI 장비 모니터링 대시보드
├── 07_aibi_dashboard/                # AI/BI Lakeview 대시보드 (바차트 8개)
├── 08_genie/                         # Genie Space (REST API, Metric View 연동)
├── 09_metric_view/                   # Metric View (WITH METRICS + YAML)
├── 10_ml_custom_model/               # LightGBM RUL 예측 모델
├── 11_model_serving/                 # Model Serving Endpoint
├── 12_agent_knowledge_assistant/     # AgentBricks 참고 코드 (DAB 미지원)
└── 13_agent_supervisor/              # AgentBricks 참고 코드 (DAB 미지원)
```

## 주요 기술 포인트

### Genie REST API
```python
# serialized_space에 data_sources.tables[].identifier로 테이블 연결
# tables는 identifier 기준 알파벳순 정렬 필수
payload = {
    "serialized_space": json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": "catalog.schema.table"}]  # 정렬 필수
        }
    }),
    "warehouse_id": "...",
    "title": "...",
    "description": "..."
}
requests.post(f"{base_url}/genie/spaces", headers=headers, json=payload)
```

### Metric View 문법
```sql
CREATE OR REPLACE VIEW catalog.schema.metric_view_name
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "설명"
  source: catalog.schema.source_table
  dimensions:
    - name: equipment_id
      expr: equipment_id
  measures:
    - name: availability_pct
      expr: ROUND(COUNT(1) FILTER (WHERE ...) * 100.0 / COUNT(1), 1)
      comment: "가동률 (%)"
$$;

-- 조회 시 MEASURE() 함수 필수
SELECT equipment_id, MEASURE(availability_pct) FROM metric_view GROUP BY equipment_id;
```

### AI/BI Dashboard (Lakeview JSON)
```json
// 위젯 필드 매핑: query.fields + encodings.scale.type 필수
// counter/table/pie는 JSON API로 필드 매핑 불가 -> bar 차트로 대체
{
  "query": {
    "datasetName": "ds_name",
    "disaggregated": true,
    "fields": [{"name": "col", "expression": "`col`"}]
  },
  "spec": {
    "widgetType": "bar",
    "encodings": {
      "x": {"fieldName": "col", "scale": {"type": "categorical"}},
      "y": {"fieldName": "val", "scale": {"type": "quantitative"}}
    }
  }
}
```

## 사용 방법

### 사전 요구사항

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.200+
- Databricks 워크스페이스 접근 권한
- Unity Catalog 활성화

### 배포

```bash
# 1. 프로필 설정
databricks auth login --host https://your-workspace.cloud.databricks.com --profile my-profile

# 2. databricks.yml에서 workspace/profile/catalog 수정

# 3. 카탈로그 내 스키마 생성
databricks schemas create predictive_maintenance your_catalog --profile my-profile

# 4. 번들 검증 및 배포
databricks bundle validate --profile my-profile
databricks bundle deploy --profile my-profile

# 5. Job 실행 (순서 중요)
# Phase 1: 데이터 생성 + 함수 생성
databricks bundle run pm_eda_notebook --profile my-profile
databricks bundle run deploy_uc_functions --profile my-profile

# Phase 2: 분석 + ML
databricks bundle run pm_daily_analysis --profile my-profile
databricks bundle run pm_model_training --profile my-profile

# Phase 3: Metric View → Genie (순서 중요)
databricks bundle run deploy_metric_views --profile my-profile
databricks bundle run deploy_genie_space --profile my-profile
```

## 테스트 환경

| 항목 | 값 |
|------|-----|
| 워크스페이스 | fevm-ebay-anomaly-detection.cloud.databricks.com |
| 카탈로그 | ebay_anomaly_detection_catalog |
| 스키마 | predictive_maintenance |
| Databricks CLI | v0.295.0 |
| 테스트 일시 | 2026-04-03 |
