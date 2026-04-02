# Databricks Asset Bundles (DAB) 기능별 배포 테스트

**예측 정비 (Predictive Maintenance) 시나리오** 기반으로 13개 Databricks 기능의 DAB 배포 가능 여부를 테스트한 프로젝트입니다.

> 📊 **[HTML 보고서 보기](https://hongguen-park_data.github.io/dab-predictive-maintenance/)** | 🏭 **[Databricks 워크스페이스](https://fevm-ebay-anomaly-detection.cloud.databricks.com)**

## 테스트 결과 요약

| # | 기능 | DAB 배포 방식 | 리소스 타입 | 실행 결과 |
|---|------|-------------|-----------|----------|
| 1 | Unity Catalog Function | Job 경유 | `schemas` + `jobs` | ✅ SUCCESS |
| 2 | Notebook | 직접 배포 | `jobs` (notebook_task) | ✅ SUCCESS |
| 3 | Job (Workflow) | 직접 배포 | `jobs` | ✅ SUCCESS |
| 4 | DLT Pipeline (Lakeflow) | 직접 배포 | `pipelines` | ✅ 배포 완료 |
| 5 | Lakebase | Job 경유 | `jobs` (REST API + SQL) | ✅ SUCCESS |
| 6 | Databricks App | 직접 배포 | `apps` | ✅ 배포 완료 |
| 7 | AI/BI Dashboard | 직접 배포 | `dashboards` | ✅ 배포 완료 |
| 8 | Genie | Job 경유 | `jobs` (REST API + SQL) | ✅ SUCCESS |
| 9 | Metric View | Job 경유 | `jobs` (SQL notebook) | ✅ SUCCESS |
| 10 | ML Custom Model | 직접 배포 | `experiments` + `registered_models` | ✅ SUCCESS |
| 11 | Model Serving | 직접 배포 | `model_serving_endpoints` | ⏳ 모델 등록 후 배포 |
| 12 | AgentBricks Knowledge Assistant | Job 경유 | `jobs` (지식 베이스 준비) | ✅ SUCCESS |
| 13 | AgentBricks Supervisor Agent | Job 경유 | `jobs` (Agent 설정 코드) | ✅ SUCCESS |

- **직접 배포 가능**: 8개 — YAML 선언만으로 자동 배포
- **Job 경유 배포**: 5개 — 노트북에서 SQL/REST API 실행하여 배포
- **실행 테스트**: 9/9 SUCCESS

## 시나리오: 예측 정비 (Predictive Maintenance)

공장 8대 핵심 설비의 IoT 센서 데이터를 수집·분석하여 장비 상태를 모니터링하고, ML 모델로 잔여 수명(RUL)을 예측합니다.

```
IoT 센서 → DLT Pipeline (Bronze→Silver→Gold) → 일일 분석 Job → ML 모델 → AI/BI Dashboard
정비 매뉴얼 → Vector Search → Knowledge Assistant → Supervisor Agent
정비 작업주문 → Lakebase (OLTP) → Databricks App (대시보드)
```

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

- `assess_vibration_status(vibration_rms, temperature)` — 장비 상태 판별 (정상/주의/위험/긴급)
- `estimate_rul(operating_hours, avg_vibration, avg_temperature)` — 잔여 수명 추정
- `maintenance_priority_score(rul_hours, criticality, last_maintenance_days)` — 정비 우선순위 점수

## 프로젝트 구조

```
├── databricks.yml                    # 메인 번들 설정
├── index.html                        # 테스트 결과 HTML 보고서
├── 01_unity_catalog_function/        # UC Function (SQL notebook → Job)
├── 02_notebook/                      # 탐색적 데이터 분석 노트북
├── 03_job/                           # 멀티태스크 일일 분석 Job
├── 04_dlt_pipeline/                  # DLT Medallion 파이프라인
├── 05_lakebase/                      # Lakebase REST API + OLTP 테이블
├── 06_databricks_app/                # FastAPI 장비 모니터링 대시보드
├── 07_aibi_dashboard/                # AI/BI Lakeview 대시보드
├── 08_genie/                         # Genie Space (REST API + 호환 뷰)
├── 09_metric_view/                   # OEE, MTBF, 가동률 메트릭 뷰
├── 10_ml_custom_model/               # LightGBM RUL 예측 모델
├── 11_model_serving/                 # Model Serving Endpoint
├── 12_agent_knowledge_assistant/     # 정비 매뉴얼 지식 베이스
└── 13_agent_supervisor/              # Supervisor Agent 설정
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

# 2. databricks.yml에서 workspace/profile 수정

# 3. 카탈로그 내 스키마 생성
databricks schemas create predictive_maintenance your_catalog --profile my-profile

# 4. 번들 검증 및 배포
databricks bundle validate --profile my-profile
databricks bundle deploy --profile my-profile

# 5. Job 실행 (순서: EDA → UC Functions → 나머지)
databricks bundle run pm_eda_notebook --profile my-profile
databricks bundle run deploy_uc_functions --profile my-profile
databricks bundle run deploy_lakebase --profile my-profile
databricks bundle run deploy_genie_space --profile my-profile
databricks bundle run deploy_metric_views --profile my-profile
databricks bundle run pm_daily_analysis --profile my-profile
databricks bundle run pm_model_training --profile my-profile
databricks bundle run deploy_knowledge_assistant --profile my-profile
databricks bundle run deploy_supervisor_agent --profile my-profile
```

## 테스트 환경

| 항목 | 값 |
|------|-----|
| 워크스페이스 | fevm-ebay-anomaly-detection.cloud.databricks.com |
| 카탈로그 | ebay_anomaly_detection_catalog |
| 스키마 | predictive_maintenance |
| Databricks CLI | v0.295.0 |
| 테스트 일시 | 2026-04-02 |
