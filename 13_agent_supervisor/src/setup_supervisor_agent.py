# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - Supervisor Agent (AgentBricks) 설정
# MAGIC
# MAGIC AgentBricks Supervisor Agent는 **DAB에서 직접 리소스로 배포할 수 없습니다**.
# MAGIC 하지만 이 노트북에서 에이전트 구성을 코드로 정의하고 MLflow로 등록할 수 있습니다.
# MAGIC
# MAGIC ### Supervisor Agent 아키텍처
# MAGIC ```
# MAGIC [사용자 질의]
# MAGIC      │
# MAGIC [Supervisor Agent] ── 작업 분배 및 결과 종합
# MAGIC      ├── [지식 검색 Agent] ── 정비 매뉴얼 RAG
# MAGIC      ├── [데이터 분석 Agent] ── 센서 데이터 SQL 쿼리
# MAGIC      └── [예측 Agent] ── RUL 예측 모델 호출
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Agent 도구(Tool) 정의

# COMMAND ----------

# 참고: 실제 AgentBricks Supervisor Agent 설정은 UI에서 수행
# 아래는 구성 개요를 코드로 문서화한 것입니다.

agent_config = {
    "name": "예측정비_슈퍼바이저",
    "description": "공장 설비 예측 정비를 위한 통합 AI 에이전트",
    "system_prompt": """당신은 공장 설비 예측 정비 전문 AI 어시스턴트입니다.
사용자의 질문에 따라 적절한 하위 에이전트를 호출하여 답변합니다.

역할:
1. 정비 지식 질의 → 지식 검색 Agent 호출
2. 장비 상태 확인 → 데이터 분석 Agent 호출
3. 수명 예측 → 예측 Agent 호출
4. 복합 질문 → 여러 Agent를 순차/병렬 호출 후 결과 종합

항상 한국어로 답변하세요.""",

    "sub_agents": [
        {
            "name": "지식_검색_Agent",
            "type": "knowledge_assistant",
            "description": "정비 매뉴얼, 절차서, 가이드 문서를 검색합니다",
            "knowledge_source": "hg_demos.predictive_maintenance.maintenance_kb_index",
            "trigger_keywords": ["매뉴얼", "절차", "가이드", "방법", "어떻게"]
        },
        {
            "name": "데이터_분석_Agent",
            "type": "sql_agent",
            "description": "센서 데이터와 장비 상태를 SQL로 분석합니다",
            "warehouse_id": "6898e1e8ebe84201",
            "allowed_tables": [
                "hg_demos.predictive_maintenance.equipment_master",
                "hg_demos.predictive_maintenance.sensor_readings",
                "hg_demos.predictive_maintenance.equipment_health_daily",
                "hg_demos.predictive_maintenance.maintenance_alerts"
            ],
            "trigger_keywords": ["상태", "데이터", "진동", "온도", "센서", "장비"]
        },
        {
            "name": "예측_Agent",
            "type": "model_serving",
            "description": "ML 모델을 호출하여 장비 잔여 수명을 예측합니다",
            "endpoint_name": "pm-rul-prediction-hg",
            "trigger_keywords": ["수명", "예측", "언제", "RUL", "고장"]
        }
    ]
}

import json
print("=== Supervisor Agent 구성 ===")
print(json.dumps(agent_config, ensure_ascii=False, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 에이전트 등록 (MLflow)

# COMMAND ----------

# Supervisor Agent를 MLflow에 등록하는 참고 코드
# 실제로는 AgentBricks UI에서 설정하거나 Mosaic AI Agent Framework 사용
#
# import mlflow
# from mlflow.models import set_model
#
# class PredictiveMaintenanceSupervisor(mlflow.pyfunc.PythonModel):
#     def predict(self, context, model_input):
#         query = model_input["query"][0]
#         # 질의 의도 분류 → 적절한 sub-agent 호출 → 결과 종합
#         return {"response": f"[Supervisor] 질의 처리 결과: {query}"}
#
# with mlflow.start_run():
#     mlflow.pyfunc.log_model(
#         artifact_path="supervisor_agent",
#         python_model=PredictiveMaintenanceSupervisor(),
#         registered_model_name="hg_demos.predictive_maintenance.supervisor_agent"
#     )

print("⚠️ Supervisor Agent는 Databricks UI > AI/BI > AgentBricks에서 수동 구성이 필요합니다.")
print("   - Supervisor Agent 유형 선택")
print("   - 위 3개 Sub-Agent 연결")
print("   - 시스템 프롬프트 설정")
print("   - Model Serving Endpoint로 배포")
