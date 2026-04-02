# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - Knowledge Assistant (AgentBricks) 설정
# MAGIC
# MAGIC AgentBricks Knowledge Assistant는 **DAB에서 직접 리소스로 배포할 수 없습니다**.
# MAGIC 하지만 이 노트북을 DAB Job으로 배포하여 프로그래밍 방식으로 설정할 수 있습니다.
# MAGIC
# MAGIC ### 구성 요소
# MAGIC 1. 정비 매뉴얼 문서를 Vector Search 인덱스에 적재
# MAGIC 2. Foundation Model + Vector Search를 결합한 RAG 에이전트 설정

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 정비 매뉴얼 데이터 준비

# COMMAND ----------

from pyspark.sql import functions as F

# 예측 정비 관련 지식 베이스 문서
knowledge_docs = [
    ("DOC-001", "CNC 선반 정비 가이드", "CNC 선반의 주기적 정비는 3개월마다 수행합니다. 주요 점검 항목: 스핀들 베어링 상태, 윤활유 레벨, 벨트 장력, 쿨런트 농도. 진동 RMS가 7.0을 초과하면 즉시 베어링 교체가 필요합니다. 온도가 75°C를 넘으면 냉각 시스템을 점검하세요."),
    ("DOC-002", "프레스기 안전 점검", "프레스기 정비 전 반드시 전원을 차단하고 잠금/태그 절차를 수행합니다. 주요 점검: 유압 실린더 누유, 가이드 마모, 클러치/브레이크 상태. 비정상 소음 발생 시 즉시 가동을 중단하세요."),
    ("DOC-003", "베어링 교체 절차", "1. 장비 정지 및 에너지 차단 2. 하우징 커버 분리 3. 풀러를 사용하여 기존 베어링 제거 4. 새 베어링에 적정 그리스 도포 5. 프레스핏으로 신품 장착 6. 적정 토크로 볼트 체결 7. 시운전 후 진동 측정으로 확인"),
    ("DOC-004", "진동 분석 해석 가이드", "진동 RMS 기준: 0-4.5(정상), 4.5-7.0(주의-모니터링 강화), 7.0-10.0(위험-정비 계획 수립), 10.0 이상(긴급-즉시 정비). FFT 분석에서 1X 성분이 높으면 언밸런스, 2X 성분이 높으면 미스얼라인먼트를 의심합니다."),
    ("DOC-005", "예비부품 관리 기준", "핵심 장비(CNC, 프레스, 로봇암)의 핵심 부품은 최소 2세트 재고 유지. 베어링 평균 수명: 20,000시간. 유압 호스: 5년마다 교체. 필터류: 3개월마다 교체. 윤활유: 6개월마다 교체."),
    ("DOC-006", "컨베이어 벨트 정비", "컨베이어 벨트 장력은 매주 점검합니다. 벨트 마모도가 50%를 넘으면 교체를 계획하세요. 풀리 정렬 상태를 월 1회 확인합니다. 벨트 사행이 발생하면 텐션 풀리를 조정합니다."),
    ("DOC-007", "IoT 센서 캘리브레이션", "진동 센서: 6개월마다 교정. 온도 센서: 12개월마다 교정. 압력 센서: 12개월마다 교정. 교정 기준은 NIST 표준을 따릅니다. 센서 이상 시 데이터 품질 알림이 트리거됩니다."),
    ("DOC-008", "긴급 정비 대응 절차", "긴급 상태 장비 발견 시: 1. 즉시 생산 관리자에게 보고 2. 해당 장비 가동 중단 3. 안전 구역 설정 4. 정비팀 긴급 출동 요청 5. 고장 원인 분석 6. 수리 후 시운전 7. 정비 이력 시스템에 기록"),
]

schema = "doc_id STRING, title STRING, content STRING"
df_docs = spark.createDataFrame(knowledge_docs, schema)
df_docs.write.mode("overwrite").saveAsTable("hg_demos.predictive_maintenance.maintenance_knowledge_base")

print(f"✅ {len(knowledge_docs)}개 정비 문서 적재 완료")
display(df_docs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vector Search 인덱스 생성 (수동 설정 필요)
# MAGIC
# MAGIC 아래 코드는 Vector Search 인덱스를 생성하는 참고 코드입니다.
# MAGIC AgentBricks Knowledge Assistant는 UI에서 설정해야 합니다.

# COMMAND ----------

# Vector Search 엔드포인트 및 인덱스 생성 참고 코드
# from databricks.vector_search.client import VectorSearchClient
#
# vs_client = VectorSearchClient()
#
# # 엔드포인트 생성 (이미 존재하면 스킵)
# vs_client.create_endpoint(name="pm_vs_endpoint", endpoint_type="STANDARD")
#
# # Delta Sync 인덱스 생성
# vs_client.create_delta_sync_index(
#     endpoint_name="pm_vs_endpoint",
#     index_name="hg_demos.predictive_maintenance.maintenance_kb_index",
#     source_table_name="hg_demos.predictive_maintenance.maintenance_knowledge_base",
#     pipeline_type="TRIGGERED",
#     primary_key="doc_id",
#     embedding_source_column="content",
#     embedding_model_endpoint_name="databricks-gte-large-en",
# )

print("⚠️ Knowledge Assistant는 Databricks UI > AI/BI > AgentBricks에서 수동 구성이 필요합니다.")
print("   - 위 Vector Search 인덱스를 지식 소스로 연결")
print("   - Foundation Model 선택 (예: DBRX, Llama 3)")
print("   - 시스템 프롬프트: '당신은 공장 설비 정비 전문 어시스턴트입니다.'")
