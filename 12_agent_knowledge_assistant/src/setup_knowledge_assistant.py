# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - Knowledge Assistant (AgentBricks) SDK 배포
# MAGIC
# MAGIC Databricks SDK의 `KnowledgeAssistantsAPI`를 사용하여 Knowledge Assistant를 프로그래밍 방식으로 생성합니다.
# MAGIC
# MAGIC ### 구성 요소
# MAGIC 1. 정비 매뉴얼 문서를 Unity Catalog 테이블에 적재
# MAGIC 2. Knowledge Assistant 생성 (SDK)
# MAGIC 3. Knowledge Source (UC 테이블) 연결

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 정비 매뉴얼 데이터 준비

# COMMAND ----------

# 카탈로그 및 스키마 설정
CATALOG = "ebay_anomaly_detection_catalog"
SCHEMA = "predictive_maintenance"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.maintenance_knowledge_base"

# 정비 매뉴얼 지식 베이스 테이블 생성 (카탈로그와 스키마는 이미 존재)

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
df_docs.write.mode("overwrite").saveAsTable(TABLE_NAME)

print(f"정비 문서 {len(knowledge_docs)}건 적재 완료: {TABLE_NAME}")
display(df_docs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Knowledge Assistant 생성 (SDK)

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.knowledgeassistants import KnowledgeAssistant

w = WorkspaceClient()

ASSISTANT_DISPLAY_NAME = "PM - 예측 정비 Knowledge Assistant"
ASSISTANT_DESCRIPTION = "공장 설비 예측 정비 전문 지식 어시스턴트"
ASSISTANT_INSTRUCTIONS = (
    "당신은 공장 설비 예측 정비 전문 AI 어시스턴트입니다. "
    "정비 매뉴얼, 장비 사양, 정비 절차를 기반으로 한국어로 답변합니다."
)

# 기존 Knowledge Assistant 확인 및 삭제 (멱등성 보장)
existing = None
try:
    ka_list = w.knowledge_assistants.list_knowledge_assistants()
    for ka_item in ka_list.knowledge_assistants or []:
        if ka_item.display_name == ASSISTANT_DISPLAY_NAME:
            existing = ka_item
            break
except Exception as e:
    print(f"기존 Knowledge Assistant 목록 조회 중 오류 (무시하고 진행): {e}")

if existing:
    print(f"기존 Knowledge Assistant 발견: {existing.name} - 삭제 후 재생성합니다.")
    try:
        w.knowledge_assistants.delete_knowledge_assistant(name=existing.name)
        print("기존 Knowledge Assistant 삭제 완료")
    except Exception as e:
        print(f"삭제 중 오류: {e}")

# Knowledge Assistant 생성
ka = w.knowledge_assistants.create_knowledge_assistant(
    knowledge_assistant=KnowledgeAssistant(
        display_name=ASSISTANT_DISPLAY_NAME,
        description=ASSISTANT_DESCRIPTION,
        instructions=ASSISTANT_INSTRUCTIONS,
    )
)

print(f"Knowledge Assistant 생성 완료: {ka.name}")
print(f"  - display_name: {ka.display_name}")
print(f"  - description: {ka.description}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Knowledge Source (UC 테이블) 연결

# COMMAND ----------

from databricks.sdk.service.knowledgeassistants import KnowledgeSource, FileTableSpec

# Knowledge Source 추가 - 정비 매뉴얼 UC 테이블 연결
try:
    ks = w.knowledge_assistants.create_knowledge_source(
        parent=ka.name,
        knowledge_source=KnowledgeSource(
            display_name="정비 매뉴얼",
            description="설비 정비 매뉴얼 및 절차 문서",
            file_table=FileTableSpec(
                table_name=TABLE_NAME,
                file_col="content",
            ),
        ),
    )
    print(f"Knowledge Source 추가 완료: {ks.name}")
    print(f"  - display_name: {ks.display_name}")
    print(f"  - table_name: {TABLE_NAME}")
except Exception as e:
    print(f"Knowledge Source 추가 중 오류 (API가 아직 지원하지 않을 수 있음): {e}")
    print("수동으로 UI에서 Knowledge Source를 추가해 주세요.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 결과 출력

# COMMAND ----------

# 최종 결과를 JSON으로 출력
result = {
    "status": "SUCCESS",
    "knowledge_assistant": {
        "name": ka.name,
        "display_name": ka.display_name,
        "description": ka.description,
    },
    "knowledge_base_table": TABLE_NAME,
    "document_count": len(knowledge_docs),
}

print(json.dumps(result, ensure_ascii=False, indent=2))
