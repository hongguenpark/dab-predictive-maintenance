# Genie Space - DAB 배포 테스트

## 결론: DAB 직접 배포 미지원

Genie Space는 현재 DAB에서 리소스로 직접 선언/배포가 **지원되지 않습니다**.

### 현재 상태
- Genie Space는 AI/BI Dashboard 내에 통합된 자연어 질의 기능으로 제공
- 독립적인 Genie Space는 REST API로만 관리 가능
- DAB 리소스 타입에 `genie_space`가 아직 없음

### 대안
1. **AI/BI Dashboard + Genie**: Dashboard에 Genie 기능이 통합되어 있으므로 Dashboard를 DAB로 배포하면 Genie 기능도 함께 사용 가능
2. **REST API**: `POST /api/2.0/genie/spaces` API를 통해 프로그래밍 방식으로 생성
3. **노트북 + REST API**: DAB Job에서 REST API를 호출하여 Genie Space 자동 생성

### 예측 정비 시나리오에서의 활용
- "EQ-002 장비의 최근 진동 추이는?" 같은 자연어 질의
- "위험 상태 장비 목록 보여줘" 등 비정형 데이터 분석
