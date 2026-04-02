# Lakebase - DAB 배포 테스트

## 결론: DAB 직접 배포 미지원

Lakebase (Databricks 관리형 PostgreSQL)는 현재 DAB에서 리소스로 직접 선언/배포가 **지원되지 않습니다**.

### 대안
1. **Databricks App + Lakebase SDK**: App 내에서 프로그래밍 방식으로 Lakebase 연결
2. **노트북/Job**: 노트북에서 `databricks-lakebase` SDK로 테이블 생성 및 데이터 적재
3. **REST API**: Lakebase REST API를 통한 프로비저닝

### 예측 정비 시나리오에서의 활용
- 실시간 장비 상태 대시보드 백엔드
- 정비 이력 관리 (CRUD 워크로드)
- 알림/티켓 관리 시스템
