# 👕 의류 데이터 임베딩 ETL 파이프라인 (Airflow)

이 프로젝트는 의류 상품 ID 목록을 기반으로 상세 정보를 수집, 정제하고 외부 GPU 서버를 통해 텍스트 임베딩을 추출하여 데이터베이스에 적재하는 **Airflow 기반의 대용량 배치 파이프라인**입니다.

## 🚀 아키텍처 및 파이프라인 흐름

대규모 데이터를 한 번에 처리할 때 발생하는 메모리 및 네트워크 과부하를 방지하기 위해 **1,000건 단위 재귀적 배치 처리(Recursive Trigger)** 구조로 설계되었습니다.

1. **Extract (추출):** MySQL `pipeline_state` 테이블에서 마지막으로 처리된 인덱스를 확인하고, `item_ids.txt` 파일에서 1,000개의 ID를 읽어옵니다.
2. **Transform (가공):**
   - **외부 API 연동:** 각 ID별로 사이즈 및 큐레이션 데이터를 수집합니다. (실패 시 3회 재시도)
   - **텍스트 정제:** 수집된 텍스트에서 옵션어, 특수문자 등 불용어를 제거합니다. (`clean_goods_name` 함수)
   - **임베딩 추출:** 가공된 텍스트를 GPU 서버 API로 일괄(Batch) 전송하여 벡터(Vector) 값을 반환받습니다.
3. **Load (적재):** MySQL DB에 원천 데이터(`raw`), 임베딩 데이터(`transformed`), 누락 로그(`error`)를 분리하여 Bulk Insert 합니다.
4. **Branching (분기):** 
   - 처리할 다음 데이터가 있다면 👉 **자신(DAG)을 다시 트리거하여 다음 1,000건 진행**
   - 남은 데이터가 없다면 👉 **파이프라인 정상 종료**

## ✨ 주요 기능 및 특징

- **안전한 Chunking & Recursion:** `max_active_runs=1` 설정과 결합하여, 한 배치(1,000건)가 완전히 끝난 후 다음 배치를 순차적으로 실행합니다.
- **복원력 (Resilience):** `tenacity` 라이브러리를 활용해 API 일시적 통신 장애 시 자동으로 재시도합니다.
- **방어적 예외 처리:** 404 에러(상품 없음)는 DB에 기록 후 건너뛰고, GPU 서버 다운 등의 치명적 에러 발생 시엔 무한 루프에 빠지지 않도록 즉시 파이프라인을 중단합니다.

## 🛠 사전 준비 사항 (Prerequisites)

1. **Airflow Connection:** 
   - ID: `my_mysql_conn` (MySQL 연결 정보 등록 필요)
2. **소스 데이터 파일:**
   - 경로: `/opt/airflow/dags/data/item_ids.txt` (줄바꿈으로 구분된 상품 ID 목록)
3. **MySQL 테이블 스키마 준비:**
   - `pipeline_state` (상태 관리용: `job_name`, `last_processed_index` 컬럼 필요)
   - `error_log`, `raw_clothing_data`, `transformed_clothing_data`
4. **GPU 서버 접근:**
   - `GPU_API_URL` 변수에 유효한 임베딩 서버 IP가 설정되어 있어야 합니다.

## ▶️ 실행 방법

이 파이프라인은 `schedule_interval=None`으로 설정되어 있어 스케줄러에 의해 자동으로 동작하지 않습니다. 

1. Airflow Web UI에 접속합니다.
2. `clothing_etl_pipeline` DAG를 활성화합니다.
3. 우측 상단의 **Trigger DAG** 버튼을 클릭하여 수동으로 실행합니다.

## 📝 향후 개선 가능 사항 (To-Do)

- **알림 기능(Alerts):** GPU 서버 장애나 DB 연결 끊김으로 파이프라인이 중단될 경우, 즉각적인 인지를 위해 `on_failure_callback`을 활용한 Slack 또는 이메일 알림 연동 기능을 추가할 수 있습니다.
