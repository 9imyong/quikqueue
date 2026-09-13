이벤트/비동기 미니 마이크로서비스
------------------------------------------------------------------------------------------
스택: FastAPI + Kafka(KRaft) + aiokafka Consumer + Celery(브로커: Redis) + MySQL + Adminer
한 줄 설명: “요청은 빨리 받고, 무거운 일은 뒤로 넘기는” 이벤트/비동기 중심 서비스 예제

이 프로젝트는 API 서버가 사용자 요청을 즉시 수락(빠른 응답) 하고, 시간이 오래 걸리는 작업은 메시지 큐로 위임하여 백그라운드에서 처리하는 마이크로서비스 아키텍처의 최소 구현체입니다.

```
Client
  │  POST /submit {"text": "..."}
  ▼
FastAPI (api)
  ├─ MySQL에 Job 레코드 생성(QUEUED)
  └─ Kafka "jobs" 토픽으로 이벤트 발행
        ▼
   aiokafka Consumer
        ├─ Celery 태스크 enqueue 성공 후 오프셋 수동 커밋 (브로커: Redis)
        └─ 해석 불가 메시지는 jobs.dlq 토픽으로
              ▼
           Celery Worker
              ├─ (예시) aiohttp로 외부 API 호출
              └─ 처리 결과를 MySQL에 업데이트 (DONE/FAILED)

조회: GET /results/{id} → MySQL에서 현재 상태/노트 조회
```

서비스 구성
------------------------------------------------------------------------------------------
api (FastAPI): 요청 수신, DB 기록, Kafka 발행 (:8000)

kafka (KRaft 모드): 이벤트 스트림 브로커 (kafka:9092) — Zookeeper 불필요

consumer (aiokafka): Kafka 구독 → Celery 태스크로 전달

worker (Celery): 비동기 작업 실행(예: 외부 API 호출) 후 DB 업데이트

beat (Celery Beat): 주기작업 스케줄링 — heartbeat, 멈춘 QUEUED 작업 재발행

mysql: 영속 데이터 저장 (:3306)

adminer: DB 웹 콘솔 (:8080)

redis: Celery 브로커(큐)

빠른시작
------------------------------------------------------------------------------------------
```
# 최초 설정: 기존 .env는 덮어쓰지 않음
[ -f .env ] || cp .env.example .env
# .env의 비밀번호와 두 DB URI를 일치하도록 설정

# 실행
docker compose up -d --build

# 작업 생성
curl -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{"text":"hello world"}'
# HTTP 202 => {"id":1,"status":"QUEUED"}

# 결과 조회 (위 응답의 id 사용)
curl http://localhost:8000/results/1
# => {"id":1,"status":"DONE","note":"Fetched ... bytes","input_text":"hello world"}
```

트러블슈팅(요약)
------------------------------------------------------------------------------------------
```
**API가 DB 연결 실패
- MySQL 시작 대기(healthcheck) + init_db()에 재시도(backoff) 권장
.env의 SQLALCHEMY_DB_URI 확인
- consumer가 Kafka에 못 붙음
ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 확인
- Kafka healthcheck + consumer.depends_on + restart 설정
run_consumer.py에 부트스트랩 재시도 코드 추가
- Celery 태스크 “unregistered”
services/worker/worker_app/__init__.py 파일 추가
celery_app.py 하단에 from . import tasks 추가
- PyMySQL 인증 에러 (caching_sha2_password)
cryptography 패키지 설치(권장) 또는 MySQL 유저를 mysql_native_password로 변경**
```

## 디렉터리 구조
------------------------------------------------------------------------------------------
```
quikqueue/
├── docker-compose.yml
├── .env.example  # 개인 .env는 Git 제외
├── README.md
├── services
│   ├── api
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app
│   │       ├── main.py
│   │       ├── db.py
│   │       ├── models.py
│   │       └── kafka_producer.py
│   ├── consumer
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── run_consumer.py
│   └── worker
│       ├── Dockerfile
│       ├── requirements.txt
│       └── worker_app
│           ├── celery_app.py
│           ├── tasks.py
│           ├── db.py
│           └── models.py
```


## 1차 안정화 변경

- API 입력: text는 1~255자, 범위 밖 입력은 HTTP 422
- 접수 응답: HTTP 202, 존재하지 않는 작업 조회는 HTTP 404
- Producer는 FastAPI lifespan에서 연결 완료 후 공개, 시작 실패 시 정리 및 API 시작 실패
- API는 Kafka healthcheck 성공 후 시작
- 외부 HTTP 요청: 기본 총 타임아웃 10초, EXTERNAL_API_TIMEOUT_SECONDS로 양수 초 단위 변경
- 외부 HTTP 4xx/5xx 및 네트워크 오류·타임아웃은 FAILED 기록
- note에는 오류 종류만 저장하여 URL·입력값 유출 및 컬럼 길이 초과 방지
- 성공 note의 길이는 응답 바이트 기준
- Consumer는 type 생략 또는 process_job만 지원, 다른 type은 partition·offset 오류 로그 후 전달하지 않음
- 로컬 개발용 공개 포트는 127.0.0.1로 제한
- 개인 .env는 파일을 보존하면서 Git 추적 제외, .env.example에 예시 설정 제공

기존 Git 이력에 들어간 .env는 이번 추적 제외만으로 삭제되지 않습니다. 실제 사용하는 비밀번호가 이력에 포함됐다면 해당 자격증명 교체가 필요합니다. 기존 MySQL 볼륨의 비밀번호는 .env 변경만으로 갱신되지 않습니다.

Kafka의 advertised listener는 내부 Docker 네트워크용 kafka:9092입니다. 호스트 포트 공개만으로 외부 Kafka 클라이언트 연결을 지원하는 설정은 아닙니다.

## 테스트

```bash
# 시스템에 venv 지원이 있는 경우
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m unittest discover -s tests -v

docker compose config --quiet
```

venv 지원 패키지가 없는 환경에서는 설치된 uv로 `uv venv .venv`와 `uv pip install --python .venv/bin/python -r requirements-dev.txt` 사용 가능.

- 31개 테스트: API 경계·202·404, Producer 생명주기, 워커 재시도·멱등성·FAILED 확정,
  컨슈머 페이로드 검증·DLQ·수동 커밋·백오프, stale 작업 재발행
- 테스트 DB는 SQLite, Kafka와 외부 HTTP는 대역 사용; 실제 MySQL/Kafka/Redis 전체 연동은 별도 확인 필요

## 2차 안정화 변경 (신뢰성)

메시지와 작업이 조용히 사라지던 경로를 막았습니다.

- Consumer: 자동 오프셋 커밋을 끄고 Celery 전달 성공 후에만 커밋(at-least-once)
- Consumer: 깨진 메시지는 `jobs.dlq` 토픽으로 보내고 건너뜀. 예전에는 메시지 하나가
  프로세스를 죽이고 재시작 후 같은 자리에서 또 죽는 크래시 루프를 만들었음
- Consumer: 일시적 전달 실패는 백오프 재시도, 소진 시 커밋 없이 종료 → 재시작이 곧 재처리
- Worker: `acks_late`로 워커가 죽어도 작업이 다시 배정됨
- Worker: 예상 못 한 예외도 FAILED로 확정 기록. QUEUED 방치 없음
- Worker: 이미 DONE인 작업은 외부 호출을 건너뛰어 중복 전달 방어
- Worker: 행이 없을 때 가짜 행을 만들던 fallback INSERT 제거(재시도로 대체)
- Beat: `requeue_stale_jobs`가 60초마다 `STALE_JOB_SECONDS` 이상 멈춰 있는
  QUEUED 작업을 다시 큐에 넣음 → Kafka 발행 실패로 갇힌 작업이 스스로 복구됨
- 스키마: `created_at`/`updated_at` 추가, `status`·`updated_at` 인덱스
- 인프라: worker/beat가 mysql·redis healthy 대기, Redis 영속화(appendonly),
  기본 파티션 3, non-root 컨테이너, api 헬스체크와 재시작 정책

### 기존 볼륨 업그레이드 주의

`create_all`은 이미 존재하는 테이블에 컬럼을 추가하지 않습니다. 기존 `mysql_data`
볼륨을 그대로 쓰면 `created_at`/`updated_at`이 없어 재발행 태스크가 실패합니다.

```
# 개발 데이터라면 볼륨을 비우는 쪽이 간단합니다
docker compose down -v && docker compose up -d --build

# 데이터를 유지해야 한다면 직접 추가
# ALTER TABLE job_results
#   ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
#   ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
#   ADD INDEX ix_job_results_status (status),
#   ADD INDEX ix_job_results_updated_at (updated_at);
```

## 다음 단계와 남은 한계

- DB 저장과 Kafka 발행은 여전히 별개. 진짜 outbox는 아니고, beat 스윕으로
  갇힌 QUEUED를 늦게(기본 5분) 복구하는 방식임
- API의 `submit_job`이 async 함수 안에서 동기 DB I/O를 수행(이벤트 루프 블로킹)
- DB 모델이 api/worker에 중복. 공통 패키지 분리와 Alembic 마이그레이션은 후속 과제
- API에 인증·레이트리밋 없음. 로컬 개발 전용 구성
- DLQ에 쌓인 메시지를 다시 처리하는 도구는 없음(수동 확인)
- bitnami/kafka 이미지는 레거시 카탈로그로 이동했으므로 apache/kafka 전환 검토 필요
