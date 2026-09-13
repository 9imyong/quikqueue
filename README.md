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
        └─ Celery 태스크 enqueue (브로커: Redis)
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

beat (Celery Beat, 선택): 주기작업 스케줄링(예: heartbeat)

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

- 검증: API 길이 경계·202·404, Producer 시작 실패 후 정리·재시작·준비 전 접근 차단·종료, HTTP 오류 검사·타임아웃 설정·FAILED 저장·바이트 길이 등 8개 테스트
- 테스트 DB는 SQLite, Kafka와 외부 HTTP는 대역 사용; 실제 MySQL/Kafka/Redis 전체 연동은 별도 확인 필요

## 다음 단계와 남은 한계

- DB 저장과 Kafka 발행은 아직 별개: Kafka 발행 실패 시 QUEUED가 남을 수 있음
- outbox·수동 offset 커밋·중복 처리 방어·재시도·실패 이벤트 보관은 후속 작업
- 미지원 이벤트는 현재 로그 후 건너뜀, 별도 실패 큐 보관 없음
- Worker의 DB 초기화·결과 저장 실패를 FAILED로 확실히 기록하는 복구 경로는 미구현
- API 내 동기 DB 접근, DB 모델 중복, 마이그레이션 및 Redis 영속화는 후속 개선 대상
