import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base

DB_URI = os.getenv("SQLALCHEMY_DB_URI")
if not DB_URI:
    # 설정이 비면 create_engine(None)이 알아보기 힘든 오류를 내므로 먼저 막는다.
    raise RuntimeError("SQLALCHEMY_DB_URI is not set (see .env.example)")

engine = create_engine(DB_URI, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def init_db():
    Base.metadata.create_all(engine)
