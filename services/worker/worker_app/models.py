from datetime import datetime, timezone

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import DateTime, Integer, String

# NOTE: services/api/app/models.py 와 동일하게 유지할 것.
# 한쪽만 고치면 두 서비스의 스키마가 조용히 갈라진다.


def utcnow() -> datetime:
    """MySQL DATETIME은 타임존을 저장하지 않으므로 naive UTC로 통일한다."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass

class JobResult(Base):
    __tablename__ = "job_results"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    input_text: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50), default="QUEUED", index=True)
    note: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False, index=True
    )
