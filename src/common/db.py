"""Database base class and session management."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()

_engine = None
_SessionLocal: type[Session] | None = None


def init_engine(url: str, **kwargs) -> None:
    global _engine, _SessionLocal
    _engine = create_engine(url, **kwargs)
    _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


def get_engine():
    return _engine


@contextmanager
def get_db() -> Generator[Session, None, None]:
    if _SessionLocal is None:
        raise RuntimeError("DB not initialized. Call init_engine() first.")
    session: Session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
