"""Small runtime boundary shared by the native AstrBot adapter."""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, declared_attr

from .config import Config


if os.getenv("OSUBOT_ASTRBOT_RUNTIME") == "1":
    try:
        from astrbot.api import logger as logger
    except ImportError:
        logger = logging.getLogger("nonebot_plugin_osubot")
else:
    try:
        from nonebot.log import logger as logger
    except ImportError:
        logger = logging.getLogger("nonebot_plugin_osubot")


class Model(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"nonebot_plugin_osubot_{cls.__name__.lower()}"


_config = Config()
_data_root = Path("data") / "osu"
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def configure(config: Config, data_root: Path, database_path: Path) -> None:
    global _config, _data_root, _engine, _session_factory
    _config = config
    _data_root = data_root.resolve()
    _data_root.mkdir(parents=True, exist_ok=True)
    database_path = database_path.resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    _engine = create_async_engine(f"sqlite+aiosqlite:///{database_path.as_posix()}")
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


def get_config() -> Config:
    if os.getenv("OSUBOT_ASTRBOT_RUNTIME") != "1":
        try:
            from nonebot import get_plugin_config

            return get_plugin_config(Config)
        except (ImportError, RuntimeError):
            pass
    return _config


def get_data_root() -> Path:
    return _data_root


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    if os.getenv("OSUBOT_ASTRBOT_RUNTIME") != "1":
        from nonebot_plugin_orm import get_session as nonebot_get_session

        async with nonebot_get_session() as session:
            yield session
        return
    if _session_factory is None:
        raise RuntimeError("OSUBot AstrBot runtime has not been configured")
    async with _session_factory() as session:
        yield session


async def create_tables() -> None:
    if _engine is None:
        raise RuntimeError("OSUBot AstrBot runtime has not been configured")
    async with _engine.begin() as connection:
        await connection.run_sync(Model.metadata.create_all)
