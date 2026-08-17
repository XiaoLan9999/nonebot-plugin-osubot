from __future__ import annotations

import asyncio
import argparse
import base64
import html
import json
import os
import random
import re
import shlex
import shutil
import sys
from difflib import SequenceMatcher
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
import astrbot.api.message_components as Comp
from astrbot.api.star import Context, Star, register
from sqlalchemy import and_, delete, func, select


PLUGIN_DIR = Path(__file__).resolve().parent
SOURCE_DIR = PLUGIN_DIR / "src"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))
os.environ["OSUBOT_ASTRBOT_RUNTIME"] = "1"


HELP_TEXT = """OSUBot 7.2.8 AstrBot 适配版
所有命令必须使用 / 前缀。发送 /osuhelp detail 查看完整图片帮助。

账号：/bind /unbind /sbbind /sbunbind /mode /update
资料：/info /mu /history /rank /recommend
成绩：/bp /bl /tbp /recent /rl /pr /pl /score /sl /bpa
谱面：/map /bmap /bg /preview /vp /dl /convert /倍速 /反键
比赛：/match /rating /medal
猜歌：/音频猜歌 /图片猜歌 /谱面猜歌，以及对应的 /音频提示 /图片提示 /谱面提示

参数语法保持 AiriBot 版：:o/:t/:c/:m、+HDHR、#7、1-30、&sb。"""

MODE_SUFFIXES = ("o", "std", "osu", "t", "taiko", "c", "catch", "ctb", "m", "mania", "0", "1", "2", "3", "4", "5", "6", "8")
MODE_SUFFIX_COMMANDS = {
    "info": "info", "osuinfo": "info",
    "bp": "bp", "osubp": "bp",
    "pfm": "pfm", "bl": "pfm", "bplist": "pfm", "osubl": "pfm",
    "tbp": "tbp", "nb": "tbp", "todaybp": "tbp", "osutbp": "tbp",
    "recent": "recent", "re": "recent", "osurecent": "recent",
    "pr": "pr", "osupr": "pr",
    "rl": "recent_list", "relist": "recent_list", "recentlist": "recent_list",
    "pl": "pass_list", "prlist": "pass_list", "passlist": "pass_list",
    "bpa": "bp_analyze", "bp分析": "bp_analyze",
    "map": "map", "m": "map", "osumap": "map",
    "score": "score", "sc": "score", "osuscore": "score",
    "scorelist": "score_list", "sl": "score_list", "scorehistory": "score_list", "历史成绩": "score_list",
    "history": "history", "hs": "history", "osuhistory": "history",
    "mu": "mu", "osumu": "mu",
    "recommend": "recommend", "推荐": "recommend", "推荐铺面": "recommend", "推荐谱面": "recommend",
    "rank": "group_rank", "群内排名": "group_rank",
    "preview": "preview", "预览": "preview", "完整预览": "preview", "视频预览": "preview",
    "完整视频": "preview", "vpreview": "preview", "vp": "preview",
    "音频猜歌": "guess_audio", "图片猜歌": "guess_picture", "谱面猜歌": "guess_chart",
}


class ModeSuffixCommandFilter(filter.CustomFilter):
    def filter(self, event: AstrMessageEvent, cfg: AstrBotConfig) -> bool:
        if not getattr(event, "is_at_or_wake_command", False):
            return False
        token = event.get_message_str().strip().split(maxsplit=1)[0]
        if ":" not in token:
            return False
        command, mode = token.split(":", 1)
        return command.lower() in MODE_SUFFIX_COMMANDS and mode.lower() in MODE_SUFFIXES


@register(
    "osu",
    "XiaoLan9999 / yaowan233",
    "AiriBot nonebot-plugin-osubot 7.2.8 的 AstrBot 原生适配",
    "0.6.0",
    "https://github.com/XiaoLan9999/nonebot-plugin-osubot/tree/astrbot-native",
)
class OSUBotPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.scheduler = AsyncIOScheduler()
        self._ready = False
        self._last_map: dict[str, tuple[int, int | None]] = {}
        self._guess_games: dict[str, dict[str, Any]] = {}
        self._guess_tasks: dict[str, asyncio.Task] = {}
        self._guess_seen: dict[str, set[int]] = {}

        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path

            root = Path(get_astrbot_data_path()) / "plugin_data" / "astrbot_plugin_osubot"
        except ImportError:
            root = PLUGIN_DIR / "data"
        self.data_dir = root
        self.database_path = root / "osubot.sqlite3"
        self.cache_dir = root / "cache"

    async def initialize(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._import_airibot_data_once()
        browser_dir = Path(
            str(self.config.get("playwright_browser_path") or self.data_dir / "playwright")
        ).expanduser()
        browser_dir.mkdir(parents=True, exist_ok=True)
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(browser_dir.resolve())

        from nonebot_plugin_osubot.config import Config
        from nonebot_plugin_osubot.runtime import configure, create_tables

        raw_client = self.config.get("osu_client", self.config.get("client_id"))
        client_id = int(raw_client) if raw_client not in (None, "") else None
        client_secret = self.config.get("osu_key", self.config.get("client_secret")) or None
        runtime_config = Config(
            osu_client=client_id,
            osu_key=client_secret,
            osu_proxy=self.config.get("osu_proxy") or None,
            osutrack_enabled=bool(self.config.get("osutrack_enabled", True)),
            osutrack_default_days=int(self.config.get("osutrack_default_days", 365)),
        )
        configure(runtime_config, self.cache_dir, self.database_path)

        # Importing the models registers their metadata on the AstrBot runtime Base.
        from nonebot_plugin_osubot.database import InfoData, SbUserData, UserData
        from nonebot_plugin_osubot import api
        from nonebot_plugin_osubot import draw
        from nonebot_plugin_osubot.info.bind import update_users_info
        from nonebot_plugin_osubot.draw.svg_render import warm_up_native_renderer
        import nonebot_plugin_osubot.mania as mania

        self.InfoData = InfoData
        self.SbUserData = SbUserData
        self.UserData = UserData
        self.api = api
        self.draw = draw
        self.update_users_info = update_users_info
        mania.osu_path = self.cache_dir / "converted"
        mania.osu_path.mkdir(parents=True, exist_ok=True)
        await create_tables()
        self._ready = True

        if not self.scheduler.running:
            self.scheduler.start()
        self.scheduler.add_job(
            self._daily_update,
            CronTrigger(hour=0, minute=0, timezone="Asia/Shanghai"),
            id="osubot_daily_update",
            replace_existing=True,
            misfire_grace_time=300,
        )
        asyncio.create_task(warm_up_native_renderer())
        logger.info(
            "OSUBot AstrBot adapter loaded from nonebot-plugin-osubot 7.2.8; "
            f"database={self.database_path}, cache={self.cache_dir}"
        )

    async def terminate(self) -> None:
        for task in self._guess_tasks.values():
            task.cancel()
        self._guess_tasks.clear()
        self._guess_games.clear()
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        try:
            from nonebot_plugin_osubot.draw.browser import close_persistent_pages

            await close_persistent_pages()
        except ImportError:
            pass

    def _import_airibot_data_once(self) -> None:
        if not bool(self.config.get("import_airibot_data", True)):
            return
        source_db = Path(
            str(
                self.config.get(
                    "airibot_database_path",
                    "/data/apps/airibot/data/nonebot_plugin_orm/db.sqlite3",
                )
            )
        )
        source_cache = Path(
            str(self.config.get("airibot_cache_path", "/data/apps/airibot/data/osu"))
        )
        if not self.database_path.exists() and source_db.is_file():
            shutil.copy2(source_db, self.database_path)
            logger.info(f"Imported AiriBot OSUBot database from {source_db}")
        if source_cache.is_dir() and not self.cache_dir.exists():
            shutil.copytree(source_cache, self.cache_dir)
            logger.info(f"Imported AiriBot OSUBot cache from {source_cache}")

    async def _daily_update(self) -> None:
        if not self._ready:
            return
        from nonebot_plugin_osubot.runtime import get_session

        async with get_session() as session:
            rows = (await session.scalars(select(self.UserData))).all()
        user_ids = list(dict.fromkeys(row.osu_id for row in rows))
        for offset in range(0, len(user_ids), 50):
            try:
                await self.update_users_info(user_ids[offset : offset + 50])
            except Exception:
                logger.exception("OSUBot daily player update failed")
        logger.info(f"OSUBot daily update finished for {len(user_ids)} bound players")

    @staticmethod
    def _argument(event: AstrMessageEvent) -> str:
        text = event.message_str.strip()
        parts = text.split(maxsplit=1)
        command = parts[0] if parts else ""
        suffix = ""
        if ":" in command:
            command, mode = command.split(":", 1)
            suffix = f":{mode}"
        argument = parts[1].strip() if len(parts) == 2 else ""
        return " ".join(part for part in (suffix, argument) if part)

    @staticmethod
    def _bytes(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, BytesIO):
            return value.getvalue()
        if hasattr(value, "getvalue"):
            return value.getvalue()
        raise TypeError(f"Unsupported image payload: {type(value)!r}")

    @classmethod
    def _image_result(cls, event: AstrMessageEvent, value: Any):
        return event.chain_result([Comp.Image.fromBytes(cls._bytes(value))])

    @staticmethod
    def _file_result(event: AstrMessageEvent, path: Path):
        return event.chain_result([Comp.File(path.name, file=str(path.resolve()))])

    @classmethod
    def _audio_result(cls, event: AstrMessageEvent, value: Any):
        encoded = base64.b64encode(cls._bytes(value)).decode("ascii")
        return event.chain_result([Comp.Record.fromBase64(encoded)])

    @staticmethod
    def _video_result(event: AstrMessageEvent, path: Path):
        return event.chain_result([Comp.Video.fromFileSystem(path)])

    @staticmethod
    def _cleanup_later(path: Path, delay: float = 180.0) -> None:
        async def cleanup() -> None:
            await asyncio.sleep(delay)
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.warning(f"OSUBot could not remove temporary file: {path}")

        asyncio.create_task(cleanup())

    @staticmethod
    def _context_key(event: AstrMessageEvent) -> str:
        origin = getattr(event, "unified_msg_origin", None)
        return str(origin or event.get_sender_id())

    async def _bound_user(self, sender_id: str, source: str = "osu"):
        from nonebot_plugin_osubot.runtime import get_session

        model = self.SbUserData if source == "ppysb" else self.UserData
        async with get_session() as session:
            return await session.scalar(select(model).where(model.user_id == sender_id))

    @staticmethod
    def _parse_filters(text: str) -> tuple[list[tuple[str, str, str]], str]:
        conditions: list[tuple[str, str, str]] = []
        shorthand_patterns = (
            (
                r"(?<!\S)(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\*(?!\S)",
                lambda match: ("stars", "=", f"{match[1]}..{match[2]}"),
            ),
            (
                r"(?<!\S)(\d+(?:\.\d+)?)(pp|acc|star|sr|p|a|s|\*)([+-]?)(?!\S)",
                lambda match: (
                    {"p": "pp", "a": "accuracy", "acc": "accuracy", "star": "stars", "sr": "stars", "s": "stars", "*": "stars"}.get(match[2].lower(), match[2].lower()),
                    {"+": ">=", "-": "<="}.get(match[3], "="),
                    match[1],
                ),
            ),
            (r"(?<!\S)(\d+(?:\.\d+)?)d(?!\S)", lambda match: ("days", "<=", match[1])),
            (r"(?<!\S)(\d+(?:\.\d+)?)h(?!\S)", lambda match: ("hours", "<=", match[1])),
            (r"(?<!\S)fc(?!\S)", lambda _match: ("fc", "=", "true")),
            (r"(?<!\S)nofc(?!\S)", lambda _match: ("fc", "=", "false")),
            (r"(?<!\S)-([a-z0-9]{2,})(?!\S)", lambda match: ("mods", "!=", match[1])),
            (r"(?<!\S)=([a-z0-9]{2,})(?!\S)", lambda match: ("mods", "=", match[1])),
        )
        for pattern, build in shorthand_patterns:
            def replace(match: re.Match, build=build) -> str:
                conditions.append(build(match))
                return " "

            text = re.sub(pattern, replace, text, flags=re.IGNORECASE)
        expression = re.compile(
            r"(?P<field>title|artist|mapper|creator|version|difficulty|pp|p|acc|a|accuracy|star|stars|sr|s|bpm|b|length|len|combo|c|miss|m|rank|client|mods)"
            r"\s*(?P<op>!=|>=|<=|~=|=|>|<|~)\s*(?P<value>\"[^\"]*\"|'[^']*'|\S+)",
            re.IGNORECASE,
        )
        for match in expression.finditer(text):
            field = match.group("field").lower()
            field = {
                "p": "pp", "acc": "accuracy", "a": "accuracy", "star": "stars", "sr": "stars", "s": "stars",
                "b": "bpm", "len": "length", "c": "combo", "m": "miss", "mapper": "creator", "version": "version",
                "difficulty": "version",
            }.get(field, field)
            conditions.append((field, match.group("op"), match.group("value").strip("\"'")))
        return conditions, expression.sub(" ", text)

    async def _state(
        self,
        event: AstrMessageEvent,
        command: str,
        require_user: bool = True,
        raw_text: str | None = None,
    ) -> dict[str, Any]:
        from nonebot_plugin_osubot.utils import NGM, extract_beatmap_id, extract_beatmapset_id, mods2list, parse_mode

        text = (
            (self._argument(event) if raw_text is None else raw_text)
            .replace("，", ",")
            .replace("：", ":")
            .replace("＆", "&")
            .replace("＃", "#")
            .replace("＋", "+")
            .replace("＝", "=")
        )
        source_match = re.search(r"(?:^|\s)&(sb|ppysb)(?=\s|$)", text, re.IGNORECASE)
        source = "ppysb" if source_match else "osu"
        text = re.sub(r"(?:^|\s)&(?:sb|ppysb)(?=\s|$)", " ", text, flags=re.IGNORECASE)
        bound = await self._bound_user(str(event.get_sender_id()), source)
        state: dict[str, Any] = {
            "user": bound.osu_id if bound else 0,
            "username": bound.osu_name if bound else "",
            "mode": str(getattr(bound, "osu_mode", 0)),
            "mode_explicit": False,
            "mods": [],
            "range": None,
            "day": 0,
            "source": source,
            "query": [],
            "target": None,
            "is_lazer": True,
        }

        if mode_match := re.search(r"(?:^|\s):(\w+)(?=\s|$)", text):
            parsed = parse_mode(mode_match.group(1), allow_special=source == "ppysb")
            if parsed is None:
                raise ValueError("模式应为 o/std、t/taiko、c/catch、m/mania 或数字 0-3")
            state["mode"] = parsed
            state["mode_explicit"] = True
            text = text[: mode_match.start()] + " " + text[mode_match.end() :]

        if mods_match := re.search(r"(?:^|\s)\+([A-Za-z0-9,]+)(?=\s|$)", text):
            state["mods"] = mods2list(mods_match.group(1))
            text = text[: mods_match.start()] + " " + text[mods_match.end() :]
        if day_match := re.search(r"(?:^|\s)#(\d+)(?=\s|$)", text):
            state["day"] = int(day_match.group(1))
            text = text[: day_match.start()] + " " + text[day_match.end() :]
        if range_match := re.search(r"(?:^|\s)(\d+)\s*-\s*(\d+)(?=\s|$)", text):
            low, high = sorted((int(range_match.group(1)), int(range_match.group(2))))
            state["range"] = f"{low}-{high}"
            text = text[: range_match.start()] + " " + text[range_match.end() :]

        if command in {"bmap", "osudl", "full_ln"}:
            url_target = extract_beatmapset_id(text)
        else:
            url_target = extract_beatmap_id(text)
        if url_target:
            state["target"] = url_target
            text = re.sub(r"(?:https?://)?osu\.ppy\.sh/\S+", " ", text)

        if command in {"bp", "map", "bmap", "score", "scorelist", "preview", "getbg", "speed"} and not state["target"]:
            numeric = list(re.finditer(r"(?<!\S)\d+(?!\S)", text))
            if numeric:
                selected = numeric[-1]
                state["target"] = selected.group(0)
                text = text[: selected.start()] + " " + text[selected.end() :]

        if command in {"bp", "pfm", "tbp", "recent_list", "pass_list"}:
            state["query"], text = self._parse_filters(text)
        username = " ".join(text.split())
        if username:
            state["username"] = username
            state["user"] = await self.api.get_uid_by_name(username, source)
        if require_user and not state["user"]:
            raise ValueError("该账号尚未绑定，请先使用 /bind <osu! 用户名>，或在命令后指定玩家")
        state["mode_name"] = NGM[state["mode"]]
        return state

    @filter.custom_filter(ModeSuffixCommandFilter, priority=100)
    async def mode_suffix_dispatcher(self, event: AstrMessageEvent):
        token = event.get_message_str().strip().split(maxsplit=1)[0]
        command = token.split(":", 1)[0].lower()
        method_name = MODE_SUFFIX_COMMANDS.get(command)
        if not method_name:
            return
        event.stop_event()
        handler = getattr(self, method_name)
        async for result in handler(event):
            yield result

    @filter.command("osuhelp", alias={"oh", "osubot", "osu帮助"})
    async def osu_help(self, event: AstrMessageEvent):
        topic = self._argument(event).strip().lower().lstrip("/")
        if topic in {"", "overview", "概览"}:
            yield self._image_result(event, (SOURCE_DIR / "nonebot_plugin_osubot" / "osufile" / "help.png").read_bytes())
            return
        if topic in {"detail", "详细", "详情"}:
            yield self._image_result(event, (SOURCE_DIR / "nonebot_plugin_osubot" / "osufile" / "detail.png").read_bytes())
            return
        from nonebot_plugin_osubot.help_data import HELP_TOPICS, TOPIC_ALIASES, TOPIC_LABELS, get_command_help

        normalized = TOPIC_ALIASES.get(topic, topic)
        if normalized in HELP_TOPICS or normalized in {"all", "全部", "完整", "所有指令"}:
            yield event.plain_result(get_command_help(normalized))
            return
        yield event.plain_result(
            f"没有找到该帮助主题。可用主题：{TOPIC_LABELS}\n"
            "发送 /osuhelp 查看快速指南，或发送 /osuhelp detail 查看完整命令。"
        )

    @filter.command("bind", alias={"osubind"})
    async def bind(self, event: AstrMessageEvent):
        name = self._argument(event)
        if not name:
            yield event.plain_result("请在命令后输入 osu! 用户名、UID 或个人主页链接。")
            return
        from nonebot_plugin_osubot.runtime import get_session

        sender_id = str(event.get_sender_id())
        async with get_session() as session:
            existing = await session.scalar(select(self.UserData).where(self.UserData.user_id == sender_id))
            if existing:
                yield event.plain_result(f"已经绑定 {existing.osu_name}，如需更换请先 /unbind。")
                return
        try:
            info = await self.api.get_osu_user(name)
            row = self.UserData(
                user_id=sender_id,
                osu_id=int(info["id"]),
                osu_name=str(info["username"]),
                osu_mode=int(info.get("playmode", "osu") == "taiko")
                if info.get("playmode") in {"osu", "taiko"}
                else {"fruits": 2, "mania": 3}.get(info.get("playmode"), 0),
                lazer_mode=True,
            )
            async with get_session() as session:
                session.add(row)
                await session.commit()
            try:
                await self.update_users_info([row.osu_id])
            except Exception:
                logger.exception("Initial OSUBot history snapshot failed after binding")
            yield event.plain_result(f"成功绑定 {row.osu_name}，默认模式为 {info.get('playmode', 'osu')}。")
        except Exception as exc:
            logger.exception("OSUBot bind failed")
            yield event.plain_result(f"绑定失败：{exc}")

    @filter.command("unbind", alias={"osuunbind"})
    async def unbind(self, event: AstrMessageEvent):
        from nonebot_plugin_osubot.runtime import get_session

        sender_id = str(event.get_sender_id())
        async with get_session() as session:
            result = await session.execute(delete(self.UserData).where(self.UserData.user_id == sender_id))
            await session.commit()
        yield event.plain_result("解绑成功。" if result.rowcount else "尚未绑定，无需解绑。")

    @filter.command("sbbind")
    async def sbbind(self, event: AstrMessageEvent):
        name = self._argument(event).strip()
        if not name:
            yield event.plain_result("请在命令后输入 ppysb 用户名。")
            return
        from nonebot_plugin_osubot.runtime import get_session

        sender_id = str(event.get_sender_id())
        async with get_session() as session:
            existing = await session.scalar(select(self.SbUserData).where(self.SbUserData.user_id == sender_id))
            if existing:
                yield event.plain_result(f"已经绑定 ppysb 用户 {existing.osu_name}，如需更换请先 /sbunbind。")
                return
        try:
            uid = await self.api.get_uid_by_name(name, "ppysb")
            async with get_session() as session:
                session.add(self.SbUserData(user_id=sender_id, osu_id=int(uid), osu_name=name))
                await session.commit()
            yield event.plain_result(f"成功绑定 ppysb 用户：{name}")
        except Exception as exc:
            logger.exception("OSUBot ppysb bind failed")
            yield event.plain_result(f"绑定 ppysb 用户失败：{exc}")

    @filter.command("sbunbind")
    async def sbunbind(self, event: AstrMessageEvent):
        from nonebot_plugin_osubot.runtime import get_session

        sender_id = str(event.get_sender_id())
        async with get_session() as session:
            result = await session.execute(delete(self.SbUserData).where(self.SbUserData.user_id == sender_id))
            await session.commit()
        yield event.plain_result("ppysb 解绑成功。" if result.rowcount else "尚未绑定 ppysb，无需解绑。")

    @filter.command("mode", alias={"osumode"})
    async def mode(self, event: AstrMessageEvent):
        from nonebot_plugin_osubot.runtime import get_session
        from nonebot_plugin_osubot.utils import GMN, NGM, parse_mode

        mode_input = self._argument(event).strip()
        if not mode_input:
            row = await self._bound_user(str(event.get_sender_id()))
            if not row:
                yield event.plain_result("尚未绑定 osu! 账号，请先使用 /bind。")
                return
            yield event.plain_result(
                f"当前默认模式为 {NGM[str(row.osu_mode)]}（{row.osu_mode}）。\n"
                "可使用 /mode o、/mode t、/mode c、/mode m 修改。"
            )
            return
        mode = parse_mode(mode_input)
        if mode is None:
            yield event.plain_result("模式应为 o/std、t/taiko、c/catch、m/mania 或数字 0-3。")
            return
        async with get_session() as session:
            row = await session.scalar(
                select(self.UserData).where(self.UserData.user_id == str(event.get_sender_id()))
            )
            if not row:
                yield event.plain_result("尚未绑定 osu! 账号，请先使用 /bind。")
                return
            row.osu_mode = int(mode)
            await session.commit()
        yield event.plain_result(f"默认模式已修改为 {GMN[NGM[mode]]}。")

    @filter.command("info", alias={"osuinfo", "Info", "INFO"})
    async def info(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "info")
            image = await self.draw.draw_info(
                state["user"], state["mode_name"], state["day"], state["source"]
            )
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot info failed")
            yield event.plain_result(f"查询玩家信息失败：{exc}")

    async def _draw_bp_command(self, event: AstrMessageEvent, command: str):
        from nonebot_plugin_osubot.draw.bp import draw_pfm

        state = await self._state(event, command)
        if command == "bp" and not state["range"] and not state["query"]:
            best = int(state["target"] or 1)
            if not 1 <= best <= 200:
                raise ValueError("只允许查询 BP 1-200。")
            image, map_id, set_id = await self.draw.draw_score(
                "bp",
                state["user"],
                state["is_lazer"],
                state["mode_name"],
                state["mods"],
                state["query"],
                state["source"],
                best=best,
                return_context=True,
            )
            self._last_map[self._context_key(event)] = (int(map_id), int(set_id) if set_id else None)
            return image
        low, high = map(int, (state["range"] or ("1-200" if command == "tbp" else "1-30")).split("-"))
        if not 0 < low < high <= 200:
            raise ValueError("只允许查询 BP 1-200。")
        if command == "tbp":
            return await self.draw.draw_bp(
                "tbp",
                state["user"],
                True,
                state["mode_name"],
                state["mods"],
                low,
                high,
                state["day"] or 1,
                state["query"],
                state["source"],
            )
        return await self.draw.draw_bp(
            "bp",
            state["user"],
            True,
            state["mode_name"],
            state["mods"],
            low,
            high,
            state["day"],
            state["query"],
            state["source"],
        )

    @filter.command("bp", alias={"osubp"})
    async def bp(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_bp_command(event, "bp"))
        except Exception as exc:
            logger.exception("OSUBot bp failed")
            yield event.plain_result(f"查询 BP 失败：{exc}")

    @filter.command("pfm", alias={"bl", "bplist", "osubl"})
    async def pfm(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_bp_command(event, "pfm"))
        except Exception as exc:
            logger.exception("OSUBot BP list failed")
            yield event.plain_result(f"查询 BP 列表失败：{exc}")

    @filter.command("tbp", alias={"nb", "todaybp", "osutbp"})
    async def tbp(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_bp_command(event, "tbp"))
        except Exception as exc:
            logger.exception("OSUBot today BP failed")
            yield event.plain_result(f"查询近期 BP 失败：{exc}")

    async def _draw_recent_list(self, event: AstrMessageEvent, include_fails: bool, project: str):
        from nonebot_plugin_osubot.api import get_user_scores
        from nonebot_plugin_osubot.draw.bp import draw_pfm
        from nonebot_plugin_osubot.draw.score import cal_score_info

        command = "recent_list" if include_fails else "pass_list"
        state = await self._state(event, command)
        low, high = map(int, (state["range"] or "1-30").split("-"))
        if not 0 < low <= high <= 200:
            raise ValueError("仅支持查询最近 1-200 条成绩。")
        scores = await get_user_scores(
            state["user"],
            state["mode_name"],
            "recent",
            state["source"],
            not state["is_lazer"],
            include_fails,
            low - 1,
            high if state["source"] == "ppysb" else high - low + 1,
        )
        for score in scores:
            cal_score_info(state["is_lazer"], score, state["source"])
        return await draw_pfm(project, state["user"], scores, scores, state["mode_name"], source=state["source"])

    @filter.command("rl", alias={"relist", "recentlist"})
    async def recent_list(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_recent_list(event, True, "relist"))
        except Exception as exc:
            logger.exception("OSUBot recent list failed")
            yield event.plain_result(f"查询最近成绩列表失败：{exc}")

    @filter.command("pl", alias={"prlist", "passlist"})
    async def pass_list(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_recent_list(event, False, "prlist"))
        except Exception as exc:
            logger.exception("OSUBot pass list failed")
            yield event.plain_result(f"查询最近通过成绩列表失败：{exc}")

    @filter.command("bpa", alias={"bp分析"})
    async def bp_analyze(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.api import get_user_scores
            from nonebot_plugin_osubot.draw.echarts import build_bpa_data, draw_bpa_plot
            from nonebot_plugin_osubot.draw.score import cal_score_info

            state = await self._state(event, "bpa")
            scores = await get_user_scores(
                state["user"], state["mode_name"], "best", state["source"], legacy_only=not state["is_lazer"]
            )
            for score in scores:
                if not state["is_lazer"] or state["source"] == "ppysb":
                    score.mods = [mod for mod in score.mods if mod.acronym != "CL"]
                for mod in score.mods:
                    if mod.acronym in {"DT", "NC"}:
                        score.beatmap.total_length /= 1.5
                    elif mod.acronym == "HT":
                        score.beatmap.total_length /= 0.75
                cal_score_info(state["is_lazer"], score, state["source"])
            data = await build_bpa_data(scores, state["source"])
            image = await draw_bpa_plot(
                f"{state['username']} {state['mode_name']} 模式",
                username=state["username"],
                mode=state["mode_name"],
                user_id=state["user"],
                source=state["source"],
                **data,
            )
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot BP analyze failed")
            yield event.plain_result(f"BP 分析失败：{exc}")

    async def _recent(self, event: AstrMessageEvent, project: str):
        state = await self._state(event, project)
        image, map_id, set_id = await self.draw.draw_score(
            project,
            state["user"],
            True,
            state["mode_name"],
            [],
            [],
            state["source"],
            state["day"] or 1,
            return_context=True,
        )
        self._last_map[self._context_key(event)] = (int(map_id), int(set_id) if set_id else None)
        return image

    @filter.command("recent", alias={"re", "RE", "Re", "rE", "osurecent"})
    async def recent(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._recent(event, "recent"))
        except Exception as exc:
            logger.exception("OSUBot recent failed")
            yield event.plain_result(f"查询最近成绩失败：{exc}")

    @filter.command("pr", alias={"PR", "Pr", "pR", "osupr"})
    async def pr(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._recent(event, "pr"))
        except Exception as exc:
            logger.exception("OSUBot passed recent failed")
            yield event.plain_result(f"查询最近通过成绩失败：{exc}")

    @filter.command("map", alias={"m", "osumap"})
    async def map(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "map", require_user=False)
            map_id = state["target"] or self._last_map.get(self._context_key(event), (None, None))[0]
            if not map_id:
                raise ValueError("请输入 mapID 或 osu! 谱面链接。")
            image = await self.draw.draw_map_info(
                int(map_id), state["mods"], int(state["mode"]) if state["mode_explicit"] else None
            )
            self._last_map[self._context_key(event)] = (int(map_id), None)
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot map failed")
            yield event.plain_result(f"查询谱面失败：{exc}")

    @filter.command("bmap", alias={"bm", "osubmap"})
    async def bmap(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "bmap", require_user=False)
            set_id = state["target"] or self._last_map.get(self._context_key(event), (None, None))[1]
            if not set_id:
                raise ValueError("请输入 setID 或 osu! 谱面集链接。")
            image = await self.draw.draw_bmap_info(int(set_id))
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot beatmapset failed")
            yield event.plain_result(f"查询谱面集失败：{exc}")

    @filter.command("score", alias={"sc", "osuscore"})
    async def score(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "score")
            map_id = state["target"] or self._last_map.get(self._context_key(event), (None, None))[0]
            if not map_id:
                raise ValueError("请输入 mapID，或先查询一张谱面。")
            image = await self.draw.get_score_data(
                state["user"], True, state["mode_name"], state["mods"], int(map_id), state["source"]
            )
            self._last_map[self._context_key(event)] = (int(map_id), None)
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot score failed")
            yield event.plain_result(f"查询谱面成绩失败：{exc}")

    @filter.command(
        "scorelist",
        alias={"sl", "scorehistory", "历史成绩"},
    )
    async def score_list(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "scorelist")
            map_id = state["target"] or self._last_map.get(self._context_key(event), (None, None))[0]
            if not map_id:
                raise ValueError("请输入 mapID，或先查询一张谱面。")
            image = await self.draw.draw_score_history(
                state["user"],
                state["is_lazer"],
                state["mode_name"],
                state["mods"],
                int(map_id),
                state["source"],
                state["range"],
            )
            self._last_map[self._context_key(event)] = (int(map_id), None)
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot score list failed")
            yield event.plain_result(f"查询谱面历史成绩失败：{exc}")

    @filter.command("getbg", alias={"bg"})
    async def get_background(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.info import get_bg

            state = await self._state(event, "getbg", require_user=False)
            map_id = state["target"] or self._last_map.get(self._context_key(event), (None, None))[0]
            if not map_id:
                raise ValueError("请输入 mapID，或先查询一张谱面。")
            image = await get_bg(int(map_id))
            output = BytesIO()
            image.convert("RGB").save(output, "jpeg")
            self._last_map[self._context_key(event)] = (int(map_id), None)
            yield self._image_result(event, output)
        except Exception as exc:
            logger.exception("OSUBot background failed")
            yield event.plain_result(f"获取谱面背景失败：{exc}")

    @filter.command("history", alias={"hs", "osuhistory"})
    async def history(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "history")
            image = await self._draw_history(state)
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot history failed")
            yield event.plain_result(f"查询历史数据失败：{exc}")

    async def _draw_history(self, state: dict[str, Any]) -> bytes:
        from nonebot_plugin_osubot.draw.echarts import draw_history_plot
        from nonebot_plugin_osubot.history_data import merge_osutrack_history
        from nonebot_plugin_osubot.runtime import get_session

        query = select(self.InfoData).where(
            self.InfoData.osu_id == state["user"], self.InfoData.osu_mode == int(state["mode"])
        )
        if state["day"] > 0:
            query = query.where(self.InfoData.date >= date.today() - timedelta(days=state["day"]))
        async with get_session() as session:
            user = await session.scalar(select(self.UserData).where(self.UserData.osu_id == state["user"]))
            rows = (await session.scalars(query.order_by(self.InfoData.date))).all()
        display_name = user.osu_name if user else state["username"] or str(state["user"])
        local_points = [(row.pp, str(row.date), row.g_rank) for row in rows if row.g_rank]
        points, used_osutrack = await merge_osutrack_history(
            state["user"], int(state["mode"]), local_points, state["day"]
        )
        if not points:
            raise ValueError(f"没有找到 {display_name} 的历史数据。")
        pp_values, dates, ranks = map(list, zip(*points))
        source_label = "本地记录"
        if used_osutrack:
            source_label = "本地记录 + osu!track" if local_points else "osu!track"
        return await draw_history_plot(
            pp_values,
            dates,
            ranks,
            f"{display_name} {state['mode_name']} pp/rank history",
            username=display_name,
            mode=state["mode_name"],
            user_id=state["user"],
            source_label=source_label,
        )

    @filter.command("update", alias={"osuupdate"})
    async def update(self, event: AstrMessageEvent):
        row = await self._bound_user(str(event.get_sender_id()))
        if not row:
            yield event.plain_result("尚未绑定 osu! 账号，请先使用 /bind。")
            return
        try:
            await self.update_users_info([row.osu_id])
            yield event.plain_result(f"{row.osu_name} 的玩家数据更新完成。")
        except Exception as exc:
            logger.exception("OSUBot manual update failed")
            yield event.plain_result(f"更新玩家数据失败：{exc}")

    @filter.command("mu", alias={"osumu"})
    async def mu(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "mu")
            yield event.plain_result(f"https://osu.ppy.sh/u/{state['user']}")
        except Exception as exc:
            yield event.plain_result(f"查询玩家主页失败：{exc}")

    @filter.command("match", alias={"mp"})
    async def match_history(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.draw.match_history import draw_match_history

            argument = self._argument(event).strip()
            if not argument:
                raise ValueError("请输入 multiplayer match ID 或比赛链接。")
            yield self._image_result(event, await draw_match_history(argument))
        except Exception as exc:
            logger.exception("OSUBot match history failed")
            yield event.plain_result(f"查询多人比赛失败：{exc}")

    @filter.command("rating", alias={"rt"})
    async def rating(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.draw.rating import draw_rating

            argument = self._argument(event).strip()
            if not argument:
                raise ValueError("请输入 multiplayer match ID 或比赛链接。")
            yield self._image_result(event, await draw_rating(argument))
        except Exception as exc:
            logger.exception("OSUBot rating failed")
            yield event.plain_result(f"计算多人房评分失败：{exc}")

    @filter.command("medal", alias={"md", "成就"})
    async def medal(self, event: AstrMessageEvent):
        try:
            name = self._argument(event).strip()
            if not name:
                raise ValueError("请输入成就名称。")
            response = await self.api.safe_async_get(
                "https://osekai.net/medals/api/public/get_medal.php", params={"medal": name}
            )
            data = response.json()
            if "MedalID" not in data:
                raise ValueError("没有找到该成就，请检查名称。")
            medal_file = SOURCE_DIR / "nonebot_plugin_osubot" / "osufile" / "medals" / "medals.json"
            local_data = json.loads(medal_file.read_text(encoding="utf-8"))
            words = ""
            if data.get("Restriction") not in {None, "NULL"}:
                words += f"限制模式：{data['Restriction']}\n"
            words += "获得方式：\n"
            if data.get("Name") in local_data:
                words += local_data[data["Name"]]["MedalSolution"]
            else:
                words += data.get("Solution") or data.get("Instructions") or "暂无说明"
            words = re.sub(r"<style[^>]*>.*?</style>", "", words, flags=re.DOTALL | re.IGNORECASE)
            words = re.sub(r"<br\s*/?>", "\n", words, flags=re.IGNORECASE)
            words = html.unescape(re.sub(r"<[^>]+>", "", words)).strip()
            pack_id = str(data.get("PackID") or "").rstrip(",")
            if pack_id:
                words += f"\nhttps://osu.ppy.sh/beatmaps/packs/{pack_id}"
            chain: list[Any] = []
            if data.get("Link"):
                chain.append(Comp.Image.fromURL(data["Link"]))
            chain.append(Comp.Plain(words))
            beatmaps = data.get("beatmaps") or []
            if beatmaps:
                rows = []
                for beatmap in beatmaps[:5]:
                    rows.append(
                        f"{beatmap['SongTitle']} [{beatmap['DifficultyName']}]\n"
                        f"{beatmap['Difficulty']} 星\nhttps://osu.ppy.sh/b/{beatmap['BeatmapID']}"
                    )
                chain.append(Comp.Plain("\n\n" + "\n\n".join(rows)))
            yield event.chain_result(chain)
        except Exception as exc:
            logger.exception("OSUBot medal failed")
            yield event.plain_result(f"查询成就失败：{exc}")

    @filter.command(
        "recommend",
        alias={"推荐", "推荐铺面", "推荐谱面"},
    )
    async def recommend(self, event: AstrMessageEvent):
        targets = {
            "farm": "farm", "pp": "farm", "吃分": "farm", "mixed": "mixed", "mix": "mixed",
            "all": "mixed", "overall": "mixed", "综合": "mixed", "总和": "mixed", "全部": "mixed",
            "balanced": "balanced", "balance": "balanced", "normal": "balanced", "普通": "balanced",
            "peak": "peak", "hard": "peak", "harder": "peak", "challenge": "peak", "难一点": "peak",
            "更难": "peak", "高难": "peak", "冲分": "peak", "style": "style", "practice": "style",
            "train": "style", "training": "style", "风格": "style", "练习": "style", "练图": "style",
        }
        try:
            raw = self._argument(event).strip()
            tokens = raw.split()
            target = "mixed"
            filtered = []
            for token in tokens:
                normalized = targets.get(token.lower())
                if normalized:
                    target = normalized
                else:
                    filtered.append(token)
            state = await self._state(event, "recommend", raw_text=" ".join(filtered))
            task = asyncio.create_task(self.api.get_recommend(state["user"], state["mode"], target))
            try:
                recommendations = await asyncio.wait_for(asyncio.shield(task), timeout=5)
            except TimeoutError:
                await event.send(MessageChain([Comp.Plain("正在获取推荐谱面，请稍候……")]))
                recommendations = await task
            if not recommendations.recommendations:
                yield event.plain_result("暂时没有找到可推荐的谱面，已加入更新队列，请明天再试。")
                return
            from nonebot_plugin_osubot.draw.recommend import draw_recommend

            image = await draw_recommend(
                recommendations,
                state["username"] or str(state["user"]),
                f"https://a.ppy.sh/{state['user']}",
            )
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot recommend failed")
            yield event.plain_result(f"获取推荐谱面失败：{exc}")

    @filter.command("rank", alias={"群内排名"})
    async def group_rank(self, event: AstrMessageEvent):
        try:
            group_id = str(event.get_group_id() or "")
            if not group_id:
                raise ValueError("群内排名只能在群聊中使用。")
            bot = getattr(event, "bot", None)
            if bot is None or not hasattr(bot, "call_action"):
                raise ValueError("当前平台无法获取群成员列表。")
            payload = await bot.call_action(
                action="get_group_member_list",
                group_id=int(group_id) if group_id.isdigit() else group_id,
            )
            if isinstance(payload, dict) and "data" in payload:
                payload = payload["data"]
            members = payload if isinstance(payload, list) else []
            if not members:
                raise ValueError("没有获取到群成员列表。")
            member_names = {
                str(member.get("user_id")): str(member.get("card") or member.get("nickname") or "")
                for member in members
                if isinstance(member, dict) and member.get("user_id") is not None
            }
            state = await self._state(event, "rank", require_user=False)
            mode = int(state["mode"])
            today = date.today()
            from nonebot_plugin_osubot.runtime import get_session

            async with get_session() as session:
                users = (
                    await session.scalars(select(self.UserData).where(self.UserData.user_id.in_(list(member_names))))
                ).all()
                osu_ids = list({user.osu_id for user in users})
                if not osu_ids:
                    raise ValueError("本群还没有已绑定 osu! 账号的成员。")
                current = (
                    await session.scalars(
                        select(self.InfoData)
                        .where(
                            self.InfoData.osu_id.in_(osu_ids),
                            self.InfoData.osu_mode == mode,
                            self.InfoData.date == today,
                            self.InfoData.pp >= 100,
                        )
                        .order_by(self.InfoData.pp.desc())
                    )
                ).all()
                latest_dates = (
                    select(self.InfoData.osu_id.label("osu_id"), func.max(self.InfoData.date).label("latest_date"))
                    .where(
                        self.InfoData.osu_id.in_(osu_ids),
                        self.InfoData.osu_mode == mode,
                        self.InfoData.date < today,
                    )
                    .group_by(self.InfoData.osu_id)
                    .subquery()
                )
                previous = (
                    await session.scalars(
                        select(self.InfoData)
                        .join(
                            latest_dates,
                            and_(
                                self.InfoData.osu_id == latest_dates.c.osu_id,
                                self.InfoData.date == latest_dates.c.latest_date,
                            ),
                        )
                        .where(self.InfoData.osu_mode == mode)
                    )
                ).all()
            user_by_osu = {}
            for user in users:
                user_by_osu.setdefault(user.osu_id, user)
            previous_by_osu = {info.osu_id: info for info in previous}
            players = []
            seen = set()
            for info in current:
                if info.osu_id in seen or info.osu_id not in user_by_osu:
                    continue
                seen.add(info.osu_id)
                user = user_by_osu[info.osu_id]
                old = previous_by_osu.get(info.osu_id)
                players.append(
                    {
                        "osu_id": info.osu_id,
                        "osu_name": user.osu_name,
                        "qq_name": member_names.get(user.user_id, ""),
                        "avatar_url": f"https://a.ppy.sh/{info.osu_id}",
                        "pp": info.pp,
                        "global_rank": info.g_rank,
                        "delta": info.pp - old.pp if old else None,
                    }
                )
            if not players:
                raise ValueError(f"今天还没有 {state['mode_name']} 模式的群排名数据。")
            from datetime import datetime
            from nonebot_plugin_osubot.draw.rank import draw_group_rank

            requester = next((user for user in users if user.user_id == str(event.get_sender_id())), None)
            image = await draw_group_rank(
                players,
                requester.osu_id if requester else None,
                f"{state['mode_name']}模式",
                datetime.now().strftime("%Y/%m/%d %H:%M"),
            )
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot group rank failed")
            yield event.plain_result(f"查询群内排名失败：{exc}")

    @filter.command("osudl", alias={"dl"})
    async def osudl(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.file import download_map
            from nonebot_plugin_osubot.utils import extract_beatmap_id, extract_beatmapset_id

            argument = self._argument(event).strip()
            set_id = extract_beatmapset_id(argument)
            if not set_id and (map_id := extract_beatmap_id(argument)):
                map_data = await self.api.osu_api("map", map_id=int(map_id))
                set_id = str(map_data["beatmapset_id"])
            if not set_id and argument.isdigit():
                set_id = argument
            if not set_id:
                remembered = self._last_map.get(self._context_key(event), (None, None))
                set_id = str(remembered[1]) if remembered[1] else None
                if not set_id and remembered[0]:
                    data = await self.api.osu_api("map", map_id=int(remembered[0]))
                    set_id = str(data["beatmapset_id"])
            if not set_id or not str(set_id).isdigit():
                raise ValueError("请输入正确的 setID，或先查询一张谱面。")
            path = await download_map(int(set_id))
            if not path:
                raise ValueError("谱面下载失败。")
            self._last_map[self._context_key(event)] = (None, int(set_id))
            self._cleanup_later(path)
            yield self._file_result(event, path)
        except Exception as exc:
            logger.exception("OSUBot beatmap download failed")
            yield event.plain_result(f"下载谱面失败：{exc}")

    @filter.command(
        "preview",
        alias={"预览", "完整预览", "视频预览", "完整视频", "vpreview", "vp"},
    )
    async def preview(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.draw.catch_preview import draw_cath_preview
            from nonebot_plugin_osubot.draw.osu_preview import draw_full_osu_preview, draw_osu_preview
            from nonebot_plugin_osubot.draw.taiko_preview import map_to_image, parse_map
            from nonebot_plugin_osubot.file import download_osu
            from nonebot_plugin_osubot.mania import generate_preview_pic
            from nonebot_plugin_osubot.utils import normalize_map_mode

            state = await self._state(event, "preview", require_user=False)
            map_id = state["target"] or self._last_map.get(self._context_key(event), (None, None))[0]
            if not map_id:
                raise ValueError("请输入正确的 mapID，或先查询一张谱面。")
            data = await self.api.osu_api("map", map_id=int(map_id))
            set_id = int(data["beatmapset_id"])
            state["mode"] = normalize_map_mode(state["mode"], int(data["mode_int"]))
            self._last_map[self._context_key(event)] = (int(map_id), set_id)
            command = event.message_str.strip().split(maxsplit=1)[0].split(":", 1)[0].lower()
            video_command = command in {"视频预览", "完整视频", "vpreview", "vp"}
            full_image = command == "完整预览"
            if video_command:
                await event.send(MessageChain([Comp.Plain("正在生成完整视频预览，请稍候……")]))
                video = await draw_full_osu_preview(int(map_id), set_id, target_mode=int(state["mode"]))
                yield self._video_result(event, video)
                return
            if state["mode"] == "3":
                osu_file = await download_osu(set_id, int(map_id))
                image = await generate_preview_pic(osu_file, full_image)
            elif state["mode"] == "2":
                image = await draw_cath_preview(int(map_id), set_id, state["mods"])
            elif state["mode"] == "1":
                osu_file = await download_osu(set_id, int(map_id))
                image = map_to_image(parse_map(osu_file))
            else:
                image = await draw_osu_preview(int(map_id), set_id, target_mode=int(state["mode"]))
            chain = [Comp.Image.fromBytes(self._bytes(image))]
            if state["mode"] == "0":
                chain.append(
                    Comp.Plain(
                        f"\n点击预览：\nhttps://beatmap.try-z.net/?b={map_id}\n"
                        f"https://beatmap.try-z.net/dev/?b={map_id}"
                    )
                )
            yield event.chain_result(chain)
        except Exception as exc:
            logger.exception("OSUBot preview failed")
            yield event.plain_result(f"生成谱面预览失败：{exc}")

    @staticmethod
    def _convert_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="/convert", add_help=False, exit_on_error=False)
        parser.add_argument("-h", "--help", action="store_true")
        parser.add_argument("--set", type=int)
        parser.add_argument("--map", type=int)
        parser.add_argument("--fln", action="store_true")
        parser.add_argument("--rate", type=float)
        parser.add_argument("--end_rate", type=float)
        parser.add_argument("--step", type=float, default=0.05)
        parser.add_argument("--od", type=float)
        parser.add_argument("--nsv", action="store_true")
        parser.add_argument("--nln", action="store_true")
        parser.add_argument("--gap", type=float, default=150)
        parser.add_argument("--thres", type=float, default=100)
        return parser

    async def _convert_map(self, arguments: list[str]) -> Path:
        from nonebot_plugin_osubot.api import get_beatmapsets_info
        from nonebot_plugin_osubot.mania import Options, convert_mania_map
        from nonebot_plugin_osubot.schema import Beatmap

        parser = self._convert_parser()
        args = parser.parse_args(arguments)
        if args.help:
            raise ValueError(
                "用法：/convert --set SETID [--fln] [--rate 1.2] [--end_rate 1.5] "
                "[--step 0.05] [--od 8] [--nsv] [--nln] [--gap 150] [--thres 100]"
            )
        options = Options(
            rate=args.rate,
            end_rate=args.end_rate,
            od=args.od,
            set=args.set,
            map=args.map,
            nsv=args.nsv,
            nln=args.nln,
            fln=args.fln,
            step=args.step,
            gap=args.gap,
            thres=args.thres,
        )
        if options.map:
            map_data = await self.api.osu_api("map", map_id=options.map)
            beatmap = Beatmap(**map_data)
            options.set = beatmap.beatmapset_id
            options.beatmapsets = await get_beatmapsets_info(beatmap.beatmapset_id)
        if not options.set:
            raise ValueError("请提供需要转换的谱面 setID。")
        if options.nln and options.fln:
            raise ValueError("--nln 与 --fln 不能同时使用。")
        path = await convert_mania_map(options)
        if not path:
            raise ValueError("未找到地图，请检查是否混淆了 mapID 与 setID。")
        return path

    @filter.command("convert", alias={"cv"})
    async def convert(self, event: AstrMessageEvent):
        try:
            path = await self._convert_map(shlex.split(self._argument(event)))
            self._cleanup_later(path)
            yield self._file_result(event, path)
        except Exception as exc:
            logger.exception("OSUBot map conversion failed")
            yield event.plain_result(f"谱面转换失败：{exc}")

    @filter.command("倍速")
    async def speed_change(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.utils import extract_beatmap_id

            arguments = self._argument(event).split()
            last_map = self._last_map.get(self._context_key(event), (None, None))[0]
            if not arguments:
                raise ValueError("请输入倍速速率，例如 /倍速 1.2；也可写 /倍速 <mapID> 1.2。")
            first = extract_beatmap_id(arguments[0]) or arguments[0]
            if first.isdigit():
                map_id = first
                rates = arguments[1:]
            else:
                map_id = str(last_map or "")
                rates = arguments
            if not map_id or not rates:
                raise ValueError("请输入正确的 mapID 和倍速速率。")
            argv = ["--map", map_id, "--rate"]
            if "-" in rates[0]:
                low, high = rates[0].split("-", 1)
                argv.extend([low, "--end_rate", high, "--step", "0.05"])
            else:
                argv.append(rates[0])
            path = await self._convert_map(argv)
            self._last_map[self._context_key(event)] = (int(map_id), None)
            self._cleanup_later(path)
            yield self._file_result(event, path)
        except Exception as exc:
            logger.exception("OSUBot speed conversion failed")
            yield event.plain_result(f"谱面倍速失败：{exc}")

    @filter.command("反键")
    async def full_ln(self, event: AstrMessageEvent):
        try:
            from nonebot_plugin_osubot.utils import extract_beatmap_id, extract_beatmapset_id

            arguments = self._argument(event).split()
            raw = arguments[0] if arguments else ""
            set_id = extract_beatmapset_id(raw)
            if not set_id and (map_id := extract_beatmap_id(raw)):
                data = await self.api.osu_api("map", map_id=int(map_id))
                set_id = str(data["beatmapset_id"])
            if not set_id and raw.isdigit():
                set_id = raw
            if not set_id:
                remembered = self._last_map.get(self._context_key(event), (None, None))
                set_id = str(remembered[1]) if remembered[1] else None
                if not set_id and remembered[0]:
                    data = await self.api.osu_api("map", map_id=int(remembered[0]))
                    set_id = str(data["beatmapset_id"])
            if not set_id:
                raise ValueError("请输入 setID，或先查询一张谱面。")
            argv = ["--set", str(set_id), "--fln"]
            if len(arguments) >= 2:
                argv.extend(["--gap", arguments[1]])
            if len(arguments) >= 3:
                argv.extend(["--thres", arguments[2]])
            path = await self._convert_map(argv)
            self._last_map[self._context_key(event)] = (None, int(set_id))
            self._cleanup_later(path)
            yield self._file_result(event, path)
        except Exception as exc:
            logger.exception("OSUBot full-LN conversion failed")
            yield event.plain_result(f"反键谱面转换失败：{exc}")

    async def _select_guess_score(self, event: AstrMessageEvent, state: dict[str, Any], mode: str):
        from nonebot_plugin_osubot.api import get_user_scores
        from nonebot_plugin_osubot.runtime import get_session

        context_key = self._context_key(event)
        seen = self._guess_seen.setdefault(context_key, set())
        mentioned_id = None
        for component in event.get_messages():
            target = getattr(component, "qq", None) or getattr(component, "target", None)
            if target not in (None, "all", event.get_self_id()):
                mentioned_id = str(target)
                break
        users = []
        async with get_session() as session:
            if mentioned_id:
                user = await session.scalar(select(self.UserData).where(self.UserData.user_id == mentioned_id))
                if not user:
                    raise ValueError("被提及的用户尚未绑定 osu! 账号。")
                users = [user]
            elif state.get("user"):
                user = await session.scalar(select(self.UserData).where(self.UserData.osu_id == int(state["user"])))
                if user:
                    users = [user]
                else:
                    users = [
                        type("GuessUser", (), {"osu_id": int(state["user"]), "osu_name": state.get("username") or str(state["user"])})()
                    ]
            else:
                users = (
                    await session.scalars(select(self.UserData).where(self.UserData.osu_mode == int(mode)))
                ).all()
        if not users:
            raise ValueError("还没有人绑定该模式的 osu! 账号。")
        candidates = []
        random.shuffle(users)
        for user in users[:5]:
            try:
                scores = await get_user_scores(user.osu_id, state["mode_name"], "best")
                candidates.extend(
                    (score, user.osu_name) for score in scores if int(score.beatmapset.id) not in seen
                )
            except Exception:
                logger.exception(f"OSUBot guess candidate fetch failed for osu_id={user.osu_id}")
        if not candidates:
            raise ValueError("可用 BP 已经猜过一遍，请稍后再试。")
        score, username = random.choice(candidates)
        seen.add(int(score.beatmapset.id))
        return score, username

    async def _guess_timeout(self, game_key: str) -> None:
        await asyncio.sleep(300)
        game = self._guess_games.pop(game_key, None)
        self._guess_tasks.pop(game_key, None)
        if not game:
            return
        score = game["score"]
        answer = score.beatmapset.title_unicode
        if answer != score.beatmapset.title:
            answer += f" [{score.beatmapset.title}]"
        try:
            await self.context.send_message(
                game["origin"], MessageChain([Comp.Plain(f"猜歌超时，游戏结束。正确答案是 {answer}")])
            )
        except Exception:
            logger.exception("OSUBot guess timeout message failed")

    def _drop_guess(self, event: AstrMessageEvent, game_type: str) -> None:
        game_key = f"{self._context_key(event)}:{game_type}"
        self._guess_games.pop(game_key, None)
        task = self._guess_tasks.pop(game_key, None)
        if task:
            task.cancel()

    async def _start_guess(self, event: AstrMessageEvent, game_type: str):
        try:
            state = await self._state(event, "guess")
        except Exception:
            mode = str(random.randint(0, 3))
            from nonebot_plugin_osubot.utils import NGM

            state = {
                "user": 0,
                "username": "",
                "mode": mode,
                "mode_name": NGM[mode],
                "source": "osu",
                "mods": [],
            }
        context_key = self._context_key(event)
        game_key = f"{context_key}:{game_type}"
        if game_key in self._guess_games:
            raise ValueError("当前会话已有同类型猜歌正在进行。")
        score, selected_user = await self._select_guess_score(event, state, state["mode"])
        hint_types = {
            "audio": {"pic", "artist", "creator"},
            "pic": {"artist", "creator", "audio"},
            "chart": {"pic", "artist", "creator", "audio"},
        }
        self._guess_games[game_key] = {
            "score": score,
            "hints": set(),
            "hint_types": hint_types[game_type],
            "origin": event.unified_msg_origin,
        }
        task = asyncio.create_task(self._guess_timeout(game_key))
        self._guess_tasks[game_key] = task
        logger.info(
            f"OSUBot {game_type} guess started in {context_key}; beatmapset_id={score.beatmapset.id}"
        )
        game_label = {"audio": "音频", "pic": "图片", "chart": "谱面"}[game_type]
        intro = f"开始{game_label}猜歌，该曲抽选自 {selected_user} 的 {state['mode_name']} BP。"
        if game_type == "audio":
            response = await self.api.safe_async_get(f"https://b.ppy.sh/preview/{score.beatmapset.id}.mp3")
            return event.chain_result(
                [
                    Comp.Plain(intro),
                    Comp.Record.fromBase64(base64.b64encode(response.content).decode("ascii")),
                ]
            )
        if game_type == "pic":
            from nonebot_plugin_osubot.info import get_bg

            image = await get_bg(score.beatmap.id)
            width, height = image.size
            crop_width = max(1, int(width * 0.3))
            crop_height = max(1, int(height * 0.3))
            left = random.randint(0, max(0, width - crop_width))
            top = random.randint(0, max(0, height - crop_height))
            output = BytesIO()
            image.crop((left, top, left + crop_width, top + crop_height)).save(output, "png")
            return event.chain_result([Comp.Plain(intro), Comp.Image.fromBytes(output.getvalue())])
        from nonebot_plugin_osubot.draw.catch_preview import draw_cath_preview
        from nonebot_plugin_osubot.draw.osu_preview import draw_osu_preview
        from nonebot_plugin_osubot.draw.taiko_preview import map_to_image, parse_map
        from nonebot_plugin_osubot.file import download_osu
        from nonebot_plugin_osubot.mania import generate_preview_pic

        if state["mode"] == "3":
            osu_file = await download_osu(score.beatmapset.id, score.beatmap.id)
            image = await generate_preview_pic(osu_file)
        elif state["mode"] == "1":
            osu_file = await download_osu(score.beatmapset.id, score.beatmap.id)
            image = map_to_image(parse_map(osu_file))
        elif state["mode"] == "2":
            image = await draw_cath_preview(
                score.beatmap.id, score.beatmapset.id, [mod.acronym for mod in score.mods]
            )
        else:
            image = await draw_osu_preview(score.beatmap.id, score.beatmapset.id)
        return event.chain_result([Comp.Plain(intro), Comp.Image.fromBytes(self._bytes(image))])

    @filter.command("音频猜歌")
    async def guess_audio(self, event: AstrMessageEvent):
        try:
            yield await self._start_guess(event, "audio")
        except Exception as exc:
            self._drop_guess(event, "audio")
            logger.exception("OSUBot audio guess failed")
            yield event.plain_result(f"无法开始音频猜歌：{exc}")

    @filter.command("图片猜歌")
    async def guess_picture(self, event: AstrMessageEvent):
        try:
            yield await self._start_guess(event, "pic")
        except Exception as exc:
            self._drop_guess(event, "pic")
            logger.exception("OSUBot picture guess failed")
            yield event.plain_result(f"无法开始图片猜歌：{exc}")

    @filter.command("谱面猜歌")
    async def guess_chart(self, event: AstrMessageEvent):
        try:
            yield await self._start_guess(event, "chart")
        except Exception as exc:
            self._drop_guess(event, "chart")
            logger.exception("OSUBot chart guess failed")
            yield event.plain_result(f"无法开始谱面猜歌：{exc}")

    async def _guess_hint(self, event: AstrMessageEvent, game_type: str):
        game_key = f"{self._context_key(event)}:{game_type}"
        game = self._guess_games.get(game_key)
        if not game:
            return event.plain_result("当前会话没有进行中的对应猜歌。")
        available = list(game["hint_types"] - game["hints"])
        if not available:
            return event.plain_result("已无更多提示，加油。")
        action = random.choice(available)
        game["hints"].add(action)
        score = game["score"]
        if action == "pic":
            return event.chain_result([Comp.Image.fromURL(score.beatmapset.covers.cover)])
        if action == "artist":
            artist = score.beatmapset.artist_unicode
            if artist != score.beatmapset.artist:
                artist += f" [{score.beatmapset.artist}]"
            return event.plain_result(f"曲师为：{artist}")
        if action == "creator":
            return event.plain_result(f"谱师为：{score.beatmapset.creator}")
        response = await self.api.safe_async_get(f"https://b.ppy.sh/preview/{score.beatmapset.id}.mp3")
        return self._audio_result(event, response.content)

    @filter.command("音频提示")
    async def audio_hint(self, event: AstrMessageEvent):
        yield await self._guess_hint(event, "audio")

    @filter.command("图片提示")
    async def picture_hint(self, event: AstrMessageEvent):
        yield await self._guess_hint(event, "pic")

    @filter.command("谱面提示")
    async def chart_hint(self, event: AstrMessageEvent):
        yield await self._guess_hint(event, "chart")

    @filter.event_message_type(filter.EventMessageType.ALL, priority=20)
    async def guess_answer_listener(self, event: AstrMessageEvent):
        if getattr(event, "is_at_or_wake_command", False):
            return
        answer = event.message_str.strip().lower()
        if not answer:
            return
        context_key = self._context_key(event)
        for game_type in ("audio", "pic", "chart"):
            game_key = f"{context_key}:{game_type}"
            game = self._guess_games.get(game_key)
            if not game:
                continue
            score = game["score"]
            titles = {
                score.beatmapset.title.lower(),
                score.beatmapset.title_unicode.lower(),
                re.sub(r"[(\[].*[)\]]", "", score.beatmapset.title.lower()).strip(),
                re.sub(r"[(\[].*[)\]]", "", score.beatmapset.title_unicode.lower()).strip(),
            }
            if max(SequenceMatcher(None, title, answer).ratio() for title in titles if title) < 0.5:
                continue
            self._guess_games.pop(game_key, None)
            task = self._guess_tasks.pop(game_key, None)
            if task:
                task.cancel()
            event.stop_event()
            yield event.plain_result(f"恭喜猜对，正确答案是 {score.beatmapset.title_unicode}")
            return

    @filter.event_message_type(filter.EventMessageType.ALL, priority=5)
    async def osu_url_listener(self, event: AstrMessageEvent):
        if getattr(event, "is_at_or_wake_command", False):
            return
        match = re.search(
            r"https?://osu\.ppy\.sh/(?:(?:beatmapsets/(\d+)(?:#[^/\s]+/(\d+))?)|(?:(?:b|beatmaps)/(\d+)))",
            event.message_str,
        )
        if not match:
            return
        set_id, set_map_id, direct_map_id = match.groups()
        map_id = set_map_id or direct_map_id
        try:
            if map_id:
                if not set_id:
                    data = await self.api.osu_api("map", map_id=int(map_id))
                    set_id = str(data["beatmapset_id"])
                image = await self.draw.draw_map_info(int(map_id), [])
                self._last_map[self._context_key(event)] = (int(map_id), int(set_id))
            else:
                image = await self.draw.draw_bmap_info(int(set_id))
                self._last_map[self._context_key(event)] = (None, int(set_id))
            mirrors = (
                f"\n镜像站1：https://catboy.best/d/{set_id}"
                f"\n镜像站2：https://osu.direct/api/d/{set_id}"
                f"\n小夜镜像站：https://txy1.sayobot.cn/beatmaps/download/novideo/{set_id}"
            )
            event.stop_event()
            yield event.chain_result([Comp.Image.fromBytes(self._bytes(image)), Comp.Plain(mirrors)])
        except Exception:
            logger.exception("OSUBot URL auto parser failed")
