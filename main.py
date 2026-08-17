from __future__ import annotations

import asyncio
import os
import re
import shutil
import sys
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
import astrbot.api.message_components as Comp
from astrbot.api.star import Context, Star, register
from sqlalchemy import delete, select


PLUGIN_DIR = Path(__file__).resolve().parent
SOURCE_DIR = PLUGIN_DIR / "src"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))
os.environ["OSUBOT_ASTRBOT_RUNTIME"] = "1"


HELP_TEXT = """OSUBot 7.2.8 AstrBot 适配版
/bind <用户名/UID/主页链接> - 绑定 osu! 账号
/unbind - 解除绑定
/mode <o/t/c/m> - 修改默认模式
/info [玩家] [:模式] [#天数] - 玩家信息
/bp [序号/玩家] [:模式] [+MOD] - 最佳成绩
/bl [范围] [玩家] - BP 列表，例如 /bl 1-30
/tbp [范围] [玩家] [#天数] - 指定天数内进入 BP 的成绩
/recent [玩家] [:模式] [#序号] - 最近成绩
/pr [玩家] [:模式] [#序号] - 最近通过成绩
/map <mapID/链接> [+MOD] - 谱面信息
/bmap <setID/链接> - 谱面集信息
/score <mapID> [玩家] - 指定谱面成绩
/history [玩家] [:模式] [#天数] - PP/排名历史
/update - 立即更新绑定玩家数据
/mu [玩家] - osu! 个人主页

参数语法保持 AiriBot 版：:o/:t/:c/:m、+HDHR、#7、1-30、&sb。"""


@register(
    "astrbot_plugin_osubot",
    "XiaoLan9999 / yaowan233",
    "AiriBot nonebot-plugin-osubot 7.2.8 的 AstrBot 原生适配",
    "0.1.0",
    "https://github.com/XiaoLan9999/nonebot-plugin-osubot/tree/astrbot-native",
)
class OSUBotPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.scheduler = AsyncIOScheduler()
        self._ready = False
        self._last_map: dict[str, tuple[int, int | None]] = {}

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

        self.InfoData = InfoData
        self.SbUserData = SbUserData
        self.UserData = UserData
        self.api = api
        self.draw = draw
        self.update_users_info = update_users_info
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
        return text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) == 2 else ""

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
        expression = re.compile(
            r"(?P<field>title|artist|mapper|creator|pp|acc|accuracy|star|stars|sr|bpm|length|combo)"
            r"\s*(?P<op>!=|>=|<=|~=|=|>|<|~)\s*(?P<value>\"[^\"]*\"|'[^']*'|\S+)",
            re.IGNORECASE,
        )
        for match in expression.finditer(text):
            field = match.group("field").lower()
            field = {"acc": "accuracy", "star": "stars", "sr": "stars"}.get(field, field)
            conditions.append((field, match.group("op"), match.group("value").strip("\"'")))
        return conditions, expression.sub(" ", text)

    async def _state(self, event: AstrMessageEvent, command: str, require_user: bool = True) -> dict[str, Any]:
        from nonebot_plugin_osubot.utils import NGM, extract_beatmap_id, extract_beatmapset_id, mods2list, parse_mode

        text = self._argument(event).replace("，", ",").replace("：", ":").replace("＆", "&").replace("＃", "#")
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

        if command in {"bmap", "osudl"}:
            url_target = extract_beatmapset_id(text)
        else:
            url_target = extract_beatmap_id(text)
        if url_target:
            state["target"] = url_target
            text = re.sub(r"(?:https?://)?osu\.ppy\.sh/\S+", " ", text)

        if command in {"bp", "map", "bmap", "score"} and not state["target"]:
            numeric = list(re.finditer(r"(?<!\S)\d+(?!\S)", text))
            if numeric:
                selected = numeric[-1]
                state["target"] = selected.group(0)
                text = text[: selected.start()] + " " + text[selected.end() :]

        if command in {"bp", "pfm", "tbp"}:
            state["query"], text = self._parse_filters(text)
        username = " ".join(text.split())
        if username:
            state["username"] = username
            state["user"] = await self.api.get_uid_by_name(username, source)
        if require_user and not state["user"]:
            raise ValueError("该账号尚未绑定，请先使用 /bind <osu! 用户名>，或在命令后指定玩家")
        state["mode_name"] = NGM[state["mode"]]
        return state

    @filter.command("osuhelp", alias={"oh", "osubot", "osu帮助"})
    async def osu_help(self, event: AstrMessageEvent):
        yield event.plain_result(HELP_TEXT)

    @filter.command("bind")
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

    @filter.command("unbind")
    async def unbind(self, event: AstrMessageEvent):
        from nonebot_plugin_osubot.runtime import get_session

        sender_id = str(event.get_sender_id())
        async with get_session() as session:
            result = await session.execute(delete(self.UserData).where(self.UserData.user_id == sender_id))
            await session.commit()
        yield event.plain_result("解绑成功。" if result.rowcount else "尚未绑定，无需解绑。")

    @filter.command("mode")
    async def mode(self, event: AstrMessageEvent):
        from nonebot_plugin_osubot.runtime import get_session
        from nonebot_plugin_osubot.utils import GMN, NGM, parse_mode

        mode = parse_mode(self._argument(event))
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

    @filter.command("info")
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

    @filter.command("bp")
    async def bp(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_bp_command(event, "bp"))
        except Exception as exc:
            logger.exception("OSUBot bp failed")
            yield event.plain_result(f"查询 BP 失败：{exc}")

    @filter.command("pfm", alias={"bl", "bplist"})
    async def pfm(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_bp_command(event, "pfm"))
        except Exception as exc:
            logger.exception("OSUBot BP list failed")
            yield event.plain_result(f"查询 BP 列表失败：{exc}")

    @filter.command("tbp", alias={"nb", "todaybp"})
    async def tbp(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._draw_bp_command(event, "tbp"))
        except Exception as exc:
            logger.exception("OSUBot today BP failed")
            yield event.plain_result(f"查询近期 BP 失败：{exc}")

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

    @filter.command("recent", alias={"re", "RE"})
    async def recent(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._recent(event, "recent"))
        except Exception as exc:
            logger.exception("OSUBot recent failed")
            yield event.plain_result(f"查询最近成绩失败：{exc}")

    @filter.command("pr", alias={"PR"})
    async def pr(self, event: AstrMessageEvent):
        try:
            yield self._image_result(event, await self._recent(event, "pr"))
        except Exception as exc:
            logger.exception("OSUBot passed recent failed")
            yield event.plain_result(f"查询最近通过成绩失败：{exc}")

    @filter.command("map", alias={"m"})
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

    @filter.command("bmap", alias={"bm"})
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

    @filter.command("score", alias={"sc"})
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

    @filter.command("history", alias={"hs"})
    async def history(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "history")
            image = await self._draw_history(state)
            yield self._image_result(event, image)
        except Exception as exc:
            logger.exception("OSUBot history failed")
            yield event.plain_result(f"查询历史数据失败：{exc}")

    async def _draw_history(self, state: dict[str, Any]) -> bytes:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from nonebot_plugin_osubot.runtime import get_session

        query = select(self.InfoData).where(
            self.InfoData.osu_id == state["user"], self.InfoData.osu_mode == int(state["mode"])
        )
        if state["day"] > 0:
            query = query.where(self.InfoData.date >= date.today() - timedelta(days=state["day"]))
        async with get_session() as session:
            rows = (await session.scalars(query.order_by(self.InfoData.date))).all()
        rows = [row for row in rows if row.g_rank]
        if not rows:
            raise ValueError("没有找到可绘制的 PP/排名历史数据。")
        figure, pp_axis = plt.subplots(figsize=(12, 6), dpi=130)
        rank_axis = pp_axis.twinx()
        dates = [row.date for row in rows]
        pp_axis.plot(dates, [row.pp for row in rows], color="#ff66aa", marker="o", label="PP")
        rank_axis.plot(dates, [row.g_rank for row in rows], color="#66ccff", marker=".", label="Global Rank")
        rank_axis.invert_yaxis()
        pp_axis.set_title(f"{state['username'] or state['user']} {state['mode_name']} PP / Rank History")
        pp_axis.set_ylabel("PP")
        rank_axis.set_ylabel("Global Rank")
        pp_axis.grid(alpha=0.25)
        figure.autofmt_xdate()
        output = BytesIO()
        figure.tight_layout()
        figure.savefig(output, format="png")
        plt.close(figure)
        return output.getvalue()

    @filter.command("update")
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

    @filter.command("mu")
    async def mu(self, event: AstrMessageEvent):
        try:
            state = await self._state(event, "mu")
            yield event.plain_result(f"https://osu.ppy.sh/u/{state['user']}")
        except Exception as exc:
            yield event.plain_result(f"查询玩家主页失败：{exc}")
