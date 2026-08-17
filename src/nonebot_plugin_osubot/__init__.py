"""OSUBot package entry point for NoneBot and the AstrBot adapter."""

import os


if os.getenv("OSUBOT_ASTRBOT_RUNTIME") != "1":
    from .nonebot_entry import __plugin_meta__ as __plugin_meta__
