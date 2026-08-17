"""Reusable Playwright pages for NoneBot HTMLRender and AstrBot."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

try:
    from nonebot.log import logger
    from nonebot_plugin_htmlrender import get_default_application
except ImportError:
    from ..runtime import logger

    get_default_application = None


_pages: dict[str, "_PersistentPage"] = {}
_locks: dict[str, asyncio.Lock] = {}
_closing = False
_playwright = None
_browser = None

_WAIT_FOR_ASSETS_SCRIPT = """
async timeoutMs => {
    const resources = [
        document.fonts.ready,
        ...Array.from(document.images, image => image.decode().catch(() => {})),
    ];
    await Promise.race([
        Promise.all(resources),
        new Promise(resolve => setTimeout(resolve, timeoutMs)),
    ]);
}
"""


@dataclass(slots=True)
class _PersistentPage:
    lease: Any
    page: Any
    goto_uri: str | None
    viewport: dict
    device_scale_factor: float


class _AstrBotPageLease:
    def __init__(self, viewport: dict, device_scale_factor: float):
        self.viewport = viewport
        self.device_scale_factor = device_scale_factor
        self.page = None

    async def __aenter__(self):
        global _playwright, _browser
        if _browser is None:
            from playwright.async_api import async_playwright

            _playwright = await async_playwright().start()
            _browser = await _playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox"],
            )
        self.page = await _browser.new_page(
            viewport=self.viewport,
            device_scale_factor=self.device_scale_factor,
        )
        return self.page

    async def __aexit__(self, exc_type, exc, traceback):
        if self.page is not None and not self.page.is_closed():
            await self.page.close()


async def _drop_page(key: str) -> None:
    entry = _pages.pop(key, None)
    if entry is not None:
        await entry.lease.__aexit__(None, None, None)


async def _create_page(
    key: str,
    goto_uri: str | None,
    viewport: dict,
    device_scale_factor: float,
):
    if get_default_application is None:
        lease = _AstrBotPageLease(viewport, device_scale_factor)
    else:
        app = get_default_application()
        lease = app.extensions.playwright.page(
            viewport=viewport,
            device_scale_factor=device_scale_factor,
        )
    page = await lease.__aenter__()
    try:
        if goto_uri:
            await page.goto(goto_uri, wait_until="domcontentloaded")
    except BaseException:
        await lease.__aexit__(None, None, None)
        raise
    _pages[key] = _PersistentPage(lease, page, goto_uri, viewport.copy(), device_scale_factor)
    return page


@asynccontextmanager
async def persistent_page(
    key: str,
    goto_uri: str | None,
    viewport: dict,
    device_scale_factor: float = 2,
):
    if _closing:
        raise RuntimeError("Playwright page pool is closing")
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        entry = _pages.get(key)
        if entry is not None and (
            entry.page.is_closed()
            or entry.goto_uri != goto_uri
            or entry.device_scale_factor != device_scale_factor
        ):
            await _drop_page(key)
            entry = None
        if entry is None:
            page = await _create_page(key, goto_uri, viewport, device_scale_factor)
        else:
            page = entry.page
        try:
            if entry is not None:
                await page.reload(wait_until="domcontentloaded")
                if entry.viewport != viewport:
                    await page.set_viewport_size(viewport)
                    entry.viewport = viewport.copy()
            yield page
        except BaseException:
            await _drop_page(key)
            raise


async def wait_for_page_assets(page: Any, timeout_ms: int = 8000) -> None:
    await page.evaluate(_WAIT_FOR_ASSETS_SCRIPT, timeout_ms)


async def close_persistent_pages() -> None:
    global _closing, _browser, _playwright
    _closing = True
    for key in list(_pages):
        lock = _locks.setdefault(key, asyncio.Lock())
        async with lock:
            try:
                await _drop_page(key)
            except Exception:
                logger.exception(f"Failed to close Playwright page: {key}")
    if _browser is not None:
        await _browser.close()
        _browser = None
    if _playwright is not None:
        await _playwright.stop()
        _playwright = None
