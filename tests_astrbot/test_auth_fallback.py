import asyncio
import shutil
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from astrbot_src.client.oauth_client import OAuthClient
from astrbot_src.client.osu_client import OsuApiClient
from astrbot_src.client.token_manager import TokenData, TokenManager


class FakeOAuth:
    def __init__(self, token=None):
        self.api = SimpleNamespace(client_id=1, client_secret="secret", redirect_uri="")
        self.token = token

    async def ensure_token(self, platform_id):
        return self.token


class FakeApi:
    def __init__(self):
        self.client_credentials_calls = 0
        self.access_token = None

    async def client_credentials(self, scopes):
        self.client_credentials_calls += 1
        return {"access_token": "application-token", "expires_in": 86400}

    def set_access_token(self, token):
        self.access_token = token


def test_public_request_falls_back_to_cached_application_token():
    async def run():
        client = OsuApiClient(FakeOAuth())
        fake_api = FakeApi()
        client._api = fake_api

        await client._set_token("user-1")
        await client._set_token("user-2")

        assert fake_api.access_token == "application-token"
        assert fake_api.client_credentials_calls == 1

    asyncio.run(run())


def test_delegated_request_does_not_use_application_token():
    async def run():
        client = OsuApiClient(FakeOAuth())
        fake_api = FakeApi()
        client._api = fake_api

        with pytest.raises(ValueError, match="/osu link"):
            await client._set_token("user-1", require_user=True)

        assert fake_api.client_credentials_calls == 0

    asyncio.run(run())


def test_refresh_failure_is_throttled():
    temp_dir = tempfile.mkdtemp(prefix="auth-fallback-", dir=Path(__file__).parent)
    try:
        manager = TokenManager(temp_dir)
        manager.save(
            "user-1",
            TokenData(
                access_token="expired",
                refresh_token="invalid",
                expires_at=time.time() - 60,
            ),
        )
        client = OAuthClient(1, "secret", "", manager)
        calls = 0

        async def fail_refresh(refresh_token):
            nonlocal calls
            calls += 1
            raise RuntimeError("invalid refresh token")

        client.api.refresh_access_token = fail_refresh

        async def run():
            assert await client.ensure_token("user-1") is None
            assert await client.ensure_token("user-1") is None

        asyncio.run(run())
        assert calls == 1
    finally:
        shutil.rmtree(temp_dir)
