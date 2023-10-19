import pytest
from common.common_infrastructure.cross_cutting.environment import ConfigurationEnvHelper

from src.infrastructure.cross_cutting import ENVIRONMENT, KeyVaultImpl


@pytest.mark.asyncio
class TestKeyVaultImpl:
    async def test_hello_world(self) -> None:
        secret: str | None = None

        async with KeyVaultImpl(stage=ENVIRONMENT.PRD) as kv:
            secret = await kv.get_secret("test-secret")

        assert secret is not None
        assert secret == "hello world"

    async def test_get_env(self):
        secret: dict[str, str] = {
            "uid": "PtUid",
            "pwd": "PtPwd",
            "host": "PtHost",
            "dbn": "PtDbn",
            "server": "PtServer",
            "PackageSizeToSync" : "PackageSizeToSync"
        }

        await ConfigurationEnvHelper(stage=ENVIRONMENT.UAT).get_secrets(secret)

        secret: dict[str, str] = {
            "uid": "PtUid",
            "pwd": "PtPwd",
            "host": "PtHost",
            "dbn": "PtDbn",
            "server": "PtServer",
            "PackageSizeToSync" : "PackageSizeToSync"
        }

        await ConfigurationEnvHelper(stage=ENVIRONMENT.UAT).get_secrets(secret)

        assert secret["PackageSizeToSync"] == "25"
