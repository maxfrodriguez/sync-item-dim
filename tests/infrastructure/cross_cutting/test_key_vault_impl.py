import pytest
from common.common_infrastructure.cross_cutting.ConfigurationEnvHelper import ConfigurationEnvHelper




@pytest.mark.asyncio
class TestKeyVaultImpl:
    async def test_get_env(self):
        secret: dict[str, str] = {
            "uid": "PtUid",
            "pwd": "PtPwd",
            "host": "PtHost",
            "dbn": "PtDbn",
            "server": "PtServer",
            "PackageSizeToSync" : "PackageSizeToSync"
        }

        await ConfigurationEnvHelper().get_secrets(secret)

        secret: dict[str, str] = {
            "uid": "PtUid",
            "pwd": "PtPwd",
            "host": "PtHost",
            "dbn": "PtDbn",
            "server": "PtServer",
            "PackageSizeToSync" : "PackageSizeToSync"
        }

        await ConfigurationEnvHelper().get_secrets(secret)

        assert secret["PackageSizeToSync"] == "25"
