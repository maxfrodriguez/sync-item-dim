import pytest

from src.infrastructure.cross_cutting import ENVIRONMENT, KeyVaultImpl


@pytest.mark.asyncio
class TestKeyVaultImpl:
    async def test_hello_world(self) -> None:
        secret: str | None = None

        async with KeyVaultImpl(stage=ENVIRONMENT.PRD) as kv:
            secret = await kv.get_secret("test-secret")

        assert secret is not None
        assert secret == "hello world"
