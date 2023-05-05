import logging

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets import KeyVaultSecret
from azure.keyvault.secrets.aio import SecretClient
from typing_extensions import Self

from . import ENVIRONMENT, KeyVaultHelper
from .key_vault_abc import KeyVaultABC


class KeyVaultImpl(KeyVaultABC):
    __credential: DefaultAzureCredential = None
    __client: SecretClient = None

    def __init__(self, stage: ENVIRONMENT) -> None:
        self.__credential, self.__client = KeyVaultHelper.get_credential_client(stage=stage)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close_all()

    async def close_all(self) -> None:
        await self.__credential.close()
        await self.__client.close()

    async def get_secret(self, secret_name: str) -> str | None:
        result: str | None = None
        try:
            bank_secret: KeyVaultSecret = await self.__client.get_secret(secret_name)
            result = bank_secret.value
            del bank_secret
        except ResourceNotFoundError as e:
            logging.exception(f"Secret not found. {str(e.reason)}")
        except HttpResponseError as e:
            logging.exception(f"Azure HttpResponseError. {str(e.message)}")
        except Exception as e:
            logging.exception(f"Unknow exception. {str(e)}")
        finally:
            return result
