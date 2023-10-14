import logging
from functools import lru_cache
from typing import Union

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import KeyVaultSecret, SecretClient
from typing_extensions import Self

from common.common_infrastructure.cross_cutting import ENVIRONMENT

from .key_vault_abc import KeyVaultABC


class KeyVaultImpl(KeyVaultABC):
    __credential: DefaultAzureCredential = None
    __client: SecretClient = None

    def __init__(self, stage: ENVIRONMENT) -> None:
        self.__credential: DefaultAzureCredential = DefaultAzureCredential()
        self.__client: SecretClient = SecretClient(
            vault_url=f"https://{stage}.vault.azure.net", credential=self.__credential
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        self.close_all()

    def close_all(self) -> None:
        self.__credential.close()
        self.__client.close()

    # este comando lo puso david el viernes 09/Junio/2021 a las 4:46Col y funcionaba antes del command
    @lru_cache(maxsize=20)
    def get_secret(self, secret_name: str) -> Union[str, None]:
        result: Union[str, None] = None
        try:
            bank_secret: KeyVaultSecret = self.__client.get_secret(secret_name)
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
