import logging
from enum import Enum
from os import getenv
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import KeyVaultSecret
from azure.keyvault.secrets import SecretClient


class ENVIRONMENT(Enum):
    UAT = getenv("UAT_KEY_VAULT_NAME")
    PRD = getenv("PRD_KEY_VAULT_NAME")

    def __str__(self) -> str:
        return self.value


class ConfigurationEnvHelper:
    _keyVaultParamsCache: dict[str, str] = {}

    def __init__(self, stage: ENVIRONMENT) -> None:
        try:
            self._stage: ENVIRONMENT = stage
            self._secret_client: SecretClient = SecretClient(vault_url=f"https://{stage}.vault.azure.net", credential=DefaultAzureCredential())
        except Exception as e:
            logging.exception(f"Unknow exception. {str(e)}")
    
    
    def get_secrets(self, keyVaults: dict[str, str]) -> dict[str, str]:
        #validate keyVaults in self_keyVaultParams
        for key, value in keyVaults.items():
            if value not in self._keyVaultParamsCache.keys():
                env_value = getenv(f"{value}")
                if not env_value:
                    try:
                        if not self._stage.value:
                            raise ValueError(f"None key vault configuration in project for environment {self._stage.name}")
                        bank_secret: KeyVaultSecret = self._secret_client.get_secret(value)
                        env_value = bank_secret.value
                    except ResourceNotFoundError as e:
                        logging.exception(f"Secret not found. {str(e.reason)}")

            else:
                env_value = self._keyVaultParamsCache[value]
            
            if not env_value:
                raise ValueError(f"None value in credentials for {key}")
            else:
                self._keyVaultParamsCache[value] = env_value
                keyVaults[key] = env_value
                    

        return keyVaults
    
    def get_secret(self, secret: str) -> str:
        if secret not in self._keyVaultParamsCache.keys():
            env_value = getenv(f"{secret}")
            if not env_value:
                try:
                    if not self._stage.value:
                        raise ValueError(f"None key vault configuration in project for environment {self._stage.name}")
                    bank_secret: KeyVaultSecret = self._secret_client.get_secret(secret)
                    env_value = bank_secret.value
                except ResourceNotFoundError as e:
                    logging.exception(f"Secret not found. {str(e.reason)}")

            if not env_value:
                raise ValueError(f"None value in credentials for {secret}")
            else:
                self._keyVaultParamsCache[secret] = env_value

        return env_value