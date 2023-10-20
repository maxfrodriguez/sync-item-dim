from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import KeyVaultSecret, SecretClient


import logging
from os import getenv


class ConfigurationEnvHelper:
    _keyVaultParamsCache: dict[str, str] = {}

    def __init__(self) -> None:
        try:
            self._stage: str = self.get_secret("KEY_VAULT_NAME")
            self._secret_client: SecretClient = None
            if self._stage:
                self._secret_client: SecretClient = SecretClient(vault_url=f"https://{self._stage}.vault.azure.net", credential=DefaultAzureCredential())
        except Exception as e:
            logging.exception(f"Unknow exception. {str(e)}")


    # def get_secrets(self, keyVaults: dict[str, str], keyvalutrefer: str = None) -> dict[str, str]:
    #     keyvalutrefer = "kv-blackfly-test"
    #     _stageReference: str = ConfigurationEnvHelper().get_secret(keyvalutrefer)
    #     _client = self._secret_client

    #     self._secret_client: SecretClient = SecretClient(vault_url=f"https://{_stageReference}.vault.azure.net", credential=DefaultAzureCredential())
    #     keyVaults = self.get_secrets(keyVaults)
        
    #     self._secret_client = _client

        

    def get_secrets(self, keyVaults: dict[str, str]) -> dict[str, str]:
        #validate keyVaults in self_keyVaultParams
        for key, value in keyVaults.items():
            if value not in self._keyVaultParamsCache.keys():
                env_value = getenv(f"{value}")
                if not env_value:
                    try:
                        if not self._secret_client:
                            raise ValueError(f"I don't found key vault name reference configuration")
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
                    if not self._secret_client:
                        raise ValueError(f"I don't found key vault name reference configuration")
                    bank_secret: KeyVaultSecret = self._secret_client.get_secret(secret)
                    env_value = bank_secret.value
                except ResourceNotFoundError as e:
                    logging.exception(f"Secret not found. {str(e.reason)}")

            if not env_value:
                raise ValueError(f"None value in credentials for {secret}")
            else:
                self._keyVaultParamsCache[secret] = env_value

        return self._keyVaultParamsCache[secret]