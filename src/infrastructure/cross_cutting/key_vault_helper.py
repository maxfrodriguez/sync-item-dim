from typing import Tuple

from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient

from src.infrastructure.cross_cutting import ENVIRONMENT


class KeyVaultHelper:
    @staticmethod
    def get_credential_client(stage: ENVIRONMENT) -> Tuple[DefaultAzureCredential, SecretClient]:
        credential: DefaultAzureCredential = DefaultAzureCredential()
        secret_client: SecretClient = SecretClient(vault_url=f"https://{stage}.vault.azure.net", credential=credential)
        return credential, secret_client
