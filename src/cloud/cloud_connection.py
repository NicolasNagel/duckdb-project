import os
import logging

from typing import Optional, List
from pathlib import Path
from dotenv import load_dotenv

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

load_dotenv()

class AzureCloud:
    """Classe responsável por fazer as conexões com a Azure."""

    def __init__(self):
        
        self.client_id = os.getenv('AZURE_CLIENT_ID')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET')
        self.account_url = os.getenv('AZURE_ACCOUNT_URL')
        self.container_name = os.getenv('AZURE_CONTAINER_NAME')

        logger.info('Criando conexão com a Cloud...')
        try:
            self.credentials = ClientSecretCredential(
                client_id=self.client_id,
                tenant_id=self.tenant_id,
                client_secret=self.client_secret
            )
            
            self.blob_service_client = BlobServiceClient(
                account_url=self.account_url,
                credential=self.credentials
            )

            logger.info('Conexão com a Azure estabelecida com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao se conectar com a Azure: {str(e)}')
            raise

    def upload_data(self, blob_name: str, data: bytes) -> None:
        """
        Faz o Upload de Arquivos para a Azure
        
        Args:
            blob_name (str): Nome do arquivo.
            data (bytes): Conteúdo do arquivo.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Upload de Dados...')

        try:
            blob_client = self.blob_service_client.get_blob_client(
                self.container_name,
                blob=blob_name
            )

            blob_client.upload_blob(data, overwrite=True)
            logger.info(f'Arquivo {blob_name} salvo com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao fazer o upload de dados: {str(e)}')
            raise

    def download_data(self, blob_name: str) -> bytes:
        """
        Faz o Upload de Arquivos para a Azure
        
        Args:
            blob_name (str): Nome do arquivo.

        Returns:
            data (bytes): Conteúdo do arquivo.
        """
        logger.info('Fazendo Download dos Arquivos...')

        try:
            blob_client = self.blob_service_client.get_blob_client(
                self.container_name,
                blob=blob_name
            )

            download_buffer = blob_client.download_blob()
            data = download_buffer.readall()

            logger.info(f'Arquivo {blob_name} salvo com sucesso.')
            return data
        
        except Exception as e:
            logger.error(f'Erro ao baixar arquivo: {str(e)}')
            raise

    def list_blob_files(self, prefix: Optional[str] = 'raw') -> List[Path]:
        """
        Lista os arquivos dentro de um blob específico.
        
        Args:
            prefix (Optional[str]): Nome do prefixo em que serão salvos os arquivos

        Returns:
            List(Path): Lista com o nome dos arquivos dentro do Container.
        """
        logger.info('Listando arquivos...')

        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_name = container_client.list_blobs(name_starts_with=prefix)

            blob = [blobs.name for blobs in blob_name]

            logger.info(f'{len(blob)} arquivos encontrados.')
            return blob
        
        except Exception as e:
            logger.error(f'Erro ao listar arquivos: {str(e)}')
            raise