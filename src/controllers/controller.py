import os
import pandas as pd
import logging

from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

from src.cloud.cloud_connection import AzureCloud

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

class DataIngestor:
    """Classe responsável por fazer a Ingestão de Dados no Banco de Dados."""

    def __init__(
            self,
            cloud_conn: Optional[AzureCloud] = None,
        ) -> None:
        
        self.cloud_conn = cloud_conn or AzureCloud()
        self.download_path = 'src/data/temp_data'

    def start(self) -> None:
        ...

    def get_cloud_data(self) -> List[Path]:
        """
        Coleta os arquivos parquet salvos na Cloud.
        
        Returns:
            List(Path): Lista com o caminho completo dos arquivos.
        """
        logger.info('Coletando Dados da Azure...')

        files = self.cloud_conn.list_blob_files()

        if not files or files is None:
            logger.warning('Coleta cancelada. Nenhum arquivo foi encontrado.')
            raise ValueError('files não pode ser vazio ou None.')

        temp_path = Path(self.download_path)
        temp_path.mkdir(exist_ok=True)

        file_list = []
        try:
            for file in files:
                file_name = Path(file).name.split('/')[-1]
                data = self.cloud_conn.download_data(file)

                full_path = os.path.join(temp_path, file_name)

                with open(full_path, 'wb') as file:
                    file.write(data)

                file_list.append(full_path)
                logger.info(f'Arquivo {file_name} salvo com sucesso em: {temp_path}')

            logger.info(f'{len(files)} arquivos salvos com sucesso.')
            return file_list

        except Exception as e:
            logger.error(f'Erro ao coletar os dados da Azure: {str(e)}')
            raise

    def transform_data(self, data: List[Path]) -> Dict[str, pd.DataFrame]:
        logger.info('Transformando arquivos...')

        if not data or data is None:
            logger.warning('Transformação Cancelada. Nenhum dado foi passado.')
            raise ValueError('data não pode estar vazio ou ser None.')
        
        df_dict = {}
        try:
            for file in data:
                name = Path(file).stem.split('_')[-1]
                df = pd.read_parquet(file)
                df['inserted_at'] = datetime.now()

                df_dict[name] = df
                logger.info(f'Arquivo {name} transformado com sucesso.')

            logger.info(f'{len(data)} arquivos transformados.')
            return df_dict
        
        except Exception as e:
            logger.error(f'Erro ao transformar arquivos: {str(e)}')
            raise

    def save_data_into_db(self):
        ...