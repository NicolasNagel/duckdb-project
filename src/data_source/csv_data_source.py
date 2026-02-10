import os
import io
import pandas as pd
import logging

from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime

from src.cloud.cloud_connection import AzureCloud

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

class CSVDataSource:
    """Classe responsável por fazer a leitura de arquivos do tipo CSV."""

    def __init__(
            self,
            default_path: Optional[str] = None,
            download_path: Optional[str] = None,
            cloud_conn: Optional[AzureCloud] = None
        ) -> None:

        self.cloud_conn = cloud_conn or AzureCloud()
        
        self.default_path = default_path or 'src/data'
        self.download_path = download_path or 'src/data'
        self.data: Dict[str, Any] = {}

    def start(self):
        logger.info('Iniciando Pipeline de Dados...')

        start_time = datetime.now()
        try:
            data = self.get_data()
            data = self.transform_data(data)
            data = self.load_data(data)

            end_time = datetime.now()
            pipeline_time = (end_time - start_time).total_seconds()

            logger.info(f'Pipeline concluída com sucesso em {pipeline_time:.2f}s')

        except Exception as e:
            logger.error(f'Erro ao executar a pipeline: {str(e)}')
            raise

    def get_data(self) -> List[Path]:
        """
        Coleta os arquivos no diretório padrão e salva em uma lista
        
        Returns:
            List[Path]: Lista com os diretórios do arquivos.
        """
        logger.info('Inicindo Coleta de Dados...')

        files = []
        try:
            list_files = os.listdir(self.default_path)
            for file in list_files:
                if file.endswith('.csv'):
                    full_path = os.path.join(self.default_path, file.lower())
                    files.append(full_path)

                    logger.info(f'Coletando arquivo: {file}...')

            logger.info(f'{len(files)} arquivos coletados.')
            return files
        
        except Exception as e:
            logger.error(f'Erro ao coletar dados: {str(e)}')
            raise

    def transform_data(self, files: List[Path]) -> Dict[str, pd.DataFrame]:
        """
        Transforma os dados e prepara para a ingestão.
        
        Args:
            files (List[Path]): Lista com o diretório dos arquivos.

        Returns:
            Dict(str, pd.DataFrame): Dicionário com nome do arquivo e conteúdo em DataFrame.
        """
        logger.info('Iniciando Transformação de Dados...')

        if not files or files is None:
            logger.warning('Transformação Cancelada. Nenhum Dado foi passado.')
            raise ValueError('files não pode ser vazio ou None.')
        
        try:
            for file in files:
                file_name = Path(file).stem
                df = pd.read_csv(file)
                self.data[file_name] = df

                logger.info(f'Transformando {file_name}...')

            logger.info(f'{len(self.data)} arquivos transformados.')
            return self.data
        
        except Exception as e:
            logger.error(f'Erro ao transformar dados: {str(e)}')
            self.data = {}
            raise

    def load_data(self, data: Dict[str, pd.DataFrame]) -> None:
        logger.info('Preparando Arquivos para Ingestão na Cloud...')

        if not data or data is None:
            logger.warning('Ingestão Cancelada. Nenhum arquivo foi passado.')
            raise ValueError('data não pode estar vazio ou ser Noner.')
        
        try:
            for name, df in data.items():
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                parquet_data = parquet_buffer.getvalue()

                file_name = f'raw/{name}.parquet'
                self.cloud_conn.upload_data(file_name, parquet_data)

            logger.info(f'{len(data)} arquivos salvos com sucesso.')

        except Exception as e:
            logger.error(f'Erro enviar arquivo para Azure: {str(e)}')
            raise