import os
import pandas as pd
import duckdb
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
        """Inicia o Controller."""
        start_time = datetime.now()
        try:
            data = self.get_cloud_data()
            data = self.create_tables(data)

            end_time = datetime.now()
            pipeline_time = (end_time - start_time).total_seconds()

            logger.info(f'Pipeline Concluída com sucesso em {pipeline_time:.2f}s')

        except Exception as e:
            logger.error(f'Erro ao rodar a Pipeline: {str(e)}')
            raise

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

    def create_tables(self, data: List[Path]) -> None:
        """
        Cria as tabelas em memória do DuckDB.

        Args:
            data (List[Path]): Arquivo com nomes dos diretórios salvos localmente.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Criando Tabelas...')

        if not data or data is None:
            logger.warning('Inserção cancelada. Nenhum dado foi passado.')
            raise ValueError('data não pode estar vazio ou ser None.')
        
        try:
            with duckdb.connect() as conn:
                for file in data:
                    filename = Path(file).stem.split('_')[-1]
                    relation = conn.read_parquet(file)
                    conn.register(filename, relation)

                    logger.info(f'Tabela {filename} criada com sucesso')

                    query = f"""
                        SELECT *
                         FROM {filename}
                    """

                    files = conn.execute(query).fetch_df()
                    print(files)

            logger.info(f'{len(data)} arquivos salvos.')

        except Exception as e:
            logger.error(f'Erro ao criar tabelas: {str(e)}')
            raise