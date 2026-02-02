import os
import io
import pandas as pd
import logging

from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

class CSVDataSource:
    """Classe responsável por fazer a leitura de arquivos do tipo CSV."""

    def __init__(self, default_path: Optional[str] = None, download_path: Optional[str] = None):
        
        self.default_path = default_path or 'src/data'
        self.download_path = download_path or 'src/data'
        self.data: Dict[str, Any] = {}

    def start(self):
        ...

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

    def load_data(self):
        ...