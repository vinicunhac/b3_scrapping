import os
import logging
import boto3
import io
import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import timedelta

# AWS CREDENTIALS
ACCESS_KEY = 'ASIAUKAQE32I42EDPCFB'
SECRET_KEY = 'Io+mJGdxmXWPgjLPDT256GPyWtDiSPn+922cXfs/'
SESSION_TOKEN = 'IQoJb3JpZ2luX2VjELj//////////wEaCXVzLXdlc3QtMiJIMEYCIQDHQpYRymrwV6TP2ofVOHUAAcN9/dQ3sm/aGRSVhffVZQIhAOTRxi7XKIdWzA5IB+DHBCQRSuoT5+/SoUzOMHKhxigtKsUCCIH//////////wEQARoMMjk2Mzg2NjE2OTc3Igwx3hktDwMgONxTgbQqmQLwpuhd5Bv+kWKmFb3XmcYqW/zHSj4uU1NMaMY1KjvW3gf6iZhN4HA+SazdrRcX/csARPG4P5jzZQqTrRWPCID5KLMgp1XrMEAlFR0cl8OP313u+xTz0PhNIHh5+3DHhJHRgwG0th7wy41ahsGUzt4gjleuCCagyRFps1bUaqSBxxRDFZ85fE4vVO3R7AgvNNvqpWmYiIXpHFGdVdoOiMsfsgRU0Ukcwf3QfB2AmVR06j+mysq8GitecCinuAugaoD5FKUDjty42ckkeTPGPoEmGX/Lt2QQzUTvnP5xw6soJ+F417SX5bZIAEiR9Q8j9TIgrcJ4DCLrgkxm9md0PuYh0I6sSGeDKdFHxa1v/jz11L57SFOcVuXSbzD415K7BjqcAQkyDJ4MwQcbDFN+5/hyo0xjjxgg8HrZZikhtnmxCDCFzJHBoq3i3aBVVXB/Nor6iWEnXZl4GPhnN8toitsj+ffBEkM04gj9ebe9rBA3V/sjf6HPgoIiTy3fDhM0eLhNUIDJusTLlSSaz86tHZ9krgjgUpzEHq9ttO71wS0LyFhXGWc0WNg49oYBITYGSkmDZDvf9wbBdJrJDkPefA=='

# Reference Date
current_date = pd.to_datetime('now') - timedelta(days=7)
current_date_str = current_date.strftime('%Y-%m-%d')

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_and_convert(x):
    try:
        return float(x.replace('.', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return np.nan

def main():
    # Configurar o driver do Selenium
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    try:
        url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
        driver.get(url)

        wait = WebDriverWait(driver, 20)
        table = wait.until(EC.presence_of_element_located((By.XPATH, '//table')))

        rows = table.find_elements(By.XPATH, './/tr')

        data = []
        for row in rows:
            cols = row.find_elements(By.XPATH, './/td')
            if cols:
                data.append([col.text for col in cols])

        if not data or not data[0]:
            raise ValueError("Nenhum dado foi encontrado na tabela.")
        else:
            logger.info("Dados coletados com sucesso")

        column_names = ["Código", "Ação", "Tipo", "Qtde. Teórica", "Part. (%)"]
        df = pd.DataFrame(data, columns=column_names)

        df['data'] = current_date.strftime('%Y-%m-%d %H:%M:%S')


        logger.info(f"Primeiras linhas do DataFrame:\n{df.head()}")

        df = df[~df['Código'].str.contains('Quantidade Teórica Total|Redutor', na=False)]

        df['Qtde. Teórica'] = df['Qtde. Teórica'].apply(clean_and_convert)
        df['Part. (%)'] = df['Part. (%)'].apply(clean_and_convert)

        if df['Qtde. Teórica'].isnull().any() or df['Part. (%)'].isnull().any():
            raise ValueError("Valores inválidos encontrados após a conversão para float.")

        logger.info(f"Schema do DataFrame:\n{df.dtypes}")
        logger.info(f"Valores nulos no DataFrame:\n{df.isnull().sum()}")

        df.rename(columns={"Qtde. Teórica": "Qtde_Teorica", "Part. (%)": "Part_Perc"}, inplace=True)
        df.reset_index(drop=True, inplace=True)

        table = pa.Table.from_pandas(df)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        # Configuração do cliente S3
        s3_client = boto3.client('s3',
                                 aws_access_key_id=ACCESS_KEY,
                                 aws_secret_access_key=SECRET_KEY,
                                 aws_session_token=SESSION_TOKEN)
        bucket_name = 'vc-ibov-data'
        parquet_file_name = f'raw/{current_date_str}/bovespa.parquet'

        # Fazer upload para o S3
        s3_client.upload_fileobj(parquet_buffer, bucket_name, parquet_file_name)
        logger.info(f"Arquivo Parquet enviado para o S3 em {bucket_name}/{parquet_file_name}")

    except Exception as e:
        logger.error(f"Erro durante o scraping ou processamento dos dados: {e}", exc_info=True)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

