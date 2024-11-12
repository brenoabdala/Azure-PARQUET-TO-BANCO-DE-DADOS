import os
import logging
import pandas as pd
import pymssql  # biblioteca para conectar com SQL Server
from azure.storage.blob import BlobServiceClient
import azure.functions as func

# Função para baixar o arquivo Parquet do Blob Storage
def download_parquet_from_blob(container_name, blob_name, connection_string):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    local_file_path = '/tmp/downloaded_data.parquet'  # Local temporário para armazenar o arquivo

    with open(local_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    
    return local_file_path

# Função para inserir dados no banco de dados SQL Server
def insert_data_to_sql_server(data, server, database, username, password, table_name):
    # Conecta ao banco de dados
    conn = pymssql.connect(server=server, user=username, password=password, database=database)
    cursor = conn.cursor()

    # Insere os dados do DataFrame no banco de dados
    for index, row in data.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (coluna1, coluna2) 
            VALUES (%s, %s)
        """, (row['coluna1'], row['coluna2']))
    
    conn.commit()
    cursor.close()
    conn.close()

# Função principal da Azure Function
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Iniciando o processamento de dados do arquivo Parquet para o banco de dados SQL Server.")

    # Configurações
    container_name = "#"  # Nome do contêiner onde o arquivo está armazenado
    blob_name = "dados/dados.parquet"  # Caminho do arquivo no Blob Storage
    blob_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")  # String de conexão do Blob Storage

    # Dados de conexão do SQL Server
    sql_server = os.getenv("SQL_SERVER")
    sql_database = os.getenv("SQL_DATABASE")
    sql_username = os.getenv("SQL_USERNAME")
    sql_password = os.getenv("SQL_PASSWORD")
    table_name = "#"  # Nome da tabela no banco de dados

    try:
        # Baixa o arquivo Parquet do Azure Blob Storage
        local_file_path = download_parquet_from_blob(container_name, blob_name, blob_connection_string)
        
        # Carrega os dados do arquivo Parquet em um DataFrame
        data = pd.read_parquet(local_file_path)
        
        # Insere os dados no banco de dados SQL Server
        insert_data_to_sql_server(data, sql_server, sql_database, sql_username, sql_password, table_name)

        logging.info("Dados inseridos no banco com sucesso.")
        return func.HttpResponse("Dados inseridos no banco com sucesso!", status_code=200)

    except Exception as e:
        logging.error(f"Erro ao processar os dados: {str(e)}")
        return func.HttpResponse("Erro ao processar os dados.", status_code=500)
