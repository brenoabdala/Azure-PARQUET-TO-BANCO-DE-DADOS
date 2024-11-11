# Azure-PARQUET-TO-BANCO-DE-DADOS

<p> Este repositório contém uma Azure Function que faz o download de arquivos Parquet armazenados no Azure Blob Storage e insere seus dados em uma tabela de banco de dados SQL Server. A função é projetada para ser acionada via uma solicitação HTTP. </p>


<p><strong>Esta Azure Function realiza as seguintes operações:</strong></p>

- Recebe uma requisição HTTP.
- Baixa um arquivo Parquet do Azure Blob Storage.
- Carrega os dados do arquivo em um DataFrame do pandas.
- Insere os dados do DataFrame em uma tabela SQL Server.

### Pré-requisitos
Para usar este projeto, você precisará dos seguintes itens:

- Conta do Azure e acesso ao Azure Blob Storage.
- Banco de dados SQL Server.
- Ferramentas e bibliotecas instaladas:
- Python 3.8 ou superior.
-  Azure Functions Core Tools.
- Azure CLI (para deploy).
- Bibliotecas pandas, pymssql, azure-functions, e azure-storage-blob.

### Configuração
- Crie um contêiner no Azure Blob Storage e armazene seus arquivos Parquet lá.
- Configure o Banco de Dados SQL Server para receber os dados e crie a tabela de destino.
### Variáveis de Ambiente

<p> Para configurar a função, crie as seguintes variáveis de ambiente no ambiente do Azure Function App ou localmente:</p>

<strong>Blob Storage</strong>

 - <strong>AZURE_STORAGE_CONNECTION_STRING:</strong> String de conexão para o Azure Blob Storage.

### SQL Server

- SQL_SERVER: Endereço do servidor SQL Server.
- SQL_DATABASE: Nome do banco de dados.
- SQL_USERNAME: Nome de usuário para acesso ao banco de dados.
- SQL_PASSWORD: Senha para acesso ao banco de dados.

Essas variáveis de ambiente podem ser configuradas nas Configurações do Application Settings no portal do Azure.
