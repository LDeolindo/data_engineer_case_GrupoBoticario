# Data Engineer - Case Grupo Boticário

Case realizado para o processo seletivo do Grupo Boticário.

## Case 02

Para realização do case foi dividido em duas partes:
- Parte 1 - Base Vendas (2017,2018,2019)
- Parte 2 - Spotify (Search, Episodes)

### Organização das Tarefas

Foi utilizado o GitHub - Projects para organização das atividades, onde foi listado tarefa por tarefa sobre o que precisava ser feito para que o Case fosse concluído.

Para visualizar as tarefas que foram criadas é possível olhar via:
* [Issues](https://github.com/LDeolindo/data_engineer_case_GrupoBoticario/issues?q=is%3Aissue+is%3Aclosed)
* [Projects](https://github.com/users/LDeolindo/projects/7)

Print de como foi organizado:
![image](https://user-images.githubusercontent.com/35854681/230960928-92537b1b-d409-41c2-98a4-495e8ac629a9.png)

### Pré Requisitos
Para o desenvolvimento foi utilizado a linguagem [SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax?hl=pt-br) e [Python](https://www.python.org/) com as bibliotecas:
* [Requests](https://requests.readthedocs.io/en/master/);
* [Pandas](https://pandas.pydata.org/docs/);
* [Airflow](https://airflow.apache.org/docs/);
* [Astronomer - Airflow](https://registry.astronomer.io/providers/Apache%20Airflow/versions/latest);
* [Astronomer - Google](https://registry.astronomer.io/providers/Google/versions/latest).

Obs: é fornecido o arquivo: requiriments.txt com as bibliotecas e versões utilizadas.

### Implementações

Para realizar as implementações foi utilizado as ferramentas do GCP:
* Cloud Composer: Para a orquestração dos ETLs.
* Cloud Storage: Para armazenar os arquivos de Vendas XLSX (2017, 2018, 2019), como zona de pouso para os dados recebidos da API do Spotify e etapas intermediárias.
* BigQuery: Para armazenar os dados em tabelas de banco de dados, e para criação de algumas procedures.

### Instalação / Configuração
Primeiramente, abra o terminal no diretório do Case e instale as dependências (Caso queira Local):
* `$ pip install -r requirements.txt`

Obs: Para realização da reprodução é preciso ativar os serviçoes ou ter uma conta no GCP.

* Crie uma conta de serviço na GCP;
* Gere uma Key dessa conta e salve como JSON;
* Crie um Bucket com nome `grupoboticario-landing-zone-vendas`, dentro do bucket uma pasta com nome base_vendas e coloque os arquivos XLSX: `Base 2017 (1).xlsx`, `Base_2018 (1).xlsx` e `Base_2019 (2).xlsx`;
* Ative a API do Composer e em seguida crie um novo ambiente, foi escolhido o `Composer 1` e instale a dependecia `openpyxl`;
* Faça o upload da key da conta de serviço no Airflow em `Admin - Connections`, adicione um novo registro:
  - Connection Id: `GCP`;
  - Connection Type: `google_cloud_platform`;
  - Keyfile JSON: Cole o conteudo do JSON.
* E ainda no Airflow vá em `Admin - Variables` e adicione a Key: `SPOTIFY_TOKEN`e Val `token gerado`;
* No BigQuery crie dois datasets: `procedures_base_vendas` e `procedures_spotify`, e execute o código SQL que está dentro `src/sql/procedures_{nome}` para a criação das procedures;
* Agora no Composer em Pasta de DAGs, irá rederecionar para a pasta onde basta copiar todo o conteudo de `src/dags/*`;
* Por fim, execute as duas DAGs:
  - `base_vendas_dag`
  - `spotify_dag`

Obs: o token do spotify expira em 1h, sendo sempre necessário gerar um novo.

### DAGs

Visualização de como as DAGs foram estruturadas.

#### `base_vendas_dag`

DAG responsável por fazer o ETL no BigQuery dos dados base de vendas.
![image](https://user-images.githubusercontent.com/35854681/230971346-49ef32ac-0a10-4421-a95e-3e79ca342cbb.png)

#### `spotify_dag`

DAG responsável por fazer o ETL no BigQuery dos dados do Spotify.
![image](https://user-images.githubusercontent.com/35854681/230971604-9f5cd224-fd7a-47ad-ba93-b3cee80a2ef3.png)


Autor: Deolindo

In: https://www.linkedin.com/in/deolindo/
