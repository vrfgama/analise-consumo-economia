Projeto de Análise de Consumo e Economia

Este projeto utiliza Apache Airflow com Postgres em containers Docker para orquestrar pipelines de dados.
A estrutura segue boas práticas de organização e versionamento, mantendo apenas código e configurações no repositório (dados e logs ficam fora do controle de versão).

* Tecnologias

Apache Airflow
 2.9.1 (Python 3.12)

Postgres
 15

Docker & Docker Compose

* Estrutura de Pastas
analise-consumo-economia/
│
├── dags/                  # DAGs do Airflow (pipelines de dados)
├── plugins/               # Plugins customizados
├── scripts/               # Scripts auxiliares
│
├── docker-compose.yml     # Arquivo principal do ambiente
├── .env.example           # Exemplo de variáveis de ambiente
├── README.md              # Documentação do projeto
│
├── data/                  # NÃO versionado - dados (raw, staging, analytics)
├── logs/                  # NÃO versionado - logs do Airflow
└── .env                   # NÃO versionado - credenciais reais

* Configuração do Ambiente
1. Pré-requisitos

Docker Desktop instalado

Git instalado

2. Clonar o repositório
git clone https://github.com/seu-usuario/analise-consumo-economia.git
cd analise-consumo-economia

3. Configurar variáveis de ambiente

Copie o arquivo .env.example para .env:

cp .env.example .env


Edite o arquivo .env e defina suas credenciais.

4. Subir os containers
docker-compose up -d

5. Acessar o Airflow

Abra no navegador:

http://localhost:8080


Login com as credenciais definidas no .env.

* Estrutura de Dados

Dentro da pasta data/, existem três camadas:

raw/ → dados brutos

staging/ → dados intermediários

analytics/ → dados prontos para análise

* Importante: a pasta data/ não é versionada no Git.

* Contribuindo

Crie um branch para sua feature:

git checkout -b minha-feature


Faça commit das alterações:

git commit -m "Adiciona nova DAG"


Envie para o repositório:

git push origin minha-feature

* Observações

O arquivo .env nunca deve ser versionado (contém senhas e chaves).

Logs e dados locais também não vão para o Git.