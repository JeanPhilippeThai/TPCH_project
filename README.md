# [Work in progress]

**Stack**: OLTP Postgres, OLAP BigQuery, DBT, Airflow, Python, SQL, GreatExpectation, CI/CD, PowerBI, N8N, Census

**Source des données**: Benchmark TPC-H, weatherAPI, données générées aléatoirements pour simuler des nouvelles transactions

**Test**: Great Expectation, CI/CD Github Action, test DBT, "Write Audit Publish" pattern pour les nouvelles transactions, AB testing

**Optimisation**: Indexing OLTP, clustering et partitionning sur l'OLAP

**Orchestration**: Airflow

**Fonctionnalités**: 
- Des données sont crées aléatoirement pour simuler des nouvelles transaction, des données sont récupérées de weatherapi.com et le tout est envoyé dans Postgres (OLTP)
- Des nouvelles données de Posgres sont envoyés vers BigQuery en suivant le pattern "Write Audit Publish"
- DBT gère la transformation Raw -> Staging -> Warehouse -> Mart
- Certaines données sont analysées en AB testing dont les rappports sont envoyés par mail à N8N aux stakeholders
- Census permet le reverse ETL (vers un CRM par exemple)
- PowerBI permet la visualisation

**Evolutions possibles**:
Intégration de Trino, Kafka, Pathway
