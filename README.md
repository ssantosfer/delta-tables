# Manipulação de Dados com Apache Spark e Delta Lake

## Descrição

Este projeto visa demonstrar como realizar operações eficientes de dados utilizando as poderosas ferramentas Apache Spark e Delta Lake. Através de uma combinação de manipulação, sincronização e administração de dados, o script mostra como criar e gerenciar tabelas, realizar operações de Captura de Dados em Mudança (CDC), e implementar um mecanismo de upsert para manter a integridade dos dados.

`Apache Spark`

O Apache Spark é um framework de código aberto projetado para processamento de dados em larga escala e análise de dados. Ele foi desenvolvido para ser rápido, flexível e fácil de usar.
O Spark processa os dados em **memória** sempre que possível, o que o torna significativamente mais rápido que os sistemas de processamento de dados tradicionais, como o Hadoop.

Sendo assim, o Spark é amplamente usado em projetos de análise de dados, aprendizado de máquina, processamento de dados em tempo real e ETL (Extract, Transform, Load). É uma escolha popular para organizações que precisam lidar com grandes volumes de dados de forma eficiente.

`Delta`

Um Delta Lake é uma camada de armazenamento de dados open-source construída sobre o Apache Spark, projetada para fornecer recursos avançados de gerenciamento de dados em ambientes de Big Data. Ele oferece funcionalidades como controle de transações ACID (Atomicidade, Consistência, Isolamento e Durabilidade), versionamento de dados, suporte a operações de merge (upsert), e otimizações de leitura e escrita para cenários de data lakes.

Uma Delta Table é uma tabela armazenada em Delta Lake, que utiliza os recursos oferecidos por essa camada de armazenamento. Essas tabelas são criadas e gerenciadas usando o formato Delta, que armazena os dados de forma eficiente e permite a execução de operações de data manipulation language (DML) como inserção, atualização, exclusão e mesclagem de dados de maneira eficiente e escalável.

Em resumo, um Delta Lake oferece um ambiente robusto e confiável para armazenar e gerenciar dados em grandes volumes, enquanto as Delta Tables proporcionam uma estrutura organizada e otimizada para a manipulação e análise desses dados.

## Requisitos

 Para realizar o projeto, é necessário ter o **Spark** e o **Delta Lake** configurado localmente no seu computador. 
    
## Execução do Projeto

**Importação das Bibliotecas:**

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import *
from delta import *
```

**Configuração da Sessão do Spark:**

 ```python
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
```

**Geração da Amostra dos dados:**

Esta função gera dados de amostra para funcionários e departamentos, cria tabelas Delta Lake e imprime seus conteúdos.

 ```python
def generate_Data(table_path, table_path2, table_path3):
    # Cria dados de amostra para as tabelas de funcionários e departamentos
    ...
    # Escreve dados nas tabelas Delta Lake
    ...
    # Mostra as tabelas criadas e seus conteúdos
    ...
```

**Captura de dados de mudança:**

Esta função simula operações de CDC, atualizando, inserindo e excluindo registros, em seguida, escreve os dados de CDC em uma tabela Delta Lake.

 ```python
def updates_CDC(table_path):
    # Simula operações de Captura de Dados de Mudança (CDC)
    ...
    # Escreve dados de CDC na tabela Delta Lake
    ...
```

**Operações de Tabela e Mecanismo de Upsert:**

Esta função carrega tabelas de origem, CDC e dimensão do Delta Lake, realiza uma operação de upsert para sincronizar tabelas de origem e CDC, e atualiza a tabela de dimensão conforme necessário.

 ```python
def upsert_tables(spark, table1, table2, table3, table4):
    # Carrega tabelas do Delta Lake
    ...
    # Realiza operação de upsert entre tabelas de origem e CDC
    ...
    # Atualiza a tabela de dimensão conforme necessário
    ...
```
**Execução:**

Por fim, estas chamadas de função executam todo o processo de gerenciamento de dados, incluindo geração de dados, simulação de CDC, exibição de dados de CDC e realização da operação de upsert.

 ```python
generate_Data(emp_path, dep_path, dim_path)
updates_CDC(emp_path_cdc)
show_table(emp_path_cdc)
upsert_tables(spark, emp_path, emp_path_cdc, dim_path, dep_path)
```


## Estrutura do Projeto

```bash
|-- upserts_delta_tables.py/        # Arquivo do Projeto em Python
|-- README.md                       # Este arquivo
|-- datalake/                       # Esta pasta contém as tabelas que foram geradas no script e os seus logs
```

## Links Úteis

- *Spark*: https://spark.apache.org/downloads.html | https://www.virtono.com/community/tutorial-how-to/how-to-install-apache-spark-on-ubuntu-22-04-and-centos/
- *Delta*: https://docs.delta.io/latest/quick-start.html#-python

## Exemplo de Uso



