-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0d28fb8-0d0f-4354-9720-79ce468b5ea8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Extraindo dados diretamente de arquivos com o Spark SQL
-- MAGIC
-- MAGIC Neste notebook, você aprenderá a extrair dados diretamente de arquivos usando o Spark SQL no Databricks.
-- MAGIC
-- MAGIC Esta opção é compatível com vários formatos de arquivo, mas ela é mais útil para formatos de dados autodescritivos (como Parquet e JSON).
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Usar o Spark SQL para consultar arquivos de dados diretamente
-- MAGIC - Dispor views e CTEs em camadas para facilitar a referência a arquivos de dados
-- MAGIC - Aproveitar os métodos **`text`** e **`binaryFile`** para revisar o conteúdo de arquivos brutos

-- COMMAND ----------

-- DBTITLE 0,--i18n-73162404-8907-47f6-9b3e-dd17819d71c9
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para a execução do restante deste notebook.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-480bfe0b-d36d-4f67-8242-6a6d3cca38dd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Visão geral dos dados
-- MAGIC
-- MAGIC Neste exemplo, trabalharemos com uma amostra de dados brutos do Kafka gravados em arquivos JSON. 
-- MAGIC
-- MAGIC Cada arquivo contendo todos os registros consumidos durante um intervalo de cinco segundos é armazenado com o esquema Kafka completo como um arquivo JSON de vários registros.
-- MAGIC
-- MAGIC | campo | tipo | descrição |
-- MAGIC | --- | --- | --- |
-- MAGIC | key | BINARY | O campo **`user_id`**, usado como key, é um campo alfanumérico exclusivo que corresponde às informações da sessão/cookie |
-- MAGIC | value | BINARY | Esta é a carga completa de dados (a ser discutida mais tarde), enviada como JSON |
-- MAGIC | topic | STRING | Embora o serviço Kafka hospede vários tópicos, apenas os registros do tópico **`clickstream`** são incluídos aqui |
-- MAGIC | partition | INTEGER | A implementação atual do Kafka usa apenas duas partições (0 e 1) |
-- MAGIC | offset | LONG | Este é um valor único e aumenta monotonicamente para cada partição |
-- MAGIC | timestamp | LONG | Este carimbo de data/hora é registrado em milissegundos, começando pela época, e representa o momento em que o produtor acrescenta um registro a uma partição |

-- COMMAND ----------

-- DBTITLE 0,--i18n-65941466-ca87-4c29-903e-658e24e48cee
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe que o diretório de origem contém vários arquivos JSON.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-f1ddfb40-9c95-4b9a-84e5-2958ac01166d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Nos passos a seguir, usaremos caminhos de arquivo relativos dos dados que foram gravados no DBFS root. 
-- MAGIC
-- MAGIC A maioria dos workflows exigirá que os usuários acessem dados de locais externos de armazenamento em nuvem. 
-- MAGIC
-- MAGIC Na maioria das empresas, o administrador do workspace será responsável por configurar o acesso a esses locais de armazenamento.
-- MAGIC
-- MAGIC Instruções para configurar e acessar esses locais podem ser encontradas nos cursos autodirigidos específicos do fornecedor de nuvem com o título "Arquitetura de nuvem e integrações de sistemas".

-- COMMAND ----------

-- DBTITLE 0,--i18n-9abfecfc-df3f-4697-8880-bd3f0b58a864
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Consultar um único arquivo
-- MAGIC
-- MAGIC Para consultar os dados contidos em um único arquivo, execute a query com o seguinte padrão:
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC
-- MAGIC Observe especialmente o uso de acentos graves (que não são aspas simples) colocados antes e depois do caminho.

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c2891f1-e055-4fde-8bf9-3f448e4cdb2b
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe que a visualização exibe todas as 321 linhas do arquivo de origem.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0f45ecb7-4024-4798-a9b8-e46ac939b2f7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Consultar um diretório de arquivos
-- MAGIC
-- MAGIC Se todos os arquivos em um diretório tiverem o mesmo formato e esquema, eles poderão ser consultados simultaneamente especificando o caminho do diretório em vez do caminho de um arquivo individual. 

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-6921da25-dc10-4bd9-9baa-7e589acd3139
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Por default, esta query mostrará apenas os primeiros 10.000 registros ou 2 MB, o que for menor.

-- COMMAND ----------

-- DBTITLE 0,--i18n-035ddfa2-76af-4e5e-a387-71f26f8c7f76
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Criar referências a arquivos
-- MAGIC A capacidade de consultar arquivos e diretórios diretamente significa que é possível encadear lógica adicional do Spark a queries em arquivos.
-- MAGIC
-- MAGIC Quando criamos uma view a partir de uma query em um caminho, podemos fazer referência a essa view em consultas posteriores.

-- COMMAND ----------

CREATE OR REPLACE VIEW event_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c29b73b-b4b0-48ab-afbb-7b1422fce6e4
-- MAGIC %md
-- MAGIC
-- MAGIC Se tiver permissão para acessar a view e o local de armazenamento subjacente, o usuário poderá usar essa definição de view para consultar os dados subjacentes. Isso se aplica a diferentes usuários no workspace, diferentes notebooks e diferentes clusters.

-- COMMAND ----------

SELECT * FROM event_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-efd0c0fc-5346-4275-b083-4ee96ce8a852
-- MAGIC %md
-- MAGIC ## Criar referências temporárias a arquivos
-- MAGIC
-- MAGIC As views temporárias também dão às queries aliases que facilitem a referência em queries posteriores.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a9f9827b-2258-4481-a9d9-6fecf55aeb9b
-- MAGIC %md
-- MAGIC
-- MAGIC As views temporárias existem apenas na SparkSession atual. No Databricks, isso significa que elas estão isoladas do notebook, job ou query DBSQL atual.

-- COMMAND ----------

SELECT * FROM events_temp_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-dcfaeef2-0c3b-4782-90a6-5e0332dba614
-- MAGIC %md
-- MAGIC ## Aplicar CTEs à referência em uma query 
-- MAGIC Expressões de tabela comuns (CTEs) são perfeitas quando você deseja criar uma referência de curta duração e legível para humanos para os resultados de uma query.

-- COMMAND ----------

WITH cte_json
AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
SELECT * FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-c85e1553-f643-47b8-b909-0d10d2177437
-- MAGIC %md
-- MAGIC As CTEs só dão um alias ao resultado de uma query enquanto a query está sendo planejada e executada.
-- MAGIC
-- MAGIC Assim, **a célula a seguir gerará um erro ao ser executada**.

-- COMMAND ----------

-- SELECT COUNT(*) FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-106214eb-2fec-4a27-b692-035a86b8ec8d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Extrair arquivos de texto como strings brutas
-- MAGIC
-- MAGIC Ao trabalhar com arquivos baseados em texto (que incluem os formatos JSON, CSV, TSV e TXT), você pode usar o formato **`text`** para carregar cada linha do arquivo como uma linha com uma coluna de string chamada **`value`**. Esse recurso pode ser útil com fontes de dados propensas a corrupção e quando funções personalizadas de análise de texto são usadas para extrair valores de campos de texto.

-- COMMAND ----------

SELECT * FROM text.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-732e648b-4274-48f4-86e9-8b42fd5a26bd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Extrair bytes brutos e metadados de um arquivo
-- MAGIC
-- MAGIC Alguns fluxos de trabalho, como os que lidam com imagens ou dados não estruturados, podem exigir o processamento de arquivos inteiros. Usar **`binaryFile`** para consultar um diretório fornecerá metadados de arquivo e a representação binária do conteúdo do arquivo.
-- MAGIC
-- MAGIC Especificamente, os campos criados indicarão **`path`**, **`modificationTime`**, **`length`** e **`content`**.

-- COMMAND ----------

SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-9ac20d39-ae6a-400e-9e13-14af5d4c91df
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
