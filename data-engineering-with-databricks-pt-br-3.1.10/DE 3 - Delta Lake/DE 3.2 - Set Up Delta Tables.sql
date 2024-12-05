-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-c6ad28ff-5ff6-455d-ba62-30880beee5cd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Configurando tabelas Delta
-- MAGIC
-- MAGIC Depois de extrair dados de fontes de dados externas, carregue-os no Lakehouse para garantir que todos os benefícios da plataforma Databricks possam ser totalmente aproveitados.
-- MAGIC
-- MAGIC Embora diferentes organizações possam ter políticas variadas sobre o carregamento inicial de dados no Databricks, normalmente recomendamos que as primeiras tabelas representem uma versão majoritariamente bruta dos dados, e que a validação e o enriquecimento ocorram em fases posteriores. Esse padrão garante que, mesmo que os tipos de dados ou os nomes das colunas não sejam como esperado, nenhum dado será descartado, e a intervenção programática ou manual ainda poderá salvar dados parcialmente corrompidos ou inválidos.
-- MAGIC
-- MAGIC Esta lição se concentrará principalmente no padrão usado para criar a maioria das tabelas, as instruções **`CREATE TABLE _ AS SELECT`** (CTAS).
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Usar instruções CTAS para criar tabelas do Delta Lake
-- MAGIC - Criar tabelas a partir de views ou tabelas existentes
-- MAGIC - Enriquecer os dados carregados com metadados adicionais
-- MAGIC - Declarar o esquema da tabela com colunas geradas e comentários descritivos
-- MAGIC - Definir opções avançadas para controlar a localização dos dados, a aplicação de medidas de qualidade e o particionamento
-- MAGIC - Criar clones superficiais e profundos

-- COMMAND ----------

-- DBTITLE 0,--i18n-fe1b3b29-37fb-4e5b-8a50-e2baf7381d24
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para a execução do restante deste notebook.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-36f64593-7d14-4841-8b1a-fabad267ba22
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC
-- MAGIC As instruções **`CREATE TABLE AS SELECT`** criam e preenchem tabelas Delta usando dados recuperados de uma query de entrada.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- DBTITLE 0,--i18n-1d3d7f45-be4f-4459-92be-601e55ff0063
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC As instruções CTAS inferem automaticamente informações de esquema a partir dos resultados da query e **não** são compatíveis com a declaração manual de esquema. 
-- MAGIC
-- MAGIC Isso significa que as instruções CTAS são úteis para a ingestão de dados externos de fontes com esquemas bem definidos, como arquivos e tabelas Parquet.
-- MAGIC
-- MAGIC As instruções CTAS também não são compatíveis com a especificação de opções de arquivo adicionais.
-- MAGIC
-- MAGIC É fácil perceber que isso apresentaria limitações significativas ao tentar ingerir dados de arquivos CSV.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ec18c0e-c56b-4b41-8364-4826d1f34dcf
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Para ingerir corretamente esses dados em uma tabela do Delta Lake, precisamos usar uma referência aos arquivos que permita especificar opções.
-- MAGIC
-- MAGIC Na lição anterior, mostramos como fazer isso registrando uma tabela externa. Neste passo, usaremos uma ligeira evolução dessa sintaxe para especificar as opções de uma view temporária que usaremos como fonte de uma instrução CTAS para registrar a tabela Delta.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- DBTITLE 0,--i18n-96f21158-ccb9-4fd3-9dd2-c7c7fce1a6e9
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Filtrar e renomear colunas de tabelas existentes
-- MAGIC
-- MAGIC Transformações simples, como alterar nomes de colunas ou omitir colunas de tabelas de destino, podem ser facilmente realizadas durante a criação da tabela.
-- MAGIC
-- MAGIC A instrução a seguir cria uma tabela contendo um subconjunto de colunas da tabela **`sales`**. 
-- MAGIC
-- MAGIC Neste passo, vamos supor que estamos omitindo intencionalmente informações que podem identificar o usuário ou que fornecem detalhes da compra. Também renomearemos os campos supondo que um sistema posterior usa convenções de nomenclatura diferentes daquela dos dados de origem.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-02026f25-a1cf-42e5-b9c3-75b9c1c7ef11
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe que poderíamos ter alcançado o mesmo objetivo com uma view, conforme mostrado abaixo.

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-23f77f7e-21d5-4977-93bc-45800b28535f
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Declarar o esquema com colunas geradas
-- MAGIC
-- MAGIC Conforme observado anteriormente, as instruções CTAS não são compatíveis com declarações de esquema. No exemplo acima, a coluna de carimbo de data/hora parece ser uma variante de um carimbo de data/hora Unix, que pode não ser muito útil para a obtenção de percepções por analistas. Nessa situação, seria melhor usar colunas geradas.
-- MAGIC
-- MAGIC Colunas geradas são um tipo especial de coluna cujos valores são gerados automaticamente com base em uma função especificada pelo usuário a partir de outras colunas na tabela Delta (introduzida no DBR 8.3).
-- MAGIC
-- MAGIC O código abaixo demonstra a criação de uma tabela ao:
-- MAGIC 1. Especificar nomes e tipos de colunas
-- MAGIC 1. Adicionar uma <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">coluna gerada</a> para calcular a data
-- MAGIC 1. Fornecer um comentário de coluna descritivo para a coluna gerada

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")
  TBLPROPERTIES ('created.by.user' = 'John', 'created.date' = '01-01-2001', 'delta.appendOnly' = 'true')

-- COMMAND ----------

-- DBTITLE 0,--i18n-33e94ae0-f443-4cc9-9691-30b8b08179aa
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC
-- MAGIC Como **`date`** é uma coluna gerada, se gravarmos dados em **`purchase_dates`** sem fornecer valores para a coluna **`date`**, o Delta Lake os calculará automaticamente.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: A célula abaixo define uma configuração que permite gerar colunas usando uma instrução **`MERGE`** do Delta Lake. Falaremos mais sobre essa sintaxe posteriormente no curso.

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-8da2bdf3-a0e1-4c5d-a016-d9bc38167f50
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC O resultado abaixo mostra que todas as datas foram calculadas corretamente à medida que os dados eram inseridos, embora nem os dados de origem nem a query de inserção tenham especificado os valores para o campo.
-- MAGIC
-- MAGIC Assim como com qualquer fonte do Delta Lake, a query lê automaticamente o snapshot mais recente da tabela para qualquer query, e você não precisa executar **`REFRESH TABLE`**.

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- DBTITLE 0,--i18n-3bb038ec-4e33-40a1-b8ff-33b388e5dda1
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC É importante observar que, se um campo que seria gerado de outra forma for inserido em uma tabela, essa inserção falhará se o valor fornecido não corresponder exatamente ao valor que seria derivado pela lógica de definição da coluna gerada.
-- MAGIC
-- MAGIC Podemos ver esse erro descomentando e executando a célula abaixo:

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- DBTITLE 0,--i18n-43e1ab8b-0b34-4693-9d49-29d1f899c210
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Adicionar uma restrição de tabela
-- MAGIC
-- MAGIC A mensagem de erro acima se refere a um **`CHECK constraint`**. As colunas geradas são uma implementação especial de restrições de verificação.
-- MAGIC
-- MAGIC Como o Delta Lake impõe o esquema na gravação, o Databricks pode oferecer suporte a cláusulas padrão de gerenciamento de restrições SQL para garantir a qualidade e a integridade dos dados adicionados a uma tabela.
-- MAGIC
-- MAGIC Atualmente, o Databricks é compatível com dois tipos de restrições:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">restrições **`NOT NULL`**</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">restrições **`CHECK`**</a>
-- MAGIC
-- MAGIC Em ambos os casos, você precisa garantir que nenhum dado que viole a restrição já existe na tabela antes de definir a restrição. Depois que a restrição for adicionada a uma tabela, os dados que a violarem resultarão em falha de gravação.
-- MAGIC
-- MAGIC No passo abaixo, adicionaremos uma restrição **`CHECK`** à coluna **`date`** da tabela. Observe que as restrições **`CHECK`** se parecem com as cláusulas **`WHERE`** padrão que você pode usar para filtrar um dataset.

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- DBTITLE 0,--i18n-32fe077c-4e4d-4830-9a80-9a6a2b5d2a61
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC As restrições da tabela são mostradas no campo **`TBLPROPERTIES`**.

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- DBTITLE 0,--i18n-07f549c0-71af-4271-a8f5-91b4237d89e4
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Enriquecer tabelas com opções e metadados adicionais
-- MAGIC
-- MAGIC Até agora, falamos muito pouco sobre as opções disponíveis para o enriquecimento de tabelas do Delta Lake.
-- MAGIC
-- MAGIC Abaixo, mostramos a evolução de uma instrução CTAS para incluir diversas configurações e metadados adicionais.
-- MAGIC
-- MAGIC A cláusula **`SELECT`** usa dois comandos integrados do Spark SQL úteis para a ingestão de arquivos:
-- MAGIC * **`current_timestamp()`** registra o carimbo de data/hora quando a lógica é executada
-- MAGIC * **`input_file_name()`** registra o arquivo de dados de origem para cada registro na tabela
-- MAGIC
-- MAGIC Também incluímos lógica para criar uma nova coluna de data derivada dos dados de carimbo de data/hora na origem.
-- MAGIC
-- MAGIC A cláusula **`CREATE TABLE`** contém várias opções:
-- MAGIC * Um **`COMMENT`** é adicionado para permitir a descoberta mais fácil do conteúdo da tabela
-- MAGIC * Um **`LOCATION`** é especificado, o que resultará em uma tabela externa (em vez de gerenciada)
-- MAGIC * Uma tabela é **`PARTITIONED BY`** uma coluna de data, ou seja, os dados originados de cada dado existirão em seu próprio diretório no local de armazenamento de destino
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: O particionamento é mostrado aqui principalmente para demonstrar a sintaxe e o impacto. A maioria das tabelas do Delta Lake (especialmente as pequenas e médias) não se beneficiarão do particionamento. Como o particionamento separa fisicamente os arquivos de dados, essa abordagem pode causar problemas com arquivos pequenos, além de impedir a compactação de arquivos e a omissão eficiente de dados. Os benefícios observados no Hive ou no HDFS não são replicados no Delta Lake, e você deve consultar um arquiteto com experiência no Delta Lake antes de particionar tabelas.
-- MAGIC
-- MAGIC **A prática recomendada é adotar tabelas não particionadas como default para a maioria dos casos de uso no Delta Lake.**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- DBTITLE 0,--i18n-431d473d-162f-4a97-9afc-df47a787f409
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Os campos de metadados adicionados à tabela fornecem informações úteis para entender quando os registros foram inseridos e de onde vieram. Isso pode ser especialmente útil para diagnosticar problemas nos dados de origem.
-- MAGIC
-- MAGIC Todos os comentários e as propriedades de uma determinada tabela podem ser revisados usando **`DESCRIBE TABLE EXTENDED`**.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: O Delta Lake adiciona automaticamente várias propriedades de tabela ao criá-la.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- DBTITLE 0,--i18n-227fec69-ab44-47b4-aef3-97a14eb4384a
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Listar o local usado para a tabela revela que os valores exclusivos na coluna de partição **`first_touch_date`** são usados para criar diretórios de dados.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-d188161b-3bc6-4095-aec9-508c09c14e0c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Clonando tabelas do Delta Lake
-- MAGIC O Delta Lake tem duas opções para a cópia eficiente de tabelas.
-- MAGIC
-- MAGIC **`DEEP CLONE`** copia todos os dados e metadados de uma tabela de origem em um destino. Como a cópia ocorre de forma incremental, executar esse comando novamente pode sincronizar as alterações entre o local de origem e o local de destino.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-c0aa62a8-7448-425c-b9de-45284ea87f8c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Como é necessário copiar todos os arquivos de dados, essa operação pode ser um pouco demorada com grandes datasets.
-- MAGIC
-- MAGIC Quando você quer copiar rapidamente uma tabela para testar a aplicação de alterações sem o risco de modificar a tabela atual, **`SHALLOW CLONE`** pode ser uma boa opção. Os clones superficiais apenas copiam os logs de transações Delta, o que significa que os dados não são movidos.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-045bfc09-41cc-4710-ab67-acab3881f128
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Em ambos os casos, as modificações de dados aplicadas à versão clonada da tabela serão rastreadas e armazenadas separadamente da fonte. A clonagem é uma ótima maneira de configurar tabelas para testar código SQL ainda em desenvolvimento.

-- COMMAND ----------

-- DBTITLE 0,--i18n-32c272d6-cbe8-43ba-b051-9fe8e5586990
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Resumo
-- MAGIC
-- MAGIC Neste notebook, nos concentramos principalmente em DDL e na sintaxe para a criação de tabelas do Delta Lake. No próximo notebook, exploraremos opções para gravar atualizações em tabelas.

-- COMMAND ----------

-- DBTITLE 0,--i18n-c87f570b-e175-4000-8706-c571aa1cf6e1
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
