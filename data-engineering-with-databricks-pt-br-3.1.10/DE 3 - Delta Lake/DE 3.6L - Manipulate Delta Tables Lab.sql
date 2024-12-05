-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-65583202-79bf-45b7-8327-d4d5562c831d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Laboratório de manipulação de tabelas Delta
-- MAGIC
-- MAGIC Este notebook fornece uma revisão prática de alguns dos recursos mais especializados que o Delta Lake traz ao data lakehouse.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final deste laboratório, você será capaz de:
-- MAGIC - Revisar a história da tabela
-- MAGIC - Consultar versões anteriores da tabela e reverter uma tabela para uma versão específica
-- MAGIC - Realizar a compactação do arquivo e a indexação com Z-order
-- MAGIC - Visualizar os arquivos marcados para exclusão permanente e confirmar essas exclusões

-- COMMAND ----------

-- DBTITLE 0,--i18n-065e2f94-2251-4701-b0b6-f4b86323dec8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Configuração
-- MAGIC Execute o script a seguir para configurar as variáveis necessárias e limpar execuções anteriores deste notebook. Observe que reexecutar esta célula permitirá que você reinicie o laboratório.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-56940be8-afa9-49d8-8949-b4bcdb343f9d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Criar a história da Bean Collection
-- MAGIC
-- MAGIC A célula abaixo inclui várias operações de tabela, resultando no seguinte esquema para a tabela **`beans`**:
-- MAGIC
-- MAGIC | Nome do campo | Tipo de campo |
-- MAGIC | --- | --- |
-- MAGIC | nome | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf6ff074-4166-4d51-92e5-67e7f2084c9b
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Revisar a história da tabela
-- MAGIC
-- MAGIC O log de transações do Delta Lake armazena informações sobre cada transação que modifica o conteúdo ou as configurações de uma tabela.
-- MAGIC
-- MAGIC Revise a história da tabela **`beans`** abaixo.

-- COMMAND ----------

-- TODO
-- <FILL-IN>

-- COMMAND ----------

-- ANSWER
DESCRIBE HISTORY beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fb56d746-8889-41c1-ba73-576282582534
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Se todas as operações anteriores foram concluídas conforme descrito, você deverá ver sete versões da tabela (**OBSERVAÇÃO**: Como o controle de versão do Delta Lake começa por 0, o número máximo da versão é 6).
-- MAGIC
-- MAGIC As operações deverão ser:
-- MAGIC
-- MAGIC | versão | operação |
-- MAGIC | --- | --- |
-- MAGIC | 0 | CREATE TABLE |
-- MAGIC | 1 | WRITE |
-- MAGIC | 2 | WRITE |
-- MAGIC | 3 | UPDATE |
-- MAGIC | 4 | UPDATE |
-- MAGIC | 5 | DELETE |
-- MAGIC | 6 | MERGE |
-- MAGIC
-- MAGIC A coluna **`operationsParameters`** permite revisar os predicados usados em atualizações, exclusões e merges. A coluna **`operationMetrics`** indica quantas linhas e arquivos são adicionados em cada operação.
-- MAGIC
-- MAGIC Revise a história do Delta Lake para identificar a versão da tabela que corresponde a uma determinada transação.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: A coluna **`version`** designa o estado de uma tabela quando uma determinada transação é concluída. A coluna **`readVersion`** indica a versão da tabela na qual uma operação foi executada. Nesta demonstração simples (sem transações concorrentes), esse relacionamento deve sempre mostrar um incremento de 1.

-- COMMAND ----------

-- DBTITLE 0,--i18n-00d8e251-9c9e-4be3-b8e7-6e38b07fac55
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Consultar uma versão específica
-- MAGIC
-- MAGIC Depois de revisar a história da tabela, você decide visualizar o estado da tabela após a inserção dos primeiros dados.
-- MAGIC
-- MAGIC Execute a query abaixo para obter esse resultado.

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- DBTITLE 0,--i18n-90e3c115-6bed-4b83-bb37-dd45fb92aec5
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC E agora revise o estado atual dos dados.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-f073a6d9-3aca-41a0-9452-a278fb87fa8c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Você deseja revisar os pesos dos grãos antes de excluir qualquer registro.
-- MAGIC
-- MAGIC Preencha a instrução abaixo para registrar uma view temporária da versão imediatamente anterior à exclusão dos dados e, em seguida, execute a célula a seguir para consultar essa view.

-- COMMAND ----------

-- TODO
-- CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
-- <FILL-IN>

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
  SELECT * FROM beans VERSION AS OF 4;

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-bad13c31-d91f-454e-a14e-888d255dc8a4
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar se a versão correta foi capturada.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- DBTITLE 0,--i18n-8450d1ef-c49b-4c67-9390-3e0550c9efbc
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Restaurar uma versão anterior
-- MAGIC
-- MAGIC Aparentemente houve um mal-entendido. Os grãos que um amigo lhe deu e que você incorporou em sua coleção não eram para você guardar.
-- MAGIC
-- MAGIC Reverta a tabela para a versão anterior à conclusão da instrução **`MERGE`**.

-- COMMAND ----------

-- TODO
-- <FILL-IN>

-- COMMAND ----------

-- ANSWER
RESTORE TABLE beans TO VERSION AS OF 5

-- COMMAND ----------

-- DBTITLE 0,--i18n-405edc91-49e8-412b-99e7-96cc60aab32d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Revise a história da tabela. Observe que a restauração para uma versão anterior adiciona outra versão da tabela.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- DBTITLE 0,--i18n-d430fe1c-32f1-44c0-907a-62ef8a5ca07b
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Compactação de arquivos
-- MAGIC Observando as métricas de transação durante a reversão, você se surpreende com o grande número de arquivos para uma coleção de dados tão pequena.
-- MAGIC
-- MAGIC Embora seja improvável que a indexação em uma tabela desse tamanho melhore o desempenho, você decide adicionar um índice Z-order no campo **`name`** prevendo o crescimento exponencial de sua bean collection ao longo do tempo.
-- MAGIC
-- MAGIC Use a célula abaixo para realizar a compactação do arquivo e a indexação com Z-order.

-- COMMAND ----------

-- TODO
-- <FILL-IN>

-- COMMAND ----------

-- ANSWER
OPTIMIZE beans
ZORDER BY name

-- COMMAND ----------

-- DBTITLE 0,--i18n-8ef4ffb6-c958-4798-b564-fd2e65d4fa0e
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Os dados devem ter sido compactados em um único arquivo. Confirme se isso ocorreu executando a célula a seguir.

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-8a63081c-1423-43f2-9608-fe846a4a58bb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar se você otimizou e indexou a tabela com sucesso.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6432b28c-18c1-4402-864c-ea40abca50e1
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Limpando arquivos de dados obsoletos
-- MAGIC
-- MAGIC Você sabe que embora todos os dados agora residam em um arquivo, os arquivos de dados de versões anteriores da tabela ainda estão armazenados com ele. Você deseja remover esses arquivos e o acesso a versões anteriores da tabela executando **`VACUUM`** na tabela.
-- MAGIC
-- MAGIC Executar **`VACUUM`** remove o lixo do diretório da tabela. Por default, será aplicado um limite de retenção de sete dias.
-- MAGIC
-- MAGIC A célula abaixo modifica algumas configurações do Spark. O primeiro comando substitui a verificação do limite de retenção para permitir a demonstração da remoção permanente de dados. 
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Limpar uma tabela de produção com uma retenção curta pode levar à corrupção de dados e/ou à falha de queries de longa execução. Estamos usando essa configuração apenas para fins de demonstração e é preciso ter extremo cuidado ao desativá-la.
-- MAGIC
-- MAGIC O segundo comando define **`spark.databricks.delta.vacuum.logging.enabled`** como **`true`** para garantir que a operação **`VACUUM`** seja registrada no log de transações.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Devido a pequenas diferenças nos protocolos de armazenamento em diversas clouds, o registro em log de comandos **`VACUUM`** não é ativado por default em algumas clouds a partir do DBR 9.1.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- DBTITLE 0,--i18n-04f27ab4-7848-4418-ac79-c339f9843b23
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Antes de excluir permanentemente os arquivos de dados, revise-os manualmente usando a opção **`DRY RUN`**.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb9ce589-09ae-47b8-b6a6-4ab8e4dc70e7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Todos os arquivos de dados que não estão na versão atual da tabela são mostrados na visualização acima.
-- MAGIC
-- MAGIC Execute o comando novamente sem **`DRY RUN`** para excluir permanentemente esses arquivos.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Não há mais nenhuma versão anterior da tabela acessível.
-- MAGIC
-- MAGIC
-- MAGIC VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-1630420a-94f5-43eb-b37c-ccbb46c9ba40
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Como **`VACUUM`** pode ser uma ação bastante destrutiva para datasets importantes, é sempre uma boa ideia reativar a verificação da duração da retenção. Execute a célula abaixo para reativar essa configuração.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- DBTITLE 0,--i18n-8d72840f-49f1-4983-92e4-73845aa98086
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe que a história da tabela indica o usuário que concluiu a operação **`VACUUM`**, o número de arquivos excluídos e registra em log que a verificação de retenção estava desativada durante a operação.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-875b39be-103c-4c70-8a2b-43eaa4a513ee
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Consulte a tabela novamente para confirmar que você ainda tem acesso à versão atual.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdb81194-2e3e-4a00-bfb6-97e822ae9ec3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> Como o Delta Cache armazena cópias de arquivos consultados na sessão atual em volumes de armazenamento implantados no cluster ativo no momento, você ainda pode acessar temporariamente versões anteriores da tabela (embora os sistemas **não** devam ser projetados para esperar esse comportamento). 
-- MAGIC
-- MAGIC Reiniciar o cluster garantirá que esses arquivos de dados armazenados em cache sejam eliminados permanentemente.
-- MAGIC
-- MAGIC Você pode ver um exemplo disso removendo o comentário e executando a célula a seguir, que pode ou não falhar
-- MAGIC (dependendo do estado do cache).

-- COMMAND ----------

--SELECT * FROM beans@v1

-- COMMAND ----------

-- DBTITLE 0,--i18n-a5cfd876-c53a-4d60-96e2-cdbc2b00c19f
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Após concluir este laboratório, você deverá ser capaz de realizar estas tarefas com confiança:
-- MAGIC * Concluir comandos padrão de criação de tabelas e manipulação de dados do Delta Lake
-- MAGIC * Revisar metadados da tabela, incluindo a história da tabela
-- MAGIC * Utilizar o controle de versão do Delta Lake para queries e reversões de snapshots
-- MAGIC * Compactar arquivos pequenos e indexar tabelas
-- MAGIC * Usar **`VACUUM`** para revisar os arquivos marcados para exclusão e confirmar essas exclusões

-- COMMAND ----------

-- DBTITLE 0,--i18n-b541b92b-03a9-4f3c-b41c-fdb0ce4f2271
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
