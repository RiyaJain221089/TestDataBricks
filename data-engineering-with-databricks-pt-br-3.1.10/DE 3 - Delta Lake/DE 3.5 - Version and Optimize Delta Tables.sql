-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d527322-1a21-4a91-bc34-e7957e052a75
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Controle de versão, otimização e limpeza no Delta Lake
-- MAGIC
-- MAGIC Agora que você é capaz de executar tarefas básicas de dados no Delta Lake com segurança, podemos discutir alguns recursos exclusivos do Delta Lake.
-- MAGIC
-- MAGIC Observe que, embora algumas das palavras-chave usadas aqui não façam parte do SQL ANSI padrão, todas as operações do Delta Lake podem ser executadas no Databricks usando SQL
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Usar **`OPTIMIZE`** para compactar arquivos pequenos
-- MAGIC * Usar **`ZORDER`** para indexar tabelas
-- MAGIC * Descrever a estrutura de diretórios dos arquivos do Delta Lake
-- MAGIC * Revisar a história de transações da tabela
-- MAGIC * Consultar e reverter para uma versão anterior da tabela
-- MAGIC * Limpar arquivos de dados obsoletos com **`VACUUM`**
-- MAGIC
-- MAGIC **Recursos**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Otimização do Delta - Documentação do Databricks</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Limpeza do Delta - Documentação do Databricks</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ef1115dd-7242-476a-a929-a16aa09ce9c1
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC A primeira coisa que faremos é executar um script de configuração. Esse script definirá um nome de usuário, um diretório inicial e um esquema com escopo definido para cada usuário.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.2 

-- COMMAND ----------

-- DBTITLE 0,--i18n-b10dbe8f-e936-4ca3-9d1e-8b471c4bc162
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Criando uma tabela Delta com história
-- MAGIC
-- MAGIC Enquanto você espera a execução dessa query, tente identificar o número total de transações sendo executado.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-e932d675-aa26-42a7-9b55-654ac9896dab
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Examinar os detalhes da tabela
-- MAGIC
-- MAGIC O Databricks usa um Hive metastore por default para registrar esquemas, tabelas e views.
-- MAGIC
-- MAGIC Usar **`DESCRIBE EXTENDED`** permite ver metadados importantes na tabela.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be5873-30b3-4e7e-9333-2c1e6f1cbe25
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** é outro comando que permite explorar os metadados da tabela.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd7b24fa-7a2d-4f31-ab7c-fcc28d617d75
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe o campo **`Location`**.
-- MAGIC
-- MAGIC Embora até agora consideramos a tabela apenas como uma entidade relacional dentro de um esquema, uma tabela do Delta Lake é, na verdade, apoiada por uma coleção de arquivos que residem no armazenamento de objetos em cloud.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ff9d64a-f0c4-4ee6-a007-888d4d082abe
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Explorar os arquivos do Delta Lake
-- MAGIC
-- MAGIC Podemos ver os arquivos que apoiam a tabela do Delta Lake usando uma função do Databricks Utilities.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Não é importante no momento saber tudo sobre esses arquivos para trabalhar com o Delta Lake, mas isso ajudará você a entender melhor como a tecnologia é implementada.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-1a84bb11-649d-463b-85ed-0125dc599524
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe que o diretório contém vários arquivos de dados Parquet e um diretório chamado **`_delta_log`**.
-- MAGIC
-- MAGIC Os registros nas tabelas do Delta Lake são armazenados como dados em arquivos Parquet.
-- MAGIC
-- MAGIC As transações para tabelas do Delta Lake são registradas no **`_delta_log`**.
-- MAGIC
-- MAGIC Podemos revisar o **`_delta_log`** para obter mais detalhes.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-dbcbd76a-c740-40be-8893-70e37bd5e0d2
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Cada transação resulta na gravação de um novo arquivo JSON no log de transações do Delta Lake. Aqui podemos ver que há oito transações no total nesta tabela (o Delta Lake tem indexação 0).

-- COMMAND ----------

-- DBTITLE 0,--i18n-101dffc0-260a-4078-97db-cb1de8d705a8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Considerações sobre os arquivos de dados
-- MAGIC
-- MAGIC Acabamos de ver muitos arquivos de dados para o que obviamente é uma tabela bastante pequena.
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** revela mais detalhes sobre a tabela Delta, incluindo o número de arquivos.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-cb630727-afad-4dde-9d71-bcda9e579de9
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Aqui vemos que a tabela contém atualmente quatro arquivos de dados na versão atual. E o que todos os outros arquivos Parquet estão fazendo no diretório de tabelas? 
-- MAGIC
-- MAGIC Em vez de substituir ou excluir imediatamente arquivos contendo dados alterados, o Delta Lake usa o log de transações para indicar se os arquivos são válidos ou não em uma versão atual da tabela.
-- MAGIC
-- MAGIC Neste passo, veremos o log de transações correspondente à declaração **`MERGE`** acima, onde os registros foram inseridos, atualizados e excluídos.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3221b77b-6d57-4654-afc3-dcb9dfa62be8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC A coluna **`add`** contém uma lista de todos os novos arquivos gravados na tabela, e a coluna **`remove`** indica os arquivos que não devem mais fazer parte da tabela.
-- MAGIC
-- MAGIC Quando consultamos uma tabela do Delta Lake, o mecanismo de query usa os logs de transações para identificar todos os arquivos válidos na versão atual e ignora todos os outros arquivos de dados.

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc6dee2e-406c-48b2-9780-74408c93162d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Compactando e indexando arquivos pequenos
-- MAGIC
-- MAGIC Arquivos pequenos podem ser criados por vários motivos. No nosso caso, realizamos uma série de operações onde apenas um ou vários registros foram inseridos.
-- MAGIC
-- MAGIC Os arquivos serão combinados em um tamanho ideal (dimensionado com base no tamanho da tabela) usando o comando **`OPTIMIZE`**.
-- MAGIC
-- MAGIC **`OPTIMIZE`** substituirá os arquivos de dados existentes combinando registros e regravando os resultados.
-- MAGIC
-- MAGIC Ao executar **`OPTIMIZE`**, o usuário pode especificar um ou vários campos para indexação **`ZORDER`**. Embora a matemática específica de Z-order não seja importante, seu uso acelera a recuperação de dados filtrando campos fornecidos e colocando dados com valores semelhantes em arquivos de dados.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- DBTITLE 0,--i18n-5f412c12-88c7-4e43-bda2-60ec5c749b2a
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Como nossos dados são pequenos, **`ZORDER`** não traz nenhum benefício, mas podemos ver todas as métricas que resultam dessa operação.

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad93f7e-4bb1-4051-8b9c-b685164e3b45
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Revisando transações do Delta Lake
-- MAGIC
-- MAGIC Como todas as alterações na tabela do Delta Lake são armazenadas no log de transações, é fácil revisar a <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">história da tabela</a>.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- DBTITLE 0,--i18n-ed297545-7997-4e75-8bf6-0c204a707956
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Como esperado, **`OPTIMIZE`** criou outra versão da tabela, o que significa que a oitava versão é a mais atual.
-- MAGIC
-- MAGIC Lembra-se de todos aqueles arquivos de dados extras que foram marcados como removidos no log de transações? Eles permitem consultar versões anteriores da tabela.
-- MAGIC
-- MAGIC Essas queries de viagem do tempo podem ser realizadas especificando a versão inteira ou um carimbo de data/hora.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Na maioria dos casos, usamos um carimbo de data/hora para recriar dados no momento de interesse. Para esta demonstração, especificaremos a versão, que é determinística (e você pode executar a demonstração no futuro).

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- DBTITLE 0,--i18n-d1d03156-6d88-4d4c-ae8e-ddfe49d957d7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC É importante observar que, na viagem do tempo, não recriamos um estado anterior da tabela desfazendo transações na versão atual. Em vez disso, apenas consultamos todos os arquivos de dados que foram indicados como válidos na versão especificada.

-- COMMAND ----------

-- DBTITLE 0,--i18n-78cf75b0-0403-4aa5-98c7-e3aabbef5d67
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Reverter versões
-- MAGIC
-- MAGIC Imagine que você cria uma query para excluir manualmente alguns registros de uma tabela e acidentalmente a executa no estado seguinte.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-7f7936c3-3aa2-4782-8425-78e6e7634d79
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe que o valor **`-1`** no número de linhas afetadas por uma exclusão indica que um diretório inteiro de dados foi removido.
-- MAGIC
-- MAGIC Vamos confirmar isso abaixo.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-9d3908f4-e6bb-40a6-92dd-d7d12d28a032
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC A exclusão de todos os registros da tabela provavelmente não era o resultado desejado. Felizmente, podemos simplesmente reverter este commit.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- DBTITLE 0,--i18n-902966c3-830a-44db-9e59-dec82b98a9c2
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Observe que um <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">comando</a> **`RESTORE`** é registrado como uma transação. Você não conseguirá esconder completamente o fato de ter excluído acidentalmente todos os registros da tabela, mas poderá desfazer a operação e trazer a tabela de volta ao estado desejado.

-- COMMAND ----------

-- DBTITLE 0,--i18n-847452e6-2668-463b-afdf-52c1f512b8d3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Limpando arquivos obsoletos
-- MAGIC
-- MAGIC O Databricks limpará automaticamente os arquivos de log obsoletos (> 30 dias por default) das tabelas do Delta Lake.
-- MAGIC Cada vez que um ponto de verificação é gravado, o Databricks limpa automaticamente as entradas de log anteriores ao intervalo de retenção.
-- MAGIC
-- MAGIC Embora o controle de versão e a viagem do tempo do Delta Lake sejam ótimos recursos para consultar versões recentes e reverter queries, manter arquivos de dados de todas as versões de grandes tabelas de produção indefinidamente é muito caro (e pode levar a problemas de compliance quando há informações pessoais identificáveis presentes).
-- MAGIC
-- MAGIC Se você quiser limpar manualmente arquivos de dados antigos, poderá usar a operação **`VACUUM`**.
-- MAGIC
-- MAGIC Remova o comentário da célula a seguir e execute-a com uma retenção de **`0 HOURS`** para manter apenas a versão atual:

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3af389e-e93f-433c-8d47-b38f8ded5ecd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Por default, **`VACUUM`** impedirá que você exclua arquivos com menos de sete dias, apenas para garantir que nenhuma operação de longa execução ainda faz referência a um dos arquivos a serem excluídos. Ao executar **`VACUUM`** em uma tabela Delta, você perde a capacidade de viagem do tempo para uma versão anterior ao período de retenção de dados especificado.  Nestas demonstrações, você poderá ver o Databricks executando código que especifica uma retenção de **`0 HOURS`**. Isso é simplesmente para demonstrar o recurso e normalmente não é algo feito em produção.  
-- MAGIC
-- MAGIC Na célula a seguir, vamos:
-- MAGIC 1. Desativar uma verificação para evitar a exclusão prematura de arquivos de dados
-- MAGIC 1. Verificar se o registro em log de comandos **`VACUUM`** está habilitado
-- MAGIC 1. Usar a versão **`DRY RUN`** de limpeza para imprimir todos os registros a serem excluídos

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-7c825ee6-e584-48a1-8d75-f616d7ed53ac
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC A execução de **`VACUUM`** e a exclusão dos dez arquivos acima remove permanentemente o acesso às versões da tabela que exigem a materialização desses arquivos.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-6574e909-c7ee-4b0b-afb8-8bac83dacdd3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Verifique o diretório da tabela para confirmar que os arquivos foram excluídos com sucesso.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3437d5f0-c0e2-4486-8142-413a1849bc40
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
