-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-a51f84ef-37b4-4341-a3cc-b85c491339a8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Funções de ordem superior no Spark SQL
-- MAGIC
-- MAGIC As funções de ordem superior no Spark SQL permitem transformar tipos de dados complexos, como objetos do tipo matriz ou mapa, preservando suas estruturas originais. Exemplos:
-- MAGIC - **`FILTER()`** filtra uma matriz usando a função lambda determinada.
-- MAGIC - **`EXIST()`** testa se uma instrução é verdadeira para um ou mais elementos em uma matriz. 
-- MAGIC - **`TRANSFORM()`** usa a função lambda determinada para transformar todos os elementos em uma matriz.
-- MAGIC - **`REDUCE()`** usa duas funções lambda para reduzir os elementos de uma matriz a um único valor, mesclando os elementos em um buffer e aplicando uma função de finalização no buffer final. 
-- MAGIC
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Usar funções de ordem superior para trabalhar com matrizes

-- COMMAND ----------

-- DBTITLE 0,--i18n-b295e5de-82bb-41c0-a470-2d8c6bbacc09
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC Execute a célula a seguir para configurar seu ambiente.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.99

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc1d8e11-d1ff-4aa0-b4e9-3c2703826cd1
-- MAGIC %md
-- MAGIC ## Filter
-- MAGIC Podemos usar a função **`FILTER`** para criar uma coluna que exclui valores de cada matriz com base em uma condição fornecida.  
-- MAGIC Vamos usar esse método para remover os produtos que não são do tamanho king da coluna **`items`** em todos os registros do dataset **`sales`**. 
-- MAGIC
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC
-- MAGIC Na instrução acima:
-- MAGIC - **`FILTER`** : é o nome da função de ordem superior <br>
-- MAGIC - **`items`** : é o nome da matriz de entrada <br>
-- MAGIC - **`i`** : é o nome da variável iteradora. Escolha o nome e use-o na função lambda. Essa variável itera na matriz, alternando cada valor na função, um de cada vez.<br>
-- MAGIC - **`->`** :  indica o início de uma função <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : é a função. A função verifica a presença da letra K maiúscula no final de cada valor. Se a letra for encontrada, o valor será filtrado para a nova coluna **`king_items`**.
-- MAGIC
-- MAGIC **OBSERVAÇÃO:** Você pode escrever um filtro que produza muitas matrizes vazias na coluna criada. Quando isso acontece, pode ser útil usar uma cláusula **`WHERE`** para mostrar apenas valores de matriz não vazios na coluna retornada. 

-- COMMAND ----------

SELECT * FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0

-- COMMAND ----------

-- DBTITLE 0,--i18n-3e2f5be3-1f8b-4a54-9556-dd72c3699a21
-- MAGIC %md
-- MAGIC
-- MAGIC ## Transform
-- MAGIC
-- MAGIC A função de ordem superior **`TRANSFORM()`** pode ser particularmente útil quando você deseja aplicar uma função existente a cada elemento de uma matriz.  
-- MAGIC Vamos aplicar essa função para criar uma nova coluna de matriz chamada **`item_revenues`** transformando os elementos contidos na coluna de matriz **`items`**.
-- MAGIC
-- MAGIC Na query abaixo: **`items`** é o nome da matriz de entrada, **`i`** é o nome da variável iteradora (com o nome que você escolhe e usa na função lambda; ela itera na matriz, alternando cada valor na função, um de cada vez) e **`->`** indica o início de uma função.  

-- COMMAND ----------

SELECT *,
  TRANSFORM (
    items, i -> CAST(i.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-ccfac343-4884-497a-a759-fc14b1666d6b
-- MAGIC %md
-- MAGIC
-- MAGIC A função lambda especificada acima lê o subcampo **`item_revenue_in_usd`** de cada valor, multiplica-o por 100, converte o resultado em um número inteiro que coloca na nova coluna de matriz **`item_revenues`**

-- COMMAND ----------

-- DBTITLE 0,--i18n-9a5d0a06-c033-4541-b06e-4661804bf3c5
-- MAGIC %md
-- MAGIC
-- MAGIC ## Laboratório da função Exists
-- MAGIC Neste passo, você usará a função de ordem superior **`EXISTS`** com dados da tabela **`sales`** para criar as colunas booleanas **`mattress`** e **`pillow`** que indicam se o item adquirido é um colchão ou um travesseiro.
-- MAGIC
-- MAGIC Por exemplo, se **`item_name`** da coluna **`items`** terminar com a string **`"Mattress"`**, o valor da coluna para **`mattress`** deverá ser **`true`**, e o valor para **`pillow`** deverá ser **`false`**. Alguns exemplos de itens e valores resultantes são mostrados a seguir.
-- MAGIC
-- MAGIC |  itens  | colchão | travesseiro |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC
-- MAGIC Consulte a documentação sobre a função <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a>.  
-- MAGIC Você pode usar a expressão de condição **`item_name LIKE "%Mattress"`** para verificar se a string **`item_name`** termina com a palavra "Mattress" (colchão).

-- COMMAND ----------

-- TODO
-- CREATE OR REPLACE TABLE sales_product_flags AS
-- <FILL_IN>
-- EXISTS <FILL_IN>.item_name LIKE "%Mattress"
-- EXISTS <FILL_IN>.item_name LIKE "%Pillow"

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE sales_product_flags AS
SELECT
  items,
  EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
  EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-3dbc22b0-1092-40c9-a6cb-76ed364a4aae
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC A função auxiliar abaixo retornará um erro com uma mensagem informando o que precisa ser alterado caso você não tenha seguido as instruções. Nenhuma saída significa que você concluiu esta etapa.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- DBTITLE 0,--i18n-caed8962-3717-4931-8ed2-910caf97740a
-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar se a tabela foi criada corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", 10510, ['items', 'mattress', 'pillow'])
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 9986, 'num_pillow': 1384}, "There should be 9986 rows where mattress is true, and 1384 where pillow is true"

-- COMMAND ----------

-- DBTITLE 0,--i18n-ffcde68f-163a-4a25-85d1-c5027c664985
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
