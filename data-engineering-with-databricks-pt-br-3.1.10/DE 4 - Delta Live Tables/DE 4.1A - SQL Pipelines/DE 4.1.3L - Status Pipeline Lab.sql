-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-54a6a2f6-5787-4d60-aea6-e39627954c96
-- MAGIC %md
-- MAGIC # Solução de problemas da sintaxe SQL do DLT
-- MAGIC
-- MAGIC Agora que concluímos o processo de configuração e execução de um pipeline com dois notebooks, simularemos o desenvolvimento e a adição de um terceiro notebook.
-- MAGIC
-- MAGIC **NÃO ENTRE EM PÂNICO!** Encontraremos alguns problemas.
-- MAGIC
-- MAGIC O código fornecido abaixo contém pequenos erros intencionais de sintaxe. Ao solucionar esses erros, você aprenderá como desenvolver código DLT de forma iterativa e identificar erros de sintaxe.
-- MAGIC
-- MAGIC Esta lição não pretende fornecer uma solução robusta para desenvolvimento e teste de código; seu objetivo é ajudar os usuários a começar a usar o DLT e a lidar com uma sintaxe desconhecida.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, os alunos deverão se sentir confortáveis:
-- MAGIC * Identificar e solucionar problemas de sintaxe do DLT 
-- MAGIC * Desenvolver iterativamente pipelines do DLT com notebooks

-- COMMAND ----------

-- DBTITLE 0,--i18n-dee15416-56fd-48d7-ae3c-126175503a9b
-- MAGIC %md
-- MAGIC ## Adicionar este notebook a um pipeline do DLT
-- MAGIC
-- MAGIC Neste ponto do curso, você deve ter um pipeline do DLT configurado com duas bibliotecas de notebook.
-- MAGIC
-- MAGIC Você provavelmente processou vários lotes de registros por esse pipeline e deve saber como acionar uma nova execução do pipeline e adicionar uma biblioteca.
-- MAGIC
-- MAGIC Para começar a lição, siga o processo de adição do notebook ao pipeline usando a UI do DLT e, em seguida, acione uma atualização.
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png">O link para este notebook pode ser encontrado em [DE 4.1- Passo a passo da UI DLT]($../DE 4.1- DLT UI Walkthrough)<br/>
-- MAGIC nas instruções impressas da **Tarefa 3** na seção **Gerar a configuração do pipeline**

-- COMMAND ----------

-- DBTITLE 0,--i18n-96beb691-44d9-4871-9fa5-fcccc3e13616
-- MAGIC %md
-- MAGIC ## Solução de erros
-- MAGIC
-- MAGIC Cada uma das três queries abaixo contém um erro de sintaxe, e cada erro será detectado e relatado de maneira ligeiramente diferente pelo DLT.
-- MAGIC
-- MAGIC Alguns erros de sintaxe são detectados durante o estágio de **inicialização**, quando o DLT não consegue analisar corretamente os comandos.
-- MAGIC
-- MAGIC Outros erros de sintaxe são detectados durante o estágio de **configuração de tabelas**.
-- MAGIC
-- MAGIC Observe que, devido à maneira como o DLT resolve a ordem das tabelas no pipeline em passos diferentes, às vezes os erros podem ser gerados primeiro em estágios posteriores.
-- MAGIC
-- MAGIC Uma abordagem que pode funcionar é corrigir uma tabela por vez, começando pelo dataset mais antigo e continuando até o final. Comentários no código serão ignorados automaticamente. Assim, você pode comentar o código em uma execução de desenvolvimento com segurança sem removê-lo completamente.
-- MAGIC
-- MAGIC Mesmo que você consiga identificar imediatamente os erros no código abaixo, tente usar as mensagens de erro da UI como um guia. O código da solução é mostrado na célula abaixo.

-- COMMAND ----------

-- TODO
-- CREATE OR REFRESH STREAMING TABLE status_bronze
-- AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
-- FROM cloud_files("${source}/status", "json");

-- CREATE OR REFRESH STREAMING LIVE TABLE status_silver
-- (CONSTRAINT valid_timestamp EXPECT (status_timestamp > 1640995200) ON VIOLATION DROP ROW)
-- AS SELECT * EXCEPT (source_file, _rescued_data)
-- FROM LIVE.status_bronze;

-- CREATE OR REFRESH LIVE TABLE email_updates
-- AS SELECT a.*, b.email
-- FROM status_silver a
-- INNER JOIN subscribed_order_emails_v b
-- ON a.order_id = b.order_id;

-- COMMAND ----------

-- DBTITLE 0,--i18n-492901e2-38fe-4a8c-a05d-87b9dedd775f
-- MAGIC %md
-- MAGIC ## Soluções
-- MAGIC
-- MAGIC A sintaxe correta para cada uma das funções acima é fornecida em um notebook com o mesmo nome na pasta Solutions.
-- MAGIC
-- MAGIC Há várias opções para lidar com esses erros:
-- MAGIC * Revise cada item, corrigindo você mesmo os problemas acima
-- MAGIC * Copie e cole a solução na célula **`# ANSWER`** do notebook de soluções com o mesmo nome
-- MAGIC * Atualize o pipeline para usar diretamente o notebook de soluções com o mesmo nome
-- MAGIC
-- MAGIC Os problemas em cada query:
-- MAGIC 1. A palavra-chave **`LIVE`** está faltando na instrução create
-- MAGIC 1. A palavra-chave **`STREAM`** está faltando na cláusula from
-- MAGIC 1. A palavra-chave **`LIVE`** está faltando na tabela referenciada pela cláusula from

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH STREAMING TABLE status_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/status", "json");

CREATE OR REFRESH STREAMING TABLE status_silver
(CONSTRAINT valid_timestamp EXPECT (status_timestamp > 1640995200) ON VIOLATION DROP ROW)
AS SELECT * EXCEPT (source_file, _rescued_data)
FROM STREAM(LIVE.status_bronze);

CREATE OR REFRESH LIVE TABLE email_updates
AS SELECT a.*, b.email
FROM LIVE.status_silver a
INNER JOIN LIVE.subscribed_order_emails_v b
ON a.order_id = b.order_id;

-- COMMAND ----------

-- DBTITLE 0,--i18n-54e251d6-b8f8-45c2-82df-60e22b127135
-- MAGIC %md
-- MAGIC ## Resumo
-- MAGIC
-- MAGIC Ao revisar este caderno, você agora deve se sentir confortável:
-- MAGIC *Identificando e solucionando problemas de sintaxe DLT 
-- MAGIC *Desenvolvimento iterativo de pipelines DLT com notebooks
