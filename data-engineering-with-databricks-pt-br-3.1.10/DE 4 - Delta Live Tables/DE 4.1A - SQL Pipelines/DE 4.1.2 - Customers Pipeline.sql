-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ff0a44e1-46a3-4191-b583-5c657053b1af
-- MAGIC %md
-- MAGIC # Sintaxe SQL adicional do DLT
-- MAGIC
-- MAGIC Os pipelines do DLT facilitam a combinação de vários datasets em uma única carga de trabalho escalável usando um ou vários notebooks.
-- MAGIC
-- MAGIC No último notebook, revisamos algumas das funcionalidades básicas da sintaxe DLT durante o processamento de dados do armazenamento de objetos em cloud por meio de uma série de queries para validar e enriquecer registros em cada etapa. Este notebook segue a arquitetura medallion, mas introduz uma série de novos conceitos.
-- MAGIC * Os registros brutos representam informações de captura de dados de alterações (CDC) sobre clientes 
-- MAGIC * A tabela bronze usa novamente o Auto Loader para ingerir dados JSON do armazenamento de objetos em cloud
-- MAGIC * Uma tabela é definida para impor restrições antes de passar os registros para a camada prata
-- MAGIC * **`APPLY CHANGES INTO`** é usado para processar automaticamente dados de CDC na camada prata como uma <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">tabela de dimensões que mudam lentamente (SCD)<a/> de Tipo 1
-- MAGIC * Uma tabela ouro é definida para calcular um agregado da versão atual da tabela de Tipo 1
-- MAGIC * É definida uma view que é unida a tabelas definidas em outro notebook
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC
-- MAGIC Ao final desta lição, os alunos deverão ser capazes de realizar estas tarefas com confiança:
-- MAGIC * Processar dados de CDC com **`APPLY CHANGES INTO`**
-- MAGIC * Declarar views ativas
-- MAGIC * Unir live tables
-- MAGIC * Descrever como os notebooks da biblioteca DLT funcionam juntos em um pipeline
-- MAGIC * Programar vários notebooks em um pipeline do DLT

-- COMMAND ----------

-- DBTITLE 0,--i18n-959211b8-33e7-498b-8da9-771fdfb0978b
-- MAGIC %md
-- MAGIC ## Ingerir dados com o Auto Loader
-- MAGIC
-- MAGIC Como no último notebook, definimos uma tabela bronze a partir de uma fonte de dados configurada com o Auto Loader.
-- MAGIC
-- MAGIC Observe que o código abaixo omite a opção Auto Loader para inferir o esquema. Quando os dados são ingeridos do JSON sem um esquema fornecido ou inferido, os campos terão os nomes corretos, mas todos serão armazenados como tipo **`STRING`**.
-- MAGIC
-- MAGIC O código abaixo também fornece um comentário simples e adiciona campos para o horário da ingestão de dados e o nome do arquivo para cada registro.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_bronze
COMMENT "Raw data from customers CDC feed"
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/customers", "json")

-- COMMAND ----------

-- DBTITLE 0,--i18n-43d96582-8a29-49d0-b57f-0038334f6b88
-- MAGIC %md
-- MAGIC ## Imposição da qualidade continuada
-- MAGIC
-- MAGIC A query abaixo demonstra:
-- MAGIC * As três opções de comportamento quando as restrições são violadas
-- MAGIC * Uma query com múltiplas restrições
-- MAGIC * Múltiplas condições fornecidas para uma restrição
-- MAGIC * O uso de uma função SQL integrada em uma restrição
-- MAGIC
-- MAGIC Sobre a fonte de dados:
-- MAGIC * Os dados são um feed do CDC que contém as operações **`INSERT`**, **`UPDATE`** e **`DELETE`**. 
-- MAGIC * As operações update e insert devem conter entradas válidas para todos os campos.
-- MAGIC * As operações delete devem conter valores **`NULL`** para todos os campos, exceto os campos de carimbo de data/hora, **`customer_id`** e os campos de operação.
-- MAGIC
-- MAGIC Para garantir que apenas dados válidos cheguem à tabela prata, escreveremos várias regras de imposição de qualidade que ignoram os valores nulos esperados nas operações delete.
-- MAGIC
-- MAGIC Descreveremos cada uma dessas restrições abaixo:
-- MAGIC
-- MAGIC ##### **`valid_id`**
-- MAGIC Esta restrição fará a transação falhar se um registro tiver um valor nulo no campo **`customer_id`**.
-- MAGIC
-- MAGIC ##### **`valid_operation`**
-- MAGIC Esta restrição descartará registros com um valor nulo no campo **`operation`**.
-- MAGIC
-- MAGIC ##### **`valid_address`**
-- MAGIC Esta restrição verifica se o campo **`operation`** é **`DELETE`**; em caso negativo, verifica se há valores nulos em qualquer um dos quatro campos que compõem um endereço. Como não há instruções adicionais sobre o que fazer com registros inválidos, as linhas que violarem serão registradas nas métricas, mas não descartadas.
-- MAGIC
-- MAGIC ##### **`valid_email`**
-- MAGIC Esta restrição usa correspondência de padrões regex para verificar se o valor no campo **`email`** é um endereço de email válido. A lógica da restrição define que ela não será aplicada aos registros se o campo **`operation`** for **`DELETE`** (porque o campo **`email`** terá um valor nulo). Os registros que violarem a restrição serão descartados.

-- COMMAND ----------

CREATE STREAMING TABLE customers_bronze_clean
(CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"),
CONSTRAINT valid_address EXPECT (
  (address IS NOT NULL and 
  city IS NOT NULL and 
  state IS NOT NULL and 
  zip_code IS NOT NULL) or
  operation = "DELETE"),
CONSTRAINT valid_email EXPECT (
  rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
  operation = "DELETE") ON VIOLATION DROP ROW)
AS SELECT *
  FROM STREAM(LIVE.customers_bronze)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5766064f-1619-4468-ac05-2176451e11c0
-- MAGIC %md
-- MAGIC ## Processar dados de CDC com **`APPLY CHANGES INTO`**
-- MAGIC
-- MAGIC O DLT introduz uma nova estrutura sintática para simplificar o processamento de feeds do CDC.
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`** tem as seguintes garantias e requisitos:
-- MAGIC * Executa a ingestão incremental/de transmissão de dados de CDC
-- MAGIC * Fornece uma sintaxe simples para especificar um ou mais campos como key primária de uma tabela
-- MAGIC * A suposição default é que as linhas terão inserções e atualizações
-- MAGIC * Opcionalmente pode aplicar exclusões
-- MAGIC * Ordena automaticamente registros de pedidos atrasados usando uma key de sequenciamento fornecida pelo usuário
-- MAGIC * Usa uma sintaxe simples para especificar colunas a serem ignoradas com a palavra-chave **`EXCEPT`**
-- MAGIC * O comportamento default é aplicar alterações como SCD de Tipo 1
-- MAGIC
-- MAGIC O código abaixo:
-- MAGIC * Cria a tabela **`customers_silver`**; **`APPLY CHANGES INTO`** exige que a tabela de destino seja declarada em uma instrução separada
-- MAGIC * Identifica a tabela **`customers_silver`** como o destino no qual as alterações serão aplicadas
-- MAGIC * Especifica a tabela **`customers_bronze_clean`** como fonte de transmissão
-- MAGIC * Identifica **`customer_id`** como a key primária
-- MAGIC * Especifica que os registros onde o campo **`operation`** é **`DELETE`** devem ser aplicados como exclusões
-- MAGIC * Especifica o campo **`timestamp`** para ordenar como as operações devem ser aplicadas
-- MAGIC * Indica que todos os campos devem ser adicionados à tabela de destino, exceto **`operation`**, **`source_file`** e **`_rescued_data`**

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze_clean)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY timestamp
  COLUMNS * EXCEPT (operation, source_file, _rescued_data)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5956effc-3fb5-4473-b2e7-297e5a3e1103
-- MAGIC %md
-- MAGIC ## Consultando tabelas com alterações aplicadas
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`**, por default, cria uma tabela SCD de Tipo 1, o que significa que cada key exclusiva terá no máximo um registro e que as atualizações substituirão as informações originais.
-- MAGIC
-- MAGIC Embora o destino da nossa operação na célula anterior tenha sido definido como uma live table de transmissão, os dados estão sendo atualizados e excluídos da tabela (o que não atende aos requisitos de fontes de live tables de transmissão do tipo somente inserção). Como tal, as operações posteriores não podem realizar queries de transmissão na tabela. 
-- MAGIC
-- MAGIC Esse padrão garante que, se alguma atualização ocorrer fora de ordem, os resultados posteriores poderão ser recalculados adequadamente para refletir as atualizações. Além disso, quando os registros são excluídos de uma tabela de origem, esses valores não serão mais refletidos nas tabelas no pipeline posteriormente.
-- MAGIC
-- MAGIC No passo abaixo, definimos uma query agregada simples para criar uma live table a partir dos dados na tabela **`customers_silver`**.

-- COMMAND ----------

CREATE LIVE TABLE customer_counts_state
  COMMENT "Total active customers per state"
AS SELECT state, count(*) as customer_count, current_timestamp() updated_at
  FROM LIVE.customers_silver
  GROUP BY state

-- COMMAND ----------

-- DBTITLE 0,--i18n-cd430cf6-927e-4a2e-a68d-ba87b9fdf3f6
-- MAGIC %md
-- MAGIC ## Views do DLT
-- MAGIC
-- MAGIC A query abaixo define uma view do DLT substituindo **`TABLE`** pela palavra-chave **`VIEW`**.
-- MAGIC
-- MAGIC As views no DLT diferem das tabelas persistentes e podem ser definidas como **`STREAMING`**.
-- MAGIC
-- MAGIC As views têm as mesmas garantias de atualização que as live tables, mas os resultados das queries não são armazenados em disco.
-- MAGIC
-- MAGIC Ao contrário das views usadas em outros lugares no Databricks, as views do DLT não são persistidas no metastore, ou seja, elas só podem ser referenciadas no pipeline do DLT do qual fazem parte. (Esse é um escopo semelhante às views temporárias na maioria dos sistemas SQL.)
-- MAGIC
-- MAGIC As views ainda podem ser usadas para impor a qualidade dos dados, e as métricas das views são coletadas e relatadas como acontece com as tabelas.
-- MAGIC
-- MAGIC ## Junções e tabelas de referência entre bibliotecas de notebooks
-- MAGIC
-- MAGIC O código que analisamos até agora mostrou dois datasets de origem se propagando por meio de uma série de etapas em notebooks separados.
-- MAGIC
-- MAGIC O DLT oferece suporte à programação de vários notebooks como parte de uma única configuração de pipeline do DLT. Você pode editar pipelines do DLT existentes para adicionar notebooks.
-- MAGIC
-- MAGIC Dentro de um pipeline do DLT, o código em qualquer biblioteca de notebook pode fazer referência a tabelas e views criadas em qualquer outra biblioteca de notebook.
-- MAGIC
-- MAGIC Essencialmente, podemos considerar o escopo da referência do esquema pela palavra-chave **`LIVE`** como estando no nível do pipeline do DLT, e não de um notebook individual.
-- MAGIC
-- MAGIC Na query abaixo, criamos uma nova view juntando as tabelas pratas dos datasets **`orders`** e **`customers`**. Observe que esta view não está definida como transmissão. Assim, o **`email`** válido atual de cada cliente será sempre capturado e os registros dos clientes serão descartados automaticamente depois que forem excluídos da tabela **`customers_silver`**.

-- COMMAND ----------

CREATE LIVE VIEW subscribed_order_emails_v
  AS SELECT a.customer_id, a.order_id, b.email 
    FROM LIVE.orders_silver a
    INNER JOIN LIVE.customers_silver b
    ON a.customer_id = b.customer_id
    WHERE notifications = 'Y'

-- COMMAND ----------

-- DBTITLE 0,--i18n-d4a1dd79-e8e6-4d18-bf00-c5d49cd04b04
-- MAGIC %md
-- MAGIC ## Adicionando este notebook a um pipeline do DLT
-- MAGIC
-- MAGIC Com a UI do DLT, é fácil adicionar bibliotecas de notebook a um pipeline existente.
-- MAGIC
-- MAGIC 1. Navegue até o pipeline do DLT que você configurou anteriormente no curso
-- MAGIC 1. Clique no botão **Configurações** no canto superior direito
-- MAGIC 1. Em **Bibliotecas de notebooks**, clique em **Adicionar biblioteca de notebooks**
-- MAGIC    * Use o seletor de arquivos para selecionar este notebook e clique em **Selecionar**
-- MAGIC 1. Clique no botão **Salvar** para salvar as atualizações
-- MAGIC 1. Clique no botão azul **Começar** no canto superior direito da tela para atualizar o pipeline e processar novos registros
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> O link para este notebook pode ser encontrado em [DE 4.1 - Passo a passo da IU do DLT]($../DE 4.1 - Passo a passo da IU do DLT)<br/>
-- MAGIC nas instruções impressas da **Tarefa 2** na seção **Gerar a configuração do pipeline**

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c0ccf2a-333a-4d7c-bfef-b4b25e56b3ca
-- MAGIC %md
-- MAGIC ## Resumo
-- MAGIC
-- MAGIC Ao revisar este caderno, você agora deve se sentir confortável:
-- MAGIC *Processando dados do CDC com**`APPLY CHANGES INTO`**
-- MAGIC *Declarando visualizações ao vivo
-- MAGIC *Juntando-se a mesas ao vivo
-- MAGIC *Descrevendo como os notebooks da biblioteca DLT funcionam juntos em um pipeline
-- MAGIC *Agendando vários notebooks em um pipeline DLT
-- MAGIC
-- MAGIC No próximo notebook, vamos analisar a saída do pipeline. Em seguida, veremos como desenvolver iterativamente e solucionar problemas de código do DLT.
