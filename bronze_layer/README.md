# Criando a Camada Bronze
## Objetivo da Camada Bronze

O objetivo é termos as tabelas na bronze idênticas às que estão na base de dados. 

## Hambientação

Antes de começar a mostrar o código, vou explicar como funciona a ingestão de dados do FiveTran pois é fundamental para entendimento da extratégia abordada. 
Basicamente, o FiveTran faz a ingestão de duas formas, uma em modo incremental, que vai sempre adicionando novos registros ao fim da tabela, nunca apagando ou atualizando. A outra em modo soft delete que funciona quase como uma tabela em um banco relacional em que os registros que sofrem modificações são de fato atualizados, com a diferença em como o delete é feito. Em vez de apagar o registro da tabela, ele atualizado adicionando uma flag informando que esse registro é um registro deletado (_fivetran_delete = true), sendo esse último o método escohido aqui nesse projeto.

Tendo isso em mente, escolhemos criar uma camada adicional na arquitetura medallion chamada Raw onde ficarão nossos dados brutos. Nessa camada o FiveTran comanda. Podem ser feito resync (essa informação é importante), criação de novas tabelas, atualizações, etc. 



## Start

Temos que pegar os registros que estão chegando na camada Raw e passar para a camada Bronze. O processo ETL usando Structure Streaming do Spark é ótimo para isso, mas aqui temos nosso primeiro problema.

~~~python
df = spark.readStream.table("raw.dbo.customer")
~~~~

Estaria tudo bem se o método de ingestão fosse o incremental. O Spark não lida muito bem com atualizações de regristos que já foram lidos por ele, ocasionando um erro. A solução é fazer o streaming usando o CDF (Change Data Feed) da tabela. Para mais detalhes sobre o que é o CDF, sujito a leitura do seguinte artigo: [link do artigo].

~~~python
df = spark.readStream \
          .option("readChangeData", "true") \
          .table("raw.dbo.customer") \
          .filter(col('_change_type').isin(["insert","delete","update_postimage"]))
~~~

Observe que adicionei um filtro no streaming para trazer somente os registros que sejam do tipo _insert_, _delete_ e _update_postimage_, deixando os do tipo _update_preimage_ de lado.

Agora precisamos gravar o resultado do streaming, mas antes é preciso fazer algo muito importante que é a deduplicação dos registros que serão gravados, pois faremos um merge na tabela bronze.

~~~python
def merge_raw_to_bronze(batch_df, batch_id):
    # Deduplicar os registros baseado em uma PK
    window = Window.partitionBy("Id").orderBy(col("_commit_timestamp").desc())
    batch_df = batch_df.withColumn("rank", row_number().over(window)) \
                        .where('rank = 1') \
                        .drop('_fivetran_deleted', '_fivetran_synced', '_change_type', '_commit_version', '_commit_timestamp', 'rank')

    # Execute o MERGE com Evolution Schema
    delta_df.alias("target") \
        .merge(batch_df.alias("source"), "target.Id = source.Id") \
        .whenMatchedDelete("source._operation = 'D'") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll("source._operation != 'D'") \
        .execute()

df.writeStream \
    .foreachBatch(merge_raw_to_bronze) \
    .option("checkpointLocation", "/save_location/_checkpoint") \
    .trigger(availableNow=True) \
.start() \
.awaitTermination()
~~~


