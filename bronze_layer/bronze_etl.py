# Created by: Bruno Harlis O. Xavier
# Date: 04-05-2024

from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window
from delta import *


def merge_raw_to_bronze(batch_df, batch_id):
    # Deduplicar os registros baseado em uma PK
    window = Window.partitionBy("Id").orderBy(col("_commit_timestamp").desc())
    batch_df = batch_df.withColumn("rank", row_number().over(window)) \
                        .where('rank = 1') \
                        .withColumnRenamed("_change_type", "_operation") \
                        .drop('_fivetran_deleted', '_fivetran_synced', '_commit_version', '_commit_timestamp', 'rank')

    # Carregue a tabela bronze em um dataframe do tipo delta
    delta_df = DeltaTable.forName(spark, "bronze.dbo.customer")

    # Execute o MERGE com Evolution Schema
    delta_df.alias("target") \
        .merge(batch_df.alias("source"), "target.Id = source.Id") \
        .whenMatchedDelete("source._operation = 'delete'") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll("source._operation != 'delete'") \
        .execute()


spark.readStream \
        .option("readChangeData", "true") \
        .table("raw.dbo.customer") \
        .filter(col('_change_type').isin(["insert","delete","update_postimage"]))
    .writeStream \
        .foreachBatch(merge_raw_to_bronze) \
        .option("checkpointLocation", "/save_location/_checkpoint") \
        .trigger(availableNow=True) \
    .start() \
    .awaitTermination()
