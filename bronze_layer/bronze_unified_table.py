from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit, when, row_number, current_timestamp, col, trim, coalesce
from pyspark.sql.window import Window
from delta import *
import datetime

from sys import argv


# Config do spark para não processar lotes que não contenham dados
spark.conf.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")

# Config de evolução de schema na delta table
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


# Variáveis que são definidas no momento de executar o job que estiver ligado a este script
SAVE_LOCATION_PATH = "/checkpoints"
SOURCE_CATALOG_NAME = argv[1] # source catalog name
TARGET_CATALOG_NAME = argv[2] # target catalog name
TABLE_NAME = argv[3] # table name



class CDC_table:
    """
    Class to represent a CDC-based processing for a given table.
    """
    def __init__(self, source_catalog_name, target_catalog_name, schema_name, table_name, columns_id_cloud, merge_condition, pk_dict: dict, fk_dict: dict):
        """
        Constructor for the CDC_table class.
        """
        self.source_catalog_name = source_catalog_name
        self.target_catalog_name = target_catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns_id_cloud = columns_id_cloud
        self.merge_condition = merge_condition
        self.pk_dict = pk_dict
        self.fk_dict = fk_dict


    def merge_raw_to_bronze(self, batch_df, batch_id):
        """
        This function applies the merge (update/insert/delete) operation on the bronze table.
        """

        pk_columns_list = []
        # Add PK columns to a list
        for key, value in self.pk_dict.items():
            pk_columns_list.append(key)

        # Deduplicate rows based on PK list
        window = Window.partitionBy(pk_columns_list).orderBy(col("_commit_timestamp").desc())
        batch_df = batch_df.withColumn("rank", row_number().over(window)).where("rank = 1")

        batch_df = batch_df.withColumn("_operation", when(batch_df._fivetran_deleted == 1, lit("D")) \
                                    .when(batch_df._fivetran_deleted == True, lit("D")) \
                                    .when(batch_df._change_type == 'delete', lit('D')) \
                                    .when((batch_df._change_type == "update_postimage") & (batch_df._fivetran_deleted == 0), lit("U")) \
                                    .when((batch_df._change_type == "update_postimage") & (batch_df._fivetran_deleted == False), lit("U")) \
                                    .when(batch_df._change_type == "insert", lit("I")) \
                                    .otherwise("NULL")) \
                                .withColumn("_bronzeUpdateDate", current_timestamp()) \
                                .withColumn('_source', lit(f'{self.source_catalog_name}.{self.schema_name}')) \
                                .drop('_fivetran_deleted', '_fivetran_synced', '_change_type', '_commit_version', '_commit_timestamp', 'rank')
        

        # Test if TARGET table exists
        try:
            # Delta DataFrame 
            delta_df = DeltaTable.forName(spark, f'{self.target_catalog_name}.dbo.{self.table_name}')
        except Exception as e:
            # Create TARGET table
            
            # ----------------------------- Metadados da Tabela -----------------------------
            schema = spark.table(f'{self.source_catalog_name}.{self.schema_name}.{self.table_name}').schema

            # List of columns that will be used to create the destination table
            lista_colunas = [] 

            # Clean up metadata
            for coluna in schema:
                coluna = str(coluna)
                lista_colunas.append(
                    coluna[13:].replace("DecimalType", "Decimal")
                                        .replace("',", "")
                                        .replace("Type()", "")
                                        .replace(", True)", "")
                                        .replace(", False)", "")
                )

            lista_colunas.remove('_fivetran_synced Timestamp')

            try:
                lista_colunas.remove('_fivetran_deleted Byte')
            except ValueError:
                lista_colunas.remove('_fivetran_deleted Boolean')
            # ----------------------------------------------------------------------------------


            # ----------------------- Create columns from target table ---------------------
            # dictionary containing the table metadata
            dict_type_columns = dict(subString.split(" ") for subString in lista_colunas)

            # list of Primary Kkey + Foreign Key columns
            ids = {}
            ids.update(self.pk_dict)
            ids.update(self.fk_dict)

            # variable containing the order of columns to create the destination table 
            str_columns = '_source STRING,'
            
            # First loop to order key columns first
            for column, value in ids.items():
                str_columns += column + ' ' + dict_type_columns[column] + ','
                dict_type_columns.pop(column)
            
            # Second loop to order normal columns
            for key, value in dict_type_columns.items():
                str_columns += key + ' ' + value + ','

            str_columns = str_columns[:-1]

            # Create the destination table
            """
            The schema name "aasi_net_dbo" is not being created dynamically and therefore its creation is being created manually
            """
            spark.sql(f"""
                CREATE TABLE {self.target_catalog_name}.dbo.{self.table_name}
                (
                    {self.columns_id_cloud}
                    {str_columns}
                )
                TBLPROPERTIES (
                    delta.enableDeletionVectors = true,
                    delta.enableChangeDataFeed = true, 
                    delta.autoOptimize.autoCompact = true,
                    delta.autoOptimize.optimizeWrite = true,
                    delta.columnMapping.mode = 'name',
                    delta.minReaderVersion = '3',
                    delta.minWriterVersion = '7',
                    delta.feature.allowColumnDefaults = 'supported'
                )
            """)
            
            delta_df = DeltaTable.forName(spark, f'{self.target_catalog_name}.dbo.{self.table_name}')
            # ----------------------------------------------------------------------------------


        # Execute the MERGE operation
        delta_df.alias("target") \
            .merge(batch_df.alias("source"), self.merge_condition) \
            .whenMatchedDelete("source._operation = 'D'") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll("source._operation != 'D'") \
            .execute()



# Function that returns a list of all schemas that contain a specific table
"""
The "table_schema LIKE 'aasi_net%_dbo'" condition it is not being created dynamically and is currently being defined manually.
"""
def table_in_schema(table_name):
    schemas_df = spark.sql(f"""
        SELECT      table_schema
        FROM        {SOURCE_CATALOG_NAME}.information_schema.tables t
        INNER JOIN  {SOURCE_CATALOG_NAME}.information_schema.schema_tags st     ON st.schema_name = t.table_schema
        WHERE       t.table_name = '{table_name}' AND st.tag_name = 'product'   AND st.tag_value = ''
        ORDER BY    table_schema     ASC
    """)

    schemas_list = [row['table_schema'] for row in schemas_df.collect()]
    return schemas_list



def get_pk_and_fk(table_name):
    """
    Get Primary Keys (PK) and Foreign Keys (FK) from a data dictionary.
    """
    data_dict_aasi = spark.table(f'{SOURCE_CATALOG_NAME}.default.data_dictionary')
    pk_table = data_dict_aasi.filter((data_dict_aasi.Destination_schema == 'NULL') & \
                            (data_dict_aasi.TabelaNome == table_name) & \
                            (data_dict_aasi.IsPK == 'PK'))

    # Filter FK columns and exclude those that are also PK columns
    fk_table = data_dict_aasi.filter((data_dict_aasi.Destination_schema == 'NULL') & \
                            (data_dict_aasi.TabelaNome == table_name) & \
                            (trim(data_dict_aasi.FKRef) != '') & \
                            (coalesce(data_dict_aasi['IsPK'], lit('0')) == '0')) 

    pk_dict = {row['ColunaNome'].lower(): [row['IsPK'], row['FKRef'], row['ColunaTipo'].lower()] for row in pk_table.collect()}
    fk_dict = {row['ColunaNome'].lower(): [row['IsPK'], row['FKRef'], row['ColunaTipo'].lower()] for row in fk_table.collect()}
    
    return pk_dict, fk_dict



def get_merge_condition(pk_dict, fk_dict):
    """
    Generate a merge condition based on the keys; create hash columns for the primary and foreign keys if needed.
    """
    columns_id_cloud = ''
    merge_condition = ''

    for key, value in pk_dict.items():
        # Create a_cloud column of bigint auto-increment for when the table has only a PK and the same is not a FK
        if len(pk_dict) == 1 and not value[1]:
            columns_id_cloud += key + '_cloud BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),'
        
        # Generate a merge condition
        merge_condition += 'target.' + key + ' = source.' + key + ' AND '
    

    merge_condition += 'target._source = source._source'

    return columns_id_cloud, merge_condition


# Begin function calls
pk_dict, fk_dict = get_pk_and_fk(TABLE_NAME)
columns_id_cloud, merge_condition = get_merge_condition(pk_dict, fk_dict)
schemas_list = table_in_schema(TABLE_NAME)

linha_integridade = []

for schema_name in schemas_list:
    print(f'{datetime.datetime.now()} - WARNING - Start Stream: {SOURCE_CATALOG_NAME}.{schema_name}.{TABLE_NAME}')

    try:
        table_obj = CDC_table(SOURCE_CATALOG_NAME, TARGET_CATALOG_NAME, schema_name, TABLE_NAME, columns_id_cloud, merge_condition, pk_dict, fk_dict)

        (
            spark.readStream
                .option("readChangeData", "true")
                .table(f'{SOURCE_CATALOG_NAME}.{schema_name}.{TABLE_NAME}')
                .filter(col('_change_type').isin(["insert","delete","update_postimage"]))
            .writeStream
                .foreachBatch(table_obj.merge_raw_to_bronze)
                .option('checkpointLocation', f'{SAVE_LOCATION_PATH}/unified/{SOURCE_CATALOG_NAME}/{schema_name}/{TABLE_NAME}/')
                .trigger(availableNow = True)
            .start()
            .awaitTermination()
        )

        # ----------------------------------------------------------- INTEGRITY CHECK ------------------------------------------------------------
        try:
            qtd_bronze = spark.table(f'{TARGET_CATALOG_NAME}.`{schema_name}`.`{TABLE_NAME}`').count()
            qtd_raw = spark.table(f'{SOURCE_CATALOG_NAME}.`{schema_name}`.`{TABLE_NAME}`').filter(col('_fivetran_deleted') == False).count()

            linha_integridade.append((schema_name, TABLE_NAME, qtd_raw, qtd_bronze))
        except:
            linha_integridade.append((schema_name, TABLE_NAME, 0, 0))
        # -------------------------------------------------------------------------------------------------------------------------------------------

    except Exception as e:
        type_erro = str(e)
        cdf_error = True if 'delta.enableChangeDataFeed=true' in type_erro else False
        checkpoint_error = True if 'delete your streaming query checkpoint' in type_erro else False

        if cdf_error:
            print(f'{datetime.datetime.now()} - WARNING - Solving CDF Error')
            # Habilitar o CDF da tabela com erro
            spark.sql(f"""
                    ALTER TABLE {SOURCE_CATALOG_NAME}.{schema_name}.{TABLE_NAME}
                    SET TBLPROPERTIES (
                        delta.enableChangeDataFeed = true,
                        delta.autoOptimize.autoCompact=true,
                        delta.autoOptimize.optimizeWrite=true
                    )
            """)
            
            # Remover possíveis checkpoints da antiga tabela 
            dbutils.fs.rm(f"{SAVE_LOCATION_PATH}/unified/{SOURCE_CATALOG_NAME}/{schema_name}/{TABLE_NAME}/", True)
        elif checkpoint_error:
            print(f'{datetime.datetime.now()} - WARNING - Solving Checkpoint Error')
            dbutils.fs.rm(f"{SAVE_LOCATION_PATH}/unified/{SOURCE_CATALOG_NAME}/{schema_name}/{TABLE_NAME}/", True)

            merge_condition = merge_condition[:-36]

            spark.sql(f"""
                    DELETE FROM {TARGET_CATALOG_NAME}.{schema_name}.{TABLE_NAME} AS target
                    WHERE NOT EXISTS (SELECT * FROM {SOURCE_CATALOG_NAME}.{schema_name}.{TABLE_NAME} AS source
                                    WHERE {merge_condition}) AND target._source = '{SOURCE_CATALOG_NAME}.{schema_name}'
            """)
        else:
            assert False, f"ERROR - {datetime.datetime.now()} - Error Description: {e}"
    
    
    print(f'{datetime.datetime.now()} - WARNING - The Ende Stream: {SOURCE_CATALOG_NAME}.{schema_name}.{TABLE_NAME}')

# Save integrity table
schema_integridade = StructType([
                StructField('nome_schema', StringType(), True),
                StructField('nome_table', StringType(), True),
                StructField('qtd_raw', IntegerType(), True),
                StructField('qtd_bronze', IntegerType(), True)
                ])
print(f'{datetime.datetime.now()} - WARNING - Start Integrity Table: {TARGET_CATALOG_NAME}.default.integridade_{schema_name}_{TABLE_NAME}')

df_integridade = spark.createDataFrame(linha_integridade, schema_integridade)
df_integridade.write.mode('overwrite').saveAsTable(f'{TARGET_CATALOG_NAME}.default.integridade_{TABLE_NAME}')

print(f'{datetime.datetime.now()} - WARNING - The End Integrity Table: {TARGET_CATALOG_NAME}.default.integridade_{schema_name}_{TABLE_NAME}')
