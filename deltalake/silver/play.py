from dataclasses import dataclass
import pyspark.sql.functions as F
from delta import DeltaTable
    
@dataclass
class SilverContext:
    
    source_database: str = "iflix_bronze"
    source_table: str = "player"

    destination_database: str = "iflix_silver"
    destination_table: str = "player"
    destination_s3_bucket : str = f"s3://very-secure-silver-bucket/player/data"
    checkpoint: str = f"s3://very-secure-silver-bucket/player/checkpoint"
    schema: str = f"s3://very-secure-silver-bucket/player/schema"
    
    def __post_init__(self):
        print(self)

class MetastoreOps:
    """
    in silver, we should start defining schemas explictly
    """

    def create_delta_table(_spark, ctx: SilverContext):
        _spark.sql(f"""CREATE TABLE IF NOT EXISTS {ctx.destination_database}.{ctx.destination_table}
                            (
                             user_id STRING
                            ,platform STRING
                            ,asset_id INT
                            ,minutes_viewed INT
                            ,seconds_viewed INT
                            ,event_id STRING
                            )
                            USING DELTA
                            LOCATIOn '{ctx.destination_s3_bucket}'
                    """)


class Transformations:

    @staticmethod
    def prefix_user_id(df):
        return (df.withColumn('user_id', F.concat(F.lit('iflix_'), F.col('user_id')))
                  .withColumn('seconds_viewed', F.col('minutes_viewed') * 60)
                )  

    @staticmethod
    def delta_upsert(_spark, _mdf, _batch_id, ctx: SilverContext):
        """
        https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

        merging here ensures that we do not get duplicated events
        """
        targetTable = DeltaTable.forPath(_spark, ctx.destination_s3_bucket)

        _mdf = _mdf.transform(Transformations.prefix_user_id)

        (targetTable.alias('target')
                        .merge(
                             source = _mdf.alias('source')
                            ,condition = 'target.event_id = source.event_id'
                                )
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute())
    
    @staticmethod
    def ReadSource(_spark, ctx: SilverContext):
        return (_spark.readStream
                .format('delta')
                .table(f'{ctx.source_database}.{ctx.source_table}')
                ) 
    
    @staticmethod          
    def PropagateData(_spark, ctx: SilverContext):

        _source_stream = Transformations.ReadSource(_spark, ctx)
        _write = (_source_stream.writeStream
                               .option('checkpointLocation', ctx.checkpoint)
                               .foreachBatch(lambda mdf, batch_id: Transformations.delta_upsert(_spark, mdf, batch_id, ctx))
                )

        _query = _write.trigger(once=True).start()
        _query.awaitTermination()
        


if __name__ == '__main__':
    run_context = SilverContext()
    
    Transformations.PropagateData(spark, run_context)