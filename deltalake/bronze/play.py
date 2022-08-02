from dataclasses import dataclass

@dataclass
class BronzeContext:
    
    source_s3_bucket: str = f"s3://somelanding-bucket"
    destination_database_name: str = "iflix_bronze"
    destination_table_name: str = "player"
    destination_s3_bucket : str = f"s3://very-secure-bucket/player/data"
    checkpoint: str = f"s3://very-secure-bucket/player/checkpoint"
    schema: str = f"s3://very-secure-bucket/player/schema"
    
    def __post_init__(self):
        print(self)

class AutoLoad:
    @staticmethod
    def micorbatch_ops(_mdf, _batch_id, ctx: BronzeContext):
        (_mdf.write.format('delta')
                   .mode('append')
                   .option('path', ctx.destination_s3_bucket)
                   .saveAsTable(f'{ctx.destination_database_name}.{ctx.destination_table_name}')
        )
    @staticmethod
    def ReadSource(_spark, ctx: BronzeContext):
        """
        adapted from https://docs.databricks.com/delta/delta-streaming.html

        unfortunately this is an AutoLoader pattern, so we need to split the read/write for unit testing

        Args:
            _spark (sparksession): if using databricks notebook, pass it the `spark` object provided
            ctx (BronzeContext)
        """
        
        return (_spark.readStream
                .format('cloudFiles')
                .option('inferSchema', 'true')
                .option('cloudFiles.inferColumnTypes', 'true')
                .option('cloudFiles.format', 'json')
                .option('cloudFiles.schemaLocation', ctx.schema)
                .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
                .load(ctx.source_s3_bucket)
                )  
                  
    def WriteData(source_stream, ctx: BronzeContext):
        _write = (source_stream.writeStream
                               .option('checkpointLocation', ctx.checkpoint)
                               .foreachBatch(lambda mdf, batch_id: AutoLoad.micorbatch_ops(mdf, batch_id, ctx))
                )

        _query = _write.trigger(once=True).start()
        _query.awaitTermination() 
        


if __name__ == '__main__':
    run_context = BronzeContext()
    
    src_stream = AutoLoad.ReadSource(spark, run_context)
    AutoLoad.WriteData(src_stream)