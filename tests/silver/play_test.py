import pyspark.sql.types as T
from deltalake.silver.play import SilverContext, Transformations, MetastoreOps
import pytest
from chispa.dataframe_comparer import assert_df_equality
import shutil

@pytest.fixture(scope="module")
def silver_context(tmp_path_factory):
    _tmp_dir = tmp_path_factory.mktemp('silver')
    _ctx = SilverContext(destination_s3_bucket= f'{_tmp_dir}/data'
                        ,schema = f'{_tmp_dir}/schema'
                        ,checkpoint = f'{_tmp_dir}/checkpoint')
    return _ctx


@pytest.fixture(scope="module", autouse=True)
def setup_silver_source(spark, silver_context: SilverContext):
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {silver_context.source_database}')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {silver_context.destination_database}')
    
    # create source table and seed it with data

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {silver_context.source_database}.{silver_context.source_table}
                    (
                     user_id STRING
                    ,platform STRING
                    ,asset_id INT
                    ,minutes_viewed INT
                    ,event_id STRING
                    )
                    USING DELTA
               """)

    spark.sql(f"""INSERT INTO {silver_context.source_database}.{silver_context.source_table} 
                    VALUES
                    ("764504178919"
                        ,"android"
                        ,13758
                        ,"28"
                        ,"f7d93131-c03f-43a7-8420-9da2991e8d39"
                    )
                    ,("489626272003"
                        ,"ios"
                        ,6226
                        ,"7"
                        ,"4f026c60-3716-417d-bee7-943a716648bb"
                    )
               """)

    # create target table

    MetastoreOps.create_delta_table(spark, silver_context)

    yield

    spark.sql(f'DROP TABLE IF EXISTS {silver_context.destination_database}.{silver_context.destination_table}')
    spark.sql(f'DROP TABLE IF EXISTS {silver_context.source_database}.{silver_context.source_table}')
    spark.sql(f'DROP DATABASE IF EXISTS {silver_context.destination_database}')
    spark.sql(f'DROP DATABASE IF EXISTS {silver_context.source_database}')
    
    shutil.rmtree(silver_context.destination_s3_bucket)


def test_source_data_init(spark, silver_context: SilverContext):
    src_tbl = spark.sql(f'select * from {silver_context.source_database}.{silver_context.source_table}')
    tar_tbl = spark.sql(f'select * from {silver_context.destination_database}.{silver_context.destination_table}')
    assert src_tbl.count() == 2
    assert tar_tbl.count() == 0

def test_silver_stream(spark, silver_context: SilverContext):
    
    _ = Transformations.ReadSource(spark, silver_context)
    Transformations.WriteData(spark, _, silver_context)
    tar_tbl =  spark.sql(f'select * from {silver_context.destination_database}.{silver_context.destination_table}')
    
    assert tar_tbl.count() == 2


def test_user_prefix(spark, silver_context: SilverContext):
    result = spark.sql(f'select * from {silver_context.destination_database}.{silver_context.destination_table}')
    result.select("user_id").show() ## this will display if test fails

    assert result.filter("user_id like 'iflix%'").count() == 2

def test_dupes_do_not_propagate(spark, silver_context: SilverContext):
    
    spark.sql(f"""INSERT INTO {silver_context.source_database}.{silver_context.source_table} 
                VALUES
                ("764504178919"
                    ,"android"
                    ,13758
                    ,"28"
                    ,"f7d93131-c03f-43a7-8420-9da2991e8d39"
                )
                ,("489626272003"
                    ,"ios"
                    ,6226
                    ,"7"
                    ,"4f026c60-3716-417d-bee7-943a716648bb"
                )
            """)
    _ = Transformations.ReadSource(spark, silver_context)
    Transformations.WriteData(spark, _, silver_context)

    src = spark.sql(f'select * from {silver_context.source_database}.{silver_context.source_table}')
    tar = spark.sql(f'select * from {silver_context.destination_database}.{silver_context.destination_table}')

    assert src.count() == 4
    assert tar.count() == 2


def test_seconds_conversion(spark, silver_context: SilverContext):
    
    expected = spark.sql(f""" select 1680 as seconds_viewed 
                                    union
                              select 420 as seconds_viewed
                          """)
    _ = Transformations.ReadSource(spark, silver_context)
    Transformations.WriteData(spark, _, silver_context)

    actual = spark.sql(f'select seconds_viewed from {silver_context.destination_database}.{silver_context.destination_table}')

    assert_df_equality(expected, actual, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)