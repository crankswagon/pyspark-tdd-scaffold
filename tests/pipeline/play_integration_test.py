import pyspark.sql.types as T
from deltalake import silver
from deltalake.bronze.play import BronzeContext, AutoLoad
from deltalake.silver.play import SilverContext, Transformations, MetastoreOps
import pytest
import json

@pytest.fixture(scope="session")
def mock_source_data(tmp_path_factory):
    _tmp_dir = tmp_path_factory.mktemp('landing')
    _seed_data = [{"user_id": "764504178919"
                    ,"platform": "android"
                    ,"asset_id": 13758
                    ,"minutes_viewed": "28"
                    ,"event_id": "f7d93131-c03f-43a7-8420-9da2991e8d39"
                    },
                 {"user_id": "489626272003"
                    ,"platform": "ios"
                    ,"asset_id": 6226
                    ,"minutes_viewed": "7"
                    ,"event_id": "4f026c60-3716-417d-bee7-943a716648bb"
                    }]

    _file = f'{_tmp_dir}/testdata.json'
    with open(_file, 'w') as f:
        f.write('\n'.join([json.dumps(_) for _ in _seed_data]))
    return f'{_tmp_dir}'

@pytest.fixture(scope="session")
def bronze_context(tmp_path_factory, mock_source_data):
    _tmp_dir = tmp_path_factory.mktemp('bronze')
    _ctx = BronzeContext(destination_s3_bucket= f'{_tmp_dir}/data'
                        ,source_s3_bucket= mock_source_data
                        ,schema = f'{_tmp_dir}/schema'
                        ,checkpoint = f'{_tmp_dir}/checkpoint')
    return _ctx
    
@pytest.fixture(scope="session")
def silver_context(tmp_path_factory):
    _tmp_dir = tmp_path_factory.mktemp('silver')
    _ctx = SilverContext(destination_s3_bucket= f'{_tmp_dir}/data'
                        ,schema = f'{_tmp_dir}/schema'
                        ,checkpoint = f'{_tmp_dir}/checkpoint')
    return _ctx


@pytest.fixture(scope="session", autouse=True)
def setup_db(spark, bronze_context: BronzeContext, silver_context: SilverContext):
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {bronze_context.destination_database}')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {silver_context.destination_database}')

    MetastoreOps.create_delta_table(spark, silver_context)
    
    yield
    
    spark.sql(f'DROP TABLE IF EXISTS {bronze_context.destination_database}.{bronze_context.destination_table}')
    spark.sql(f'DROP TABLE IF EXISTS {silver_context.destination_database}.{silver_context.destination_table}')
    spark.sql(f'DROP DATABASE IF EXISTS {bronze_context.destination_database}')
    spark.sql(f'DROP DATABASE IF EXISTS {silver_context.destination_database}')

@pytest.fixture(scope="session")
def infer_bronze_schema(spark, bronze_context: BronzeContext):
    """
    without autoloader, we cannot use `readStream` without definine a schema, so this is a hack to automatically generate the schema
    """
    return (spark.read
          .format("json")
          .load(f'{bronze_context.source_s3_bucket}/testdata.json')
          .schema
          )

def test_destionation_is_clean(spark, silver_context: SilverContext):
    _ = spark.sql(f'select * from {silver_context.destination_database}.{silver_context.destination_table}')

    assert _.count() == 0

def test_pipeline_run(spark, bronze_context: BronzeContext, infer_bronze_schema, silver_context: SilverContext):
    _src_stream = (spark.readStream
                        .schema(infer_bronze_schema)
                        .format("json")
                        .load(bronze_context.source_s3_bucket)
                    )
    AutoLoad.WriteData(_src_stream, bronze_context)

    Transformations.PropagateData(spark, silver_context)

    _ = spark.sql(f'select * from {silver_context.destination_database}.{silver_context.destination_table}')
    
    assert _.count() == 2

def test_user_id_has_prefix(spark, silver_context: SilverContext):
    _ = spark.sql(f"""select user_id from {silver_context.destination_database}.{silver_context.destination_table}
                    """)
    _.show() #displays if test errors

    assert "iflix_764504178919" in list(_.toPandas()['user_id'])
