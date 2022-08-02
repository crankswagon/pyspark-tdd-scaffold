import pytest
from pyspark.sql.session import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark():
    """
    refer to https://docs.delta.io/latest/delta-batch.html#-sql-support
    this fixture provides a spark session to our tests
    """
    _builder = (configure_spark_with_delta_pip(SparkSession.builder
                         .appName('unit_test')
                         .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                         )
                )
    return _builder.getOrCreate()
