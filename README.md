# pyspark-tdd-scaffold
example project demonstrating test driven pyspark delta stream development


# Get started

## Environment management

```
conda create -n pyspark python=3.10 poetry=1.1.13 openjdk=11

conda activate pyspark
```

## Package management
```
poetry install -vvv
```

## Reference dataset

basing off of the iFlix dataset here (just referencing the data shape)

https://www.kaggle.com/datasets/aungpyaeap/movie-streaming-datasets-iflix


## Run Tests

```
poetry run pytest -rx tests/bronze/play_test.py
```