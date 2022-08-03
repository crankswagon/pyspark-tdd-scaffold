# pyspark-tdd-scaffold
example project demonstrating test driven pyspark delta stream development

we are using a deltalake [medallion architecture](https://databricks.com/it/glossary/medallion-architecture) in this example, but the considerations and benefits apply more generally

## Project Layout

each sub directory contains an `__init__.py`, which configures them as packages to facilitate importing and [test scope isolation](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html)

`deltalake` contains the processing code
`tests` contains the tests

```
.
├── deltalake
│   ├── bronze
│   │   └── *.py
│   ├── silver
│   │   └── *.py
│   └── gold
│   │   └── *.py
└── tests
    ├── bronze
    │   └── *_test.py    
    ├── silver 
    │   └── *_test.py
    ├── gold 
    │   └── *_test.py          
    └── pipeline
        └── *_integration_test.py    

```
## Considerations

### Unit Testing

*code should be modularised so that new functionality can be validated by `unit tests` in isolation*

- easy for someone unfamiliar with project/repo to start contributing indepedently
- easy to verify test cases against specification
- pytest `fixture` scopes should be set to `module`
  
### Integration Testing

*repo should be logically laid out so that pipeline can be validated end-to-end by `integration tests`*

- easy for reviewers to rule out any regression in functionality
- pytest `fixture` scopes should be set to `session`
  
## Environment management

We use `conda` to manage environmental depdendencies to simulate our runtime environments as closely as possible across multiple languages. Explicitly definining `java` version here will save you from obscure conflicts later with `pyspark`.

```
conda create -n pyspark python=3.10 poetry=1.1.13 openjdk=11

conda activate pyspark
```

## Package management



```
poetry install -vvv
```

## Reference dataset

loosely basing off of the iFlix dataset here (just referencing the data shape)

https://www.kaggle.com/datasets/aungpyaeap/movie-streaming-datasets-iflix


## Run Tests

Run a single test
```
poetry run pytest -rx tests/bronze/play_test.py
```

Run all the tests
```
poetry run pytest -rx tests/
```