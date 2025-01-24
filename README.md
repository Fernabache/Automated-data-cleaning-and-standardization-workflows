# Data Cleaning Pipeline

Simple, flexible data cleaning pipeline for pandas DataFrames.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
import pandas as pd
from src.pipeline import Pipeline

# Configure pipeline
config = {
    'cleaner_config': {
        'missing_strategy': 'drop'
    },
    'validator_rules': {
        'required_columns': ['id', 'name'],
        'unique_columns': ['id'],
        'value_ranges': {
            'age': (0, 120),
            'score': (0, 100)
        }
    }
}

# Create and run pipeline
pipeline = Pipeline(config)
cleaned_data, results = pipeline.run(your_dataframe)
```

## Features

- Automated data cleaning
- Configurable validation rules
- Type inference and conversion
- Missing value handling
- Duplicate removal
- Text standardization

## Running Tests

```bash
pytest tests/
```
