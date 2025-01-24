import pytest
import pandas as pd
import numpy as np
from src.cleaner import DataCleaner
from src.validator import DataValidator
from src.pipeline import Pipeline

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'id': [1, 2, 2, 3],
        'name': ['Test ', 'test', 'Sample', np.nan],
        'value': ['10', '20', '30', 'invalid'],
        'date': ['2024-01-01', '2024-01-02', 'invalid', np.nan]
    })

def test_cleaner(sample_data):
    cleaner = DataCleaner({'missing_strategy': 'drop'})
    cleaned = cleaner.clean(sample_data)
    
    assert len(cleaned) < len(sample_data)  # Duplicates removed
    assert cleaned['name'].str.contains(' ').sum() == 0  # No extra spaces
    assert cleaned['value'].dtype in ['int64', 'float64']  # Numeric conversion
    assert pd.api.types.is_datetime64_any_dtype(cleaned['date'])  # Date conversion

def test_validator(sample_data):
    rules = {
        'required_columns': ['id', 'name', 'value'],
        'unique_columns': ['id'],
        'value_ranges': {'value': (0, 100)}
    }
    
    validator = DataValidator(rules)
    is_valid, errors = validator.validate(sample_data)
    
    assert not is_valid  # Should fail due to duplicates
    assert any('duplicate' in error for error in errors)

def test_pipeline(sample_data):
    config = {
        'cleaner_config': {'missing_strategy': 'drop'},
        'validator_rules': {
            'required_columns': ['id', 'name'],
            'unique_columns': ['id']
        }
    }
    
    pipeline = Pipeline(config)
    cleaned_data, results = pipeline.run(sample_data)
    
    assert results['rows_before'] > results['rows_after']
    assert cleaned_data['id'].is_unique
