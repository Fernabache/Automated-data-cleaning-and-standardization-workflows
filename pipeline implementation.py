from typing import Dict, Tuple
import pandas as pd
from .cleaner import DataCleaner
from .validator import DataValidator

class Pipeline:
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.cleaner = DataCleaner(config.get('cleaner_config'))
        self.validator = DataValidator(config.get('validator_rules'))
        
    def run(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Run the complete data cleaning pipeline"""
        results = {
            'validation_passed': False,
            'validation_errors': [],
            'rows_before': len(data),
            'rows_after': 0
        }
        
        # Initial validation
        is_valid, errors = self.validator.validate(data)
        if not is_valid and not self.config.get('force_clean', False):
            results['validation_errors'] = errors
            return data, results
            
        # Clean data
        cleaned_data = self.cleaner.clean(data)
        results['rows_after'] = len(cleaned_data)
        
        # Final validation
        is_valid, errors = self.validator.validate(cleaned_data)
        results['validation_passed'] = is_valid
        results['validation_errors'] = errors
        
        return cleaned_data, results
        
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'Pipeline':
        """Create pipeline instance from YAML config"""
        import yaml
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        return cls(config)
