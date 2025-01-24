import pandas as pd
from typing import Dict, List, Tuple

class DataValidator:
    def __init__(self, rules: Dict = None):
        self.rules = rules or {}
        
    def validate(self, data: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate dataframe against defined rules"""
        errors = []
        
        if self.rules.get('required_columns'):
            errors.extend(self._check_required_columns(data))
            
        if self.rules.get('value_ranges'):
            errors.extend(self._check_value_ranges(data))
            
        if self.rules.get('unique_columns'):
            errors.extend(self._check_unique_columns(data))
            
        return len(errors) == 0, errors
    
    def _check_required_columns(self, df: pd.DataFrame) -> List[str]:
        """Validate required columns are present"""
        errors = []
        required = set(self.rules['required_columns'])
        missing = required - set(df.columns)
        
        if missing:
            errors.append(f"Missing required columns: {', '.join(missing)}")
            
        return errors
    
    def _check_value_ranges(self, df: pd.DataFrame) -> List[str]:
        """Validate numeric columns are within specified ranges"""
        errors = []
        ranges = self.rules.get('value_ranges', {})
        
        for col, (min_val, max_val) in ranges.items():
            if col not in df.columns:
                continue
                
            out_of_range = df[
                (df[col] < min_val) | (df[col] > max_val)
            ]
            
            if not out_of_range.empty:
                errors.append(
                    f"Column {col} has {len(out_of_range)} values outside range "
                    f"[{min_val}, {max_val}]"
                )
                
        return errors
    
    def _check_unique_columns(self, df: pd.DataFrame) -> List[str]:
        """Validate uniqueness constraints"""
        errors = []
        unique_cols = self.rules.get('unique_columns', [])
        
        for col in unique_cols:
            if col not in df.columns:
                continue
                
            duplicates = df[df[col].duplicated()]
            if not duplicates.empty:
                errors.append(
                    f"Column {col} has {len(duplicates)} duplicate values"
                )
                
        return errors
