import pandas as pd
import numpy as np
from typing import Union, List, Dict

class DataCleaner:
    def __init__(self, config: Dict = None):
        self.config = config or {}
        
    def clean(self, data: Union[pd.DataFrame, pd.Series]) -> pd.DataFrame:
        """Main cleaning method that applies all cleaning steps"""
        if isinstance(data, pd.Series):
            data = pd.DataFrame(data)
            
        df = data.copy()
        df = self._remove_duplicates(df)
        df = self._handle_missing(df)
        df = self._standardize_text(df)
        df = self._convert_dtypes(df)
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows"""
        return df.drop_duplicates()
    
    def _handle_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values based on config"""
        strategy = self.config.get('missing_strategy', 'drop')
        
        if strategy == 'drop':
            return df.dropna()
        elif strategy == 'mean':
            return df.fillna(df.mean(numeric_only=True))
        elif strategy == 'median':
            return df.fillna(df.median(numeric_only=True))
        return df
    
    def _standardize_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize text columns"""
        text_columns = df.select_dtypes(include=['object']).columns
        
        for col in text_columns:
            df[col] = df[col].str.lower().str.strip()
            
        return df
    
    def _convert_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types"""
        for col in df.columns:
            if self._is_numeric(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif self._is_datetime(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    
    def _is_numeric(self, series: pd.Series) -> bool:
        """Check if series can be converted to numeric"""
        try:
            pd.to_numeric(series, errors='raise')
            return True
        except:
            return False
            
    def _is_datetime(self, series: pd.Series) -> bool:
        """Check if series can be converted to datetime"""
        try:
            pd.to_datetime(series, errors='raise')
            return True
        except:
            return False
