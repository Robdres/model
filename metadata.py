import json
import pandas as pd
from typing import Dict, Any, Optional

class DatabaseProcessor:
    """
    A class to process and clean medical database columns using metadata.
    """
    
    def __init__(self, metadata_file: str = 'db_metadata.json'):
        """
        Initialize the processor with metadata file.
        
        Args:
            metadata_file (str): Path to the JSON metadata file
        """
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()
        self.column_mappings = self.metadata['column_mappings']
    
    def load_metadata(self) -> Dict[str, Any]:
        """
        Load metadata from JSON file.
        
        Returns:
            Dict containing the metadata
        """
        try:
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Metadata file {self.metadata_file} not found. Please ensure the JSON file exists.")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in {self.metadata_file}")
    
    def save_metadata(self, metadata: Dict[str, Any], filename: str = 'db_metadata.json'):
        """
        Save metadata to JSON file.
        
        Args:
            metadata (Dict): Metadata dictionary to save
            filename (str): Output filename
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print(f"Metadata saved to {filename}")
    
    def get_column_mapping(self) -> Dict[str, str]:
        """
        Get mapping of old column names to new column names.
        
        Returns:
            Dict mapping old names to new names
        """
        return {old_name: info['new_name'] for old_name, info in self.column_mappings.items()}
    
    def get_categorical_columns(self) -> Dict[str, Dict]:
        """
        Get all columns that have categories defined.
        
        Returns:
            Dict of column names and their categories
        """
        categorical = {}
        for old_name, info in self.column_mappings.items():
            if info['type'] == 'categorical' and info['categories']:
                categorical[info['new_name']] = info['categories']
        return categorical
    
    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename DataFrame columns using the metadata mapping.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with renamed columns
        """
        column_mapping = self.get_column_mapping()
        
        # Only rename columns that exist in the DataFrame
        existing_mapping = {old: new for old, new in column_mapping.items() if old in df.columns}
        
        if not existing_mapping:
            print("Warning: No matching columns found to rename")
            return df
        
        df_renamed = df.rename(columns=existing_mapping)
        
        print(f"Renamed {len(existing_mapping)} columns:")
        for old, new in existing_mapping.items():
            print(f"  '{old}' -> '{new}'")
        
        return df_renamed
    
    def apply_categorical_mapping(self, df: pd.DataFrame, column_name: str) -> Optional[pd.Series]:
        """
        Apply categorical mapping to a specific column.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            column_name (str): Name of the column to process
            
        Returns:
            pd.Series or None: Processed series or None if column not found
        """
        if column_name not in df.columns:
            print(f"Warning: Column '{column_name}' not found in DataFrame")
            return None
        
        categorical_cols = self.get_categorical_columns()
        if column_name not in categorical_cols:
            print(f"Warning: No categorical mapping found for '{column_name}'")
            return df[column_name]
        
        categories = categorical_cols[column_name]
        
        # Create a copy of the series
        series = df[column_name].copy()
        
        # Apply mapping
        for code, description in categories.items():
            if code.replace('.', '').isdigit():  # Handle numeric codes
                numeric_code = float(code)
                series = series.replace(numeric_code, f"{code}: {description}")
            else:
                series = series.replace(code, f"{code}: {description}")
        
        return series
    
