"""
Data management utilities for handling large datasets efficiently.
"""
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import logging
from typing import Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataManager:
    """Manages data files and their conversions."""
    
    def __init__(self, data_dir: str = "data"):
        """Initialize DataManager with data directory."""
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def create_sample(self, 
                     input_file: str, 
                     output_file: Optional[str] = None,
                     n_rows: int = 1000,
                     random_state: int = 42) -> str:
        """
        Create a sample dataset from a large file.
        
        Args:
            input_file: Path to input file
            output_file: Path to output file (optional)
            n_rows: Number of rows to sample
            random_state: Random seed for reproducibility
            
        Returns:
            Path to the sample file
        """
        input_path = self.raw_dir / input_file
        if not output_file:
            output_file = f"sample_{input_file}"
        output_path = self.raw_dir / output_file
        
        logger.info(f"Creating sample of {n_rows} rows from {input_file}")
        
        # Read and sample data
        df = pd.read_csv(input_path)
        sample_df = df.sample(n=min(n_rows, len(df)), random_state=random_state)
        
        # Save sample
        sample_df.to_csv(output_path, index=False)
        logger.info(f"Sample saved to {output_file}")
        
        return str(output_path)
    
    def convert_to_parquet(self,
                          input_file: str,
                          output_file: Optional[str] = None) -> str:
        """
        Convert a CSV file to Parquet format.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output Parquet file (optional)
            
        Returns:
            Path to the Parquet file
        """
        input_path = self.raw_dir / input_file
        if not output_file:
            output_file = input_file.replace('.csv', '.parquet')
        output_path = self.processed_dir / output_file
        
        logger.info(f"Converting {input_file} to Parquet format")
        
        # Read CSV and convert to Parquet
        df = pd.read_csv(input_path)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)
        
        logger.info(f"Parquet file saved to {output_file}")
        return str(output_path)
    
    def get_file_size(self, file_path: Union[str, Path]) -> float:
        """Get file size in MB."""
        return os.path.getsize(file_path) / (1024 * 1024)
    
    def optimize_storage(self, input_file: str) -> None:
        """
        Optimize storage by converting to Parquet and creating a sample.
        
        Args:
            input_file: Name of the input file
        """
        input_path = self.raw_dir / input_file
        
        # Get original file size
        original_size = self.get_file_size(input_path)
        logger.info(f"Original file size: {original_size:.2f} MB")
        
        # Create sample
        sample_path = self.create_sample(input_file)
        sample_size = self.get_file_size(sample_path)
        logger.info(f"Sample file size: {sample_size:.2f} MB")
        
        # Convert to Parquet
        parquet_path = self.convert_to_parquet(input_file)
        parquet_size = self.get_file_size(parquet_path)
        logger.info(f"Parquet file size: {parquet_size:.2f} MB")
        
        # Print savings
        savings = original_size - parquet_size
        logger.info(f"Storage savings: {savings:.2f} MB ({savings/original_size*100:.1f}%)")

if __name__ == "__main__":
    # Example usage
    data_manager = DataManager()
    
    # Optimize storage for each large file
    large_files = ["inventory.csv", "sales.csv", "customers.csv", "products.csv"]
    for file in large_files:
        try:
            data_manager.optimize_storage(file)
        except FileNotFoundError:
            logger.warning(f"File {file} not found, skipping...") 