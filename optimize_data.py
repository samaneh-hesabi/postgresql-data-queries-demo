"""
Script to optimize data storage by converting large files to more efficient formats
and creating sample datasets for development.
"""
import logging
from utils.data_manager import DataManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run data optimization process."""
    logger.info("Starting data optimization process...")
    
    # Initialize data manager
    data_manager = DataManager()
    
    # List of files to optimize
    files_to_optimize = [
        "inventory.csv",
        "sales.csv",
        "customers.csv",
        "products.csv",
        "stores.csv",
        "time_dimension.csv"
    ]
    
    # Process each file
    for file in files_to_optimize:
        try:
            logger.info(f"\nProcessing {file}...")
            data_manager.optimize_storage(file)
        except FileNotFoundError:
            logger.warning(f"File {file} not found, skipping...")
        except Exception as e:
            logger.error(f"Error processing {file}: {str(e)}")
    
    logger.info("\nData optimization completed!")

if __name__ == "__main__":
    main() 