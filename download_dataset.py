import pandas as pd
import os

def download_titanic_dataset():
    """Download and prepare the Titanic dataset"""
    # Create data directory if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # Download the dataset
    url = 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'
    try:
        df = pd.read_csv(url)
        
        # Save to CSV
        output_path = 'data/titanic.csv'
        df.to_csv(output_path, index=False)
        print(f"Dataset successfully downloaded and saved to {output_path}")
        print("\nDataset Preview:")
        print(df.head())
        print("\nDataset Information:")
        print(df.info())
        
    except Exception as e:
        print(f"Error downloading dataset: {e}")

if __name__ == "__main__":
    download_titanic_dataset() 