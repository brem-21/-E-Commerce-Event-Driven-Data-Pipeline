import os
import sys
import boto3
from dotenv import load_dotenv

def load_environment_variables():
    """Load and validate required environment variables."""
    load_dotenv()
    
    required_vars = [
        "Access_key_ID",
        "Secret_access_key",
        "BUCKET_NAME",
        "ORDER_DATA_PATH",
        "ORDER_ITEMS_DATA_PATH",
        "PRODUCT_DATA_PATH",
        "REGION_NAME"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return {
        "access_key_id": os.getenv("Access_key_ID"),
        "secret_access_key": os.getenv("Secret_access_key"),
        "bucket_name": os.getenv("BUCKET_NAME"),
        "order_data_path": os.getenv("ORDER_DATA_PATH"),
        "order_items_data_path": os.getenv("ORDER_ITEMS_DATA_PATH"),
        "product_data_path": os.getenv("PRODUCT_DATA_PATH"),
        "region_name": os.getenv("REGION_NAME")
    }

def initialize_s3_client(access_key, secret_key, region):
    """Initialize and return an S3 client."""
    try:
        return boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
    except Exception as e:
        raise RuntimeError(f"Failed to initialize S3 client: {str(e)}")

def upload_files_to_s3(s3_client, bucket_name, local_path, s3_prefix):
    """Upload all files from local_path to s3_prefix in the bucket."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local path does not exist: {local_path}")
    
    files = os.listdir(local_path)
    if not files:
        print(f"No files found in {local_path}")
        return
    
    for file in files:
        file_path = os.path.join(local_path, file)
        s3_key = f"{s3_prefix}/{file}"
        
        try:
            s3_client.upload_file(file_path, bucket_name, s3_key)
            print(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error uploading {file_path}: {str(e)}", file=sys.stderr)

def upload_single_file_to_s3(s3_client, bucket_name, local_path, s3_key):
    """Upload a single file to S3."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File does not exist: {local_path}")
    
    try:
        s3_client.upload_file(local_path, bucket_name, s3_key)
        print(f"Successfully uploaded {local_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        raise RuntimeError(f"Error uploading {local_path}: {str(e)}")

def main():
    try:
        # Load configuration
        config = load_environment_variables()
        
        # Initialize S3 client
        s3_client = initialize_s3_client(
            config["access_key_id"],
            config["secret_access_key"],
            config["region_name"]
        )
        
        # Upload orders data
        upload_files_to_s3(
            s3_client,
            config["bucket_name"],
            config["order_data_path"],
            "orders"
        )
        
        # Upload order items data
        upload_files_to_s3(
            s3_client,
            config["bucket_name"],
            config["order_items_data_path"],
            "order_items"
        )
        
        # Upload product data (single file)
        upload_single_file_to_s3(
            s3_client,
            config["bucket_name"],
            config["product_data_path"],
            os.path.basename(config["product_data_path"])
        )
        
        print("All upload operations completed.")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()