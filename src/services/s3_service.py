import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import logging
from src.core.config import settings

logger = logging.getLogger(__name__)

class S3Service:
    def __init__(self):
        config = settings.STORAGE_CONFIG
        
        self.bucket_name = config["bucket"]
        self.region = config["region"]
        self.endpoint_url = config["endpoint_url"]
        self.s3_client = None
        
        if config["access_key"] and config["secret_key"]:
            try:
                client_kwargs = {
                    'service_name': 's3',
                    'aws_access_key_id': config["access_key"],
                    'aws_secret_access_key': config["secret_key"],
                    'region_name': self.region
                }
                
                # Add endpoint_url if provided (MinIO)
                if self.endpoint_url:
                    # Clean endpoint URL
                    self.endpoint_url = self.endpoint_url.rstrip('/')
                    client_kwargs['endpoint_url'] = self.endpoint_url
                    
                    # MinIO requires signature v4 and often path style addressing
                    client_kwargs['config'] = Config(
                        signature_version='s3v4',
                        s3={'addressing_style': 'path'}
                    )
                    # Uncomment below if self-signed certs cause issues
                    client_kwargs['verify'] = False 
                
                self.s3_client = boto3.client(**client_kwargs)
                
                logger.info(f"S3 Service initialized using {config['type'].upper()}")
                
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {e}")
        else:
            logger.warning("AWS/MinIO credentials not found. Upload will be disabled.")

    def upload_file(self, file_content: bytes, object_name: str, content_type: str = "application/pdf") -> str | None:
        """
        Upload a file to an S3 bucket and return the URL.

        :param file_content: Bytes of the file to upload
        :param object_name: S3 object name (key)
        :param content_type: MIME type of the file
        :return: Public URL if successful, else None (or internal S3 path depending on requirement)
        """
        if not self.s3_client:
            logger.error("S3 client not initialized.")
            return None

        try:
            self.s3_client.put_object(
                Body=file_content,
                Bucket=self.bucket_name,
                Key=object_name,
                ContentType=content_type
                # ACL='public-read' # Optional: depends on bucket policy
            )
            
            # Construct URL
            if self.endpoint_url:
                # MinIO Style: endpoint/bucket/key
                # Remove trailing slash from endpoint if present
                base_url = self.endpoint_url.rstrip('/')
                url = f"{base_url}/{self.bucket_name}/{object_name}"
            else:
                # AWS Virtual-hosted-style
                url = f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{object_name}"
            
            return url
            
        except ClientError as e:
            logger.error(f"S3 Upload Error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error uploading to S3: {e}")
            return None

s3_service = S3Service()
