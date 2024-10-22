import os
import boto3
import logging
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create an S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Retrieve the bucket name from the environment variable
    bucket_name = os.environ.get('BUCKET_NAME')
    if not bucket_name:
        logger.error('BUCKET_NAME environment variable is not set.')
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }

    # Retrieve the target storage class from the environment variable
    target_storage_class = os.environ.get('TARGET_STORAGE_CLASS', 'INTELLIGENT_TIERING')

    # Check if the 'Records' key exists in the event object
    if 'Records' not in event:
        logger.error('Invalid event structure. No "Records" key found.')
        return {
            'statusCode': 400,
            'body': 'Invalid event structure.'
        }

    # Move the object to the new storage class
    try:
        object_key = event['Records'][0]['s3']['object']['key']

        # Check if the object exists and get its current storage class
        try:
            response = s3.head_object(Bucket=bucket_name, Key=object_key)
            current_storage_class = response.get('StorageClass', 'STANDARD')
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f'Error: The specified key {object_key} does not exist in bucket {bucket_name}.')
                return {
                    'statusCode': 404,
                    'body': 'The specified key does not exist.'
                }
            else:
                logger.error(f'Unexpected error: {e}')
                return {
                    'statusCode': 500,
                    'body': 'Internal Server Error'
                }

        # If the object is already in the target storage class, skip the copy operation
        if current_storage_class == target_storage_class:
            logger.info(f'Object {object_key} is already in {target_storage_class} storage class. Skipping.')
            return {
                'statusCode': 200,
                'body': 'Object already in target storage class.'
            }

        copy_source = {'Bucket': bucket_name, 'Key': object_key}

        s3.copy_object(
            Bucket=bucket_name,
            Key=object_key,
            CopySource=copy_source,
            StorageClass=target_storage_class,
            MetadataDirective='COPY'
        )

        logger.info(f'Moved object {object_key} in bucket {bucket_name} to {target_storage_class} storage class.')

        return {
            'statusCode': 200,
            'body': 'Object storage class moved successfully.'
        }
    except ClientError as e:
        logger.error(f'Unexpected error: {e}')
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
