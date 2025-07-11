import boto3
import requests
import json
import os
import sys
from datetime import datetime, timezone

# Debug setup
DEBUG = True  # Set to False in production

def log(message):
    if DEBUG:
        print(f"[DEBUG] {message}", file=sys.stderr)

# Configuration with validation
try:
    YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']
    VIDEO_ID = os.environ['YOUTUBE_VIDEO_ID']
    BUCKET_NAME = 'scalable-youtube-comments-bucket'

    log(f"Config loaded - Bucket: {BUCKET_NAME}, Video: {VIDEO_ID[:5]}...")
    log(f"Python version: {sys.version}")

except KeyError as e:
    log(f"CRITICAL: Missing environment variable: {str(e)}")
    raise

s3 = boto3.client(
    's3',
    region_name='us-east-1',
    config=boto3.session.Config(
        connect_timeout=10,
        retries={'max_attempts': 3}
    )
)

def lambda_handler(event, context):
    url = f"https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": VIDEO_ID,
        "maxResults": 50,
        "textFormat": "plainText",
        "key": YOUTUBE_API_KEY
    }
    
    response = requests.get(url, params=params)
    comments_data = response.json()
    
    # Save to S3
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"comments_{timestamp}.json"
    s3.put_object(    
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=json.dumps(comments_data),
        ContentType='application/json'
    )
    
    return {
        'statusCode': 200,
        'body': f"Saved {filename} to S3."
    }

