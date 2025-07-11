import boto3
import requests
import json
import sys
from datetime import datetime, timezone
import time

DEBUG = True  # Set False for production

def log(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}", file=sys.stderr)

# Config
YOUTUBE_API_KEY = 'AIzaSyCAVhVrSjV4a-BhvYaxtF0KNheHPTI7whY'
VIDEO_ID = 'gdZLi9oWNZg'
BUCKET_NAME = 'scalable-youtube-comments-bucket'

s3 = boto3.client('s3', region_name='us-east-1')

def fetch_comments():
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": VIDEO_ID,
        "textFormat": "plainText",
        "key": YOUTUBE_API_KEY,
        "maxResults": 100
    }

    comments_only = []
    next_page_token = None
    total_requests = 0

    while True:
        if next_page_token:
            params["pageToken"] = next_page_token
        else:
            params.pop("pageToken", None)

        log(f"Making API request #{total_requests+1} to {url}")
        response = requests.get(url, params=params, timeout=30)
        total_requests += 1

        if response.status_code == 429:
            log("Rate limit hit. Sleeping for 60 seconds before retry...")
            time.sleep(60)
            continue
        elif response.status_code >= 500:
            log(f"Server error {response.status_code}. Retrying in 30 seconds...")
            time.sleep(30)
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            log(f"HTTP error: {e}. Response: {response.text}")
            break

        data = response.json()

        items = data.get("items", [])
        if not items:
            log("No comments returned in this response.")
            break

        for item in items:
            try:
                comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comments_only.append(comment_text)
            except KeyError:
                log("Skipping malformed comment item")

        log(f"Fetched {len(items)} comments (Total so far: {len(comments_only)})")

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            log("No more pages. Completed fetching comments.")
            break

        # Limit total requests to avoid quota exhaustion, remove or adjust as needed
        if total_requests >= 9000:
            log("Reached API request limit to avoid quota exhaustion.")
            break

    return comments_only

def upload_comments(comments):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"comments_{timestamp}.json"

    try:
        log(f"Uploading {len(comments)} comments to s3://{BUCKET_NAME}/{filename}")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(comments, ensure_ascii=False),
            ContentType='application/json',
            ACL='bucket-owner-full-control'
        )
        log("Upload successful")
        return filename
    except Exception as e:
        log(f"S3 Upload Failed: {e}")
        raise

def lambda_handler(event, context):
    log("Execution started")
    comments = fetch_comments()
    filename = upload_comments(comments)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f"Saved {filename} to S3",
            'comment_count': len(comments)
        })
    }

if __name__ == "__main__":
    print("=== LOCAL TEST RUN ===")
    class FakeContext: pass
    lambda_handler({}, FakeContext())
    print("=== TEST COMPLETE ===")

