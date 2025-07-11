import boto3
import pandas as pd
import time
import json
from datetime import datetime

# AWS configuration
STREAM_NAME = "bookReviewStream"
REGION = "us-east-1"

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

# Load dataset
file_path = "/home/ec2-user/amazon_book_review_filename/Books_rating.csv"
df = pd.read_csv(file_path, usecols=["review/text"]).dropna().reset_index(drop=True)

print(f"Producer started... Total reviews to stream: {len(df)}")

# Send each review to Kinesis
for i, row in df.iterrows():
    review = row["review/text"]
    payload = {
        "text": review,
        "sent_at": datetime.utcnow().isoformat()
    }

    try:
        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload),
            PartitionKey="partitionKey"
        )
        print(f"Sent [{i + 1}]: {review[:80]}...")
    except Exception as e:
        print(f"Error sending record {i + 1}: {e}")

    time.sleep(0.5)  # Adjust for load testing: 0.5s, 0.2s, 0.1s

