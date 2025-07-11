import pandas as pd
from textblob import TextBlob
from tqdm import tqdm
import boto3
import time

bucket_name = "scalable-youtube-comments-bucket-personal"
output_key = "amazon_sentiment_output_sequential.txt"
file_path = "/home/ec2-user/amazon_book_review_filename/Books_rating.csv"

def upload_to_s3(content):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=output_key, Body=content.encode("utf-8"))

def analyze_sentiment(text):
    return TextBlob(str(text)).sentiment.polarity

def main():
    start = time.time()
    df = pd.read_csv(file_path, usecols=["review/text"])
    reviews = df["review/text"].dropna().tolist()

    sentiments = [analyze_sentiment(line) for line in tqdm(reviews, desc="Analyzing")]

    positive = sum(1 for s in sentiments if s > 0)
    negative = sum(1 for s in sentiments if s < 0)
    neutral = sum(1 for s in sentiments if s == 0)
    avg_score = sum(sentiments) / len(sentiments)

    summary = (
        f"Average Sentiment Score: {avg_score:.4f}\n"
        f"Positive Reviews: {positive}\n"
        f"Negative Reviews: {negative}\n"
        f"Neutral Reviews: {neutral}\n"
        f"Total Reviews: {len(sentiments)}\n"
        f"Time Taken: {round(time.time() - start, 2)} seconds\n"
    )

    print(summary)
    upload_to_s3(summary)

if __name__ == "__main__":
    main()

