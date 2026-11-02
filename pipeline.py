
import boto3
import json
import time
from uuid import uuid4

# Connect to LocalStack
config = {
    "endpoint_url": "http://localstack:4566",
    "region_name": "us-east-1",
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test"
}

kinesis = boto3.client("kinesis", **config)
s3 = boto3.client("s3", **config)

STREAM_NAME = "MyStream"
BUCKET_NAME = "my-bucket"
BATCH_SIZE = 500
RUN_ID = uuid4().hex

total_articles = 0
total_words = 0
batch_number = 0
articles = []



def save_batch(batch):
    global batch_number

    if not batch:
        return

    batch_number += 1

    key = f"training-data/run-{RUN_ID}/batch-{batch_number:05d}.jsonl"

    body = "\n".join(
        json.dumps(article) for article in batch
    ) + "\n"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    print(f"Saved {len(batch)} articles to s3://{BUCKET_NAME}/{key}")


# Discover all stream shards
shards = kinesis.list_shards(
    StreamName=STREAM_NAME
)["Shards"]

for shard in shards:
    shard_id = shard["ShardId"]

    iterator = kinesis.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]

    empty_reads = 0

    while iterator and empty_reads < 3:
        response = kinesis.get_records(
            ShardIterator=iterator,
            Limit=1000
        )

        records = response["Records"]
        iterator = response.get("NextShardIterator")

        if not records:
            empty_reads += 1
            time.sleep(1)
            continue

        empty_reads = 0

        for record in records:
            article = json.loads(record["Data"])

            content = article.get("content", "")
            word_count = len(content.split())

            article["word_count"] = word_count

            articles.append(article)

            total_articles += 1
            total_words += word_count

            if len(articles) >= BATCH_SIZE:
                save_batch(articles)
                articles = []

# Save remaining records
save_batch(articles)

if total_articles:
    average = total_words / total_articles

    print("\nPipeline completed successfully!")
    print("Total articles processed:", total_articles)
    print("Average word count:", round(average, 2))
    print("Total batches saved:", batch_number)
else:
    print("No records found in Kinesis.")
