import json
import os

import snowflake.connector
from dotenv import load_dotenv
from openai import OpenAI


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

load_dotenv()

MODEL = "gpt-4o-mini"
SAMPLE_N = 5

AI_SCHEMA = "ZOMATO.AI"
OUTPUT_TABLE = f"{AI_SCHEMA}.REVIEW_ENRICHED"

TOPICS = [
    "food quality",
    "delivery",
    "pricing",
    "service",
    "packaging",
    "other",
]

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# ---------------------------------------------------------
# Prompt
# ---------------------------------------------------------

SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery application.

For the review provided, return:

- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: exactly one of {TOPICS}
- key_issue: a short phrase of 6 words or less describing
  the main issue in the review, if there is one.
  If there is no issue, return null.

Return ONLY valid JSON using this exact structure:

{{
    "sentiment_label": "<positive|negative|neutral>",
    "sentiment_score": <number>,
    "topic": "<topic>",
    "key_issue": "<short phrase or null>"
}}
"""


# ---------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------

def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


# ---------------------------------------------------------
# Create AI output table
# ---------------------------------------------------------

def create_output_table(cursor):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {AI_SCHEMA}")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
            REVIEW_ID STRING,
            SENTIMENT_LABEL STRING,
            SENTIMENT_SCORE FLOAT,
            TOPIC STRING,
            KEY_ISSUE STRING,
            MODEL STRING,
            ENRICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)


# ---------------------------------------------------------
# Get reviews that have not been enriched yet
# ---------------------------------------------------------

def get_reviews_to_enrich(cursor):
    query = f"""
        SELECT
            r.REVIEW_ID,
            r.COMMENT
        FROM ZOMATO.RAW.REVIEWS r
        WHERE r.REVIEW_ID NOT IN (
            SELECT REVIEW_ID
            FROM {OUTPUT_TABLE}
        )
        AND r.COMMENT IS NOT NULL
        LIMIT {SAMPLE_N}
    """

    cursor.execute(query)

    return cursor.fetchall()


# ---------------------------------------------------------
# Classify one review
# ---------------------------------------------------------

def classify_review(comment):
    response = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": comment,
            },
        ],
    )

    answer = response.choices[0].message.content

    return json.loads(answer)


# ---------------------------------------------------------
# Validate LLM response
# ---------------------------------------------------------

def validate_labels(labels):
    required_keys = {
        "sentiment_label",
        "sentiment_score",
        "topic",
        "key_issue",
    }

    if not required_keys.issubset(labels.keys()):
        raise ValueError("LLM response is missing required fields.")

    if labels["sentiment_label"] not in {
        "positive",
        "negative",
        "neutral",
    }:
        raise ValueError(
            f"Invalid sentiment_label: {labels['sentiment_label']}"
        )

    score = float(labels["sentiment_score"])

    if not -1.0 <= score <= 1.0:
        raise ValueError(
            f"Invalid sentiment_score: {score}"
        )

    if labels["topic"] not in TOPICS:
        raise ValueError(
            f"Invalid topic: {labels['topic']}"
        )

    key_issue = labels["key_issue"]

    if key_issue is not None and len(key_issue.split()) > 6:
        raise ValueError(
            f"key_issue contains more than 6 words: {key_issue}"
        )


# ---------------------------------------------------------
# Save results to Snowflake
# ---------------------------------------------------------

def save_results(cursor, results):
    if not results:
        return

    print(
        f"Saving {len(results)} enriched reviews to Snowflake..."
    )

    cursor.executemany(
        f"""
        INSERT INTO {OUTPUT_TABLE}
        (
            REVIEW_ID,
            SENTIMENT_LABEL,
            SENTIMENT_SCORE,
            TOPIC,
            KEY_ISSUE,
            MODEL
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        results,
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    print("Connecting to Snowflake...")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        create_output_table(cursor)

        reviews = get_reviews_to_enrich(cursor)

        if not reviews:
            print("No new reviews to enrich.")
            return

        print(
            f"Found {len(reviews)} reviews to enrich."
        )

        results = []

        for review_id, comment in reviews:

            print(
                f"\nClassifying review {review_id}:"
            )
            print(comment)

            try:

                labels = classify_review(comment)

                validate_labels(labels)

                print(
                    f"Labels: {labels}"
                )

                results.append(
                    (
                        str(review_id),
                        labels["sentiment_label"],
                        float(labels["sentiment_score"]),
                        labels["topic"],
                        labels["key_issue"],
                        MODEL,
                    )
                )

            except Exception as exc:

                print(
                    f"Error processing review "
                    f"{review_id}: {exc}"
                )

        save_results(cursor, results)

        conn.commit()

        print(
            f"\nSaved {len(results)} enriched reviews."
        )

    finally:

        cursor.close()
        conn.close()

        print("Snowflake connection closed.")


if __name__ == "__main__":
    main()

