import os

import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

EMBEDDING_MODEL = "text-embedding-3-small"
CHAT_MODEL = "gpt-4o-mini"

MAX_REVIEWS = 500
TOP_K = 5

CACHE_FILE = "review_embeddings.parquet"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="STAGING",
        role="DBT_ROLE",
    )


def read_reviews_from_snowflake():
    conn = get_snowflake_connection()

    query = f"""
        SELECT
            REVIEW_ID,
            CITY,
            RATING,
            COMMENT
        FROM ZOMATO.STAGING.STG_REVIEWS
        WHERE COMMENT IS NOT NULL
          AND TRIM(COMMENT) <> ''
        ORDER BY REVIEW_DATE DESC
        LIMIT {MAX_REVIEWS}
    """

    try:
        df = conn.cursor().execute(query).fetch_pandas_all()
    finally:
        conn.close()

    df.columns = [column.lower() for column in df.columns]

    return df


def embed(texts):
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
    )

    return [item.embedding for item in response.data]


@st.cache_data
def load_reviews():
    if os.path.exists(CACHE_FILE):
        print(f"Loading embeddings from {CACHE_FILE}")
        return pd.read_parquet(CACHE_FILE)

    print("Loading reviews from Snowflake...")
    df = read_reviews_from_snowflake()

    if df.empty:
        raise ValueError("No reviews found in ZOMATO.STAGING.STG_REVIEWS.")

    print(f"Creating embeddings for {len(df)} reviews...")

    df["embedding"] = embed(df["comment"].tolist())

    df.to_parquet(CACHE_FILE, index=False)

    return df


def cosine_similarity(vec_a, vec_b):
    denominator = (
        np.linalg.norm(vec_a) *
        np.linalg.norm(vec_b)
    )

    if denominator == 0:
        return 0.0

    return np.dot(vec_a, vec_b) / denominator


def find_similar_reviews(question, df):
    question_vector = embed([question])[0]

    scores = []

    for review_vector in df["embedding"]:
        scores.append(
            cosine_similarity(
                question_vector,
                review_vector,
            )
        )

    result = df.copy()
    result["score"] = scores

    return result.nlargest(TOP_K, "score")


def ask_llm(question, top_reviews):
    context = ""

    for _, row in top_reviews.iterrows():
        context += (
            f"City: {row['city']}\n"
            f"Rating: {row['rating']} stars\n"
            f"Review: {row['comment']}\n\n"
        )

    system_prompt = """
You answer questions about customer reviews for a food delivery application.

Use ONLY the customer reviews provided in the context.

Do not invent information.

If the provided reviews do not contain enough information
to answer the question, say that the available reviews
do not provide enough information.

Be concise and factual.
"""

    user_prompt = f"""
Question:
{question}

Customer reviews:
{context}
"""

    response = client.chat.completions.create(
        model=CHAT_MODEL,
        temperature=0.2,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ],
    )

    return response.choices[0].message.content


st.set_page_config(
    page_title="Zomato Review RAG",
    page_icon="🍽️",
    layout="wide",
)

st.title("🍽️ Chat with your Zomato Reviews")

st.caption(
    f"Searching up to {MAX_REVIEWS} reviews "
    f"with {EMBEDDING_MODEL} embeddings "
    f"and answering with {CHAT_MODEL}."
)

try:
    review_df = load_reviews()

    st.success(
        f"{len(review_df)} reviews available for semantic search."
    )

except Exception as exc:
    st.error(f"Could not load reviews: {exc}")
    st.stop()


question = st.text_input(
    "Ask a question about your reviews:",
    placeholder=(
        "e.g. What are the most common complaints "
        "about delivery?"
    ),
)

if question:
    with st.spinner("Searching similar reviews..."):
        top_reviews = find_similar_reviews(
            question,
            review_df,
        )

    with st.spinner("Generating answer..."):
        answer = ask_llm(
            question,
            top_reviews,
        )

    st.markdown("### Answer")
    st.write(answer)

    with st.expander("Reviews used to build this answer"):
        display_columns = [
            "city",
            "rating",
            "comment",
            "score",
        ]

        st.dataframe(
            top_reviews[display_columns],
            hide_index=True,
        )