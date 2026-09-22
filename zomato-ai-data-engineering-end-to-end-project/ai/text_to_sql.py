import json
import os
import re

import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI


load_dotenv()


MODEL = "gpt-4o-mini"

FORBIDDEN_WORDS = [
    "drop",
    "delete",
    "truncate",
    "alter",
    "update",
    "insert",
    "create",
    "replace",
    "grant",
    "revoke",
    "merge",
    "call",
]

EXAMPLE_QUESTIONS = [
    "Top 10 cities by GMV",
    "Which cuisine has the most orders?",
    "Average delivery time by city",
    "Cancellation rate by payment method",
    "Top 10 restaurants by revenue",
]


client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)


SCHEMA = """
You are working with a Snowflake database called ZOMATO.

The available tables are:

DIM_CUSTOMER(
    CUSTOMER_ID NUMBER,
    CUSTOMER_NAME TEXT,
    EMAIL TEXT,
    AGE NUMBER,
    AGE_SEGMENT TEXT,
    GENDER TEXT,
    MARITAL_STATUS TEXT,
    OCCUPATION TEXT,
    INCOME_BAND TEXT,
    EDUCATION TEXT,
    FAMILY_SIZE NUMBER
)

DIM_DATE(
    DATE_DAY DATE,
    YEAR NUMBER,
    MONTH NUMBER,
    MONTH_NAME TEXT,
    DAY_NAME TEXT,
    IS_WEEKEND BOOLEAN
)

DIM_FOOD(
    F_ID TEXT,
    FOOD_NAME TEXT,
    VEG_OR_NON_VEG TEXT
)

DIM_RESTAURANTS(
    RESTAURANT_ID NUMBER,
    RESTAURANT_NAME TEXT,
    CITY TEXT,
    CUISINE TEXT,
    RATING NUMBER,
    RATING_COUNT NUMBER,
    COST_FOR_TWO NUMBER
)

FACT_ORDER_ITEMS(
    ORDER_ITEM_ID NUMBER,
    ORDER_ID NUMBER,
    RESTAURANT_ID NUMBER,
    F_ID TEXT,
    ORDER_TS TIMESTAMP_NTZ,
    ORDER_DATE DATE,
    CITY TEXT,
    PRICE NUMBER,
    QUANTITY NUMBER,
    LINE_AMOUNT NUMBER
)

FCT_ORDERS(
    ORDER_ID NUMBER,
    ORDER_TIMESTAMP TIMESTAMP_NTZ,
    ORDER_DATE DATE,
    CUSTOMER_ID NUMBER,
    RESTAURANT_ID NUMBER,
    CITY TEXT,
    CUISINE TEXT,
    PAYMENT_METHOD TEXT,
    ORDER_STATUS TEXT,
    IS_DELIVERED BOOLEAN,
    ITEMS_COUNT NUMBER,
    SALES_QTY NUMBER,
    SUBTOTAL NUMBER,
    DISCOUNT NUMBER,
    DELIVERY_FEE NUMBER,
    GST NUMBER,
    SALES_AMOUNT NUMBER,
    CUSTOMER_RATING NUMBER,
    DELIVERY_TIME_MIN NUMBER
)

MART_DAILY_CITY_REVENUNE(
    ORDER_DATE DATE,
    CITY TEXT,
    ORDERS NUMBER,
    DELIVERED_ORDERS NUMBER,
    CANCEL_RATE NUMBER,
    GMV NUMBER,
    AOV NUMBER
)

MART_DELIVERY_SLA(
    CITY TEXT,
    ORDER_HOUR NUMBER,
    DELIVERED_ORDERS NUMBER,
    P50 NUMBER,
    P90 NUMBER
)

MART_RESTAURANT_PERFORMANCE(
    RESTAURANT_ID NUMBER,
    RESTAURANT_NAME TEXT,
    CITY TEXT,
    CUISINE TEXT,
    ORDERS NUMBER,
    REVENUE NUMBER,
    AVG_CUSTOMER_RATING NUMBER,
    AVG_DELIVERY_MIN NUMBER
)

IMPORTANT TABLE GUIDELINES:

- Prefer MART_DAILY_CITY_REVENUNE for city-level revenue and GMV questions.
- Prefer MART_RESTAURANT_PERFORMANCE for restaurant performance questions.
- Prefer MART_DELIVERY_SLA for delivery-time and delivery-SLA questions.
- Use FCT_ORDERS when detailed order-level analysis is required.
- Use FACT_ORDER_ITEMS for food-item and quantity analysis.
- Use DIM_CUSTOMER for customer demographic questions.
- Use DIM_RESTAURANTS for restaurant attributes.
- Use DIM_FOOD for food attributes.
- Use DIM_DATE for calendar attributes.

Important definitions:

- GMV means delivered revenue.
- CANCEL_RATE is a rate, not a count.
- AOV means average order value.
- IS_DELIVERED identifies delivered orders.
- P50 and P90 are delivery-time percentiles in minutes.
"""


SYSTEM_PROMPT = f"""
You are a Snowflake SQL expert.

Your task is to translate a user's natural-language question
into ONE safe Snowflake SELECT query.

Rules:

1. Generate exactly ONE SQL query.
2. SELECT or WITH queries only.
3. Never modify data.
4. Never use INSERT, UPDATE, DELETE, DROP, ALTER,
   CREATE, REPLACE, TRUNCATE, MERGE, CALL, GRANT or REVOKE.
5. Use bare table names only.
6. Do not use database or schema prefixes.
7. Add LIMIT 100 or less for queries returning multiple rows.
8. If the user asks for a top N, respect that N but never
   exceed LIMIT 100.
9. Use appropriate aggregations such as COUNT, SUM, AVG
   when needed.
10. Use the MART tables when they directly answer the question.
11. Return valid JSON only.

Required JSON format:

{{
    "sql": "SELECT ..."
}}

Database schema:

{SCHEMA}
"""


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MARTS",
        role="DBT_ROLE",
    )


def generate_sql(question):
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
                "content": question,
            },
        ],
    )

    answer = response.choices[0].message.content

    data = json.loads(answer)

    sql = data["sql"]

    sql = sql.strip().rstrip(";")

    # Remove accidental database/schema prefixes.
    sql = re.sub(
        r"ZOMATO\.MARTS\.",
        "",
        sql,
        flags=re.IGNORECASE,
    )

    sql = re.sub(
        r"ZOMATO\.",
        "",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def is_safe(sql):
    normalized = re.sub(
        r"\s+",
        " ",
        sql.strip().lower(),
    )

    if not (
        normalized.startswith("select ")
        or normalized.startswith("with ")
    ):
        return False

    for word in FORBIDDEN_WORDS:
        pattern = rf"\b{re.escape(word)}\b"

        if re.search(pattern, normalized):
            return False

    return True


def add_limit_if_needed(sql):
    """
    Add LIMIT 100 when the query does not already contain
    a LIMIT clause.
    """

    if re.search(
        r"\blimit\s+\d+",
        sql,
        flags=re.IGNORECASE,
    ):
        return sql

    return f"{sql}\nLIMIT 100"


def run_query(sql):
    conn = get_connection()

    try:
        return (
            conn.cursor()
            .execute(sql)
            .fetch_pandas_all()
        )
    finally:
        conn.close()


st.set_page_config(
    page_title="Zomato Text-to-SQL",
    page_icon="🍽️",
    layout="wide",
)


st.title("🍽️ Chat with your Zomato Data")

st.caption(
    f"Ask in English → {MODEL} generates SQL → "
    "Snowflake executes it"
)


with st.sidebar:
    st.header("Example Questions")

    for question in EXAMPLE_QUESTIONS:
        st.markdown(f"- {question}")


question = st.text_input(
    "Enter your question:",
    placeholder=(
        "e.g. Top 10 restaurants by revenue"
    ),
)


if question:

    try:
        with st.spinner("Generating SQL..."):
            sql = generate_sql(question)

        sql = add_limit_if_needed(sql)

        st.markdown("### Generated SQL")

        st.code(
            sql,
            language="sql",
        )

        if not is_safe(sql):
            st.error(
                "The generated SQL is not safe to execute."
            )

        else:

            with st.spinner(
                "Running query in Snowflake..."
            ):
                df = run_query(sql)

            st.success(
                f"{len(df)} rows returned."
            )

            st.markdown("### Results")

            st.dataframe(
                df,
                hide_index=True,
            )

            if (
                len(df.columns) == 2
                and len(df) > 0
                and pd.api.types.is_numeric_dtype(
                    df.iloc[:, 1]
                )
            ):
                st.markdown("### Visualization")

                st.bar_chart(
                    df,
                    x=df.columns[0],
                    y=df.columns[1],
                )

    except Exception as exc:

        st.error(
            f"Error: {exc}"
        )