import pycountry
import pycountry_convert as pcc
from fuzzywuzzy import process

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, trim
from pyspark.sql.types import StringType

# ------------------------------------------------------------------
# Spark session
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Japan Visa Data Processing") \
    .getOrCreate()

# ------------------------------------------------------------------
# Lecture des données
# ------------------------------------------------------------------
df = spark.read.csv(
    "/opt/bitnami/spark/input/visa_number_in_japan.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------------------------
# Nettoyage des noms de colonnes
# ------------------------------------------------------------------
new_column_names = [
    col_name.replace(' ', '_')
            .replace('/', '')
            .replace('.', '')
            .replace(',', '')
    for col_name in df.columns
]

df = df.toDF(*new_column_names)

# ------------------------------------------------------------------
# Nettoyage des données
# ------------------------------------------------------------------
df = df.dropna(how='all')
df = df.select("year", "country", "number_of_issued_numerical")
df = df.withColumn("country", trim(df["country"]))

# ------------------------------------------------------------------
# Fonctions de correction
# ------------------------------------------------------------------
def correct_country_name(name, threshold=85):
    if name is None:
        return None

    countries = [country.name for country in pycountry.countries]
    corrected_name, score = process.extractOne(name, countries)

    if score >= threshold:
        return corrected_name

    return name


def get_continent_name(country_name):
    try:
        country_code = pcc.country_name_to_country_alpha2(country_name)
        continent_code = pcc.country_alpha2_to_continent_code(country_code)
        return pcc.convert_continent_code_to_continent_name(continent_code)
    except:
        return None


correct_country_name_udf = udf(correct_country_name, StringType())
continent_udf = udf(get_continent_name, StringType())

# ------------------------------------------------------------------
# Application des corrections
# ------------------------------------------------------------------
df = df.withColumn("country", correct_country_name_udf(df["country"]))

country_corrections = {
    "Andra": "Russia",
    "Antigua Berbuda": "Antigua and Barbuda",
    "Barrane": "Bahrain",
    "Brush": "Bhutan",
    "Komoro": "Comoros",
    "Benan": "Benin",
    "Kiribass": "Kiribati",
    "Gaiana": "Guyana",
    "Court Jiboire": "Côte d'Ivoire",
    "Lesot": "Lesotho",
    "Macau travel certificate": "Macao",
    "Moldoba": "Moldova",
    "Naure": "Nauru",
    "Nigail": "Niger",
    "Palao": "Palau",
    "St. Christopher Navis": "Saint Kitts and Nevis",
    "Santa Principa": "Sao Tome and Principe",
    "Saechel": "Seychelles",
    "Slinum": "Saint Helena",
    "Swaji Land": "Eswatini",
    "Torque menistan": "Turkmenistan",
    "Tsubaru": "Zimbabwe",
    "Kosovo": "Kosovo"
}

df = df.replace(country_corrections, subset="country")
df = df.withColumn("continent", continent_udf(df["country"]))

# ------------------------------------------------------------------
# Agrégations Spark (PAS de toPandas ici)
# ------------------------------------------------------------------
df.createOrReplaceTempView("japan_visa")

df_continent_year = spark.sql("""
    SELECT
        year,
        continent,
        SUM(number_of_issued_numerical) AS visa_issued
    FROM japan_visa
    WHERE continent IS NOT NULL
    GROUP BY year, continent
""")

df_top_country_2017 = spark.sql("""
    SELECT
        country,
        SUM(number_of_issued_numerical) AS visa_issued
    FROM japan_visa
    WHERE country NOT IN ('total', 'others')
      AND country IS NOT NULL
      AND year = 2017
    GROUP BY country
    ORDER BY visa_issued DESC
    LIMIT 10
""")

df_country_year = spark.sql("""
    SELECT
        year,
        country,
        SUM(number_of_issued_numerical) AS visa_issued
    FROM japan_visa
    WHERE country NOT IN ('total', 'others')
      AND country IS NOT NULL
    GROUP BY year, country
""")

# ------------------------------------------------------------------
# Écriture des outputs (cluster-safe)
# ------------------------------------------------------------------
df.write.mode("overwrite").parquet("output/visa_number_in_japan_cleaned")

df_continent_year.write.mode("overwrite").parquet("output/visa_by_continent_year")

df_top_country_2017.write.mode("overwrite").parquet("output/top_country_2017")

df_country_year.write.mode("overwrite").parquet("output/visa_by_country_year")

# ------------------------------------------------------------------
spark.stop()
