from pyspark.sql.functions import split, explode, concat_ws, regexp_replace, lead, col, monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import zipfile
import re


def get_text(archive_path):
    text_string = ""
    
    with zipfile.ZipFile(archive_path, 'r') as zip_file:
        for name in zip_file.namelist():
            if name.endswith('.txt'):
                text_string += re.sub(r'\s+|\n', ' ', zip_file.read(name).decode("utf-8")).lower()
                
    return text_string


def get_transformed_dataframe(text_df):
    words_df = text_df.select(explode(split(regexp_replace("value", r"[^a-zA-Z\s']", ""), " ")).alias("word"))
    
    bigrams_df = words_df.withColumn("word_id", monotonically_increasing_id()) \
                         .withColumn("next_word", lead("word").over(Window.orderBy("word_id"))) \
                         .filter(col("next_word").isNotNull()) \
                         .select(concat_ws(" ", "word", "next_word").alias("bigram"))
    
    bigrams_count_df = bigrams_df.groupBy("bigram") \
                                 .count() \
                                 .orderBy("count", ascending=False)
    
    return bigrams_count_df


def show_result(bigrams_count_df, total_bigrams):
    print(f"total word pairs: {total_bigrams}\nword_pair_counts:")
    bigrams_count_df.show()


def main():
    archive_path = '../data/archive.zip'
    spark = SparkSession.builder.appName("BigramCount").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    text_string = get_text(archive_path)
    text_df = spark.createDataFrame([text_string], StringType())

    bigrams_count_df = get_transformed_dataframe(text_df)
    total_bigrams = bigrams_count_df.count()

    show_result(bigrams_count_df, total_bigrams)

    spark.stop()


if __name__ == '__main__':
    main()
