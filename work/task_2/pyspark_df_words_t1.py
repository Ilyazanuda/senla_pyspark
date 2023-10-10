from pyspark.sql.functions import split, explode, regexp_replace
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
    word_count_df = words_df.groupBy("word") \
                            .count() \
                            .orderBy("count", ascending=False)
    
    return word_count_df


def show_result(word_count_df, total_words):
    print(f"total words: {total_words}\nword_counts:")
    word_count_df.show()


def main():
    archive_path = '../data/archive.zip'
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    text_string = get_text(archive_path)
    text_df = spark.createDataFrame([text_string], StringType())

    word_count_df = get_transformed_dataframe(text_df)
    total_words = word_count_df.count()

    show_result(word_count_df, total_words)

    spark.stop()
    

if __name__ == '__main__':
    main()