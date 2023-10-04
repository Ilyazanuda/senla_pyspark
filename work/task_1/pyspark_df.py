from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, when, col, count
import pyspark.sql.functions as func
from datetime import datetime
import argparse


def get_args():
    parser = argparse.ArgumentParser(description="Shows movies",
                                     prog="get_movies")
    parser.add_argument('-pd', '--part_date',
                        type=str,
                        metavar='<yyyy-mm-dd>',
                        help='searched date')

    args = parser.parse_args()
    p_date = datetime.strptime(args.part_date, '%Y-%m-%d')
    return p_date


def get_searched_date_transactions(transactions_train_raw, articles_raw, part_date):
    return transactions_train_raw.filter((func.year(transactions_train_raw['t_dat']) == part_date.year) &
                                         (func.month(transactions_train_raw['t_dat']) == part_date.month)) \
                                 .join(articles_raw, 'article_id', 'left') \
                                 .withColumn('row_num', func.monotonically_increasing_id()) \
                                 .withColumn('price', col('price').cast(DecimalType(38, 20))) \
                                 .select('t_dat', 'customer_id', 'article_id', 'price', 'product_group_name', 'row_num')


def get_most_exp_articles(transactions_train_df):
    window_most_exp_art = Window.partitionBy('customer_id').orderBy(func.desc('price'), 'row_num')
    return transactions_train_df.withColumn('row_number', func.row_number().over(window_most_exp_art)) \
                                .filter(col('row_number') == 1) \
                                .select('customer_id', 'article_id') \
                                .withColumnRenamed('article_id', 'most_exp_article_id')


def get_count_unic_groups(transactions_train_df):
    return transactions_train_df.select('customer_id', 'product_group_name') \
                                .groupBy('customer_id', 'product_group_name') \
                                .count() \
                                .groupBy('customer_id') \
                                .agg(count('product_group_name').alias('number_of_product_groups'))


def grouped_transactions_by_customer(transactions_train_df):
    return transactions_train_df.select('customer_id', 'price') \
                                .groupBy('customer_id') \
                                .agg(func.sum('price').alias('transaction_amount'),
                                     func.count('price').alias('number_of_articles'))


def save_output(most_exp_articles_df, number_of_product_groups_df, grouped_transactions_df,
                customers_raw, part_date, output_path):
    result = grouped_transactions_df.join(number_of_product_groups_df, 'customer_id', 'left') \
                                    .join(most_exp_articles_df, 'customer_id', 'left') \
                                    .join(customers_raw, 'customer_id', 'left') \
                                    .withColumn('customer_group_by_age', when(col('age') < 23, 'S')
                                                .when(col('age') > 59, 'R')
                                                .otherwise('A')) \
                                    .withColumn('part_date', func.lit(datetime.strftime(part_date, '%Y-%m'))) \
                                    .select('part_date', 'customer_id', 'customer_group_by_age',
                                            'transaction_amount', 'most_exp_article_id',
                                            'number_of_articles', 'number_of_product_groups')

    result.write \
          .mode('overwrite') \
          .option('header', 'True') \
          .option('delimiter', ',') \
          .csv(output_path)


def main():
    part_date = get_args()
    articles_path = '../data/articles.csv'
    customers_path = '../data/customers.csv'
    transactions_train_path = '../data/transactions_train.csv'
    output_path = 'output_csv'

    spark = SparkSession(SparkContext())
    articles_raw = spark.read.csv(articles_path, header=True)
    customers_raw = spark.read.csv(customers_path, header=True)
    transactions_train_raw = spark.read.csv(transactions_train_path, header=True)

    transactions_train_df = get_searched_date_transactions(transactions_train_raw, articles_raw, part_date)
    
    most_exp_articles_df = get_most_exp_articles(transactions_train_df)
    number_of_product_groups_df = get_count_unic_groups(transactions_train_df)
    grouped_transactions_df = grouped_transactions_by_customer(transactions_train_df)
    
    save_output(most_exp_articles_df, number_of_product_groups_df, grouped_transactions_df,
                customers_raw, part_date, output_path)


if __name__ == '__main__':
    main()

