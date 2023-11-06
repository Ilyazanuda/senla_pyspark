from pyspark.sql.types import DecimalType, StructType, DoubleType, StructField, StringType
from pyspark.sql.functions import lit, when, col, count, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime
from configparser import ConfigParser
import argparse
import json


def get_args():
    parser = argparse.ArgumentParser(description="Create data mart",
                                     prog="create_data_mart")
    
    parser.add_argument('-pd', '--part_date',
                        type=str,
                        metavar='<yyyy-mm-dd>',
                        help='searched date')

    parser.add_argument('-dc', '--dm_currency',
                        type=str,
                        metavar='<currency>',
                        default='USD',
                        help='currency: USD, BYN, PLN, EUR')

    args = parser.parse_args()
    
    return (datetime.strptime(args.part_date, '%Y-%m-%d'),
            args.dm_currency.upper() if args.dm_currency.upper() in ('USD', 'PLN', 'BYN', 'EUR') else 'USD')


def uppercase_exchange_rate(currency_exchange_rate):
    return currency_exchange_rate.upper().replace("'", '"')


def change_currency(price, dm_currency, currency, exchange_rate):
    rate_dict = json.loads(exchange_rate.replace("'", '"'))
    return float(price) * rate_dict.get(dm_currency, 1)


def fix_article_id(article_id):
    if len(article_id) < 10:
        return '0' * (10 - len(article_id)) + article_id
    else:
        return article_id


def get_transformed_transactions(transactions_train_raw, articles_raw, years, months, dm_currency):
    uppercase_exchange_rate_udf = udf(uppercase_exchange_rate, StringType())
    change_currency_udf = udf(change_currency, StringType())
    fix_article_id_udf = udf(fix_article_id, StringType())
    
    return transactions_train_raw.filter((func.year(col('t_dat')).isin(years)) &
                                         (func.month(col('t_dat')).isin(months))) \
                                 .withColumn('exchange_rate',
                                             uppercase_exchange_rate_udf(col('current_exchange_rate'))) \
                                 .withColumn('price',
                                             change_currency_udf(col('price'), lit(dm_currency), col('currency'),
                                                                 col('exchange_rate')).cast(DecimalType(38, 20))) \
                                 .withColumn('article_id', fix_article_id_udf(col('article_id'))) \
                                 .join(articles_raw, 'article_id', 'left') \
                                 .withColumnRenamed('_c0', 'row_num') \
                                 .select('row_num', 't_dat', 'customer_id', 'article_id', 'price', 'product_group_name')


def get_searched_date_transactions(transactions_train_raw, articles_raw, part_date, dm_currency):
    return transactions_train_raw.filter((func.year(col('t_dat')) == part_date.year) &
                                         (func.month(col('t_dat')) == part_date.month))


def get_prev_data_mart(output_path, part_date, dm_currency, loyality_level, spark):
    path = output_path + part_date.strftime('%Y-%m-%d') + f'_{dm_currency}' + f'_loyal_nr_{loyality_level}/part*.csv'
    prev_data_mart_raw = spark.read.csv(path, header=True)
    prev_data_mart_df = prev_data_mart_raw.filter(col('customer_loyality') == 1) \
                                          .select('customer_id', 'customer_loyality')
    return prev_data_mart_df
    

def get_most_exp_articles(transactions_train_df):
    window_most_exp_art = Window.partitionBy('customer_id').orderBy(func.desc('price'), 'row_num')
    
    return transactions_train_df.withColumn('row_number', func.row_number().over(window_most_exp_art)) \
                                .filter(col('row_number') == 1) \
                                .select('customer_id', 'article_id') \
                                .withColumnRenamed('article_id', 'most_exp_article_id')


def get_grouped_product_groups(transactions_train_df):
    return transactions_train_df.groupBy('customer_id', 'product_group_name') \
                                .agg(count('*').alias('product_count')) \
                                .orderBy(func.asc(col('customer_id')), func.desc(col('product_count'))) \
                                .groupBy('customer_id') \
                                .agg(func.first('product_group_name').alias('most_freq_product_group_name'),
                                     func.count('product_group_name').alias('number_of_product_groups')) \
                                .orderBy('customer_id')


def grouped_transactions_by_customer(transactions_train_df):
    return transactions_train_df.select('customer_id', 'price') \
                                .groupBy('customer_id') \
                                .agg(func.sum('price').alias('transaction_amount'),
                                     func.count('price').alias('number_of_articles'))


def save_output(most_exp_articles_df, number_of_product_groups_df, grouped_transactions_df,
                customers_raw, part_date, output_path, dm_currency, loyality_level, prev_df):

    result_df = grouped_transactions_df.join(number_of_product_groups_df, 'customer_id', 'left') \
                                       .join(most_exp_articles_df, 'customer_id', 'left') \
                                       .join(customers_raw, 'customer_id', 'left')

    if not prev_df:
        result_df = result_df.withColumn('customer_loyality', lit(1))
    else:
        result_df = result_df.join(prev_df, 'customer_id', 'left')

    result_df.select('customer_id', 'customer_loyality').show(truncate=False)

    result_df = result_df.withColumn('customer_group_by_age', when(col('age') < 23, 'S')
                                     .when(col('age') > 59, 'R')
                                     .otherwise('A')) \
                         .withColumn('customer_loyality', when(
                                        col('customer_loyality').isNotNull(),
                                        col('customer_loyality')
                                        ).otherwise(0)) \
                         .withColumn('part_date', func.lit(datetime.strftime(part_date, '%Y-%m'))) \
                         .withColumn('dm_currency', lit(dm_currency)) \
                         .withColumn('loyal_months_nr', lit(loyality_level)) \
                         .withColumn('offer', when(
                                        (col('customer_loyality') == 1) &
                                        (col('club_member_status') == 'ACTIVE') &
                                        (col('fashion_news_frequency') == 'Regularly'),
                                        1
                                        ).otherwise(0)) \
                         .select('part_date', 'customer_id', 'customer_group_by_age',
                                 'transaction_amount', 'dm_currency', 'most_exp_article_id',
                                 'number_of_articles', 'number_of_product_groups',
                                 'most_freq_product_group_name', 'loyal_months_nr', 'customer_loyality', 'offer')

    result_df.show(truncate=False)

    result_df.coalesce(1) \
             .write \
             .mode('overwrite') \
             .option('header', 'True') \
             .option('delimiter', ',') \
             .csv(output_path + part_date.strftime('%Y-%m-%d') + f'_{dm_currency}' + f'_loyal_nr_{loyality_level}')


def process_data_mart(part_dates, dm_currency):
    config = ConfigParser()
    config.read('config.ini')
    
    articles_path = config.get('PATHS', 'articles_path')
    customers_path = config.get('PATHS', 'customers_path')
    transactions_train_path = config.get('PATHS', 'transactions_train_path')
    output_path = config.get('PATHS', 'output_path')

    spark = SparkSession.builder.appName('create_data_mart').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    articles_raw = spark.read.csv(articles_path, header=True)
    customers_raw = spark.read.csv(customers_path, header=True)
    transactions_train_raw = spark.read.csv(transactions_train_path, header=True)
    
    transactions_train_raw.show()

    if type(part_dates) != list:
        part_dates = [(part_dates, True, 1), ]

    years = [d[0].year for d in part_dates]
    months = [m[0].month for m in part_dates]

    transactions_train_transformed_df = get_transformed_transactions(transactions_train_raw, articles_raw,
                                                                     years, months, dm_currency)

    for num, (part_date, need_to_create, loyality_level) in enumerate(part_dates):
        if not need_to_create:
            continue
            
        if loyality_level != 1:
            prev_date, _, prev_loyality_level = part_dates[num - 1]
            prev_df = get_prev_data_mart(output_path, prev_date, dm_currency, prev_loyality_level, spark)
            prev_df.show()
        else:
            prev_df = False
        
        transactions_train_df = get_searched_date_transactions(transactions_train_transformed_df,
                                                               articles_raw, part_date, dm_currency)
        transactions_train_df.show(truncate=False)

        most_exp_articles_df = get_most_exp_articles(transactions_train_df)
        most_exp_articles_df.show(truncate=False)

        number_and_most_freq_product_groups_df = get_grouped_product_groups(transactions_train_df)
        number_and_most_freq_product_groups_df.show(truncate=False)

        grouped_transactions_df = grouped_transactions_by_customer(transactions_train_df)
        grouped_transactions_df.show(truncate=False)

        save_output(most_exp_articles_df, number_and_most_freq_product_groups_df, grouped_transactions_df,
                    customers_raw, part_date, output_path, dm_currency, loyality_level, prev_df)


def get_min_date(transactions_train_path):
    ss = SparkSession.builder.appName('min_date').getOrCreate()
    
    transactions_df = ss.read.csv(transactions_train_path, header=True)
    min_date_row = transactions_df.select(func.min(transactions_df['t_dat'])).first()
    
    min_date = datetime.strptime(min_date_row[0], '%Y-%m-%d')
    
    return min_date


if __name__ == '__main__':
    p_date, dm_curr = get_args()
    process_data_mart(p_date, dm_curr)

