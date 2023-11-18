from pyspark.sql.types import DecimalType, StringType   # StructType, DoubleType, StructField
from pyspark.sql.functions import lit, when, col, count, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from datetime import datetime
from configparser import ConfigParser
import argparse
import json


def get_args() -> tuple:
    """
    Returns a tuple of parsed arguments from the command line.

    :return: (part_date: datetime, dm_currency: str)
    """
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


def uppercase_exchange_rate(currency_exchange_rate: str) -> str:
    """
    Converts the string with exchange rates to uppercase.

    :param currency_exchange_rate: json format string with exchange rates
    :return: currency_exchange_rate string in upper case
    """
    return currency_exchange_rate.upper().replace("'", '"')


def price_conversion(price: str,
                     dm_currency: str,
                     exchange_rate: str) -> float:
    """
    Converts the price based on the currency exchange rate for dm_currency.

    :param price: article price
    :param dm_currency: selected currency symbol
    :param exchange_rate: json format string with exchange rates
    :return: converted price
    """
    rate_dict = json.loads(exchange_rate.replace("'", '"'))

    return float(price) * rate_dict.get(dm_currency, 1)


def fix_article_id(article_id: str) -> str:
    """
    Restores the article_id.

    :param article_id: invalid string with article id
    :return: correct article_id
    """
    if len(article_id) < 10:
        return '0' * (10 - len(article_id)) + article_id

    else:
        return article_id


def get_transformed_transactions(transactions_train_raw: DataFrame,
                                 articles_raw: DataFrame,
                                 years: list,
                                 months: list,
                                 dm_currency: str) -> DataFrame:
    """
    Transforms and returns a DataFrame:
        - Selects data only from the specified months and years.
        - Converts the price based on the selected currency symbol (dm_currency).
        - Adjusts the article_id.

    :param transactions_train_raw: raw DataFrame from transactions_train_with_currency.csv
    :param articles_raw: raw DataFrame from articles.csv
    :param years: list with all selected years
    :param months: list with all selected months
    :param dm_currency: selected currency symbol
    :return: transformed_transactions DataFrame
    """
    uppercase_exchange_rate_udf = udf(uppercase_exchange_rate, StringType())
    price_conversion_udf = udf(price_conversion, StringType())
    fix_article_id_udf = udf(fix_article_id, StringType())
    
    return transactions_train_raw.filter((func.year(col('t_dat')).isin(years)) &
                                         (func.month(col('t_dat')).isin(months))) \
                                 .withColumn('exchange_rate',
                                             uppercase_exchange_rate_udf(col('current_exchange_rate'))) \
                                 .withColumn('price',
                                             price_conversion_udf(col('price'), lit(dm_currency),
                                                                  col('exchange_rate')).cast(DecimalType(38, 20))) \
                                 .withColumn('article_id', fix_article_id_udf(col('article_id'))) \
                                 .join(articles_raw, 'article_id', 'left') \
                                 .withColumnRenamed('_c0', 'row_num') \
                                 .select('row_num', 't_dat', 'customer_id', 'article_id', 'price', 'product_group_name')


def get_selected_date_transactions(transformed_transactions_train_df: DataFrame,
                                   part_date: DataFrame) -> DataFrame:
    """
    Returns the transactions_train DataFrame with data for a specific (desired) year-month.

    :param transformed_transactions_train_df: transformed transactions_train with data from all selected dates
    :param part_date: selected date
    :return: transactions_train DataFrame with data for a specific year-month
    """
    return transformed_transactions_train_df.filter((func.year(col('t_dat')) == part_date.year) &
                                                    (func.month(col('t_dat')) == part_date.month))


def get_prev_data_mart(output_path: str,
                       prev_date: datetime,
                       dm_currency: str,
                       loyalty_level: int,
                       spark: SparkSession) -> DataFrame:
    """
    Returns a DataFrame with columns representing customer loyalty for the previous date.

    :param output_path: path with saved results
    :param prev_date: previous date
    :param dm_currency: selected currency symbol
    :param loyalty_level: loyalty level number
    :param spark: spark session object
    :return: DataFrame with customer loyalty columns
    """
    path = output_path + prev_date.strftime('%Y-%m-%d') + f'_{dm_currency}' + f'_loyal_nr_{loyalty_level}/part*.csv'
    prev_data_mart_raw = spark.read.csv(path, header=True)
    prev_data_mart_df = prev_data_mart_raw.select('customer_id', 'customer_loyalty', 'loyal_months_nr')

    return prev_data_mart_df
    

def get_most_exp_articles(transactions_train_df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the IDs of the most expensive articles purchased by each customer
    for a specific year-month.

    :param transactions_train_df: transactions_train DataFrame for a specific year-month
    :return: most_exp_articles DataFrame
    """
    window_most_exp_art = Window.partitionBy('customer_id').orderBy(func.desc('price'), 'row_num')
    
    return transactions_train_df.withColumn('row_number', func.row_number().over(window_most_exp_art)) \
                                .filter(col('row_number') == 1) \
                                .select('customer_id', 'article_id') \
                                .withColumnRenamed('article_id', 'most_exp_article_id')


def get_grouped_product_groups(transactions_train_df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the count of product groups purchased by each customer and
    names of the most frequently purchased product group for a specific year-month.

    :param transactions_train_df: transactions_train DataFrame for a specific year-month
    :return: grouped_product_groups DataFrame
    """
    return transactions_train_df.groupBy('customer_id', 'product_group_name') \
                                .agg(count('*').alias('product_count')) \
                                .orderBy(func.asc(col('customer_id')), func.desc(col('product_count'))) \
                                .groupBy('customer_id') \
                                .agg(func.first('product_group_name').alias('most_freq_product_group_name'),
                                     func.count('product_group_name').alias('number_of_product_groups')) \
                                .orderBy('customer_id')


def grouped_transactions_by_customer(transactions_train_df: DataFrame) -> DataFrame:
    """
    Returns a transactions DataFrame with data grouped by customer for each year-month.

    :param transactions_train_df: transactions_train DataFrame for a specific year-month
    :return: grouped_transactions_by_customer DataFrame
    """
    return transactions_train_df.select('customer_id', 'price') \
                                .groupBy('customer_id') \
                                .agg(func.sum('price').alias('transaction_amount'),
                                     func.count('price').alias('number_of_articles'))


def save_datamart(most_exp_articles_df: DataFrame,
                  grouped_product_groups_df: DataFrame,
                  grouped_transactions_df: DataFrame,
                  customers_raw: DataFrame,
                  part_date: datetime,
                  output_path: str,
                  dm_currency: str,
                  loyalty_level: int,
                  prev_df: bool or DataFrame) -> None:
    """
    Performs loyalty calculations, determines customer age groups,
    determines whether to make an offer, joins DataFrames with other calculations,
    and saves the resulting DataFrame.

    :param most_exp_articles_df: DataFrame with most expensive articles id
    :param grouped_product_groups_df: DataFrame with amount of purchased products groups and
    names of the most frequently purchased product group by customer
    :param grouped_transactions_df: DataFrame with grouped data by customer and dates
    :param customers_raw: raw DataFrame with info about customers
    :param part_date: selected date
    :param output_path: path to folder for saved data marts
    :param dm_currency: selected currency symbol
    :param loyalty_level: loyalty level number
    :param prev_df: previous DataFrame
    """
    result_df = grouped_transactions_df.join(grouped_product_groups_df, 'customer_id', 'left') \
                                       .join(most_exp_articles_df, 'customer_id', 'left') \
                                       .join(customers_raw, 'customer_id', 'left')

    if not prev_df:
        result_df = result_df.withColumn('customer_loyalty', lit(1)) \
                             .withColumn('loyal_months_nr', lit(1))
    else:
        result_df = result_df.join(prev_df, 'customer_id', 'left') \
                             .withColumn('customer_loyalty',
                                         when(col('customer_loyalty').isNotNull(),
                                              col('customer_loyalty'))
                                         .otherwise(0).cast('int')) \
                             .withColumn('loyal_months_nr',
                                         when(col('loyal_months_nr').isNotNull(),
                                              col('loyal_months_nr') + 1)
                                         .otherwise(1).cast('int'))

    result_df = result_df.withColumn('customer_group_by_age',
                                     when(col('age') < 23, 'S')
                                     .when(col('age') > 59, 'R')
                                     .otherwise('A')) \
                         .withColumn('part_date', func.lit(datetime.strftime(part_date, '%Y-%m-%d'))) \
                         .withColumn('dm_currency', lit(dm_currency)) \
                         .withColumn('offer',
                                     when((col('customer_loyalty') == 1) &
                                          (col('club_member_status') == 'ACTIVE') &
                                          (col('fashion_news_frequency') == 'Regularly'),
                                          1)
                                     .otherwise(0)) \
                         .select('part_date', 'customer_id', 'customer_group_by_age',
                                 'transaction_amount', 'dm_currency', 'most_exp_article_id',
                                 'number_of_articles', 'number_of_product_groups',
                                 'most_freq_product_group_name', 'loyal_months_nr', 'customer_loyalty', 'offer')

    result_df.coalesce(1) \
             .write \
             .mode('overwrite') \
             .option('header', 'True') \
             .option('delimiter', ',') \
             .csv(output_path + part_date.strftime('%Y-%m-%d') + f'_{dm_currency}' + f'_loyal_nr_{loyalty_level}')


def process_data_mart(part_dates: list or datetime, dm_currency: str) -> None:
    """
    Creates DataFrames from source CSV files, creates and saves data marts for specific dates.

    :param part_dates: list with dates, the data marts creation flag, and loyalty level number
    :param dm_currency: selected currency symbol
    """
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

    if type(part_dates) != list:
        part_dates = [(part_dates, True, 1), ]

    years = [d[0].year for d in part_dates]
    months = [m[0].month for m in part_dates]

    # get fixed dataframe
    transactions_train_transformed_df = get_transformed_transactions(transactions_train_raw, articles_raw,
                                                                     years, months, dm_currency)

    # creating data marts by date
    for num, (part_date, flag, loyalty_level) in enumerate(part_dates):

        # if the flag is False, previous data mart exists
        if not flag:
            continue

        # if loyalty_level is 1, previous data mart is not necessary
        if loyalty_level != 1:
            prev_date, _, prev_loyalty_level = part_dates[num - 1]
            prev_df = get_prev_data_mart(output_path, prev_date, dm_currency, prev_loyalty_level, spark)
        else:
            prev_df = False

        transactions_train_df = get_selected_date_transactions(transactions_train_transformed_df, part_date)

        most_exp_articles_df = get_most_exp_articles(transactions_train_df)

        grouped_product_groups_df = get_grouped_product_groups(transactions_train_df)

        grouped_transactions_df = grouped_transactions_by_customer(transactions_train_df)

        save_datamart(most_exp_articles_df, grouped_product_groups_df, grouped_transactions_df,
                      customers_raw, part_date, output_path, dm_currency, loyalty_level, prev_df)


def get_min_date(transactions_train_path: str) -> datetime:
    """
    Returns the earliest date from the transactions_train_with_currency.

    :param transactions_train_path: path for transactions_train_with_currency
    :return: the earliest date in transactions_train_with_currency
    """
    ss = SparkSession.builder.appName('min_date').getOrCreate()
    
    transactions_df = ss.read.csv(transactions_train_path, header=True)
    min_date_row = transactions_df.select(func.min(transactions_df['t_dat'])).first()

    min_date = datetime.strptime(min_date_row[0], '%Y-%m-%d')
    
    return min_date


if __name__ == '__main__':
    p_date, dm_curr = get_args()
    process_data_mart(p_date, dm_curr)
