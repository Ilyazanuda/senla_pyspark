from dateutil.relativedelta import relativedelta
from datetime import datetime
from configparser import ConfigParser
import create_datamart
import argparse
import os


def get_args() -> tuple:
    """
    Returns a tuple of parsed arguments from command line.

    :return: (part_date: datetime, dm_currency: str, loyalty_level: int)
    """
    parser = argparse.ArgumentParser(description="start create data mart",
                                     prog="start_create_datamart")
    
    parser.add_argument('-pd', '--part_date',
                        type=str,
                        metavar='<yyyy-mm-dd>',
                        help='searched date')

    parser.add_argument('-dc', '--dm_currency',
                        type=str,
                        metavar='<currency>',
                        default='USD',
                        help='currency: USD, BYN, PLN, EUR')
    
    parser.add_argument('-ll', '--loyalty_level',
                        type=int,
                        metavar='<number>',
                        default=1,
                        help='number of months for loyalty calculations')

    args = parser.parse_args()
    
    return (datetime.strptime(args.part_date, '%Y-%m-%d'),
            args.dm_currency.upper() if args.dm_currency and args.dm_currency.upper() in ('USD', 'PLN', 'BYN', 'EUR')
            else 'USD',
            args.loyalty_level)


def get_folder_size(path: str) -> float:
    """
    Returns the total size of the folder.

    :param path: the path to the folder
    :return: size
    """
    total_size = 0
    
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if not filename.startswith('.'):
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
                
    return total_size / (1024 ** 2)


def get_prev_date(part_date: datetime, min_date: datetime) -> datetime or None:
    """
    Returns the previous date if it exists in transactions_train_with_currency.csv

    :param part_date: current date
    :param min_date: the earliest date from transactions_train_with_currency
    :return: previous date or None (if date does not exist)
    """
    prev_date = part_date.replace(day=1) - relativedelta(days=1)
    if prev_date.replace(day=1) >= min_date.replace(day=1):
        return prev_date
    else:
        return None


def check_prev_date_mart(part_date: datetime, output_path: str, dm_currency: str, loyalty_num: int) -> tuple:
    """
    Checks whether it is necessary to create a data mart for the given date or not.

    :param part_date: selected date
    :param output_path: path with saved results
    :param dm_currency: selected currency symbol
    :param loyalty_num: loyalty number
    :return: (part_date: datetime, flag: bool)
    """
    if get_folder_size(output_path + part_date.strftime('%Y-%m-%d') +
                       f'_{dm_currency}' + f'_loyal_nr_{loyalty_num}') < 0.001:
        return part_date, True
        
    else:
        return part_date, False


def main(part_date: datetime, dm_currency: str, loyalty_level: int) -> None:
    """
    Creates a list with dates, the data marts creation flag and loyalty level.
    Runs a script to create data marts.

    :param part_date: selected date
    :param dm_currency: selected currency symbol
    :param loyalty_level: loyalty level number
    """
    config = ConfigParser()
    config.read('config.ini')
    
    transactions_path = config.get('PATHS', 'transactions_train_path')
    output_path = config.get('PATHS', 'output_path')

    part_dates = []
    min_date = create_datamart.get_min_date(transactions_path)

    # a list of possible dates in descending order is created
    for _ in range(loyalty_level, 0, -1):
        part_dates.append(part_date)

        prev_date = get_prev_date(part_date, min_date)
        part_date = prev_date

        if prev_date is None:
            break

    # a list is reversed to ascending order
    part_dates = part_dates[::-1]
    part_dates_amount = len(part_dates)

    # flags and loyalty level numbers for values in list are added
    for ll_num in range(part_dates_amount):
        if ll_num == part_dates_amount - 1:
            part_dates[ll_num] = (part_dates[ll_num], True, ll_num + 1)
            continue
            
        part_date, need_to_create = check_prev_date_mart(part_dates[ll_num], output_path, dm_currency, ll_num + 1)
        part_dates[ll_num] = (part_date, need_to_create, ll_num + 1)

    create_datamart.process_data_mart(part_dates, dm_currency)
    

if __name__ == '__main__':  
    p_date, dm_curr, loyalty_lvl = get_args()
    
    main(p_date, dm_curr, loyalty_lvl)

