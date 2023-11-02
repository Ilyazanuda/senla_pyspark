from dateutil.relativedelta import relativedelta
from datetime import datetime
from configparser import ConfigParser
import create_datamart
import argparse
import shutil
import os


def get_args():
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

    parser.add_argument('-cpd', '--check_prev_dates',
                        action='store_true',
                        default=False,
                        help='argument to check previous months data mart')

    args = parser.parse_args()
    
    return (datetime.strptime(args.part_date, '%Y-%m-%d'),
            args.dm_currency.upper() if args.dm_currency and args.dm_currency.upper() in ('USD', 'PLN', 'BYN', 'EUR') else 'USD',
            args.check_prev_dates)


def get_folder_size(path):
    total_size = 0
    
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if not filename.startswith('.'):
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
                
    return total_size / (1024 ** 2)
    

def check_prev_dates_mart(part_date, output_path, dm_currency, min_date):
    last_month = part_date.replace(day=1) - relativedelta(days=1)
    pre_last_month = part_date.replace(day=1) - relativedelta(months=1, days=1)

    results = []

    if get_folder_size(output_path + last_month.strftime('%Y-%m-%d') + f'_{dm_currency}') < 0.001 and \
            last_month.replace(day=1) >= min_date.replace(day=1):
        results.append(last_month)
        
    if get_folder_size(output_path + pre_last_month.strftime('%Y-%m-%d') + f'_{dm_currency}') < 0.001 and \
            pre_last_month.replace(day=1) >= min_date.replace(day=1):
        results.append(pre_last_month)
        
    return results


def main(part_date, dm_currency, check_prev_dates):
    config = ConfigParser()
    config.read('config.ini')
    
    transactions_path = config.get('PATHS', 'transactions_train_path')
    output_path = config.get('PATHS', 'output_path')

    part_dates = [part_date, ]

    if check_prev_dates:
        min_date = create_datamart.get_min_date(transactions_path)
        
        for prev_date in check_prev_dates_mart(part_date, output_path, dm_currency, min_date):
            print('have not data mart with date:', prev_date.strftime('%Y-%m-%d'))
            part_dates.append(prev_date)

    create_datamart.process_data_mart(part_dates, dm_currency)
    

if __name__ == '__main__':  
    p_date, dm_curr, ch_prev_dates = get_args()
    
    main(p_date, dm_curr, ch_prev_dates)

