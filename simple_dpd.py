from math import ceil, floor
from datetime import date
import datetime
import calendar

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from workalendar.asia import Philippines

# BigQuery Credentials
KEY_PATH = 'keys/google_key.json'
CREDENTIALS = service_account.Credentials.from_service_account_file(KEY_PATH)
PROJECT_ID = 'su-scgc-prod-dw'
pandas_gbq.context.credentials = CREDENTIALS
pandas_gbq.context.project = PROJECT_ID

CURRENT_DATE = date.today()

cal = Philippines()


def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return datetime.date(year, month, day)


def format_date(df, cols=('FDD', 'LDD', 'TR_DATE'), date_format='%Y%m%d'):
    # use date_format='%Y-%m-%d' for processed files
    for col in cols:
        try:
            df[col] = pd.to_datetime(df[col], format=date_format).dt.date
        except:
            continue
    return df


def get_account(acct_no):
    query = f"""
            SELECT
              distinct
              sltran.ACCT_NO,
              sltran.TR_DATE,
              sltran.PARTICULAR,
              sltran.DEBIT,
              sltran.CREDIT,
              sltran.REBATE,
              sltran.PENALTY,
              sltran.BALANCE,
              slhdr.AMORT ma,
              slhdr.FDD,
              slhdr.LDD
            FROM
              `su-scgc-prod-dw.mis_10_source.sltran` sltran
            LEFT JOIN mis_10_source.slhdr slhdr
              ON sltran.ACCT_NO = slhdr.ACCT_NO 
            WHERE
              sltran.ACCT_NO = '{acct_no}'
            ORDER BY
              sltran.TR_DATE ASC
            """

    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID)


def compute_penalty(current_payment_date, last_due_date, ma, penalty_rate=0.05):
    penalty = ceil(ma * penalty_rate * (current_payment_date - last_due_date).days/30)
    print(f"Penalty for {current_payment_date} from last due date {last_due_date} "
          f"({(current_payment_date - last_due_date).days} days): {penalty}")
    return penalty


def compute_rebate(ma, total_paid_trans, temp_pd_digit, rebate=200):
    if total_paid_trans > 0:
        rebate = floor(total_paid_trans/(ma - rebate) - temp_pd_digit) * rebate
        print(f"Rebate for this term {rebate}")
    return rebate


def get_next_working_day(source_date):
    # while not cal.is_working_day(source_date):
    while source_date.weekday() == 6 or cal.is_holiday(source_date):
        source_date = datetime.date(source_date.year, source_date.month, source_date.day + 1)
    return source_date


# def compute_rebate(ma, current_balance, due_date, tr_date, rebate=200):
#     if not cal.is_working_day(tr_date):
#         due_date = get_next_working_day(due_date)
#     if tr_date <= due_date:
#         rebate = (floor(abs(current_balance / ma)) + 1) * rebate
#     print(f"Rebate for this term {rebate}")
#     return rebate


def reconcile(df, verbose=False):
    fdd = df['FDD'].dropna().iloc[0]
    ldd = df['LDD'].dropna().iloc[0]

    max_tr_date = df['TR_DATE'].max()

    ma = df.ma.iloc[0]

    due_date = fdd
    due_date_old = add_months(due_date, -1)

    installment_details = df.loc[df.PARTICULAR == 'INSTALLMENT SALES']
    installment_sales = df.loc[df.PARTICULAR == 'INSTALLMENT SALES']['DEBIT'].values[0]
    df = df.loc[~df.PARTICULAR.str.contains('INSTALLMENT SALES')]

    # remove unrecognized rebates
    unrecognized_rebates = df.loc[df.PARTICULAR.str.contains('UNRECOGNIZED REBATE')]
    df = df.loc[~df.PARTICULAR.str.contains('UNRECOGNIZED REBATE')]

    # remove rev of earned s
    rev_earned_s = df.loc[df.PARTICULAR.str.contains('REV OF EARNED S')]
    df = df.loc[~df.PARTICULAR.str.contains('REV OF EARNED S')]

    try:
        dp_details = df.loc[df.PARTICULAR == 'DOWN PAYMENT']
        dp = df.loc[df.PARTICULAR == 'DOWN PAYMENT']['CREDIT'].values[0]
        df = df.loc[~df.PARTICULAR.str.contains('DOWN PAYMENT')]
    except:
        dp = 0

    # last_payment_date = add_months(due_date, -1)
    payment_ma_total = df.loc[(df.TR_DATE <= due_date_old)]['CREDIT'].sum()
    payment_due = 0
    current_month_balance = payment_due - payment_ma_total
    current_total_balance = installment_sales - dp - payment_ma_total
    pd_string = ''
    step = 0
    temp_pd_digit = 0
    rebate_final_total = 0
    penalty_final_total = 0

    bayanihan_start_date = date(2020, 4, 17)
    bayanihan_end_date = date(2020, 7, 17)
    is_bayanihan = False

    while CURRENT_DATE > due_date:
        if verbose:
            print('-+' * 45)
        if add_months(due_date, -1) > max_tr_date > ldd:
            print('broken')
            break

        step += 1
        if due_date < add_months(ldd, 1):
            payment_due += ma

        transactions = df.loc[(df.TR_DATE <= due_date) & (df.TR_DATE > due_date_old)]

        payment_made = 0
        penalty_total = 0
        rebate_payment_total = 0

        if pd_string:
            temp_pd_digit = int(pd_string[-1])

        temp_balance = current_month_balance + ma

        for tr_date in transactions.TR_DATE.unique():

            day_transaction = transactions.loc[transactions.TR_DATE == tr_date]

            total_paid_trans = day_transaction['CREDIT'].sum()
            total_rebate_trans = day_transaction['REBATE'].sum()
            actual_paid_trans = total_paid_trans - total_rebate_trans

            if temp_pd_digit == 0 or temp_balance == ma:
                temp_balance -= 200
            temp_balance -= total_paid_trans

            if verbose:
                print(f'temp pd digit {temp_pd_digit}')
                print(f'temp balance {tr_date}: {temp_balance}')

            # get rebate
            if temp_balance <= 200:
                rebate = compute_rebate(ma, total_paid_trans, temp_pd_digit)
                df.loc[df.TR_DATE == tr_date, 'recomputed_rebate'] = rebate
            else:
                rebate = 0

            # bayanihan rebate
            if is_bayanihan:
                rebate = floor(actual_paid_trans / (ma - 200)) * 200
                print(f'Bayanihan rebate: {rebate}')

            payment_made += actual_paid_trans + rebate

            rebate_payment_total += rebate
            rebate_final_total += rebate

            temp_pd_digit = ceil(temp_balance / ma) - 1
            # print(f't bal {temp_balance}')
            print(f'temp_pd_digit {temp_pd_digit}')
            current_total_balance -= actual_paid_trans + rebate
            df.loc[df.TR_DATE == tr_date, 'recomputed_total_balance'] = current_total_balance

        payment_ma_total += payment_made
        current_month_balance = payment_due - payment_ma_total

        pd_char = str(ceil(current_month_balance/ma))
        if current_month_balance < 0:
            pd_char = '0'

        try:
            df.loc[df.TR_DATE == tr_date, 'PD Char'] = pd_char
        except:
            pass

        pd_string += pd_char

        if verbose:

            print(due_date)
            print(transactions[['TR_DATE', 'PARTICULAR', 'REBATE', 'DEBIT', 'CREDIT']])
            # print('PAYMENT MADE: ', payment_made)
            print('PAYMENT DUE: ', payment_due)
            print('PENALTY:', penalty_total)
            print('REBATE:', rebate_payment_total)
            print('TOTAL PAID: ', payment_ma_total)
            print('BALANCE: ', current_month_balance)
            print('PD String: ', pd_string)

            print('TOTAL BALANCE:', installment_sales - payment_ma_total - dp)

        due_date_old = due_date
        due_date = add_months(fdd, step)

        # if not cal.is_working_day(due_date):
        if due_date.weekday() == 6 or cal.is_holiday(due_date):
            due_date = get_next_working_day(due_date)

        is_bayanihan = False
        if bayanihan_start_date < due_date < bayanihan_end_date:
            is_bayanihan = True

    df = df.append(installment_details)
    df = df.append(rev_earned_s)
    df = df.append(unrecognized_rebates)
    try:
        df = df.append(dp_details)
    except:
        pass

    penalty_final_total -= df.loc[df.PARTICULAR == 'PENALTY']['DEBIT'].sum() - rev_earned_s['CREDIT'].sum()
    rebate_final_total -= unrecognized_rebates['CREDIT'].sum() + df['REBATE'].sum()
    return df, [pd_string, rebate_final_total, penalty_final_total]


def sample():
    acct_no = '00561289'
    df = get_account(acct_no)
    df = format_date(df)
    df, other_info = reconcile(df, verbose=True)

    pd_string = other_info[0]
    df = df.sort_values(by='TR_DATE', ascending=False)
    unrecognized_rebate = other_info[1]
    rev_earned_s = other_info[2]
