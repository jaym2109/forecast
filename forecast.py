import pandas as pd
import numpy as np
from pandas import datetime as dt
import sqlConfig as sql
import os
import shutil
import datetime
from csv import writer

# Open SQL Connections for Viewpoint & SpruceWare
vp_sql = sql.SQL_Config('JAM-APP-002.jamacdonald.com', 'Viewpoint')
wbs_sql = sql.SQL_Config('WOL-APP-001.jamacdonald.com', 'SpruceDotNet')
bis_sql = sql.SQL_Config('JAM-SQL-001.jamacdonald.com', 'GPPRD')

# grabs the actual date from the users inputed fiscal year and month.


def getActualDate(fiscal_year, month):
    year = fiscal_year
    if(month > 10):
        year = fiscal_year - 1
    print(datetime.date(year, month, 1))
    return datetime.date(year, month, 1)


# determines the fiscal period start and end dates
def getFiscalDates(curr_date):
    start_year = curr_date.year
    end_year = curr_date.year
    if(curr_date.month <= 10):
        start_year = curr_date.year - 1
    else:
        end_year = curr_date.year + 1
    return {
        "start_date": datetime.date(start_year, 11, 1),
        "end_date": datetime.date(end_year, 10, 31)
    }

# gets the financial forecasts from Viewpoint and SpruceWare and consolidates
# them into one dataset for the fiscal year.


def getFinancialForecasts(curr_date, fy_begin_date, fy_end_date):
    vp_df = getViewpointFinancials(curr_date, fy_begin_date, fy_end_date)
    dynamics_df = getDynamicsFinancials(curr_date)
    vp_df = vp_df.append(dynamics_df, ignore_index=True)
    wbs_df = getWBSFinancials(curr_date)
    return vp_df.append(wbs_df, ignore_index=True)


# get the financials from dynamics GP for ALX and Woollatts Building supply
def getDynamicsFinancials(curr_date):
    curr_date = datetime.date(curr_date.year, curr_date.month + 1, 1)
    print(curr_date)
    dynamics_actuals = bis_sql.sqlStatement(f"""
        SELECT CASE WHEN b.ACTNUMBR_1 = 1001 THEN 10 ELSE 06 END as GLCo, TRXDATE as Mth, b.ACTNUMBR_2 as GLAcct, a.DEBITAMT - a.CRDTAMNT as Amount, 'Actuals' as Type
        FROM GL20000 a
        JOIN GL00100 b
            ON a.ACTINDX = b.ACTINDX
        WHERE TRXDATE >= '2019-04-01' and TRXDATE <= '{curr_date}'
    """)
    return dynamics_actuals

# gets the viewpoint financial datasets for actuals and forecasts based on the inputed
# month as a seperator


def getViewpointFinancials(curr_date, fy_begin_date, fy_end_date):
    vp_actuals = vp_sql.sqlStatement(f"""
                        SELECT GLCo, Mth, GLAcct as GLAcct, SUM(Amount) as Amount, 'Actuals' as Type
                        FROM GLDT
                        WHERE GLCo < 10 and GLCo <> 6 and Mth <= '{curr_date}' and Mth >= '{fy_begin_date}' and LEFT(GLAcct,4) >= 4000
                        GROUP BY GLCo, Mth, GLAcct
                        ORDER BY GLCo, GLAcct
                        """)

    vp_actuals_alx = vp_sql.sqlStatement(f"""
                        SELECT GLCo, Mth, GLAcct as GLAcct, SUM(Amount) as Amount, 'Actuals' as Type
                        FROM GLDT
                        WHERE GLCo = 6 and Mth < '2019-04-01'  and Mth >= '{fy_begin_date}' and LEFT(GLAcct,4) >= 4000
                        GROUP BY GLCo, Mth, GLAcct
                        ORDER BY GLCo, GLAcct
                        """)

    vp_actuals = vp_actuals.append(vp_actuals_alx, ignore_index=True)

    vp_forecasts = vp_sql.sqlStatement(f"""
                        SELECT GLCo, Mth, GLAcct as GLAcct, SUM(BudgetAmt) as Amount, 'Budget' as Type
                        FROM GLBD
                        WHERE GLCo < 10 and Mth > '{curr_date}' and Mth < '{fy_end_date}' and BudgetCode LIKE '%.1%' and LEFT(GLAcct, 4) >= 4000
                        GROUP BY GLCo, Mth, GLAcct
                        """)
    return vp_actuals.append(vp_forecasts, ignore_index=True)


def initializeWBSBudgets(dataframe, columns, fiscal_month, fiscal_year):
    updated_df = dataframe.filter(columns, axis=1)
    updated_df.columns = ['glacct', 'amount']
    updated_df['glco'] = 10
    updated_df['fiscal_month'] = fiscal_month
    updated_df['fiscal_year'] = fiscal_year
    updated_df['type'] = 'Budget'
    return updated_df

# gets the wbs financial datasets for actual and budget based on the inputed
# month seperator


def getWBSFinancials(curr_date):
    # seperate dates into fy year and month
    fiscal_month = curr_date.month
    fiscal_year = curr_date.year

    if (fiscal_month > 10):
        fiscal_year = fiscal_year + 1
        fiscal_month = fiscal_month - 10
    else:
        fiscal_month = fiscal_month + 2

    wbs_actuals = wbs_sql.sqlStatement(f"""
                                        SELECT 10 as GLCo, b.GLAcct, c.PostCycleNumber, c.PostFiscalYear, SUM(a.GLDebAmt - a.GLCredAmt) as Amount
                                        FROM GLJournalDtl a
                                        LEFT OUTER JOIN GLAccounts b
                                            ON a.GLID = b.GLIDInternal
                                        LEFT OUTER JOIN GLJournalHdr c
                                            ON a.DocIDInternal = c.DocIDInternal
                                        WHERE c.PostCycleNumber <> 13 and (c.PostCycleNumber <= 5 and c.PostFiscalYear = {fiscal_year}) and b.GLAcct >= 30000 and b.GLAcct < 99999
                                        GROUP BY b.GLAcct, c.PostFiscalYear, c.PostCycleNumber
                                        ORDER BY b.GLAcct, c.PostCycleNumber
                                        """)

    wbs_actuals['act_month'] = np.where(wbs_actuals['postcyclenumber'] < 3,
                                        wbs_actuals['postcyclenumber'] + 10, wbs_actuals['postcyclenumber'] - 2)
    wbs_actuals['act_year'] = np.where(
        wbs_actuals['postcyclenumber'] < 3, wbs_actuals['postfiscalyear'] - 1, wbs_actuals['postfiscalyear'])
    wbs_actuals['mth'] = wbs_actuals['act_year'].map(
        str) + '-' + wbs_actuals['act_month'].map(str) + '-01'
    wbs_actuals.drop(['postcyclenumber', 'postfiscalyear',
                      'act_month', 'act_year'], axis=1, inplace=True)
    wbs_actuals['type'] = 'actuals'

    wbs_budgets_raw = wbs_sql.sqlStatement(f"""
                                        SELECT b.GLAcct, a.GLBTotFY, a.GLBCycleAmount1, a.GLBCycleAmount2, a.GLBCycleAmount3, a.GLBCycleAmount4, a.GLBCycleAmount5, a.GLBCycleAmount6, a.GLBCycleAmount7, a.GLBCycleAmount8, a.GLBCycleAmount9, a.GLBCycleAmount10, a.GLBCycleAmount11, a.GLBCycleAmount12
                                        FROM GLAccountTotalsBudget a
                                        LEFT OUTER JOIN GLAccounts b
                                            ON a.GLIDInternal = b.GLIDInternal
                                        WHERE a.GLBTotAmount <> 0 and a.GLBTotFY = {fiscal_year}
                                        """)

    wbs_budgets_df1 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount1'], 1, fiscal_year)
    wbs_budgets_df2 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount2'], 2, fiscal_year)
    wbs_budgets_df3 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount3'], 3, fiscal_year)
    wbs_budgets_df4 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount4'], 4, fiscal_year)
    wbs_budgets_df5 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount5'], 5, fiscal_year)
    wbs_budgets_df6 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount6'], 6, fiscal_year)
    wbs_budgets_df7 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount7'], 7, fiscal_year)
    wbs_budgets_df8 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount8'], 8, fiscal_year)
    wbs_budgets_df9 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount9'], 9, fiscal_year)
    wbs_budgets_df10 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount10'], 10, fiscal_year)
    wbs_budgets_df11 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount11'], 11, fiscal_year)
    wbs_budgets_df12 = initializeWBSBudgets(
        wbs_budgets_raw, ['glacct', 'glbcycleamount12'], 12, fiscal_year)

    wbs_budgets = wbs_budgets_df1.append([wbs_budgets_df2, wbs_budgets_df3, wbs_budgets_df4, wbs_budgets_df5, wbs_budgets_df6,
                                          wbs_budgets_df7, wbs_budgets_df8, wbs_budgets_df9, wbs_budgets_df10, wbs_budgets_df11, wbs_budgets_df12])

    wbs_budgets['act_month'] = np.where(
        wbs_budgets['fiscal_month'] < 3, wbs_budgets['fiscal_month'] + 10, wbs_budgets['fiscal_month'] - 2)
    wbs_budgets['act_year'] = np.where(
        wbs_budgets['fiscal_month'] < 3, wbs_budgets['fiscal_year'] - 1, wbs_budgets['fiscal_year'])
    wbs_budgets['mth'] = wbs_budgets['act_year'].map(
        str) + '-' + wbs_budgets['act_month'].map(str) + '-01'
    wbs_budgets['mth'] = pd.to_datetime(wbs_budgets['mth'])
    mask = wbs_budgets['mth'] > curr_date
    wbs_budgets = wbs_budgets.loc[mask]
    wbs_budgets.drop(['fiscal_month', 'fiscal_year',
                      'act_month', 'act_year'], axis=1, inplace=True)

    wbs_budgets['amount'] = np.where(
        (wbs_budgets['glacct'] < 40000) |
        ((wbs_budgets['glacct'] > 81000) & (wbs_budgets['glacct'] <= 81600)) |
        (wbs_budgets['glacct'] == 81900) |
        (wbs_budgets['glacct'] == 82000), -wbs_budgets['amount'], wbs_budgets['amount'])

    return wbs_actuals.append(wbs_budgets, ignore_index=True)


def initializeForecasts():
    # ask the user to input the month and year of the forecast being generated
    fiscal_year = input('Enter Fiscal Year: ')
    month = input('Enter Month: ')

    curr_date = getActualDate(int(fiscal_year), int(month))
    fy_dates = getFiscalDates(curr_date)
    financial_df = getFinancialForecasts(
        curr_date, fy_dates['start_date'], fy_dates['end_date'])
    print(financial_df)
    financial_df.to_csv('forecast.csv', index=False)
    shutil.move('forecast.csv',
                'F:/Jason MacCuaig/BI Reports/Excel Reports/forecast.csv')


initializeForecasts()
