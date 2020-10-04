import pandas as pd
import datetime

CSV_DATE_FORMAT = '%Y-%m-%d'

def extract_data(csv_file1, csv_file2):
    df1 = pd.read_csv(csv_file1, parse_dates=['date'], infer_datetime_format=True)
    df2 = pd.read_csv(csv_file2, usecols=['Date', 'Country/Region', 'Recovered'], parse_dates=['Date'], infer_datetime_format=True)
    return df1, df2

def validate_data(df1, df2):
    # we're interested only in US data
    df2 = df2[df2['Country/Region'] == 'US']

    # 1. date
    df1['date'] = pd.to_datetime(df1['date'])
    df2['date'] = pd.to_datetime(df2['Date'])

    # 2. cases, deaths, recovered must be non=negative integers
    df1['cases'] = df1['cases'].astype(int)
    df1['deaths'] = df1['deaths'].astype(int)
    df2['Recovered'] = df2['Recovered'].astype(int)
    
    neg_int_count = len(df1.loc[(df1['cases']<0) | (df1['deaths']<0)].index)    
    if neg_int_count>0:
        raise ValueError('There are negative values in NYT dataset')

    neg_int_count = len(df2.loc[df2['Recovered']<0].index)
    if neg_int_count>0:
        raise ValueError('There are negative values in John Hopkins dataset')

    # 3. no empty values
    if (df1.isna().sum().sum()!=0) or (df2.isna().sum().sum()!=0):
        raise ValueError('Some data is missing in csv file')

def transform_data(df1, df2):
    # doing inner join here because the requirement is to remove all days that don't exist in both datasets
    df = pd.merge(df1, df2[df2['Country/Region'] == 'US'], left_on='date', right_on='Date', how='inner')
    df.drop(['Date', 'Country/Region'], axis=1, inplace=True)
    df.rename(columns={'Recovered': 'recovered'}, inplace=True)
    df['recovered'] = df['recovered'].astype(int)
    #df = df.set_index('date')
    return df

def prepare_data(csv_file1, csv_file2):
    df1, df2 = extract_data(csv_file1, csv_file2)
    validate_data(df1, df2)
    df = transform_data(df1, df2)
    return df