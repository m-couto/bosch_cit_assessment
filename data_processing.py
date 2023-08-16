import pandas as pd
from scipy import stats


def remove_duplicates(df):
    '''
    Remove repeated rows and a repeated column in the dep_energy dataset.
    '''

    # remove duplicate rows
    df = df.loc[~df.duplicated()]

    # remove repeated column
    if 'Groups With Access Code (French)' in df.columns:
        df = df.drop('Groups With Access Code (French)', axis=1)

    return df


def deal_with_missing_data(df, source, verbose):
    '''
    Handle missing data:
    - remove features with over 50% missing data.
    - discard entries in features with less than 2% missing data.

    The dataset from the Dep. of Energy exhibits many missing entries in EV-related features,
    due to the fact that those entries pertain to non-electric cars.
    I fill those missing entries with 'Not applicable' or 0 (depending on the feature type).
    '''

    n_missing_data = df.isna().sum()
    perc_missing_data = n_missing_data / df.shape[0] * 100


    # remove features with more than 50% missing data
    df = df.loc[:, perc_missing_data<50]
    if verbose:
        print(
            'Too many missing values - dropped these columns:',
            list(perc_missing_data[perc_missing_data>=50].index)
        )
    perc_missing_data = perc_missing_data[perc_missing_data<50]


    # remove entries for features with little missing data
    if verbose:
        print(
            'These columns have few missing values - dropped those entries:',
            list(perc_missing_data[perc_missing_data<2].index)
        )
    for col in perc_missing_data[perc_missing_data<2].index:
        df = df[~df[col].isna()]
    perc_missing_data = perc_missing_data[perc_missing_data>=2]


    # fill missing data in EV features
    if source=='dep_energy':
        
        # fill with 'Not Applicable'
        ev_cols_with_missing_data = [
            'EV Network', 'EV Network Web', 'EV Connector Types'
        ]
        for col in ev_cols_with_missing_data:
            df.loc[df[col].isna() & (df['Fuel Type Code']!='ELEC'), col] = 'Not Applicable'
            df = df[~df[col].isna()]

        # fill with zero values
        df.loc[
            df['EV Level2 EVSE Num'].isna() & (df['Fuel Type Code']!='ELEC'),
            'EV Level2 EVSE Num'
        ] = 0
        df = df[~df['EV Level2 EVSE Num'].isna()]

    elif source=='epa':

        df = df[~df['drive'].isna()]

    return df


def fix_data_types(df, source, verbose):
    '''
    Set date features to datetime type.
    '''

    if source=='dep_energy':

        df['Date Last Confirmed'] = pd.to_datetime(df['Date Last Confirmed'], format='%Y-%m-%d')
        df['Open Date'] = pd.to_datetime(df['Open Date'], format='%Y-%m-%d', errors='coerce')
        df['Updated At'] = pd.to_datetime(df['Updated At'])

        df = df.loc[~df['Open Date'].isna()]
    
    elif source=='epa':

        df['createdOn'] = pd.to_datetime(
            df['createdOn'].str.replace('EST|EDT', '', regex=True)
        ).dt.tz_localize('US/Eastern')
        df['modifiedOn'] = pd.to_datetime(
            df['modifiedOn'].str.replace('EST|EDT', '', regex=True)
        ).dt.tz_localize('US/Eastern')

    if verbose:
        print('Date types fixed')

    return df


def remove_unnecessary_columns(df, source):
    '''
    Remove some unnecessary columns.
    '''

    if source=='dep_energy':
        # a deprecated column, according to documentation
        df = df.drop('Groups With Access Code', axis=1)

    elif source=='epa':
        # these columns had the same value for all entries
        df = df.drop(['charge120', 'range', 'rangeCity', 'rangeHwy'], axis=1)

    return df


def fix_typos(df):
    '''
    Fix some typos, currently only in the Country feature of the Dep. of Energy dataset.
    '''

    # these states are in Canada, not USA
    df.loc[df['State'].isin(['BC', 'ON', 'QC']), 'Country'] = 'CA'

    return df


def remove_outliers(df):
    '''
    Remove outliers from the numerical features:
    - if a variable follows a normal distribution (determined by a KS-test),
        discard all values outside the interval (mean +/- 3*std_dev)
    - if not, discard all values outside the interval [Q1 - 1.5*IQR, Q3 + 1.5*IQR].
    '''

    for col in df.select_dtypes('float'):

        # test if the distribution is normal
        mean = df[col].mean()
        std = df[col].std()
        norm_col = (df[col] - mean) / std
        norm_test_results = stats.kstest(norm_col, stats.norm.cdf)

        # identify outliers with zscore for normal data
        if norm_test_results.pvalue > .05:
            outlier_flag = (df[col] < mean - 3*std) | (df[col] > mean + 3*std)

        # identify outliers with IQR for non-normal data
        else:
            q1, q3 = df[col].quantile([.25, .75])
            iqr = q3-q1

            outlier_flag = (df[col] < q1-1.5*iqr) | (df[col] > q3 + 1.5*iqr)

        df = df[~outlier_flag]

        return df


def process_data(dataset_path, verbose=True):
    '''
    Load the data and perform data preprocessing:
    - remove duplicate data
    - deal with missing data
    - fix data with incorrect types
    - remove unnecessary columns
    - fix typos
    - remove outliers
    '''

    df = pd.read_csv(dataset_path)

    source = dataset_path.split('/')[-2]

    # if source is unknown, exit function with warning
    if source not in ['dep_energy', 'epa']:
        print('Unknown source')
        return df

    df = remove_duplicates(df)
    df = deal_with_missing_data(df, source, verbose)
    df = fix_data_types(df, source, verbose)
    df = remove_unnecessary_columns(df, source)

    if source=='dep_energy':
        df = fix_typos(df)

    elif source=='epa':
        df = remove_outliers(df)

    return df

