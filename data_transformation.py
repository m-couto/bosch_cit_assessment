import pandas as pd
import datetime

# features per type for each dataset
features_to_keep_per_source = {

    'dep_energy': {

        'num_features_to_keep': [
            'EV Level2 EVSE Num', 
            'Latitude', 'Longitude', 
        ],

        'date_features_to_keep': [
            'Open Date', 
        ],

        'cat_features_to_keep': [
            'Fuel Type Code',
            'Country', 'State', 'Geocode Status',
            'Status Code', 'Access Code',
            'EV Network', 
        ],

        'tag_features_to_keep': [
            'EV Connector Types'
        ]
    },


    'epa': {

        'num_features_to_keep': [
            'barrels08', 'barrelsA08', 'charge240', 'city08', 'city08U', 'cityA08',
            'cityA08U', 'cityCD', 'cityE', 'cityUF', 'co2', 'co2A',
            'co2TailpipeAGpm', 'co2TailpipeGpm', 'comb08', 'comb08U', 'combA08',
            'combA08U', 'combE', 'combinedCD', 'combinedUF', 'cylinders', 'displ',
            'engId', 'feScore', 'fuelCost08', 'fuelCostA08', 'ghgScore',
            'ghgScoreA', 'highway08', 'highway08U', 'highwayA08', 'highwayA08U',
            'highwayCD', 'highwayE', 'highwayUF', 'hlv', 'hpv', 'id', 'lv2', 'lv4',
            'pv2', 'pv4', 'rangeCityA', 'rangeHwyA', 'UCity', 'UCityA', 'UHighway',
            'UHighwayA', 'year', 'youSaveSpend', 'charge240b', 'phevCity',
            'phevHwy', 'phevComb'
        ],

        'date_features_to_keep': [
            'createdOn', 'modifiedOn',
        ],

        'cat_features_to_keep': [
            'drive',
            'fuelType1',
            'make',
            'mpgData',
            'trany',
            'VClass',
            'phevBlended', # boolean
        ],

        'tag_features_to_keep': [
            'fuelType'
        ]
    }
}


def process_num_features(df, num_feats):
    '''
    Normalise numerical features.
    '''

    # discard numerical features with same value for all entries
    if any(df[num_feats].std()==0):
        num_feats = [feat for feat in num_feats if df[feat].std()!=0]
        df = df.drop([feat for feat in num_feats if df[feat].std()==0], axis=1)

    # normalise
    df[num_feats] = (df[num_feats] - df[num_feats].mean()) / df[num_feats].std()

    return df


def process_date_features(df, date_feats, source):
    '''
    Convert date columns into the number of days between those dates and now.
    '''
    
    for col in date_feats:
        
        current_timestamp = (
            datetime.datetime.now(datetime.timezone.utc) if source=='epa'
            else datetime.datetime.now()
        )
        df[col] = (current_timestamp - df[col]).dt.days

    return df


def process_cat_features(df, cat_feats):
    '''
    In these datasets, all categorical features seem to be non-ordinal,
    so we process them using one-hot encoding.
    Note: we drop one of the levels to prevent colinearity between features.
    '''

    for col in cat_feats:

        n_levels = len(df[col].unique())

        # for binary cat features, 1 represents the smallest category
        if n_levels==2:
            level_counts = df[col].value_counts()
            smallest_level = level_counts.index[-1]
            df[col] = (df[col]==smallest_level).astype(int)

        # one-hot encoding
        else:
            df = pd.concat([
                df.drop(col, axis=1),
                pd.get_dummies(df[col], drop_first=True, prefix=col)
            ], axis=1)

        # fix one class in the drive column
        if col=='drive':
            df['drive_4-Wheel Drive'] = df['drive_4-Wheel Drive'] + df['drive_4-Wheel or All-Wheel Drive']
            df['drive_All-Wheel Drive'] = df['drive_All-Wheel Drive'] + df['drive_4-Wheel or All-Wheel Drive']
            df = df.drop('drive_4-Wheel or All-Wheel Drive', axis=1)

    return df


def process_tag_features(df, tag_feats):
    '''
    For each tag feature, create new binary features - one for each unique tag of this tag feature -
    representing the rows where that tag is mentioned.
    '''

    for tag_feat in tag_feats:

        # splitting into tags depends on the tag feature
        if tag_feat=='EV Connector Types':
            list_tags_per_entry = df.loc[df[tag_feat] != 'Not Applicable', tag_feat].str.split(' ')

        elif tag_feat=='fuelType':
            list_tags_per_entry = df['fuelType'].str.split(' (and|or) ', regex=True)
            list_tags_per_entry = list_tags_per_entry.apply(
                lambda x: [item for item in x if item!='or' and item!='and']
            )

        unique_tags = set(item for sublist in list_tags_per_entry.to_list() for item in sublist)

        # create a dataframe with the new binary features
        tag_new_cols = []
        for tag in unique_tags:
            tag_new_cols.append(
                list_tags_per_entry.apply(lambda x: tag in x).rename(tag)
            )
        tag_new_cols = pd.concat(tag_new_cols, axis=1).astype(int)

        # merge with the original dataframe
        df = (df.drop(tag_feat, axis=1)).join(tag_new_cols)
        df[list(unique_tags)] = df[list(unique_tags)].fillna(0)

    return df


def transform_data(df, source, verbose=True):
    '''
    Process each feature according to its type.
    '''

    # if source is neither dep_energy or epa, exit the function with warning
    if source not in features_to_keep_per_source.keys():
        print('Unknown source')
        return df
    
    feats_dict = features_to_keep_per_source[source]
    features_to_keep = [item for sublist in list(feats_dict.values()) for item in sublist]

    # select only features listed in the dictionary above
    df = df.loc[:, features_to_keep]

    if 'num_features_to_keep' in feats_dict.keys():
        df = process_num_features(df, feats_dict['num_features_to_keep'])
        if verbose:
            print('Numerical features processed')
    
    if 'date_features_to_keep' in feats_dict.keys():
        df = process_date_features(df, feats_dict['date_features_to_keep'], source)
        if verbose:
            print('Date features processed')

    if 'cat_features_to_keep' in feats_dict.keys():
        df = process_cat_features(df, feats_dict['cat_features_to_keep'])
        if verbose:
            print('Categorical features processed')

    if 'tag_features_to_keep' in feats_dict.keys():
        df = process_tag_features(df, feats_dict['tag_features_to_keep'])
        if verbose:
            print('Tag features processed')

    return df

