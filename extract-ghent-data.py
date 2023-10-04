#!/usr/bin/env python
"""\
A small ETL script to extract some public data from the city of Ghent and load it into a local sqlite3 database.
"""
import pandas as pd
import sqlite3
import os.path
import requests

STAGING_DIR = 'staging'

DATABASE_FILE = 'ghent-data.db'

DISTRICT_URL = ('https://data.stad.gent/api/explore/v2.1/catalog/datasets/stadswijken-gent/exports/csv?lang=en&timezone'
                '=Europe%2FBrussels&use_labels=true&delimiter=%3B')
DISTRICT_CSV_FILE = 'stadswijken-gent.csv'
DISTRICT_COLUMN_MAP = {
    'nieuwnr': 'stadswijk_id',
    'naam': 'stadsdeel',
    'wijk': 'stadswijk_naam'
}
DISTRICT_COLUMN_LIST = {
    'stadswijk_id',
    'stadsdeel',
    'stadswijk_naam'
}

DISTRICT_POPULATION_URL = ('https://data.stad.gent/api/explore/v2.1/catalog/datasets/bevolkingsaantal-per-wijk-per-jaar'
                           '-gent/exports/csv?lang=en&timezone=Europe%2FBrussels&use_labels=true&delimiter=%3B')
DISTRICT_POPULATION_CSV_FILE = 'bevolkingsaantal-per-wijk-per-jaar-gent.csv'
DISTRICT_POPULATION_COLUMN_MAP = {
    'Period': 'jaar',
    'ValueString': 'bevolkingsaantal',
    'wijkcode': 'stadswijk_id'
}
DISTRICT_POPULATION_COLUMN_LIST = {
    'jaar',
    'bevolkingsaantal',
    'stadswijk_id'
}

CITY_POPULATION_URL = ('https://data.stad.gent/api/explore/v2.1/catalog/datasets/bevolkingsaantal-per-jaar-gent/exports'
                       '/csv?lang=en&timezone=Europe%2FBrussels&use_labels=true&delimiter=%3B')
CITY_POPULATION_CSV_FILE = 'bevolkingsaantal-per-jaar-gent.csv'
CITY_POPULATION_COLUMN_MAP = {
    'Period': 'jaar',
    'ValueString': 'bevolkingsaantal'
}
CITY_POPULATION_COLUMN_LIST = {
    'jaar',
    'bevolkingsaantal'
}

CRIMEDATA_URL = ('https://data.stad.gent/api/explore/v2.1/catalog/datasets/criminaliteitscijfers-per-wijk-per-maand'
                 '-gent-{year_value}/exports/csv?lang=en&timezone=Europe%2FBrussels&use_labels=true&delimiter=%3B')
CRIMEDATA_YEARS = {2018, 2019, 2020, 2021, 2022, 2023}
CRIMEDATA_CSV_FILE = 'criminaliteitscijfers-per-wijk-per-maand-gent-{year_value}.csv'
CRIMEDATA_COL_DICT = {
    'jaar_maand': 'jaar_maand_text',
    'Categorie': 'misdrijf_categorie_naam',
    'Wijkcode': 'stadswijk_id',
    'Totaal': 'misdrijf_aantal'
}
CRIMEDATA_COL_LIST = {
    'jaar_maand',
    'misdrijf_categorie_id',
    'stadswijk_id',
    'misdrijf_aantal',
    'misdrijf_kwartaal_totaal',
    'misdrijf_jaar_totaal'
}


def rename_columns(df: pd.DataFrame, mapping_dict: dict) -> pd.DataFrame:
    df.rename(columns=mapping_dict, inplace=True)
    return df


def filter_columns(df: pd.DataFrame, filter_list: set) -> pd.DataFrame:
    exclude_cols = list(set(df.columns.tolist()) - set(filter_list))
    df.drop(columns=exclude_cols, inplace=True)
    return df


def read_or_fetch_data(file_name: str, url: str) -> pd.DataFrame:
    target = f'{STAGING_DIR}/{file_name}'

    if os.path.exists(target):
        print(f'{target} is already available.')
    else:
        print(f'{target} is not available. Downloading..')
        response = requests.get(url)
        with open(target, "wb") as f:
            f.write(response.content)

    return pd.read_csv(target, delimiter=';')


def create_date_dim(dbcon: sqlite3.Connection) -> None:
    df = pd.DataFrame(pd.date_range('1/1/2018', '12/31/2024'), columns=['date'])
    df['datum_id'] = df['date'].dt.strftime("%Y%m%d").astype('int64')
    df['jaar'] = df['date'].dt.year
    df['maand'] = df['date'].dt.month
    df['dag'] = df['date'].dt.day
    df['kwartaal_nummer'] = df['date'].dt.quarter
    df['week_nummer'] = df['date'].dt.isocalendar().week

    # save some space until needed
    # df['kwartaal_naam'] = df['date'].apply(lambda x: f'Q{x.quarter}')
    # df['kwartaal_jaar'] = df['date'].apply(lambda x: f'Q{x.quarter} {x.strftime("%Y")}')
    # df['maand_naam'] = df['date'].dt.strftime("%B")
    # df['weekdag'] = df['date'].dt.strftime("%A")

    # exclude date
    df.drop(columns='date', inplace=True)

    df.to_sql(name='datum', con=dbcon, if_exists='append', index=False)


def process_district(dbcon: sqlite3.Connection) -> None:
    df = read_or_fetch_data(DISTRICT_CSV_FILE, DISTRICT_URL)

    df = rename_columns(df, DISTRICT_COLUMN_MAP)
    df = filter_columns(df, DISTRICT_COLUMN_LIST)

    df.to_sql(name='stadswijk', con=dbcon, if_exists='append', index=False)


def process_district_population(dbcon: sqlite3.Connection) -> None:
    df = read_or_fetch_data(DISTRICT_POPULATION_CSV_FILE, DISTRICT_POPULATION_URL)

    df = rename_columns(df, DISTRICT_POPULATION_COLUMN_MAP)
    df['bevolkingsaantal'] = df['bevolkingsaantal'].astype('int64')
    df = filter_columns(df, DISTRICT_POPULATION_COLUMN_LIST)

    df.to_sql(name='stadswijk_bevolkingsaantal', con=dbcon, if_exists='append', index=False)


def process_city_population(dbcon: sqlite3.Connection) -> None:
    df = read_or_fetch_data(CITY_POPULATION_CSV_FILE, CITY_POPULATION_URL)

    df = rename_columns(df, CITY_POPULATION_COLUMN_MAP)
    df['bevolkingsaantal'] = df['bevolkingsaantal'].astype('int64')
    df = filter_columns(df, CITY_POPULATION_COLUMN_LIST)

    df.to_sql(name='bevolkingsaantal', con=dbcon, if_exists='append', index=False)


def process_crime_data_for_year(dbcon: sqlite3.Connection, year: int, category_dict: dict) -> None:
    df = read_or_fetch_data(CRIMEDATA_CSV_FILE.format(year_value=year), CRIMEDATA_URL.format(year_value=year))

    df = rename_columns(df, CRIMEDATA_COL_DICT)

    # convert date to int64
    df['jaar_maand_text'] = pd.to_datetime(df['jaar_maand_text'], format='%Y-%m-%d')
    df['jaar_maand'] = df['jaar_maand_text'].dt.strftime("%Y%m%d").astype('int64')

    # date helpers
    df['jaar'] = df['jaar_maand_text'].dt.strftime("%Y").astype('int')
    df['kwartaal_nummer'] = df['jaar_maand_text'].dt.quarter

    # fix data typos
    df['misdrijf_categorie_naam'] = df['misdrijf_categorie_naam'].str.replace('Verkeerongevallen', 'Verkeersongevallen')

    # extend crime category id dict
    for category in df['misdrijf_categorie_naam'].unique():
        if not category_dict.get(category):
            category_dict[category] = len(category_dict) + 1

    # map category to unique id
    df['misdrijf_categorie_id'] = df['misdrijf_categorie_naam'].map(category_dict).fillna(999)
    df['misdrijf_categorie_id'] = df['misdrijf_categorie_id'].astype('int')

    # generate totals
    df['misdrijf_kwartaal_totaal'] = df.groupby(['jaar', 'kwartaal_nummer', 'stadswijk_id', 'misdrijf_categorie_id'])['misdrijf_aantal'].transform('sum')
    df['misdrijf_jaar_totaal'] = df.groupby(['jaar', 'stadswijk_id', 'misdrijf_categorie_id'])['misdrijf_aantal'].transform('sum')

    df = filter_columns(df, CRIMEDATA_COL_LIST)

    df.to_sql(name='misdrijf', con=dbcon, if_exists='append', index=False)


def process_crime_data(dbcon: sqlite3.Connection) -> None:
    category_dict = {}

    for year in CRIMEDATA_YEARS:
        process_crime_data_for_year(dbcon, year, category_dict)

    # write category dim
    category_df = pd.DataFrame(list(category_dict.items()),
                               columns=['misdrijf_categorie_naam', 'misdrijf_categorie_id'])
    category_df.to_sql(name='misdrijf_categorie', con=dbcon, if_exists='append', index=False)


if __name__ == '__main__':

    if os.path.exists(DATABASE_FILE):
        os.remove(DATABASE_FILE)

    if not os.path.exists(STAGING_DIR):
        os.mkdir(STAGING_DIR)

    with open('create-db.sql', 'r') as sql_file:
        sql_script = sql_file.read()

    conn = sqlite3.connect(DATABASE_FILE)
    conn.executescript(sql_script)

    create_date_dim(conn)
    process_city_population(conn)
    process_district(conn)
    process_district_population(conn)
    process_crime_data(conn)
