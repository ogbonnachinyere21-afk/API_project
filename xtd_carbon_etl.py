#  Import libraries
import requests
import pandas as pd
from datetime import date, timedelta
import yaml
import sqlalchemy

# 1. Extraction
def API_REQUEST(target_date):
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"
    headers = {
    'Accept': 'application/json'
    } # state to the api with the api request that the response should be in JSON.

    response = requests.get(url, headers=headers)

    print(f"response gotten from {url} for {target_date}")
    return response.json()['data']

# 2. Transformation
# 2.1 Flatten the data
def DATA_TRANSFORMATION_FLAT(data):
    records = []

    for interval in data:
        for region in interval['regions']:
            # create flat dictionary for each region at the 30-mins mark
            row = {
                'regionid': region['regionid'],
                'shortname': region['shortname'],
                'dno': region['dnoregion'],
                'intensity': region['intensity']['forecast'],
                'index': region['intensity']['index']
            }
            # further flatten the generation mix
            for fuel in region['generationmix']:
                row[fuel['fuel']] = fuel['perc']

            records.append(row)
    
    print(f"Flattened API data from {len(data)} to {len(records)}")
    return records

# 2.2 Convert to a dataframe and aggregate
def DATA_TRANSFORMATION_DF(flat_data, target_date):
    df = pd.DataFrame(flat_data)

    # Aggregate and round to 2 decimal places
    agg_df = df.groupby('regionid').agg({
        'shortname': 'first', # Keeps the name
        'dno': 'first', # keeps the dno
        'intensity': 'mean',
        'index': lambda x: x.mode()[0],
        'biomass': 'mean', 'coal': 'mean', 'imports': 'mean',
        'gas': 'mean', 'nuclear': 'mean', 'other': 'mean',
        'hydro': 'mean', 'solar': 'mean', 'wind': 'mean'
    }).reset_index()

    agg_df['date_recorded'] = target_date - timedelta(days=1)
    
    print(f"Data aggregated from {len(flat_data)} to {len(agg_df)}")
    return agg_df.round(2)

# 3. Loading
# 3.1 Create Engine
def CREATE_ENGINE(config_file = 'config.yaml'):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
        
    db_url = sqlalchemy.URL.create(
                drivername="postgresql+psycopg2",  # driver
                username=config['user'],
                password=config['password'],
                host=config.get('host', 'localhost'),
                port=config.get('port', 5432),
                database=config['database']
            )
    engine = sqlalchemy.create_engine(db_url)
    print(f"Engine Created for {config['database']} via {config.get('host', 'localhost')}:{config.get('port', 5432)}")

    return engine

# 3.2 Loading to the database through engine connection
def LOAD_TO_DB(df, engine, schema='carbon'):

    # Load the Intensity Fact Table
    fact_intensity = df[['regionid', 'date_recorded', 'intensity', 'index']]

    fact_intensity.to_sql('fact_carbon_intensity', engine, schema=schema, if_exists='append', index=False)
    print("fact_carbon_intensity loaded.")

    # Load the Generation Mix Fact Table
    fact_gen_mix = df[['regionid', 'date_recorded', 'biomass', 'coal',
        'imports', 'gas', 'nuclear', 'other', 'hydro', 'solar', 'wind']]

    fact_gen_mix.to_sql('fact_generation_mix', engine, schema=schema, if_exists='append', index=False)
    print("fact_generation_mix loaded.")

def main():
    today = date.today()
    print(f"Data pipeline started for {today}")
    api_data = API_REQUEST(target_date=today)
    flattened_data = DATA_TRANSFORMATION_FLAT(data=api_data)
    agg_df = DATA_TRANSFORMATION_DF(flat_data=flattened_data, target_date=today)
    engine = CREATE_ENGINE()
    LOAD_TO_DB(df= agg_df, engine=engine)

if __name__ == "__main__":
    main()