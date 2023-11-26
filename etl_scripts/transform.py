# def transform_data(data):
#     new_data = data.copy()
#     new_data['dob'] = new_data["Date of Birth"]
#     new_data['Birth-Month, Birth-Day, Birth-Year'] = new_data.dob.str.split(' ', expand=True)
#     new_data.drop(columns = ["dob", "Date of Birth"], inplace = True)
#     return new_data

import pandas as pd
import numpy as np
# from collections import OrderedDict
from pathlib import Path

def transform_data(source_csv, target_dir):
    new_data = pd.read_csv(source_csv)
    new_data = prep_data(new_data)

    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    # dim_outcome_types = prep_outcome_types_dim(new_data)
    fct_outcomes = prep_outcomes_fct(new_data)

    output_dir = Path(target_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dim_animal.to_parquet(target_dir+'/dim_animals.parquet')
    dim_dates.to_parquet(target_dir+'/dim_dates.parquet')
    # dim_animal.to_parquet(target_dir+'dim_animals.parquet')
    fct_outcomes.to_parquet(target_dir+'/fct_outcomes.parquet')


def prep_data(data):
    # remove stars from animal names. Need regex=False so that * isn't read as regex
    data['name'] = data['Name'].str.replace("*","",regex=False)

    # separate the "sex upon outcome" column into property of an animal (male or female) 
    # and property of an outcome (was the animal spayed/neutered at the shelter or not)
    data['sex'] = data['Sex upon Outcome'].replace({"Neutered Male":"male",
                                                    "Intact Male":"male", 
                                                    "Intact Female":"female", 
                                                    "Spayed Female":"female", 
                                                    "Unknown":np.nan})

    data['is_fixed'] = data['Sex upon Outcome'].replace({"Neutered Male":True,
                                                        "Intact Male":False, 
                                                        "Intact Female":False, 
                                                        "Spayed Female":True, 
                                                        "Unknown":np.nan})

    # prepare the data table for introducing the date dimension
    # we'll use condensed date as the key, e.g. '20231021'
    # time can be a separate dimension, but here we'll keep it as a field
    data['ts'] = pd.to_datetime(data.DateTime)
    data['date_id'] = data.ts.dt.strftime('%Y%m%d')
    data['time'] = data.ts.dt.time

    data['Outcome Type'] = data["Outcome Type"].fillna("N/A")

    return data

def prep_animal_dim(data):
    
    # extract columns only relevant to animal dim
    animal_dim = data[['Animal ID','name','Date of Birth', 'Animal Type', 'sex', 'is_fixed', 'Breed', 'Color', 'Outcome Type', 'Outcome Subtype']]
    
    # rename the columns to agree with the DB tables
    animal_dim.columns = ['animal_id', 'name', 'dob', 'animal_type', 'sex', 'is_fixed', 'breed', 'color', 'outcome_type', 'outcome_subtype']
    
    # drop duplicate animal records
    return animal_dim.drop_duplicates()

def prep_date_dim(data):
    # use string representation as a key
    # separate out year, month, and day
    dates_dim = pd.DataFrame({
        'date_id':data.ts.dt.strftime('%Y%m%d'),
        'date':data.ts.dt.date,
        'year':data.ts.dt.year,
        'month':data.ts.dt.month,
        'day':data.ts.dt.day,
        })
    return dates_dim.drop_duplicates()

# def prep_outcome_types_dim(data):
#     outcomes_dict = {
#         'Rto-Adopt':1, 
#         'Adoption':2, 
#         'Euthanasia':3, 
#         'Transfer':4,
#         'Return to Owner':5,
#         'Disposal':6, 
#         'Died':7, 
#         "N/A":8, 
#         'Missing':9, 
#         'Relocate':10,
#         'Stolen':11
#     }
#     # map outcome string values to keys
#     outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()
    
    # # keep only the necessary fields
    # outcome_types_dim.columns=['outcome_type', 'outcome_type_id']    
    # return outcome_types_dim

def prep_outcomes_fct(data):
    # pick the necessary columns and rename
    outcomes_fct = data[["Animal ID", 'date_id','time']]
    outcomes_fct.columns = ['animal_id', 'date_id', 'time']
    return outcomes_fct