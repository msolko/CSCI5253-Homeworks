#import pandas as pd
#import numpy as np

#df = pd.read_csv('https://shelterdata.s3.amazonaws.com/shelter1000.csv')

#df[['month', 'year']] = df["MonthYear"].str.split(" ", expand=True)

#df['sex'] = df['Sex upon Outcome'].replace('Unknown', np.nan)


#df.to_csv('test.txt')

#print("Job complete")


import pandas as pd
import numpy as np
import argparse

def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    new_data = data.copy()
    new_data['dob'] = new_data["Date of Birth"]
    new_data['Birth-Month, Birth-Day, Birth-Year'] = new_data.dob.str.split(' ', expand=True)
    new_data.drop(columns = ["dob", "Date of Birth"], inplace = True)
    return new_data

def load_data(data, target):
    data.to_csv(target)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Doing the thing")
    df = extract_data(args.source)
    temp_df = transform_data(df)
    load_data(temp_df, args.target)
    print("Done doing the thing")






























