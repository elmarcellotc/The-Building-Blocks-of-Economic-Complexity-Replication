import pandas as pd
import os

def unify_data():

    df = pd.DataFrame()

    for i in os.listdir('data_raw/'):
        df1 = pd.read_sas(f'data_raw/{i}', format='sas7bdat', encoding='utf-8')
        print(f'Opening file {i}')
        new_cols = [i.lower() for i in df1.columns]
        df1.columns = new_cols
        df1 = df1[['year', 'icode', 'importer', 'ecode', 'exporter', 'sitc4', 'quantity']]
        df1 = df1.groupby(['year', 'icode', 'importer', 'ecode', 'exporter', 'sitc4']).sum().reset_index()
        df1.columns = ['year', 'icode', 'importer', 'ecode', 'exporter', 'product', 'share']
        df = pd.concat([df, df1])

    for c in ['year', 'icode', 'importer', 'ecode', 'exporter', 'product']:
        df[c] = df[c].astype(str)
        
    df['share'] = df['share'].astype(float)
    df.to_csv('data_cleaned/data.csv', index=False)
    print(df)