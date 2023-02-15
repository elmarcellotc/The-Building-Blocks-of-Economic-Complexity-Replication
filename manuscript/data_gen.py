import pandas as pd
import os

def unify_data():

    df = pd.DataFrame()

    for i in os.listdir('data_raw/'):
        if i != '.gitkeep':
            df1 = pd.read_sas(f'data_raw/{i}', format='sas7bdat', encoding='utf-8')
            try:
                df1.columns = [i.lower() for i in ['year', 'Icode', 'Importer', 'Ecode', 'Exporter', 'sitc4', 'Value']]
                df1 = df1[[i.lower() for i in ['year', 'Icode', 'Importer', 'Ecode', 'Exporter', 'sitc4', 'Value']]]
                df1 = df1.groupby([i.lower() for i in ['year', 'Icode', 'Importer', 'Ecode', 'Exporter', 'sitc4', 'Value']]).sum().reset_index()
                df1.columns = ['year', 'icode', 'importer', 'ecode', 'exporter', 'product', 'share']
                df = pd.concat([df,df1])
            except:
                print('Error with file '+i)
                print(df1)

    for c in ['year', 'icode', 'importer', 'ecode', 'exporter', 'product']:
       df[c] = df[c].astype(str)
        
    df['share'] = df['share'].astype(float)
    df.to_csv('data_cleaned/data.csv', index=False)
    print(df)