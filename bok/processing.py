import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

def get_statdate_table(url, year):
    tables = pd.read_html(url)

    if len(tables) < 6:
        logger.warning(f"There doesn't exist data in {year}")
        return

    table = tables[4]
    table.index = tables[3].to_numpy().ravel()
    table.columns = tables[2].columns
    
    df = pd.melt(table.T.reset_index(), id_vars = 'index', var_name='static', value_name='info').replace('-', pd.NA).dropna()
    
    df['info_processing'] = df['info'].apply(lambda x: re.findall(r'\d+\.\d+\s\d+:\d+\s\(.*?\)|\w+\d+\s\(.*?\)', x))
    df['info'] = df.apply(lambda x: x['info_processing'] if len(x['info_processing']) > 0 else x['info'], axis = 1)
    df = df.explode('info')
    return df
    df[['date', 'info']] = df['info'].str.extract(r'(\d+\.\d+\s\d+:\d+|\w+\d+) \((.*?)\)')
    return df
    df['date'] = df.apply(lambda x: re.sub('NLT', f"{x['index'][-2:]}.", f"{x['date']} 00:00") if 'NLT' in x['date'] else x['date'], axis = 1)
    
    
    df['date'] = f'{year}.'+df['date']
    # return df['date']
    df['date'] = pd.to_datetime(df['date'], format='%Y.%m.%d %H:%M')
    df.drop(columns = 'index', inplace = True)
    
    return df