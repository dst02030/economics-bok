import logging
import os
import sys

import numpy as np
import pandas as pd

from datetime import datetime, date
from logging.handlers import TimedRotatingFileHandler


from bok.api import Bok
from bok.utils import get_jinja_yaml_conf, create_db_engine, Postgres_connect
from bok.processing import get_statdate_table

def main():
    os.chdir(os.path.dirname(__file__))
    conf = get_jinja_yaml_conf('./conf/api.yml', './conf/logging.yml')


    # logger 설정
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=eval(conf['logging']['level']),
        format=conf['logging']['format'],
        handlers = [TimedRotatingFileHandler(filename =  conf['logging']['file_name'],
                                    when=conf['logging']['when'],
                                    interval=conf['logging']['interval'],
                                    backupCount=conf['logging']['backupCount']), logging.StreamHandler()]
                    )



    engine = create_db_engine(os.environ)
    postgres_conn = Postgres_connect(engine)
    bok = Bok(auth_key = os.environ['auth_key'])

    if sys.argv[1] in conf['data']:
        logger.info(f"Run {sys.argv[1]}!")
    
    else:
        raise Exception(f"You only enter following args: {', '.join(conf['data'])}")


    if sys.argv[1] == 'stat_list':
        
        stat_list = bok.get_statistic_list(rename = conf['data']['stat_list']['rename'], _ts = os.environ['_ts'])
        upload_stat_list = postgres_conn.ext_notin_db(schema_name = conf['schema_name'],
            table_name = conf['data']['stat_list']['table_name'],
            data = stat_list,
            subset = conf['data']['stat_list']['dup_cols'])

        upload_stat_list.to_sql(conf['data']['stat_list']['table_name'], schema = conf['schema_name'], con = engine, index = False, if_exists = "append")
        
    
    
    elif sys.argv[1] == 'stat_word':
        stat_word = bok.get_statistic_word(rename = conf['data']['stat_word']['rename'], _ts = os.environ['_ts'])
        
        stat_word['수정날짜'] = stat_word['수정날짜'].map(lambda x: pd.to_datetime(x))
        
        upload_stat_word = postgres_conn.ext_notin_db(schema_name = conf['schema_name'],
            table_name = conf['data']['stat_word']['table_name'],
            data = stat_word,
            subset = conf['data']['stat_word']['dup_cols'])
        
        postgres_conn.upsert(data = upload_stat_word,
                            schema_name = conf['schema_name'],
                            table_name = conf['data']['stat_word']['table_name'])
        
    
    elif sys.argv[1] == 'stat_item':
        stat_list = postgres_conn.get_data(schema_name = conf['schema_name'],
            table_name = conf['data']['stat_list']['table_name'],
            columns = ['통계표코드'],
            where = ["검색가능여부 = 'Y'"], orderby_cols = '통계표코드').to_numpy().ravel()

        for idx, stat in enumerate(stat_list):
            logger.info(f"Upload {stat} starts ({idx+1}/{len(stat_list)})!")
            stat_item = bok.get_statistic_item(stat, _ts = os.environ['_ts'], rename = conf['data']['stat_item']['rename'])

            upload_stat_item = postgres_conn.ext_notin_db(schema_name = conf['schema_name'],
                    table_name = conf['data']['stat_item']['table_name'],
                    data = stat_item,
                    subset = conf['data']['stat_item']['dup_cols'])

            postgres_conn.upsert(data = upload_stat_item,
                            schema_name = conf['schema_name'],
                            table_name = conf['data']['stat_item']['table_name'])



    elif sys.argv[1] == 'stat_search':
        stat_list = postgres_conn.get_data(schema_name = conf['schema_name'],
            table_name = conf['data']['stat_list']['table_name'],
            columns = ['통계표코드', '주기', ],
            where = ["검색가능여부='Y'"])
        
        stat_item = postgres_conn.get_data(schema_name = conf['schema_name'],
            table_name = conf['data']['stat_item']['table_name'],
            columns = ['통계표코드', '통계항목코드', '주기', '수록시작일자', '수록종료일자', '자료수'],
            orderby_cols = ['통계표코드', '통계항목코드', '주기', '수록시작일자', '수록종료일자', '자료수'])

        stat_item = stat_item.merge(stat_list, how = 'inner')
        

        stat_group = stat_item.groupby(['통계표코드', '주기']).agg({
            '수록시작일자': 'first',
            '수록종료일자': 'last',
            '자료수': 'sum'
        }).reset_index()
        
        for idx, stat_series in stat_group.iterrows():
            stat, period, start, end, count = stat_series
            logger.info(f"Upload {stat} for period {period} starts! ({idx+1}/{stat_group.shape[0]} items...)")


            db_count = postgres_conn.get_count(schema_name = conf['schema_name'],
             table_name = conf['data']['stat_search']['table_name'],
             where = [f"통계표코드 = '{stat}'", f"주기 = '{period}'"])
            
            if db_count >= count:
                logger.info(f"All data is uploaded in DB ({db_count}/{count}). Skip API call.")
                continue

            try:
                stat_search = bok.get_statistic_search(stat, 
                                             period = period, 
                                             start = start, 
                                             end = end, 
                                             _ts = os.environ['_ts'], rename = conf['data']['stat_search']['rename'])

            except Exception as e:
                logger.warning(e)
                continue

            if db_count == stat_search.shape[0]:
                logger.info(f"All data is uploaded in DB when calling API({db_count}/{stat_search.shape[0]}). Skip API call.")
                continue
            
            stat_search['주기'] = period
            
            stat_search.fillna({col: '' for col in conf['data']['stat_search']['dup_cols']} | {'값': np.nan}, inplace = True)
            
            upload_stat_search = postgres_conn.ext_notin_db(schema_name = conf['schema_name'],
                    table_name = conf['data']['stat_search']['table_name'],
                    data = stat_search,
                    subset = conf['data']['stat_search']['dup_cols'])

            # 한은에서 중복 데이터 조회 오류 수정 시 제거할 코드
            upload_stat_search.drop_duplicates(conf['data']['stat_search']['dup_cols'], inplace = True)
            
            postgres_conn.upsert(data = upload_stat_search,
                            schema_name = conf['schema_name'],
                            table_name = conf['data']['stat_search']['table_name'])


    elif sys.argv[1] == 'stat_key':
        stat_key = bok.get_statistic_key(_ts = os.environ['_ts'], rename = conf['data']['stat_key']['rename'])

        upload_stat_key = postgres_conn.ext_notin_db(schema_name = conf['schema_name'],
                    table_name = conf['data']['stat_key']['table_name'],
                    data = stat_key,
                    subset = conf['data']['stat_key']['dup_cols'])

        postgres_conn.upsert(data = upload_stat_key,
                            schema_name = conf['schema_name'],
                            table_name = conf['data']['stat_key']['table_name'])


    elif sys.argv[1] == 'stat_meta':
        stat_meta = bok.get_statistic_meta(rename = conf['data']['stat_meta']['rename'])
        stat_meta['메타데이터_key'] = stat_meta['메타데이터'].map(lambda x: x[:30] if isinstance(x, str) else '')
        stat_meta = stat_meta.drop_duplicates(conf['data']['stat_meta']['dup_cols'])

        upload_stat_meta = postgres_conn.ext_notin_db(schema_name = conf['schema_name'],
                    table_name = conf['data']['stat_meta']['table_name'],
                    data = stat_meta,
                    subset = conf['data']['stat_meta']['dup_cols'])

        upload_stat_meta['레벨'] = upload_stat_meta['레벨'].astype(str)
        
        postgres_conn.insert_df(data = upload_stat_meta,
                     schema_name = conf['schema_name'],
                     table_name = conf['data']['stat_meta']['table_name'])

    elif sys.argv[1] == 'stat_date':
        for url_name, url in conf['data']['stat_date']['urls'].items():
            start_year = conf['data']['stat_date']['start_year']

            for year in range(start_year, datetime.now().year + 1):
                call_url = f"{url}&year={year}"
                data = get_statdate_table(url)
        pass
        


        
if __name__ == "__main__":
    main()