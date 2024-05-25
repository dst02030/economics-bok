import logging
import requests
import json
import time
import numpy as np
import pandas as pd


from datetime import datetime

logger = logging.getLogger(__name__)

class Bok:
    def __init__(self, auth_key, lang = "kr", max_rows = 100000, raw = False):
        self.allow_max_rows = 100000
        self.allow_lang = ["kr", "en"]
        
        self.auth_key = auth_key
        self.main_url = "http://ecos.bok.or.kr/api"
        
        
        if lang not in self.allow_lang:
            raise ValueError(f"ERROR: Value lang only accepts value of {self.allow_lang}.")
        
        if max_rows > self.allow_max_rows:
            raise ValueError(f"ERROR: Value max_rows exceeds the allowed range (<= {self.allow_max_rows}).")
        
        if not isinstance(raw, bool):
            raise ValueError(f"ERROR: Value raw only accepts boolean type.")
        
        self.lang = lang
        self.max_rows = max_rows
        self.raw = raw


    def get_statistic_list(self, stat_code = None, rename = {}, _ts = datetime.astimezone(datetime.now())):
        return self._get_api_results("StatisticTableList", stat_code, rename = rename, _ts = _ts)
    
    
    def get_statistic_word(self, rename = {}, _ts = datetime.astimezone(datetime.now())):
        params = {
        "header": {
            "guidSeq": 1,
            "trxCd": "OSUSD01R01",
            "sysCd": "03",
            "fstChnCd": "WEB",
            "langDvsnCd": "KO",
            "envDvsnCd": "D",
            "sndRspnDvsnCd": "S",
            "sndDtm": pd.to_datetime(_ts).strftime('%Y%m%d'),
            "usrId": "IECOSPC",
            "pageNum": 1,
            "pageCnt": 1000
        },
        "data": {
        }
        }
        
        response = requests.post('https://ecos.bok.or.kr/serviceEndpoint/httpService/request.json', json = params)
        words = json.loads(response.text)
        data = pd.DataFrame(words['data']['statTermList']).rename(columns = rename)
        data = data[rename.values()]
        data['_ts'] = _ts
        
        
        return data
    
    def get_statistic_item(self, stat_code, rename = {}, _ts = datetime.astimezone(datetime.now())):
        self._value_check(stat_code = stat_code)
        return self._get_api_results("StatisticItemList", stat_code, rename = rename, _ts = _ts)
        
    def get_statistic_search(self, stat_code, period, start, end, item_code1 = None, item_code2 = None, item_code3 = None, item_code4 = None, _ts = datetime.astimezone(datetime.now()), rename = {}):
        self._value_check(stat_code = stat_code, period = period, start = start, end = end)
        return self._get_api_results("StatisticSearch", stat_code, period, start, end, item_code1, item_code2, item_code3, item_code4, rename = rename)
    
    
    def get_statistic_key(self, rename = {}, _ts = datetime.astimezone(datetime.now())):
        return self._get_api_results("KeyStatisticList", rename = rename, _ts = _ts)
    
    def get_statistic_meta(self, rename = {}, _ts = datetime.astimezone(datetime.now())):
        
        params = {
        "header": {
            "guidSeq": 1,
            "trxCd": "OMMDB01R01",
            "sysCd": "03",
            "fstChnCd": "WEB",
            "langDvsnCd": "KO",
            "envDvsnCd": "D",
            "sndRspnDvsnCd": "S",
            "sndDtm": pd.to_datetime(_ts).strftime('%Y%m%d'),
            "usrId": "IECOSPC",
            "pageNum": 1,
            "pageCnt": 1000
        },
        "data": {
        }
        }
        
        response = requests.post('https://ecos.bok.or.kr/serviceEndpoint/httpService/request.json', json = params)
        words = json.loads(response.text)
        web_data = pd.DataFrame(words['data']['statDescList']).rename(columns = rename)
        web_data = web_data[[col for col in rename.values() if col in web_data.columns]]

        
        data = pd.DataFrame()

        for idx, row in web_data.iterrows():
            logger.info(f"Get statistic meta data ({idx + 1}/{web_data.shape[0]})")
            api_data = self._get_api_results("StatisticMeta", row['데이터명'], rename = rename, _ts = _ts)
            api_data[row.index] = row.tolist()
            data = pd.concat([data, api_data])
        
        return data
        
        
    
    def _get_api_results(self, detail_url, *args, rename = {}, _ts = datetime.astimezone(datetime.now()), sleep = .6):
        idx = 0
        total_count = np.inf
        args = [arg for arg in args if arg is not None and len(str(arg)) > 0]
        args_url = "/".join(args)
        data = pd.DataFrame()
        
        while total_count > idx * self.max_rows:
            api_results = requests.get(
                f"{self.main_url}/{detail_url}/{self.auth_key}/json/{self.lang}/{idx*self.max_rows+1}/{(idx+1)*self.max_rows}/{args_url}"
            )
            api_results.raise_for_status()
            
            api_text = json.loads(
                api_results.text
                    )
            
            if detail_url in api_text:
                api_text = api_text[detail_url]
            
            elif 'INFO' in api_text['RESULT']['CODE']:
                logger.warning(api_text['RESULT'])
                return pd.DataFrame(columns = rename.values())

            else:
                raise Exception(api_text['RESULT'])

            total_count = api_text['list_total_count']
            this_data = pd.DataFrame(api_text['row'])
            data = pd.concat([data, this_data])
    
            logger.info(f"Read {min((idx+1) * self.max_rows, total_count)}/{total_count} rows....")
            idx += 1

        data['_ts'] = _ts
        
        if rename:
            rename = {key: val for key, val in rename.items() if key in data.columns}
            data.rename(columns = rename, inplace = True)
        
        time.sleep(sleep)

        return data
        
    def _value_check(self, **kwargs):
        missing_value_list = []
        for key, val in kwargs.items():
            if val is None or val == "":
                missing_value_list.append(key)
        
        if len(missing_value_list) > 0:
            raise ValueError(f"The following variables are required to have a value: {missing_value_list}.")
        