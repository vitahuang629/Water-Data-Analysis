from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import datetime, time


class flow:
    def __init__(self, river_number):
        self.river_number = river_number
        
        
    def scrape(self):
        match_list= []
        
        day = datetime.date.today() 
        today = time.strftime("%Y-%m-%d")
        #https://waterdata.usgs.gov/nwis/dv?cb_00060=on&format=rdb&site_no=05200450&referred_module=sw&period=&begin_date=1984-06-02&end_date=2022-05-30   
        response = requests.get(f'https://waterdata.usgs.gov/nwis/dv?cb_00060=on&format=rdb&site_no={self.river_number}&referred_module=sw&period=&begin_date=1900-01-01&end_date='+today)
        filter_response = BeautifulSoup(response.text, 'lxml')

        after_response = filter_response.prettify()
        pattern = r"(\d{8}\s\d{4}-\d{1,2}-\d{1,2}\s\d{1,5})"
        match = re.findall(pattern, after_response)

        for i in match:
            match_list.append(i.split('\t'))
        return match_list

    
    
    def excel_list(self):
        match_list = self.scrape()
        df = pd.DataFrame(match_list)
        df.columns = ['River_Code', 'Date', 'Stream']
        writer = pd.ExcelWriter(f'{self.river_number}.xlsx', engine='xlsxwriter')
    
        df['Stream'] = df['Stream'].astype(int)
        df['Flow_7d_Sum'] = (df['Stream'].rolling(min_periods = 0, window = 7).sum())
        df['Flow_7d'] = (df['Stream'].rolling(min_periods = 0, window = 7).sum())/7
        df.to_excel(writer, sheet_name='water_flow', index=False)
        writer.save()

       

#extract data from: https://waterdata.usgs.gov/mn/nwis/current/?type=flow&group_key=basin_cd
get_waterlist = requests.get(f'https://waterdata.usgs.gov/mn/nwis/current/?type=flow&group_key=basin_cd')
filter_list = BeautifulSoup(get_waterlist.text, 'lxml')
after_list = filter_list.prettify()
pattern = r"(0\d{7})"
match = re.findall(pattern, after_list)

for i in match:
    river = flow(i)
    #flow_list = river.scrape()
    river.excel_list()
    #print(flow_list)