from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import datetime, time
import numpy as np
from scipy import stats

#Calculate daily flow percentile of each site
#The new_df data frame including all data from the beginning to today         


class percentile:
    def __init__(self, river_number):
        self.river_number = river_number
        
    def scrape(self):
        match_list= []
        
        day = datetime.date.today() 
        today = time.strftime("%Y-%m-%d")
        #https://waterdata.usgs.gov/nwis/dv?cb_00060=on&format=rdb&site_no=05200450&referred_module=sw&period=&begin_date=1984-06-02&end_date=2022-05-30   
        response = requests.get(f'https://waterdata.usgs.gov/nwis/dv?cb_00060=on&format=rdb&site_no={self.river_number}&referred_module=sw&period=&begin_date=1800-01-01&end_date='+today)
        filter_response = BeautifulSoup(response.text, 'lxml')

        after_response = filter_response.prettify()
        #regular expression finds the river code, date, and stream
        pattern = r"(\d+\s\d{4}-\d{1,2}-\d{1,2}\s\d{1,5})"
        match = re.findall(pattern, after_response)

        for i in match:
            match_list.append(i.split('\t'))
        return match_list
        print(match_list)
    
    def excel_percentile(self):
        match_list = self.scrape()
        df = pd.DataFrame(match_list)
        writer = pd.ExcelWriter(f'percentile_{self.river_number}.xlsx', engine='xlsxwriter')
        df.columns = ['Gage', 'Date', 'Flow']
        
        all_dates = pd.DataFrame(pd.date_range(df['Date'].min(), df['Date'].max()), columns=['Date'])
        all_dates['Date'] = all_dates['Date'].astype(str)

        new_df = pd.concat([df, all_dates], axis = 0)
        new_df = new_df.drop_duplicates(subset='Date')
        new_df = new_df.sort_values('Date')
        new_df['Gage'] = new_df['Gage'].replace('', np.nan).ffill()
        new_df['Flow'] = (new_df['Flow'].replace('', np.nan).fillna(0)).astype(float)
        
        new_df['Flow_7d_Sum'] = (new_df['Flow'].rolling(min_periods = 0, window = 7).sum()) #sum
        new_df['Flow_7d'] = (new_df['Flow'].rolling(min_periods = 0, window = 7).sum())/7 #average      
        new_df['Date'] = pd.to_datetime(new_df['Date']).dt.date
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        percentile_list = []
        for i in new_df['Flow'].values:
            percentile_of_flow = stats.percentileofscore(new_df['Flow'].values, i, kind='rank')
            percentile_list.append(percentile_of_flow)
        new_df['Percentile'] = percentile_list
        new_df.to_excel(writer, sheet_name='Daily_flow_percentile', index=False)
        writer.save()










