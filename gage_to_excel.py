from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import datetime, time
import numpy as np
import calendar
from scipy.stats import skew
from scipy import stats


def leap_year(year):
    if year % 4 == 0:
        return True
    else:
        return False

def days_in_month(year, month):
    if month in [1, 3, 5, 7, 8, 10, 12]:
        return 31
    elif month == 2:
        if leap_year(year):
            return 29
        return 28
    return 30

"""
Designing flow class:
1. def __init__: call the river_number
2. def scrape(self): collect the daily data of river_number from the website
3. def excel_list(self): get the data frames(
    new_df: from the first date to the end
    Ym_df: the data in a month/year
    Y_df: the data in a year
    After_df: the data frame of filtering over missing 10 days of a year
    Final_df: calculate Q7.10 for each year) and export an excel file
"""

class flow:
    def __init__(self, river_number):
        self.river_number = river_number
        
    """
    Scraping data from the website:
       1. Use the requests.get function to obtain data from the USGS. 
       2. The today variable helps us update the URL.
       3. After getting the data, I use lthe xml parser to parse them, and the prettify function helps export them as HTML format.
       4. Use regular expression to collect the site number, year, month, date, and flow.
    """
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

    """
    New data frame
    1. df is the dates' data frame with values
    2. new_df is the data frame with all dates from start to nowadays and including zero values.
    3. The pd.date_range function helps collect all dates of each site.
    """
    def excel_list(self):
        match_list = self.scrape()
        #the first dataframe
        df = pd.DataFrame(match_list)
        writer = pd.ExcelWriter(f'{self.river_number}.xlsx', engine='xlsxwriter')
        df.columns = ['Gage', 'Date', 'Flow']
        #find missing dates and fill out blanks with zero 
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
        
        """
        Year_month data frame
        1. Use the lambda to extract only Year and Month from df['Date']
        2. Use the dictionary to count the number of collected days of each month from df, then build Ym_df
        3. The all_dates dataframe with all date values combines the Ym_df.
        4. Since i got the number of collected dates, 
           I define the days_in_month function to provide the number of days in a month.
        """
        #Year_month data frame
        dict_yrmon = dict()
        Date_list = list(df['Date'].apply(lambda x: x.strftime('%Y-%m')))
        for i in range(len(Date_list)):
            if Date_list[i] not in dict_yrmon.keys():
                cnt = 1
                dict_yrmon[Date_list[i]] = cnt
            elif Date_list[i] in dict_yrmon.keys():
                cnt +=1
                dict_yrmon[Date_list[i]] = cnt
        Ym_df = pd.DataFrame.from_dict(dict_yrmon, orient="index").reset_index()
        Ym_df.columns = ['Year_month', 'Collected_days']
        #Then use Date column from new_df data frame to create a complete year_month column
        all_dates = pd.DataFrame(pd.date_range(new_df['Date'].min(), new_df['Date'].max()), columns=['Year_month'])
        all_dates['Year_month'] = all_dates['Year_month'].apply(lambda x: x.strftime('%Y-%m'))
        Ym_df = pd.concat([Ym_df, all_dates], axis = 0)
        Ym_df = Ym_df.drop_duplicates(subset = 'Year_month')
        Ym_df = Ym_df.sort_values('Year_month')
        Ym_df['Collected_days'] = Ym_df['Collected_days'].fillna(0).astype(float)
        
        #define day_in_month function to find how many days in each month
        days_in_month_list = []
        day_list = []
        for i in list(Ym_df['Year_month']):
            i = i.split('-')
            days_in_month_list.append(i)
        for i in days_in_month_list:
            day_list.append(days_in_month(int(i[0]), int(i[1])))
        Ym_df['Days_in_month'] = day_list
        Ym_df['Day_differences'] = Ym_df['Days_in_month']-Ym_df['Collected_days']
        
        """
        Year data frame
        1. Use the dictionary to count collected dates and build as Y_df.
        2. Pair Y_df with the all_dates data frame.
        3. Use the calendar.isleap to obtain the number of days in a year.
        4. Then count the differences between collected_days and days_in_year.
        5. The count which is lower than ten, the year data would be used as final data.
        6. Use a dictionary to find the Minimum_of_flow7d, row by row
        """
        #Year data frame
        #use a dictionary to count the collected day of each year
        Year_list = list(df['Date'].apply(lambda x: x.strftime('%Y')))
        dict_yr = dict()
        for i in range(len(Year_list)):
            if Year_list[i] not in dict_yr.keys():
                cnt = 1
                dict_yr[Year_list[i]] = cnt
            elif Year_list[i] in dict_yr.keys():
                cnt+=1
                dict_yr[Year_list[i]] = cnt
        Y_df = pd.DataFrame.from_dict(dict_yr, orient="index").reset_index()
        Y_df.columns = ['Year', 'Collected_days']
        #Then use Ym_df data frame to create a Year column
        all_dates = pd.DataFrame(pd.date_range(Ym_df['Year_month'].min(), Ym_df['Year_month'].max()), columns=['Year'])
        all_dates['Year'] = all_dates['Year'].apply(lambda x: x.strftime('%Y'))
        Y_df = pd.concat([Y_df, all_dates], axis = 0)
        Y_df = Y_df.drop_duplicates(subset = 'Year')
        Y_df = Y_df.sort_values('Year')
        Y_df['Collected_days'] = Y_df['Collected_days'].fillna(0).astype(float)

        Days_in_year = []
        for i in Y_df['Year'].values:
            if calendar.isleap(int(i)):
                Days_in_year.append(int(366))
            else:
                Days_in_year.append(int(365))
        Y_df['Year'] = Y_df['Year'].astype(int)
        Y_df['Days_in_year'] = Days_in_year
        Y_df['Day_differences'] = Y_df['Days_in_year']-Y_df['Collected_days']
        Y_df.loc[Y_df['Day_differences'] <=10, 'Can_use_data'] = 'True'
        Y_df.loc[Y_df['Day_differences'] >10, 'Can_use_data'] = 'False'
        #calculate the minimum of flow_7d in a year by using a dictionary
        minimum_dict = {}
        time_list = list(new_df['Date'].apply(lambda x: x.strftime('%Y')))
        flow_list = list(new_df['Flow_7d'])
        for i in range(len(time_list)):
            if time_list[i] not in minimum_dict.keys():
               minimum_dict[time_list[i]] = flow_list[i]
            elif time_list[i] in minimum_dict.keys() and flow_list[i] < minimum_dict[time_list[i]]:
               minimum_dict[time_list[i]] = flow_list[i]
        Y_df['Minimum_of_flow7d'] = list(minimum_dict.values())
        
        """
        After data frame
        1. Select those years whose missing days are lower than 10 days from new_df.
        2. Calculate the percentile of each flow
        """
        #The Dataframe I am going to use(available data with lower 10 missing days)
        Use_list = list(Y_df.loc[Y_df['Day_differences'] <=10,'Year'])
        Use_list_st = list(map(str, Use_list)) #change to a string list
        #if the years are in the Use_list_st
        After_df  = new_df.loc[new_df["Date"].apply(lambda x: x.strftime('%Y')).isin(Use_list_st)]
        After_df = After_df.copy()
        After_df['Year'] = pd.DatetimeIndex(After_df.loc[:,'Date']).year
        #percentile_list = []
        #for i in After_df['Flow'].values:
            #percentile_of_flow = stats.percentileofscore(After_df['Flow'].values, i, kind='rank')
            #percentile_list.append(percentile_of_flow)
        #After_df['Percentile'] = percentile_list
            

        
        #----------------------------------------------------------------------
        """
        Calculation
        1. Copy rows that are True in the Can_use_data column from the Y_df to the Final_df.
        2. Calculate log with Minimum_of_flow7d.
        3. k is the skew of a log with Minimum_of_flow7d.
        """          
        #The last data frame       
        #copy the Year and flow_7d columns from the Daily flow sheet
        selected_columns = Y_df[["Year","Minimum_of_flow7d", 'Can_use_data']]
        Final_df = selected_columns.copy()
        Final_df = Final_df[(Final_df[['Can_use_data']] != 'False').all(axis=1)]

        Final_df['Log_space'] = np.log10(Final_df['Minimum_of_flow7d']) 
        
        Final_df['k'] = Final_df['Log_space'].skew(axis = 0, skipna = True)
        #k = skew(Final_df['Log_space'])
        k = Final_df['Log_space'].skew(axis = 0, skipna = True)
        Final_df['Mean'] = Final_df['Log_space'].mean()
        Mean = Final_df['Log_space'].mean()
        Final_df['Std'] = np.std(Final_df['Log_space'])
        Std = np.std(Final_df['Log_space'])
        ln_Q7_10 = Mean + (k*Std)
        Final_df['ln_Q7_10'] = Mean + (k*Std)
        Q7_10 = np.exp(ln_Q7_10)
        Final_df['Q7_10'] = Q7_10
        
        
        
      

        #convert data frames into excel sheets
        new_df.to_excel(writer, sheet_name='Daily_flow', index=False)
        Ym_df.to_excel(writer, sheet_name = 'Year_month_flow', index = False)
        Y_df.to_excel(writer, sheet_name = 'Year_flow', index = False)
        After_df.to_excel(writer, sheet_name = 'After_Daily_flow', index = False)
        Final_df.to_excel(writer, sheet_name = 'Final_Yearly_flow', index = False)
        writer.save()
        
        
        