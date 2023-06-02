import pandas as pd
import numpy as np
DMR = "DMR FLOW Data For IL SWS raw.xlsx"
dmr_df = pd.read_excel(DMR)
#dmr10_df = dmr_df.head(50)
#print(list(dmr10_df.columns))
'''
sort value on Statistical Base Long Desc (measurements)'
'''
#dmr10_new = dmr10_df.sort_values('Statistical Base Long Desc')
dmr_new = dmr_df.sort_values('Statistical Base Long Desc')
#print(dmr10_new)
'''
check how many unique values in the unit column
'''
#a = dmr_new.groupby('Statistical Base Long Desc')['Limit Unit Desc'].unique().explode().reset_index()

'''
create new columns
'''
dmr_new['day'] = dmr_new['Monitoring Period End Date'].dt.day
dmr_new['month'] = dmr_new['Monitoring Period End Date'].dt.month
dmr_new['year'] = dmr_new['Monitoring Period End Date'].dt.year
#print(dmr_new.head(1))

'''
convert different units to "Million Gallons per Day"
'''
# Create boolean masks
month = dmr_new['Limit Unit Desc'].str.contains('Million Gallons per Month')
gallons_day = dmr_new['Limit Unit Desc'].str.contains('Gallons per Day')
# Apply the mask logic and change the name of unit
dmr_new.loc[month, 'DMR Value'] = dmr_new.loc[month, 'DMR Value'] / dmr_new['day']
dmr_new.loc[month, 'Limit Unit Desc'] = 'Million Gallons per Day'
dmr_new.loc[gallons_day, 'DMR Value'] = dmr_new.loc[gallons_day, 'DMR Value'] / 1000000
dmr_new.loc[gallons_day, 'Limit Unit Desc'] = 'Million Gallons per Day'
#a = dmr_new.groupby('Statistical Base Long Desc')['Limit Unit Desc'].unique().explode().reset_index()
#print(a)

'''
combine rows which have same values under 'Limit Unit Desc','day', 'month', 'year'
'''
f = {'NPDES ID': 'first', 'Permit Name': 'first', 'Latitude in DMS': 'first', 'Longitude in DMS': 'first', 'State Water Body Name': 'first', 'Perm Feature ID': 'first','Limit Set Designator': 'first', 'Limit Set Name': 'first', 'Parameter Code': 'first', 'Parameter Desc': 'first', 'Monitoring Location Code': 'first', 'Monitoring Location Desc': 'first', 'Monitoring Period End Date': 'first', 'DMR Value Qualifier Code': 'first', 'DMR Value': 'sum', 'DMR Value Unit Short Desc': 'first', 'DMR Value Type Code': 'first' ,'NODI Code': 'first', 'Violation Code': 'first', 'Limit Value Qualifier Code': 'first', 'Limit Value': 'first', 'DMR Value Type Code': 'first'}
dmr_new.groupby(['Statistical Base Long Desc', 'Limit Unit Desc','day', 'month', 'year'], as_index=False).agg(f)
dmr_new = dmr_new.sort_values('NPDES ID')
dmr_new = dmr_new.reset_index(drop = True)
dmr_new = dmr_new.drop("Unnamed: 0",axis=1)
#print(dmr_new.head(10))
dmr_new.to_excel("output.xlsx")







