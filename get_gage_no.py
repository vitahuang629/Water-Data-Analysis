# get the flow class from gage_to_excel 
from gage_to_excel import flow
# get the percentile of each flow from percentile module
from percentile import percentile
import pandas as pd
"""
Get the data from the excel file: copy of Illinois gages 2022-3225.xlsx
Then, convert the datatype of site_no into a string list.
Since the site number in the URL is 8 digits, I add the zero before the site number from the excel file.
"""
file = "Copy of Illinois gages 2022-3225.xlsx"
il_gate_df = pd.read_excel(file)
il_gate_df['site_no'] = il_gate_df['site_no'].astype(str)
il_gate_df['site_no'] = il_gate_df['site_no'].str.zfill(8)
il_gate_list = list(il_gate_df['site_no'])


for i in il_gate_list:
    river = flow(i)
    river.excel_list()
    daily_flow = percentile(i)
    daily_flow.excel_percentile()
#river = flow('402004090030901')
#river.excel_list()
#daily_flow = percentile('402004090030901')
#daily_flow.excel_percentile()


