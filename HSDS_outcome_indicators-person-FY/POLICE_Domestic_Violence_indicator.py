import pandas as pd
import numpy as np
from datetime import datetime, date

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/POLICE/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/POLICE/"

outcome_file_name = "POLICE_Domestic_Violence_person_FY_indicator"

#set the start and end for the range of financial years
financial_year_start = '1011'
financial_year_end = '1819'

def read_victims_sensitive_data():

   df_victims_sensitive = pd.read_csv(inputDir + 'victims_sensitive_v2.csv',usecols=['ppn','Event_Reported_date','Incident_Category'],parse_dates=['Event_Reported_date'],infer_datetime_format=True)
   df_victims_sensitive = df_victims_sensitive.drop_duplicates()
   df_victims_sensitive = df_victims_sensitive.loc[df_victims_sensitive.Incident_Category =='Domestic Violence-No Offence']
   
   return df_victims_sensitive

def aggregate_to_person_FY(df_victims_sensitive,financial_year_start,financial_year_end):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_victims_sensitive['DV'] = df_victims_sensitive.Event_Reported_date.dt.to_period('Q-JUN').dt.qyear.apply(lambda x : str(x-1)[2:]+str(x)[2:])
  df_victims_sensitive = pd.get_dummies(df_victims_sensitive,columns=['DV'])
  df_victims_sensitive = df_victims_sensitive.reset_index()
   
  #define a new naming for new columns
  years = list(range(int(financial_year_start[2:]),int(financial_year_end[2:])+1))
  usecols = ['DV_'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_victims_sensitive = df_victims_sensitive.loc[df_victims_sensitive[usecols].sum(1)>0]
  
  #keep only the columns needed and remove duplicates 
  usecols = ["ppn"] + usecols
  df_victims_sensitive = df_victims_sensitive[usecols]
  df_victims_sensitive = df_victims_sensitive.drop_duplicates()
  
  return df_victims_sensitive
  

if __name__ == '__main__':

  df_victims_sensitive = read_victims_sensitive_data()
  df_victims_sensitive = aggregate_to_person_FY(df_victims_sensitive,financial_year_start,financial_year_end)
  df_victims_sensitive.to_csv(OutputDir + outcome_file_name + '_'+ financial_year_start + '_' +financial_year_end + '.csv' , index=False)

