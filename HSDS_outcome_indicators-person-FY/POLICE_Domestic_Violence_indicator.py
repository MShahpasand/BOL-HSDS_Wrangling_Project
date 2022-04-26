import dask.dataframe as dd
import dask.multiprocessing
from dask.diagnostics import ProgressBar
import pandas as pd
import numpy as np
import tables
import glob
import os

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/POLICE/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/POLICE/"

def read_victims_sensitive_data():

   df_victims_sensitive = pd.read_csv(inputDir + 'victims_sensitive.csv',usecols=['ppn','Event_Reported_date','Incident_Category'],parse_dates=['Event_Reported_date'],infer_datetime_format=True)
   df_victims_sensitive=df_victims_sensitive.drop_duplicates()
   df_victims_sensitive=df_victims_sensitive.loc[df_victims_sensitive.Incident_Category=='Dpmestic Violence-No Offence']
   return df_admissions

def aggregate_to_FY(df_victims_sensitive):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_victims_sensitive['DV'] = df_victims_sensitive.Event_Reported_date.dt.to_period('Q-JUN').dt.qyear.apply(lambda x : str(x-1)[2:]+str(x)[2:])
  df_victims_sensitive=pd.get_dummies(df_victims_sensitive,columns=['DV'])
  
  #define a new naming for new columns
  years=list(range(11,20))
  usecols=['DV_'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_victims_sensitive=df_victims_sensitive.loc[df_victims_sensitive[usecols]].sum(1)>0
  
  #keep only the columns needed and remove duplicates 
  usecols=["ppn"] + usecols
  df_victims_sensitive=df_victims_sensitive[usecols]
  df_victims_sensitive=df_victims_sensitive.drop_duplicates()
  
  df_victims_sensitive.to_csv(OutputDir + 'DV_1011_1819_FY_indicator.csv', index=False)
  

if __name__ == '__main__':

  df_victims_sensitive=read_victims_sensitive_data()
  aggregate_to_FY(df_victims_sensitive)
