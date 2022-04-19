import dask.dataframe as dd
import dask.multiprocessing
from dask.diagnostics import ProgressBar
import pandas as pd
import numpy as np
import tables
import glob
import os

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/DCJ_HOMELESSNESS/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/DCJ_HOMELESSNESS/"

def read_homelessness_data():

  df_homelessness = pd.read_csv(inputDir + 'homelessness_nsw_1518_1920.csv',usecols=['PPN', 'ReportingPeriod_YTD'],parse_dates=['ReportingPeriod_YTD'], infer_datetime_format=True)
  df_homelessness=df_homelessness.drop_duplicates()
  return df_homelessness


def aggregate_to_FY(df_homelessness):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  #HS stands for Homelessness Service
  df_homelessness['HS'] = df_homelessness.ReportingPeriod_YTD.dt.to_period('Q-JUN').dt.qyear.apply(lambda x : str(x-1)[2:]+str(x)[2:])
  df_homelessness=pd.get_dummies(df_homelessness,columns=['HS'])
  
  #define a new naming for financial year columns
  years=list(range(15,20))
  usecols=['HS_'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with atleast one flage for a financial year 
  df_homelessness=df_homelessness.loc[df_homelessness[usecols]].sum(1)>0
  
  #keep only the columns needed and remove duplicates 
  usecols=["PPN"] + usecols
  df_homelessness=df_homelessness[usecols]
  df_homelessness=df_homelessness.drop_duplicates()
  
  df_homelessness.to_csv(OutputDir + 'HS_1516_1819_FY_indicator.csv', index=False)
  

if __name__ == '__main__':

  df_homelessness=read_homelessness_data()
  aggregate_to_FY(df_homelessness)
