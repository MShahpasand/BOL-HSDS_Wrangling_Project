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

  df_homelessness = pd.read_csv(inputDir + 'homelessness_nsw_1518_1920.csv',usecols=['ppn', 'Initial_SP_Start','Latest_SP_Finished'],parse_dates=['Initial_SP_Start','Latest_SP_Finished'], infer_datetime_format=True)
  
  #drop duplicates
  df_homelessness=df_homelessness.drop_duplicates()
  
  #drop cases with negaive lenght of service use
  df_homelessness=df_homelessness.drop(df_homelessness.loc[(df_homelessness.Latest_SP_Finished- df_admissions.Initial_SP_Start).dt.days<0].index)
    
  return df_homelessness


def aggregate_to_FY(df_homelessness):

  #take the list of financial years between the start and end date of a service episode 
  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_homelessness = df_homelessness.merge(df_homelessness.apply(lambda s:pd.date_range(str(s.Initial_SP_Start)[:10], s.Latest_SP_Finished, freq='AS-JUK'),1)
             .explode()
  #HS stands for Homelessness Service
             .rename('HS')
             .dt.strftime("%Y")
             .drop(columns=['Initial_SP_Start','Latest_SP_Finished'],axis=0),
             left_index=True,
             right_index=True)
  
  #fill nan values with the year from start date 
  df_homelessness.HS=np.where(df_homelessness.HS.isnull(),df['Initial_SP_Start'].dt.to_period('Y'),df_homelessness.HS)
  
  #change format to FY (e.g. 2017 -> 1718)
  df_homelessness.HS=df_homelessness.HS.apply(lambda s: str(s)[2:]+str(int(str(x))+1)[2:])

  df_homelessness=pd.get_dummies(df_homelessness,columns=['HS'])
  
  #define a new naming for financial year columns
  years=list(range(15,20))
  usecols=['HS_'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_homelessness=df_homelessness.loc[df_homelessness[usecols]].sum(1)>0
  
  #keep only the columns needed and remove duplicates 
  usecols=["ppn"] + usecols
  df_homelessness=df_homelessness[usecols]
  df_homelessness=df_homelessness.drop_duplicates()
  
  df_homelessness.to_csv(OutputDir + 'HS_1516_1819_FY_indicator.csv', index=False)
  

if __name__ == '__main__':

  df_homelessness=read_homelessness_data()
  aggregate_to_FY(df_homelessness)
