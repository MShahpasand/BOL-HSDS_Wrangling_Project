import dask.dataframe as dd
import dask.multiprocessing
from dask.diagnostics import ProgressBar
import pandas as pd
import numpy as np
import tables
import glob
import os

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/DCJ_JUVENILE_JUSTICE/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/DCJ_JUVENILE_JUSTICE/"

def read_admission_data():

   df_admissions = pd.read_csv(inputDir + 'jj_admissions.csv',usecols=['PPN', 'AdmissionDate','DischargeDate'],parse_dates=['AdmissionDate','DischargeDate'], infer_datetime_format=True)
   df_admissions=df_admissions.drop_duplicates()
   df_admissions=df_admissions.drop(df_admissions.loc[(df_admissions.DischargeDate- df_admissions.AdmissionDate).dt.days<0].index)
   return df_admissions

def aggregate_to_FY(df_admissions):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_admissions['JJ_Adm'] = df_admissions.AdmissionDate.dt.to_period('Q-JUN').dt.qyear.apply(lambda x : str(x-1)[2:]+str(x)[2:])
  df_admissions=pd.get_dummies(df_admissions,columns=['JJ_Adm'])
  
  #define a new naming for new columns
  years=list(range(11,20))
  usecols=['JJ_Adm_'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_admissions=df_admissions.loc[df_admissions[usecols]].sum(1)>0
  
  #keep only the columns needed and remove duplicates 
  usecols=["PPN"] + usecols
  df_admissions=df_admissions[usecols]
  df_admissions=df_admissions.drop_duplicates()
  
  df_admissions.to_csv(OutputDir + 'jj_admissions_1011_1819_FY_indicator.csv', index=False)
  

if __name__ == '__main__':

  df_admissions=read_admission_data()
  aggregate_to_FY(df_admissions)
