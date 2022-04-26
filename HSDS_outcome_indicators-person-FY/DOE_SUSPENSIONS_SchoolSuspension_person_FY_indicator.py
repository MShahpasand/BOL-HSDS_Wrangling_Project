import pandas as pd
import numpy as np
from datetime import datetime, date

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/DOE_SUSPENSIONS/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/DOE_SUSPENSIONS/"

outcome_file_name = "DOE_SUSPENSIONS_SchoolSuspension_person_FY_indicator"

#set the start and end for the range of financial years
financial_year_start = '1011'
financial_year_end = '1819'

def read_school_suspension_data():

   df_suspension = pd.read_csv(inputDir + 'student_suspensionssensitive_v2.csv',usecols=['ppn', 'SUSPENSION_FROM_DATE','SUSPENSION_TO_DATE'],parse_dates=['AdmissionDate','DischargeDate'],infer_datetime_format=True)
   df_suspension = df_suspension.drop_duplicates()
   df_suspension=df_suspension.drop(df_suspension.loc[(df_suspension.SUSPENSION_TO_DATE - df_jj_admissions.SUSPENSION_FROM_DATE).dt.days<0].index)

   return df_suspension

def aggregate_to_person_FY(df_suspension,financial_year_start,financial_year_end):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_suspension = df_suspension.merge(df_suspension.apply(lambda s:pd.date_range(str(s.SUSPENSION_FROM_DATE)[:10] , str(s.SUSPENSION_TO_DATE)[:10],freq='AS-JUL'),1)
                                          .explode()
                                          .rename('Sus')
                                          .drop(columns=['SUSPENSION_FROM_DATE','SUSPENSION_TO_DATE'],axis=0)
                                          lef_index=True,
                                          right_index=True)
  
  df_suspension.Sus = np.where(df_suspension.Sus.isnull(),df_suspension.SUSPENSION_FROM_DATE.dt.to_period('Y'), df_suspension.Sus)
  df_suspension.Sus = df_suspension.Sus.apply(lambda x: str(x)[2:]+str(int(str(x))+1)[2:])


  df_suspension = pd.get_dummies(df_suspension,columns=['Sus'])

  #define a new naming for new columns
  years = list(range(int(financial_year_start[2:]),int(financial_year_end[2:])+1))
  usecols = ['JJ_Adm'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_suspension = df_suspension.loc[df_suspension[usecols].sum(1)>0]
  
  #keep only the columns needed and remove duplicates 
  usecols = ["ppn"] + usecols
  df_suspension = df_suspension[usecols]
  df_suspension = df_suspension.drop_duplicates()
  
  return df_suspension
  

if __name__ == '__main__':

  df_suspension = read_school_suspension_data()
  df_suspension = aggregate_to_person_FY(df_suspension,financial_year_start,financial_year_end)
  df_suspension.to_csv(OutputDir + outcome_file_name + '_'+ financial_year_start + '_' +financial_year_end + '.csv' , index=False)
