import pandas as pd
import numpy as np
from datetime import datetime, date

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/POLICE/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/POLICE/"

outcome_file_name = "DCJ_HOMELESSNESS_ServiceUse_person_FY_indicator"

#set the start and end for the range of financial years
financial_year_start = '1415'
financial_year_end = '1819'

def read_homelessness_data():

   df_homelessness = pd.read_csv(inputDir + 'homelessness_nsw_1518_1920.csv',usecols=['ppn', 'Initial_SP_Start','Latest_SP_Finished'],parse_dates=['Event_Reported_date'],infer_datetime_format=True)
   df_homelessness = df_homelessness.drop_duplicates()
  
   #drop cases with negaive lenght of service use
  df_homelessness = df_homelessness.drop(df_homelessness.loc[(df_homelessness.Latest_SP_Finished- df_admissions.Initial_SP_Start).dt.days<0].index)
   
   return df_homelessness

def aggregate_to_person_FY(df_homelessness,financial_year_start,financial_year_end):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_homelessness = df_homelessness.merge(df_homelessness.apply(lambda s:pd.date_range(str(s.Initial_SP_Start)[:10] , s.Latest_SP_Finished,freq='AS-JUL'),1)
                                          .explode()
                                          .rename('HS')
                                          .drop(columns=['Initial_SP_Start','Latest_SP_Finished'],axis=0)
                                          lef_index=True,
                                          right_index=True)
  
  df_homelessness.HS = np.where(df_homelessness.HS.isnull(),df_homelessness.Initial_SP_Start.dt.to_period('Y'), df_homelessness.HS)
  df_homelessness.HS = df_homelessness.HS.apply(lambda x: str(x)[2:]+str(int(str(x))+1)[2:])
  
  df_homelessness= pd.get_dummies(df_homelessness, columns=['HS'])
   
  #define a new naming for new columns
  years = list(range(int(financial_year_start[2:]),int(financial_year_end[2:])+1))
  usecols = ['HS_'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_homelessness = df_homelessness.loc[df_homelessness[usecols].sum(1)>0]
  
  #keep only the columns needed and remove duplicates 
  usecols = ["ppn"] + usecols
  df_homelessness = df_homelessness[usecols]
  df_homelessness = df_homelessness.drop_duplicates()
  
  return df_homelessness
  

if __name__ == '__main__':

  df_homelessness = read_homelessness_data()
  df_homelessness = aggregate_to_person_FY(df_homelessness,financial_year_start,financial_year_end)
  df_homelessness.to_csv(OutputDir + outcome_file_name + '_'+ financial_year_start + '_' +financial_year_end + '.csv' , index=False)
