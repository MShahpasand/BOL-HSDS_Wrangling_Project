import pandas as pd
import numpy as np
from datetime import datetime, date

#change the <inputDir> as where the csv is located in your drive
inputDir = "../HSDS/DCJ_JUVENILE_JUSTICE/"
#change the <OutputDir> as where you want to save the indicator list
OutputDir = "../HSDS/DCJ_JUVENILE_JUSTICE/"

outcome_file_name = "DCJ_JUVENILE_JUSTICE_Admission_person_FY_indicator"

#set the start and end for the range of financial years
financial_year_start = '1011'
financial_year_end = '1819'

def read_jj_admission_data():

   df_jj_admissions = pd.read_csv(inputDir + 'jj_admissions.csv',usecols=['PPN', 'AdmissionDate','DischargeDate'],parse_dates=['AdmissionDate','DischargeDate'],infer_datetime_format=True)
   df_jj_admissions = df_jj_admissions.drop_duplicates()
   df_jj_admissions=df_jj_admissions.drop(df_jj_admissions.loc[(df_jj_admissions.DischargeDate - df_jj_admissions.AdmissionDate).dt.days<0].index)
   df_jj_admissions.DischargeDate = np.where(df_jj_admissions.DischargeDate.isnull(),df_jj_admissions.AdmissionDate, df_jj_admissions.DischargeDate)

   return df_jj_admissions

def aggregate_to_person_FY(df_jj_admissions,financial_year_start,financial_year_end):

  # financial year is considered from 01-07-CurrentYear to 30-06-NextYear
  df_jj_admissions = df_jj_admissions.merge(df_homelessness.apply(lambda s:pd.date_range(str(s.AdmissionDate)[:10] , s.DischargeDate,freq='AS-JUL'),1)
                                          .explode()
                                          .rename('JJ_Adm')
                                          .drop(columns=['AdmissionDate','DischargeDate'],axis=0)
                                          lef_index=True,
                                          right_index=True)
  
  df_jj_admissions.JJ_Adm = np.where(df_jj_admissions.JJ_Adm.isnull(),df_jj_admissions.AdmissionDate.dt.to_period('Y'), df_jj_admissions.JJ_Adm)
  df_jj_admissions.JJ_Adm = df_jj_admissions.JJ_Adm.apply(lambda x: str(x)[2:]+str(int(str(x))+1)[2:])


  df_jj_admissions = pd.get_dummies(df_jj_admissions,columns=['JJ_Adm'])

  #define a new naming for new columns
  years = list(range(int(financial_year_start[2:]),int(financial_year_end[2:])+1))
  usecols = ['JJ_Adm'+str(x-1)+str(x) for x in years]
  
  #keep only the rows with at least one flag for a financial year 
  df_jj_admissions = df_jj_admissions.loc[df_jj_admissions[usecols].sum(1)>0]
  
  #keep only the columns needed and remove duplicates 
  usecols = ["ppn"] + usecols
  df_jj_admissions = df_jj_admissions[usecols]
  df_jj_admissions = df_jj_admissions.drop_duplicates()
  
  return df_jj_admissions
  

if __name__ == '__main__':

  df_jj_admissions = read_jj_admission_data()
  df_jj_admissions = aggregate_to_person_FY(df_jj_admissions,financial_year_start,financial_year_end)
  df_jj_admissions.to_csv(OutputDir + outcome_file_name + '_'+ financial_year_start + '_' +financial_year_end + '.csv' , index=False)
