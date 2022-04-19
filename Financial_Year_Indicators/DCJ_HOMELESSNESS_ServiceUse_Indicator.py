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
  
