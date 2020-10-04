import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import curve_fit
from multiprocessing import Pool
from os import listdir
from os.path import isfile, join
import os
from datetime import datetime
import QuantLib as ql 

def generate_implied_div(path):
	r = 0.02
	df = pd.read_csv(path)
	df['Expiration'] = pd.to_datetime(df['Expiration'])
	df['DataDate'] = pd.to_datetime(df['DataDate']) 
	df['TTM'] = [t.days for t in (df['Expiration'] - df['DataDate'])]
	df = df[df.TTM>150]
	df = df[df.TTM<180]
	df = df.reset_index(drop=True)
	
	# read risk-free rate
	year = path[-12:-8]
	print(year)
if __name__ == '__main__':    
	generate_implied_div("C://Users/ttkad/Dropbox/Summer20/SB/2016con/option500/500option_L2_options_20160630.csv")