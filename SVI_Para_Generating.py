import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import curve_fit
from multiprocessing import Pool
from os import listdir
from os.path import isfile, join
import os

'''
In order to allow this code work on your computer, please make sure the paths are changed
to local paths. There are 2 places need to be changed. First is the first line in the Main
function, the second path is in the last 2 lines of the calibarate_file function, the place
.to_csv(PATH).

Also before everything. All file names should be corrected to legit format before calibaration.
For more detail, please check the File_Rename.ipynb.


The whole logic is as following:

The calibarate_file() function takes 3 parameters
	The first one is the file path which is locate the function to a .csv file
	The second and third parameters are trivial just help us to print 
	and track Multiprocessing so we can track the calculation process clearly.

In the Main function. Since the calibarate_file function takes path(s) as input.
The Main function first figure out the paths of all the .csv we need, and save the
paths in the array called "data_file_paths"

Then multi-process is applied.
'''

def calibarate_file(file_path,ctr,total):

	def SVI(moneyness,a,b,rho,m,s):
		return np.sqrt(a+b*(rho*(np.log(moneyness)-m)+np.sqrt((np.log(moneyness)-m)**2+s**2)))

	'''
	Read .csv and data cleaning. In specific, I only pick OTM options to calibarate the curve
	This might be controversial, but it is probably correct because we need to do variance
	swap pricing.
	'''

	file_name = 'SVI_Para_'+file_path.split('/')[-1]
	print('The '+str(ctr+1)+"th out of "+str(total)+" files has been started calibaration")
	df = pd.read_csv(file_path)
	df['Pos'] = df['UnderlyingPrice'] - df['Strike']
	df = df[np.logical_or(np.logical_and(df['Pos']>0,df['Type']=='put'),np.logical_and(df['Pos']<0,df['Type']=='call'))]
	df = df.reset_index(drop=True)

	'''
	As the .csv file contains 500 stocks and each stock has several maturities.
	This is exactly the reason that we have 2 for loops. One iterate through stocks
	the other iterate through maturities. Calabarated parameters are documented in
	2D array vol_paras, which is saved as .csv at the end.
	'''
	stocks = df['UnderlyingSymbol'].unique()
	vol_paras = []
	initial = 0.1,0.1,0.1,0.1,0.1
	ctr = 0
	for stock in stocks:
	    df_stock = df[df['UnderlyingSymbol']==stock]
	    maturities = df_stock['Expiration'].unique()
	    S = df_stock['UnderlyingPrice'].max()
	    for day in maturities:
	        strikes = np.array(df_stock[df_stock['Expiration']==day]['Strike'])
	        iv = df_stock[df_stock['Expiration']==day]['IV']
	        try:
	            para,_ = curve_fit(SVI, strikes/S, np.array(iv), initial, bounds = ((0,0,-1,-np.inf,0),(np.inf,np.inf,1,np.inf, np.inf)), maxfev = 3000)
	        except:
	            para = [0,0,0,0,0]
	        vol_paras += [[day,stock]+list(para)+[strikes]+[iv]]
	# Please make sure you change to_csv(PATH) to your own path :P
	vol_paras = pd.DataFrame(vol_paras,columns=['Maturity','Ticker','a','b','rho','m','s','strikes','iv']).to_csv("C://Users/ttkad/Dropbox/Summer20/SB/SVI_Paras/"+file_name)
	print('The '+str(ctr+1)+"th out of "+str(total)+" files has finished calibaration")

if __name__ == '__main__':    
	# SB is the Folder Path of the gigantic 12GB folder. Pleaes change it to your local path
	SB = 'C://Users/ttkad/Dropbox/Summer20/SB'                                  
	folders = [str(i)+"con" for i in range(2002,2019)]
	data_file_paths = []
	for folder in folders:
	    path = SB+'/'+folder+"/option500"
	    files = [f for f in listdir(path) if f[-4:]=='.csv']
	    data_file_paths += [path+'/'+file for file in files]
	total = len(data_file_paths)-1                        

	'''
	Multiprocessing. Please make sure you check the max number or process your computer has.
	If your computer blows up, I do not take responsibility. In that case, please contact
	Trader Klaus Xiang (yx1083@nyu.edu). He will purchase a new Mac Labtop for you :3
	'''

	num_proc = 8
	pool = Pool(processes=num_proc)                        
	multiple_processes = [pool.apply_async(calibarate_file,(data_file_paths[ctr+1],ctr,total)) for ctr in range(len(data_file_paths)-1)]
	tes = [proc.get() for proc in multiple_processes]
	pool.close()
	pool.join()
