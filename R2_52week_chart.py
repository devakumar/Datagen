"""
Author: T. Devakumar
Created: 29th April 2017
Description:
	Find the moving average of a pandas dataframe and plot the same w.r.t base
	data

	# Adding average volume over last 10 days
"""
import pandas as pd
import matplotlib.pyplot as plt
import datetime, os
from sklearn import linear_model

def getData(start, end, tag = 'hi'):
	df = pd.read_csv('Data/{0}.data'.format(tag))
	cols = df.columns.tolist()

	df[cols[0]] = pd.to_datetime(df[cols[0]])

	after_start = df[df[cols[0]]>start]

	return after_start[after_start[cols[0]] < end]

def get52Week(company, From, df=None, days = 365):
	if df is None:
		now = From#datetime.datetime.today()
		start = now - datetime.timedelta(days = days)
		#start = datetime.datetime(now.year-1, now.month, now.day)

		df = getData(start, now, 'lo')
		df.columns = [var.upper() for var in df.columns]
	
	cols = df.columns.tolist()
	data = df[[cols[0], company]].dropna()
	stats = data.describe().to_dict()[company]
	stats['present'] = data[company].tolist()[-1]
	stats['start_date'] = data[cols[0]].tolist()[0]
	stats['present_date'] = data[cols[0]].tolist()[-1]
	return stats, df

def get52WeekSummary(From, write=False, latest=None, days = 365):
	df = pd.read_csv('Data/hi.data')
	cols = df.columns.tolist()

	wk52 = {}
	df = None
	n = len(cols) -1
	for i, com in enumerate(cols[1:]):
		try :
			if i %500 == 0: print n-i, 
			wk52[com], df = get52Week(com, From, df, days = days)
			latest_date = wk52[com]['present_date']
			if latest_date == latest:
				print "skipping the run... same as last one"
				return IOError("Same data as of previous run")
			#wait = raw_input("X:")
		except Exception as e:
			#print e,
			continue
	
	latest_date = wk52[cols[1]]['present_date']
	if write:
		wk52df = pd.DataFrame(wk52).T
		with open('52week_low.txt', 'w') as f:
			f.write(wk52df.to_csv())

	return wk52, latest_date

if __name__ == "__main__":
	# now = datetime.datetime.today()
	# wk52 = get52WeekSummary(now, write=True)

	#end = datetime.date(2017, 03, 10)
	end = datetime.datetime.today()

	n = 30 # No of data points required
	count, i = 0, 0
	ans = {}
	prevDay = None
	dataObtained = False
	while not (count == n):
		From = end - datetime.timedelta(days = i)
		try :
			wk52, latest_date = get52WeekSummary(From, write = (count==0), latest=prevDay, days = 180)
		except Exception as e:
			print e
			i += 1
			continue
		wk52_df = pd.DataFrame(wk52).T
		wk52_df['distance_from_max'] = (2*wk52_df['present'] - (wk52_df['max'] + wk52_df['min']))/(wk52_df['max'] - wk52_df['min'] + 1e-10)

		print i, count, latest_date
		ans[n-count-1] = wk52_df['distance_from_max'].to_dict()
		prevDay = latest_date
		count += 1
	
	# Last 10 days index trend
	trend = pd.DataFrame(ans)
	trend_tr = trend.T
	cols = trend_tr.columns.tolist()
	x = trend_tr.index.values
	x = x.reshape(x.shape[0], 1)
	regr = linear_model.LinearRegression()
	ans['m'], ans['c'], ans['2xpred'] = {}, {}, {}
	ans['max'], ans['min'], ans['present'], ans['pred'] = {}, {}, {}, {}
	ans['gain'] = {}
	ans['vol_mean'] = {}

	# Volume data - Only for past few days
	start = end - datetime.timedelta(days = 2*n)
	vol_past = getData(start, end, 'vl')
	for com in cols[1:]:
		try :
			y = trend_tr[com].values
			y = y.reshape(y.shape[0], 1)
			#print com, y, x
			#wait = raw_input("X:")
			regr.fit(x, y)
			m, c, x2pred = regr.coef_[0][0], regr.intercept_[0], regr.predict(i*2)[0][0]
		except Exception as e:
			print e
			m, c, x2pred = 0, 0, 0
		ans['m'][com] = m
		ans['c'][com] = c
		ans['2xpred'][com] = x2pred
		try :
			ans['max'][com], ans['min'][com], ans['present'][com] = wk52_df['max'][com],wk52_df['min'][com],wk52_df['present'][com]
			ans['pred'][com] = 0.5*(x2pred*(ans['max'][com] - ans['min'][com]) + (ans['max'][com] + ans['min'][com]))
			#ans['gain'][com] = (min(ans['pred'][com], ans['max'][com]) - ans['present'][com])/ans['present'][com]*100.0
			ans['gain'][com] = (ans['pred'][com] - ans['present'][com])/ans['present'][com]*100.0
		except Exception as e:	
			ans['max'][com], ans['min'][com], ans['present'][com] = 0, 0, 0
			ans['pred'][com], ans['gain'][com] = 0, 0

		# Get volume related data
		try :
			vol = vol_past[com].dropna()
			ans['vol_mean'][com] = vol.mean()
		except Exception as e:
			# print error in volume avg computation
			ans['vol_mean'][com] = 0

	trend = pd.DataFrame(ans)
	with open('max_dist_trend_month.txt', 'w') as f:
		f.write(trend.to_csv())
