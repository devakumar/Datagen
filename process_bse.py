"""
Created: 25th May 2017
Author : T. Devakumar

Description : Download latest bulk data using BC parse
"""
from BCparse import *
import datetime
import glob
import pandas as pd

if __name__ == "__main__":
	start = raw_input("Start date for processing (DD-MM-YY) : ").strip().split('-')
	start = datetime.date(int('20'+start[-1]), int(start[-2]), int(start[0]))
	end = datetime.date.today()

	delta = end - start
	pwd = os.getcwd() + os.sep + 'Data'
	op, hi, lo, cl, la, pcl, vol, trval, ntr = {}, {}, {}, {}, {}, {}, {}, {}, {}
	files = [op, hi, lo, cl, la, pcl, vol, trval, ntr]
	tags = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTTRDQTY',
	'TOTTRDVAL', 'TOTALTRADES']
	n_success = 0
	dates = []
	for i in range(delta.days + 1):
		date = start + datetime.timedelta(days = i)
		
		# BCparse main section
		# Download the Bhavcopies according to the default / specified date
		date_str_fmt = "{}-{}-{}".format(str(date.day).zfill(2), str(date.month).zfill(2), str(date.year))
		path = pwd + os.sep + date_str_fmt
		if os.path.exists(path):
			# Process BSE data of date_str_fmt
			f = glob.glob(path + os.sep + 'cm*.csv')
			df = pd.read_csv(f[0])
			companies = df['SYMBOL'].tolist()
			for j, tag in enumerate(tags):
				values = df[tag].tolist()
				for k, company in enumerate(companies):
					if files[j].has_key(company):
						files[j][company].append(values[k])
					else :
						files[j][company] = ['']*n_success
						files[j][company].append(values[k])

			# For the companies not in todays file, add empty element to end
			for company in files[0].keys():
				if company not in companies:
					for j, tag in enumerate(tags):
						files[j][company].append('')
			print date_str_fmt, '... processed'
			n_success += 1
			dates.append(date_str_fmt)
		else :
			print date_str_fmt, 'not present'
	
	# Temperorily deleting the files with more than required datas
	for f in files:
		for company in f.keys():
			if len(f[company]) != len(dates):
				f.pop(company)

	# convert each item in files to dataframe & give them dates as index
	save_prefix = os.getcwd() + os.sep + 'Synthesis' + os.sep + 'bse'
	for i, f in enumerate(files):
		print 'Writing ', tags[i].lower() + '.csv', '...',
		a = pd.DataFrame(f)
		a.index = dates
		a.to_csv(save_prefix + os.sep + tags[i].lower() + '.csv')
		print 'done'
