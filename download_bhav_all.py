"""
Created: 25th May 2017
Author : T. Devakumar

Description : Download latest bulk data using BC parse
"""
from BCparse import *
import datetime

if __name__ == "__main__":
	start = raw_input("Start date for download (DD-MM-YY) : ").strip().split('-')
	start = datetime.date(int('20'+start[-1]), int(start[-2]), int(start[0]))
	end = datetime.date.today()

	delta = end - start
	for i in range(delta.days + 1):
		date = start + datetime.timedelta(days = i)
		
		# BCparse main section
		# Download the Bhavcopies according to the default / specified date
		date_str_fmt = "{}/{}/{}".format(str(date.day).zfill(2), str(date.month).zfill(2), str(date.year)[-2:])
		print date_str_fmt
		if date:
			get_bhavcopy = DownloadBhavCopy(date_str_fmt)
		else:
			get_bhavcopy = DownloadBhavCopy(None)

		# Delete the newly created directory in case none of the Bhavcopies get downloaded
		if not get_bhavcopy.csv_path_nse and not get_bhavcopy.csv_path_bse:
			os.rmdir(get_bhavcopy.final_dir)
			#exit(1)
			continue

		# Or process both of them in turn
		file_date = str(get_bhavcopy.bcdate.day).zfill(2) + '-' + str(get_bhavcopy.bcdate.month).zfill(2) + '-' + str(get_bhavcopy.bcdate.year)
		try :
			if get_bhavcopy.csv_path_bse:
				ParseBhavCopy(get_bhavcopy.csv_path_bse, 'bse', file_date)
			if get_bhavcopy.csv_path_nse:
				ParseBhavCopy(get_bhavcopy.csv_path_nse, 'nse', file_date)
		except Exception as e:
			print date_str_fmt, 'parsing error... skipping', e
