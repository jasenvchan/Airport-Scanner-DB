'''

Created May 23rd, 2018

@author: Jasen

- merge of download2.py and dateCreator.py

Updated June 5th, 2018
- added Flash Challenge data pull option
- removed RND Lab data pull option

TODO:
- turn the raw_input collection into command line args so that commands can be passed to this script
- add timer
- add total size of data pulled

'''

import time
import csv
import os
import subprocess
import sys

#create the list of dates we want to pull from

daysPerMonth = [31,28,31,30,31,30,31,31,30,31,30,31]

leapYears = [2008,2012,2016,2020,2024]

#make all of the below sys.argv arguments so that they can be run with nohup on the server

pullType = int(raw_input("What type of pull would you like to make? (0 = normal trials, 1 = Flash challenge trials) "))

startYear = int(raw_input("Enter start year: "))
startMonth = int(raw_input("Enter start month: "))
startDay = int(raw_input("Enter start day: "))
#TODO: make sure that these values are correct either by correcting them or making limited options

endYear = int(raw_input("Enter end year: "))
endMonth = int(raw_input("Enter end month: "))
endDay = int(raw_input("Enter end day: "))
dates = ""

#loop through once for each year between the specified start and end year
for i in range(0,(endYear-startYear)+1):
	#check if the current year is the start or end year
	isStartYear = True if i==0 else False
	isEndYear = True if i==endYear-startYear else False

	startMonthForCurrentYear = startMonth-1 if isStartYear else 0
	endMonthForCurrentYear = endMonth if isEndYear else len(daysPerMonth)

	daysPerMonth[1] = 29 if startYear + i in leapYears else 28

	#loops through for months between start month and end month
	for j in range(startMonthForCurrentYear,endMonthForCurrentYear):

		isStartMonth = True if isStartYear and j==startMonth-1 else False
		isEndMonth = True if isEndYear and j==endMonth-1 else False

		startDayForCurrentMonth = startDay-1 if isStartMonth else 0
		endDayForCurrentMonth = endDay if isEndMonth else daysPerMonth[j]

		for k in range(startDayForCurrentMonth, endDayForCurrentMonth):
			isLastDay = True if isEndYear and isEndMonth and k == endDayForCurrentMonth-1 else False
			dates = dates + str(int(startYear)+i) + '\t' + str(j+1) + '\t' + str(k+1) if isLastDay else dates + str(int(startYear)+i) + '\t' + str(j+1) + '\t' + str(k+1) + '\n'


#do pulls based on the dates

months = ['Jan','Feb','March','April','May','June','July','Aug','Sept','Oct','Nov','Dec']

rootCommand = 'curl "https://www.airportscannergame.com/data-exporter-api?accessKey='
accessKey = ######

try:
	datadir = sys.argv[1] + "/Data/" if not sys.argv[1].endswith("/") else sys.argv[1] + "Data/"
	
except IndexError:
	datadir = "../../Data/"

os.system("mkdir " + datadir)

commandMid1 = '&export=flash-challenge&startDate=' if pullType == 1 else '&export=day-bag-item&startDate='
commandMid2 = '&endDate='
commandMid3 = '" > '
commandMidAdditional = ''
commandMid4 = 'FLASH_' if pullType == 1 else 'day-bag-item_'
commandEnd = '.csv'

#startTime = time.time()
dayCount = 0
oldMonth = 0
cMonthString = ""

allDates = dates.split("\n")

for row in allDates:
	currentDate = row.split("\t")
	
	#fileStartTime = time.time()

	cYear = currentDate[0]
	cMonth = currentDate[1] if int(currentDate[1]) > 9 else '0' + currentDate[1]
	cDay = currentDate[2] if int(currentDate[2]) > 9 else '0' + currentDate[2]

	dateStr = cYear + '-' + cMonth + '-' + cDay


	if not int(cMonth) == oldMonth:
		cMonthString = months[int(cMonth)-1]
		os.system("mkdir " + datadir + cMonthString + cYear + "_FLASH" if pullType == 1 else "mkdir " + datadir + cMonthString + cYear)
		 
		# this part had to be removed because the subprocess module was introduced in Python 2.7, and ColonialOne only has Python 2.6
		# try:	
		#	subprocess.check_output("mkdir "+cMonthString+cYear+"_RND", shell=True)
		# except subprocess.CalledProcessError:
		#	pass 
		
		oldMonth = int(cMonth)

	commandMidAdditional = datadir + cMonthString + cYear + '_FLASH/' if pullType == 1 else datadir + cMonthString + cYear + '/'

	command = rootCommand + accessKey + commandMid1 + dateStr + commandMid2 + dateStr + commandMid3 + commandMidAdditional + commandMid4 + dateStr + commandEnd
	#print command

	dayCount = dayCount + 1
	print "Pull #" + str(dayCount) + " | Pulling data from " + cMonthString + " " + cDay + ", " + cYear + "..."

	os.system(command)

print "********** " + str(dayCount) + " PULLS COMPLETED **********"