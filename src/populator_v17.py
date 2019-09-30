'''

Created June 21st, 2018

@author: Jasen

- Purpose: populate the database with proper values

TODO:

- eventually, make tables for items (distractors and targets), sessions, individuals
- figure out how to make client not write over files when it gets months with nothing
- rewrite writeOut method; it does its job, but it's poorly written

	TODO FOR MODULARIZATION:
	- make neglected columns read from a file
	- change outfiles to be CSVs not TXTs
	- eventually, join populator and prepper
	- make an fillWithNulls method which fills empty columns at the end of all commands with NULLs
	- COMMMENT CODE!!!

COMMAND LINE: python populator_v14.py targetmonth hostname datadirectory configdirectory outputdirectory
TODO: make commands follow dashes; e.g. python populator_v14.py -h node991 -m June

-h = specify host
-m = specify target month
-d = specify data directory
-l = specify log/output directory


'''

import pymysql, os, sys
from csv import reader
import time,datetime,calendar


def duration_detailed(duration):

	days = float(duration)/(24.0*60.0*60.0)
	hours = (days - int(days)) * 24.0
	minutes = (hours - int(hours)) * 60.0
	seconds = (minutes - int(minutes)) * 60.0

	print "\n === TOTAL RUNTIME === "
	print str(int(days)),"days",str(int(hours)),"hours",str(int(minutes)),"minutes",str(int(seconds)),"seconds"


def convertDate(dateString):

	try:
		return calendar.timegm(datetime.datetime.strptime(dateString,'%Y-%m-%d %H:%M:%S').timetuple())

	except ValueError:

		try:
			return calendar.timegm(datetime.datetime.strptime(dateString,'"%Y-%m-%d %H:%M:%S"').timetuple())

		except ValueError:

			return calendar.timegm(datetime.datetime.strptime(dateString,"%m/%d/%y %H:%M").timetuple())


def convertLocal(timeString):

	try:
		extracted = timeString.split('"')[1].split(":")

	except IndexError:
		extracted = timeString.split(":")

	return int(extracted[0])*60*60 + int(extracted[1])*60 + int(extracted[2])

def writeOut(targetFile,format,data): #this is a bad function; needs to eventually be rewritten
	# specify output directory

	outdir = "../logs/" if specificConfigDict["logs"] == "none" else specificConfigDict["logs"]
 
	with open(outdir + targetFile + ".txt",format) as file:

		try:
			file.write("\n".join("\'" + str(datum) + "\'," + str(data[datum]) for datum in data))

		except TypeError:

			try:
				file.write("\n".join(str(datum) for datum in data))

			except TypeError:
				file.write(str(data) + "\n")

		except IndexError:

			try:
				file.write("\n".join(str(datum) for datum in data))

			except TypeError:
				file.write(str(data) + "\n")


def getInsertCommand(tablename,tableheaders,tablevalues): #deprecated

	return "INSERT INTO " + tablename + " (" + ",".join(tableheaders) + ") VALUES (" + ",".join("'" + str(value) + "'" for value in tablevalues) + ");"

def loadHeaders(headerspath):
	returndict = {}
	for headerfilename in os.listdir(headerspath):
		f = open(headerspath + headerfilename,'r')
		data = f.read()
		f.close()
		headersList = []
		for datalist in reader([data]):
			headersList = datalist
		returndict[headerfilename.split(".")[0]] = headersList
	return returndict

def makeEndString(tabletype,aggregatedValues):
	if tabletype == "daybag":
		return "," + "\'" + "".join(aggregatedValues) + "\'" + ");" if aggregatedValues else " "
	elif tabletype == "flash":
		return "," + "\'" + "".join(aggregatedValues) + "\'" + " " if aggregatedValues else " "
	#return "," + "".join(["T"]*750) + ");"

def parseSysArgs(args):
	returnDict = {"host":"none","datadirectory":"none","targetmonth":"none","logs":"none"}
	commandDict = {"-h":"host","-d":"datadirectory","-m":"targetmonth","-l":"logs"}

	for i in range(0,len(args)):

		if args[i] in commandDict:

			try:
				if not args[i+1].startswith("-"):
					returnDict[commandDict[args[i]]] = args[i+1]

				else:
					print "Missing argument value; entering default value for argument " + args[i]
					returnDict[commandDict[args[i]]] = "none"

			except IndexError:
				print "Missing argument value; entering default value for argument " + args[i]
				returnDict[commandDict[args[i]]] = "none"

	return returnDict

# specify configuration directory

fileExtension = "../config/"

#MySQL Database connection info; import the MySQL database connection details

host = ""
port = ""
user = ""
password = ""

with open(fileExtension + "MySQLdbConfig.txt") as dbconfigfile:

	info = ""
	for row in dbconfigfile:
		for temp in reader([row]):
			info = temp
			print info

		try:
			host = info[1] if info[0] == "host" and host == "" else host
		except IndexError:
			print "Could not read hostname from config file...attmepting to connect to localhost instead"
			host = "localhost"

		try:
			port = int(info[1]) if info[0] == "port" and port == "" else port
		except ValueError:
			port = 3306
		except IndexError:
			print "Could not read port number from config file...attempting to establish connection at default port 3306"
			port = 3306

		try:
			user = info[1] if info[0] == "user" and user == "" else user
		except IndexError:
			print "Could not read username from config file...attempting to establish connection with user \'root\'"
			user = "root"

		try:
			password = info[1] if info[0] == "password" and password == "" else password
		except IndexError:
			print "Could not read password from config file...attempting to establish connection with no password"
			password = ""

#conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='airportscanner1', db='mysql',autocommit=True)

specificConfigDict = parseSysArgs(sys.argv)

host = host if specificConfigDict["host"] == "none" else specificConfigDict["host"]

try:
	conn = pymysql.connect(host = host, port = port, user = user, passwd = password, db = 'mysql', autocommit = True)
except Exception as e:
	print "There was an error connecting to the MySQL database\nThe error is as follows:\n" + e + "\n"
	print "There may be an error with your configuration which is as follows:\n" + "host: " + host + "\nport: " + str(port) + "\nuser: " + user + "\npassword: " + password + "\n"
	print "The program will exit in 3 seconds"
	time.sleep(3)
	sys.exit()

cur = conn.cursor()

allHeaders = loadHeaders(fileExtension + "headers/")

anomalies = []
errors = []
errorFile = "errorlog.txt"

devices = {}
platforms = {}
systemVersions = {}
fileCount = 0
monthCount = 0
totalRowCount = 0.0

numLegalItems = 406
numIllegalItems = 344

cur.execute("USE data")

# specify target data month

monthsDict = {'January':'Jan','Jan':'Jan','Feb':'Feb','February':'Feb','Mar':'March','March':'March','Apr':'April','April':'April','Jun':'June','June':'June','Jul':'July','July':'July','Aug':'Aug','August':'Aug','Sept':'Sept','September':'Sept','Oct':'Oct','October':'Oct','Nov':'Nov','November':'Nov','Dec':'Dec','December':'Dec'}

targetFolderType = raw_input("You didn't specify a target month. What month should this populator process target? ") if specificConfigDict["targetmonth"] == "none" else specificConfigDict["targetmonth"]

while targetFolderType not in monthsDict:
	targetFolderType = raw_input("The specified target month \"" + targetFolderType + "\" is not a valid month; please respecify: ")

targetFolderType = monthsDict[targetFolderType]

# specify data directory

fileExtension = "../../DataSample/" if specificConfigDict["datadirectory"] == "none" else specificConfigDict["datadirectory"]


for foldername in os.listdir(fileExtension):

	if os.path.isfile(os.path.abspath(fileExtension + foldername)) or ".DS_Store" in os.path.abspath(fileExtension + foldername) or not foldername.startswith(targetFolderType):
		continue

	monthCount += 1

	for filename in os.listdir(fileExtension + foldername):
		startTime = time.time()
		fileCount += 1
		withinFileRowCount = 0
		currentTable = ""

		if not os.path.isfile(os.path.abspath(fileExtension + foldername + "/" + filename)):
			continue


		if filename.startswith("day-bag"):
			headersList = allHeaders['daybagheaders']
			numHeaders = len(headersList)
			headers = ",".join(header for header in (allHeaders['daybagheaders'] + allHeaders['aggregatedheaders']))
			neglectedColumns = [8,10,12,22,25,31,33,35,39,44,72,80,88,96,104,111,118,125,132,139,146,153,160,167,174,181,188,195,202,209,216,223,230,237]
			currentTable = "daybag"

		elif filename.startswith("FLASH"):
			headersList = allHeaders['flashheaders']
			numHeaders = len(headersList)
			headers = ",".join(header for header in (allHeaders['flashheaders'] + allHeaders['flashaggregatedheaders']))
			neglectedColumns = [14]
			currentTable = "flash"

		else:
			continue

		sqlcommand = ""

		with open(fileExtension + foldername + "/" + filename) as dataFile:

			#skip the first line (header line)
			next(dataFile)
			anomalies = []
			errors = []
			for row in dataFile:

				itemCombination = ["0"] * 750 if not currentTable == "flash" else None

				totalRowCount += 1
				withinFileRowCount += 1
				test = {}
				counter = 0
				supplement = 0
				cells = ""
				flashUniqueIdentifiers = []

				for temp in reader([row]):
					cells = temp

				#check if there are length anomalies
				if len(cells) > numHeaders + len(neglectedColumns):
					anomalies.append(filename + ":" + str(withinFileRowCount) + ":" + "row overpacked")
					errors.append(filename + ":" + str(withinFileRowCount) + ":" + "row overpacked")
					continue
				if len(cells) < 2:
					anomalies.append(filename + ":" + str(withinFileRowCount) + ":" + "nothing present in row")
					errors.append(filename + ":" + str(withinFileRowCount) + ":" + "row overpacked")
					continue

				sqlcommand = "INSERT INTO " + currentTable + " (" + headers + ") VALUES (" if not currentTable == "flash" else "INSERT INTO " + currentTable + " (" + headers + ") SELECT "


				for columnNum in range(0,len(cells)):

					fullyPopulated = True if len(cells)-len(neglectedColumns) == numHeaders else False

					if columnNum in neglectedColumns:
						continue

					else:

						if cells[columnNum] == "" or cells[columnNum] == "\r":
							sqlcommand = sqlcommand + "NULL" + makeEndString("flash" if currentTable == "flash" else "daybag",flashUniqueIdentifiers if currentTable == "flash" else itemCombination) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + "NULL,"

						#special exceptions for date, device specification columns 

						elif columnNum == 0 and currentTable == "flash":
							flashUniqueIdentifiers.append(cells[columnNum])

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("flash",flashUniqueIdentifiers) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

##### DON'T WANT DUPLICATED TRIALS; COLUMN INDEX 2 IS CHALLENGE ID
#						elif columnNum == 2 and currentTable == "flash":
#							flashUniqueIdentifiers.append(cells[columnNum])
#
#							try:
#								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("flash",flashUniqueIdentifiers) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","
#
#							except Exception as e:
#								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
#								break

						elif columnNum == 3:

							try:
								sqlcommand = sqlcommand + str(convertDate(cells[columnNum])) + ","
								if currentTable == "flash":
									flashUniqueIdentifiers.append(str(convertDate(cells[columnNum])))

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum == 6:

							try:
								sqlcommand = sqlcommand + str(convertLocal(cells[columnNum])) + ","
								if currentTable == "flash":
									flashUniqueIdentifiers.append(str(convertLocal(cells[columnNum])))

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum == 18 and currentTable == "flash":
							flashUniqueIdentifiers.append(cells[columnNum])

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("flash",flashUniqueIdentifiers) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum == 19 and currentTable == "flash":
							flashUniqueIdentifiers.append(cells[columnNum])

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("flash",flashUniqueIdentifiers) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum == 20 and currentTable == "flash":
							flashUniqueIdentifiers.append(cells[columnNum])

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("flash",flashUniqueIdentifiers) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum == 21 and currentTable == "flash":
							flashUniqueIdentifiers.append(cells[columnNum])

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("flash",flashUniqueIdentifiers) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum == 22 and currentTable == "flash":
							flashUniqueIdentifiers.append(cells[columnNum])

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("flash",flashUniqueIdentifiers) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum == 61:

							try:
								if not cells[columnNum] in devices:	
						
									devices[cells[columnNum]] = len(devices)

									try:
										cur.execute("INSERT INTO devices (DeviceName) SELECT \'" + cells[columnNum][:75] + "\' WHERE NOT EXISTS(SELECT 1 FROM devices WHERE DeviceName=\'" + cells[columnNum][:75] + "\');")
										#cur.execute(getInsertCommand("devices",[allHeaders['deviceheaders'][1]],[cells[columnNum]]))
									except Exception as e:
										errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":INSERTIONPROBLEM")

								try:
									cur.execute("SELECT DeviceId FROM devices WHERE DeviceName = \'" + cells[columnNum][:75] + "\';")
								except Exception as e:
									errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":SELECTIONPROBLEM")
									break

								try:						
									sqlcommand = sqlcommand + str(cur.fetchone()[0]) + ","
								except Exception as e:
									errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":FETCHPROBLEM")
									break

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":OTHERPROBLEM")
								break

						elif columnNum == 62:

							try:
								if not cells[columnNum] in platforms:

									platforms[cells[columnNum]] = len(platforms)

									try:
										cur.execute("INSERT INTO platforms (PlatformName) SELECT \'" + cells[columnNum] + "\' WHERE NOT EXISTS(SELECT 1 FROM platforms WHERE PlatformName=\'" + cells[columnNum] + "\');")

									except Exception as e:
										errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":INSERTIONPROBLEM")

								try: 		
									cur.execute("SELECT PlatformId FROM platforms WHERE PlatformName = \'" + cells[columnNum] + "\';")
								except Exception as e:
									errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":SELECTIONPROBLEM")
									break

								try:															
									sqlcommand = sqlcommand + str(cur.fetchone()[0]) + ","
								except Exception as e:
									errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":FETCHPROBLEM")
									break

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":OTHERPROBLEM")
								break							

						elif columnNum == 63:

							try:
								if not cells[columnNum] in systemVersions:

									systemVersions[cells[columnNum]] = len(systemVersions)

									try:
										cur.execute("INSERT INTO systemversions (SystemVersion) SELECT \'" + cells[columnNum] + "\' WHERE NOT EXISTS(SELECT 1 FROM systemversions WHERE SystemVersion=\'" + cells[columnNum] + "\');")
										#cur.execute(getInsertCommand("systemversions",[allHeaders['systemversionheaders'][1]],[cells[columnNum]]))
									except Exception as e:
										errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":INSERTIONPROBLEM")

								try:
									cur.execute("SELECT SystemVersionId FROM systemversions WHERE SystemVersion = \'" + cells[columnNum] + "\';")
								except Exception as e:
									errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":SELECTIONPROBLEM")
									break

								try:														
									sqlcommand = sqlcommand + str(cur.fetchone()[0]) + ","
								except Exception as e:
									errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":FETCHPROBLEM")
									break

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e) + ":OTHERPROBLEM")
								break

						elif columnNum > 72 and columnNum < 90 and ((73 - columnNum)%8) == 0:

							itemCombination[numLegalItems + int(cells[columnNum])] = "1"

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("daybag",itemCombination) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						elif columnNum > 104 and ((105 - columnNum)%7) == 0:
							itemCombination[int(cells[columnNum])] = "1"

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("daybag",itemCombination) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break

						else:

							try:
								sqlcommand = sqlcommand + cells[columnNum] + makeEndString("daybag",itemCombination) if columnNum == len(cells)-1 and fullyPopulated else sqlcommand + cells[columnNum] + ","

							except Exception as e:
								errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))
								break							

						counter += 1

				if not fullyPopulated:

					sqlcommand = sqlcommand + "".join(["NULL" + makeEndString("daybag",itemCombination) if remaining == numHeaders - counter - 1 else "NULL," for remaining in range(0,numHeaders-counter)])

				sys.stdout.flush()
				sys.stdout.write("\r" + filename + " " + str(withinFileRowCount) + " " + str(totalRowCount) + " " + str(counter) + " " + str(numHeaders))
				sys.stdout.flush()

				try:

					if currentTable == "flash":
						
						sqlcommand = sqlcommand + makeEndString("flash",flashUniqueIdentifiers) + "WHERE NOT EXISTS(SELECT 1 FROM flash WHERE FlashUId=\'" + "".join(flashUniqueIdentifiers) + "\');"

					cur.execute(sqlcommand)

				except Exception as e:
					errors.append(filename + ":" + str(withinFileRowCount) + ":" + str(e))

		duration = time.time() - startTime
		duration_detailed(duration)
		writeOut("populationSpeed(MBpS)",'a',["filename: " + filename + ", filesize: " + str(os.path.getsize(os.path.abspath(fileExtension + foldername + "/" + filename))) + ", duration: " + str(duration) + ", speed(MBpS): " + str(((os.path.getsize(os.path.abspath(fileExtension + foldername + "/" + filename))) * (10 ** -6)) / duration) + "\n"])
		writeOut("anomalies",'a',anomalies)
		writeOut("errors",'a',errors)

conn.commit()
cur.close()
conn.close()
sys.exit(0)