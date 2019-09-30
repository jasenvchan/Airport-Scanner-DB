'''

Created June 19th, 2018

@author: Jasen

- Purpose: set up the database so that it can be populated

TODO:
- find appropriate value->int id mappings for Sound and SoundID column
- finish mapping all columns to specific type
- finish code which creates table format (i.e. column headers with corresponding types) based off headers_formatted.txt

'''


import pymysql, os, sys
from csv import reader

#establish connection
host = ""
port = ""
user = ""
password = ""
fileExtension = "../config/" 

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
try:
	conn = pymysql.connect(host = host, port = port, user = user, passwd = password, db = 'mysql', autocommit = True)
except Exception as e:
	print "There was an error connecting to the MySQL database\nThe error is as follows:\n" + e + "\n"
	print "There may be an error with your configuration which is as follows:\n" + "host: " + host + "\nport: " + str(port) + "\nuser: " + user + "\npassword: " + password + "\n"
	print "The program will exit in 3 seconds"
	time.sleep(3)
	sys.exit()
cur = conn.cursor()

#set up database
cur.execute("DROP DATABASE data")
cur.execute("CREATE DATABASE data")
cur.execute("USE data")

#import headers from files
fileExtension = "../config/headers_formatted_datatypes/"

for filename in os.listdir(fileExtension):

	with open(fileExtension + filename) as headerFile:

		commandString = "CREATE TABLE " + filename.split("_")[0] + " ("

		for header in headerFile:
			for temp in reader([header],delimiter="|",skipinitialspace=True):
				headerInfo = temp
			print headerInfo

			try:
				commandString = commandString + headerInfo[0] + " " + headerInfo[1] + " " + headerInfo[2] + ","

			except IndexError:

				try:
					commandString = commandString + headerInfo[0] + " " + headerInfo[1] + ","

				except IndexError:

					try:
						commandString = commandString + headerInfo[0] + ","

					except Exception as e:
						print "There was an error loading loading the headers for your database. Please check the file format of the headers."
						sys.exit(0)

		commandString = commandString[:len(commandString)-1] + ");"

	print commandString
	cur.execute(commandString)
	print(cur.description)

#cur.execute("""CREATE TABLE daybag IF NOT EXISTS (
#	id VARCHAR(20),
#	value VARCHAR(20),
#	type CHAR(1));
#	""")


#for row in cur:
#   print(row)
conn.commit()
cur.close()
conn.close()