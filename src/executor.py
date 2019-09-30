'''

Execute many scripts on colonialone

by Jasen

'''

import os,sys,time,subprocess

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

specificArgs = parseSysArgs(sys.argv)

specific = raw_input("Would you like to prep the database first or just populate? (1 for prep, 2 for populate)")

if specific == 1:

	confirmation = raw_input("You have decided to prep the database. Do you understand that this will ENTIRELY DELETE any existing data? Please confirm. Type \"confirm\" to proceed or \"no\" to go just run the populator. ")
	
	if confirmation == "confirm": 
		os.system("python prepper_v6.py")
	elif confirmation == "no":
		pass
	else:
		print "Unidentified confirmation. Please run the program again. Exiting..."
		sys.exit(0)

for i in ["June","July"]:#['Jan','Feb','March','April','May','June','July','Aug','Sept','Oct','Nov','Dec']:
	process = subprocess.Popen("python populator_v17.py -m " + i + " -h " + specificArgs["host"] + " -d " + specificArgs["datadirectory"] + " -l " + specificArgs["logs"],shell=True)
	print "executed process"

print "DONE."
sys.exit(0)