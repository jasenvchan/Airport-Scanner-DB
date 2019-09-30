AirportScanner DB
authored by Jasen Chan

for support please contact me at jasenvchan@gmail.com or text 1-(646)-808-6498

***** SYSTEM REQUIREMENTS *****

- MySQL 5.5 or greater
	- https://dev.mysql.com/downloads/file/?id=479845

- Python 2.7 or greater
	- should come with modern Unix based systems

- pymysql Python module
	- you may need to download pip by running this command:
		curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

		- and then this command:
			python get-pip.py

			* if you get an EnvironmentError run this command:
			sudo python get-pip.py

			- although it's not advised, it's the easiest way to install

	- if you already have pip or completed the above, run this command to install pymysql
		pip install pymysql

***** HOW TO RUN *****

1. unzip the MySQL package

2. double click to run the installer

3. follow the instructions and create a new password for the root account

4. Unzip the AirportScannerDB_Final.zip

5. navigate to the 'config' folder and modify the 'MySQLdbConfig.txt' file so that it reflects your MySQL db configuration
	- really, all you'll need to change is the user and password
	- you will later be able to specify the host when running the code

6. navigate to the 'src' folder and run either the populator_v17.py or executor.py scripts

	- populator_v17.py
		- purpose: single process population script which targets a specific month
		- when to use: use this script for small population efforts; only uses a single process
		- arguments none are required; to be used in the same format as bash commands (e.g. 'python populator_v17 -m Jan -d /Users/jachan/Desktop/Datasample'):
			- '-m': specify the target month to be pushed into the database
			- '-d': specify the path to the data which needs to be pushed to the database
			- '-l': specify the path which the program write output logs to
			- '-h': specify the host of the MySQL db...more useful when dealing with ColonialOne or external (nonlocal) servers

	- executor.py
		- purpose: run the prepper (if necessary) and start multiple populator processes
		- when to use: use this script when starting a long population effort; will consume up to 12 processes

	- prepper_v6.py
		- purpose: set up the MySQL database
		- when to use: if you decide to run this standalone, do so carefully as it will RESET THE ENTIRE DATABASE