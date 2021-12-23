Steps:
1. Replace WORKDIR value with your real root directory
2. Set source connection info in ../long-run/connection-info.properties
3. Apply your source table names in ../long-run/table-list.txt
	e.g.
	SYSTEM.TABLE1
	SYSTEM.TABLE2
	SYSTEM.TABLE3
	...
4. 	ParallelNumPerThread	- how many parallel threads to do INSERT-DELETE on one table
	ConnectionInfo 				- connection config file name. No need to change it
	TablesPerThread 			- how many tables are inserted/deleted in one thread.
	TableListFileName 			- table list file name. No need to change it
	DeleteThreshold 			- if row count in table is larger than this threshold, delete them all
	DeleteOnStart 				- delete all at first
	StartHintValue 				- every thread per table, start INSERT from this value.
	RandomString 				- use random string
	MaxNumberRowsToGenPerRun		- max num rows would be inserted per every round
	NumberOfRunsToPause 			- pause after running these rounds
	TimeToPause 				- time to pause(in milliseconds)