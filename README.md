# SprocHammer
Console app for generating load against a Sql Server database by running concurrent writes and reads, with statistics reporting.

To try it out:
	* Compile solution
	* Execute 'rebuild-hammertime-db.sql' and 'create-sample-schema.sql' against local DB server.
	* From the project directory, run 'bin\debug\sprocHammer sample-run.json'
