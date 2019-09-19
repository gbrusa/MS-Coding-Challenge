# MS-Coding-Challenge
This repo stores my solution to the MS Entry Level Coding Challenge. It contains the necessary files and settings as created by Eclipse, allowing it to be easily cloned and run on other devices.

Overall, the solution has a time complexity of O(n) and took approx. 1 minute to run on my laptop for all 6003 records.

## How to run
The project should already include all dependencies as .jars in the .lib folder. However, in the event that it does not work, you can find a link to download the JDBC for SQLite .jar in the **Dependencies** section of this README. Include this as an external JAR and the dependencies should be fulfilled.

The application takes a single argument, `filename` containing the records to validate and insert into the SQLite database. The application will then create a .db, bad output CSV, processing counts log files in the same location as the input file.

### Assumptions
* The SQLite database will always have 10 columns.
* Any records with more or less than 10 values is considered invalid.
* The table name in the database is irrelevant and set to `tbl`.
* Values in the records are not malicious.

### Known errors
* Application will always add a record in the table which is simply the title of the columns as that is a record in the input CSV file

## Dependencies
* JDBC for SQLite: https://www.sqlitetutorial.net/sqlite-java/sqlite-jdbc-driver/
