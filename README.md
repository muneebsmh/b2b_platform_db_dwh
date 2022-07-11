Please read the below manual to correctly build and populate the database and data warehouse for B2B platform application.

This repository (main branch) contains four directories, each for different domains for this test project.

1) **Database_Scripts:** This directory has the DDL and DML statements to initialize the b2bplatform database and populate it with a few thousand records.
2) **Combined_Log_Format_Generator:** This directory has a python program that generates a log file containing 50000 logs in the combined_log_format which would be later on analyzed in the data warehouse.
3) **Data_Warehouse_Scripts:** This directory has the DDL statements which would initialize the b2b platform data warehouse including the DDLs of all facts and dimensions, as well as the stored procedures that will load the dimensional model using the SCD type - 2 data capture from the relational database created in step 1.
4) **DWH_Runner:** This directory has python program that would call the stored procedures initialized in step 3 and this python file will be responsible for executing the overall ETL pipeline to refresh the b2b platform data warehouse.  

All the codes are needed to be executed in the following sequential manner for proper execution: 
**Database_Scripts -> Combined_Log_Format_Generator -> Data_Warehouse_Scripts -> DWH_Runner**
