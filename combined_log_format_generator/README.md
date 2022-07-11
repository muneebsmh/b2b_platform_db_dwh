After we have created and populated the the **b2b_platform** database, we will now work on generating the weblogs in combined log format and store it in one of the transactional tables as well as generate a csv of generated logs. The steps are:

1) Clone the **combined_log_format_generator** directory in your local directory and load the python project.
2) Set the arguments required to run this project in the configuration, you may as well create the virtual environment if you wish.
3) Run the python program. 

_These are the sample arguments which I used in my local project:_
--host
localhost
--username
test
--password
test
--database
b2b_platform
--username_csv
/resources/usernames.csv
--useragent_csv
/resources/useragents.csv
--output_logs
/resources/generated_logs.csv

**Next Step: data_warehouse_scripts**
