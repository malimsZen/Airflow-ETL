# This script
# Extracts data from /etc/passwd file into a CSV file.

# The csv data file contains the user name, user id and 
# home directory of each user account defined in /etc/passwd

# Transforms the text delimiter from ":" to ",".
# Loads the data from the CSV file into a table in PostgreSQL database.


#Extract phase
echo "Extracting data" 

#Extract thehe columns 1(Title), 2(Country) and 3 )(time) from ff_calendar_this week.csv

cut -d"," -f1,2,3 ff_calendar_thisweek.csv > extracted-data.text 


#Transform phase
echo "Transforming data"
# read the extracted data and replace the commas with colons.

tr "," ":" < extracted-data.text > transformed-data.csv 

# Load phase
echo "Loading data"
#Send the instructions to connect to 'template1' and 
# Copy the file to the table 'users' through command pipeline.

echo "\c postgres; \COPY forexfactory FROM 'transformed-data.csv' DELIMITERS ':' CSV;" | psql --username=postgres --host=localhost

# The command below is used to verify that the table is populated with the data.
echo '\c postgres; \\SELECT * from forexfactory;' | psql --username=postgres --host=localhost