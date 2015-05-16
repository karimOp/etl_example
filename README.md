# etl_example
Easy-quick-and-dirty ETL program to manage all different sources of data present in csv.
Usage:
mvn clean install
mvn eclipse:eclipse

then run each class modifying :

String MOVIES_PATH;
String CONSUMP_PATH;
String USERS_PATH;

Takes less than 1 minute per dataset to merge, concatenate the FileDate and recreate the files.
Note that Users files contain Windows EOL, you can remove it by:

perl -pi -e 's/\r\n|\n|\r/\n/g' *.csv

Inside the folder.

Disclaimer:
Quick and dirty here, really mean quick and dirty.
