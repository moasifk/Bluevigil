#!bin/sh
echo "Going to invoke phoenix"
echo "Change Phoenix server details and file location as required"
sqlline.py localhost createTable.sql
#:2181:/hbase-unsecure 
# 
echo "successfully invoked phoenix"
