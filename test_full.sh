#!/bin/bash

#------------------------------
# This script is developed for JIRA - DATA 1132. 
# This shell scripts automate end to end testing for Customer Nector Initial load
# Data Flow for Customer Nector Load is as follows 
# EDW SFTP -> AWS S3 -> Raw Layer (raw schema in hive) -> Staging layer (staging schema in hive) ->Information layer (Infomation Schema in hive)
# EDW SFTP feed is direct load to AWS S3 with PIITS deployed for sensitive colummns.
# AWS S3 anonymised data is moved to raw layer (one to one mapping).
# Staging layer receives data from the raw layer tables joined on conditions and loaded into seven staging layer tables.
# Infomation layer has four tables populated from staging layer tables. 
#------------------------------

##clear

## Prerequisite :-  Login to edgenode1 & do Kinit  (kinit ubuntu, password - ubuntu) 

# Change user to ubuntu, 
##sudo -su ubuntu

# Export the home directory to ubuntu's home
##export HOME=/home/edie.service/automation-poc/

##cd

## ------------------- JSConnect Test ----------------------------
echo "Truncating automation tables"
hive -f truncate_automation_tables.sql 2> hive.log
if [ $? -ne 0 ]; then echo "The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;

echo "Delete files from edw source"
sftp -i /home/edie.service/automation-poc/b2bgw -P 2224 edwsftp@dsproxy.mgt.data-platform.io:/data/abinitio/EDW/prod/serial/common/output_data/edwbatch/DPP/Initial/Nectar/ <<EOF
rm *
quit
EOF
if [ $? -ne 0 ]; then echo "Failed to connect EDWSFTP or remove files from EDW" ; exit 1; fi;

echo "Delete files from s3 bucket"
aws s3 rm  s3://js-dpp-sit-data-customer-uvc-nectar-app/initial/ --recursive 2> aws.log
if [ $? -ne 0 ]; then echo "AWS rm failed" ; exit 1; fi;

echo "Getting list of files currently in secondary source"
aws s3 ls  s3://js-dpp-sit-data-customer-uvc-nectar-app/initial/ | sed 's/ \+/ /g' | cut -d ' ' -f 4 > secondary_source_before
if [ $? -ne 0 ]; then echo "AWS ls failed" ; exit 1; fi;


## To copy the file from sftp location for comparison
echo "Copying file to SFTP"
filename="`date +%Y%m%d_%H%M%S`_customer_nectar_initial_test.dat"
scp -i /home/edie.service/automation-poc/b2bgw -P 2224 sftp_file.dat edwsftp@dsproxy.mgt.data-platform.io:/data/abinitio/EDW/prod/serial/common/output_data/edwbatch/DPP/Initial/Nectar/$filename 2>sftp.log
if [ $? -ne 0 ] ; then echo "File is not copied" ; exit 1; fi;

echo "Waiting for JS-Connect about 2 minutes"
sleep 130 # runs on interval 1 minute, we leave an overhead of 10 extra seconds over the maximum waiting time of 2 minutes

echo "Getting list of files currently in secondary source"
aws s3 ls  s3://js-dpp-sit-data-customer-uvc-nectar-app/initial/ | sed 's/ \+/ /g' | cut -d ' ' -f 4 > secondary_source_after 
if [ $? -ne 0 ]; then echo "AWS ls failed" ; exit 1; fi;


echo "Downloading file from S3 to verify JS-Connect"
rm -f /home/edie.service/automation-poc/aws_file.dat
aws s3 cp  s3://js-dpp-sit-data-customer-uvc-nectar-app/initial/$filename /home/edie.service/automation-poc/aws_file.dat 2> aws.log
if [ $? -ne 0 ] ; then echo "File could not be downloaded" ; exit 1; fi;

echo "checking obfuscation"
diff <(cut -c 1-14,16-23,99-100,102-111 aws_file.dat) <(cut -c 1-14,26-33,116-117,121-130 sftp_file.dat)

wc=$(echo `diff <(cut -c 25-60,62-97 aws_file.dat) <(cut -c 51-61,76-83 sftp_file.dat)| wc -l`)
if [ $wc -eq 0 ]; then echo "Obfuscation failed" ; echo "JSConnect Test Failed" ; exit 1; fi;

echo "Obfuscation passed"

## Assertions on JSConnect
echo "Asserting JSConnect output" 
wc=`wc -l < /home/edie.service/automation-poc/aws_file.dat`
if [ $wc -ne 10 ]; then echo "JSConnect failed" ; exit 1; fi;

echo "Running EDIE source to raw stage"
spark2-submit --master yarn --deploy-mode client --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" /opt/software/edif/current/dpp-etl.jar file:/home/edie.service/automation-poc/customer_profile_nectar_dat_initial_csv.yaml 2> edie-log.log
if [ $? -ne 0 ] ; then echo "EDIE S3 to raw stage failure" ; exit 1; fi;

## Created file raw_hive_query.sql that contains the hive query to return the data from raw.nectar_dat_initial table.
## Following query is present in raw_hive_query.sql
## Select distinct nectar_dat_initial.ic_key,nectar_dat_initial.sty_rcnum,nectar_dat_initial.lms_party_account_no,nectar_dat_initial.lms_status_code,nectar_dat_initial.lms_customer_create_datefrom raw.nectar_dat_initial where created_ts = (Select max(created_ts) from raw.nectar_dat_initial) order by nectar_dat_initial.ic_key 
## This query retrives the data from the hive raw.nectar_dat_initial table and copy into the output_raw.txt file for comparison.
## But the result set is tab delimited and hence required to be changed to csv format for comparison. 
## AWS file data needs to be sorted for comparison.
## Expected output of comparison is that all the data in source file and target file matches.

## Step2: To retrive HIVE query output into the output_raw.txt file
echo "Querying Raw table"
hive -f raw_hive_query.sql | tail -11 |head -10 1> output_raw.txt 2>> hive.log
if [ $? -ne 0 ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;


## Step3: To convert tab delimited output into csv format
##sed -i 's/\t/,/g' output_raw.txt 

## Step4: To sort the AWS S3 file data
##sort aws_file.dat> aws_file1.txt

## Step5: To compare the AWS file data with Raw layer table result set. raw_hive_query_count.txt
##echo "S3 to Raw comparison"
##diff output_raw.txt <(cut -c 1-14,24-105,107,108,110,111 aws_file1.txt)
##if [ $? -ne 0 ]; then echo " S3 to Raw comparison failed " ; exit 1; fi;

echo "Source to raw Comparison"
sed -i 's/\t/,/g' output_raw.txt 

sort sftp_file.dat> sftp_file1.txt

sed -i 's/\-//g' sftp_file1.txt

## 
diff -w  <(cut -c 1-14,90-91,93-100 output_raw.txt ) <(cut -c 1-14,116-117,121-130 sftp_file1.txt)
if [ $? -ne 0 ]; then echo " S3 to Raw comparison failed " ; exit 1; fi;

wc=$(echo `diff -w  <(cut -c 16-51,53-88 output_raw.txt) <(cut -c 51-61,76-83 sftp_file1.txt)| wc -l`)
if [ $wc -eq 0 ]; then echo "Obfuscation failed" ; echo "JSConnect Test Failed" ; exit 1; fi;

##raw_count=`wc -l < /home/edie.service/automation-poc/output_raw.txt` 

##echo "$raw_count" > "$raw_hive_query_count.txt"
hive -f raw_hive_query_count.sql 1> raw_hive_query_count.txt 2>> hive.log
if [ $? -ne 0  ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;

## ---------------- Raw layer to Staging layer test  -------------------
## Spark job should be executed to load data from raw layer to staging layer.
## Created a file  that contains the hive query to return the data from raw layer tables staging_hive_query_source.sql.
## Created a file  that contains the hive query to return the data from staging layer tables staging_hive_query_target.sql
## Write a command to execute that raw layer hive query and store result set in file for comparison.
## Write a command to execute that staging layer hive query and store result set in file for comparison.
## Use diff command to compare the result.

## Step 1: Run spark job for customer_profile_raw_to_staging_init.yaml to load the data into staging layer tables.
echo "Running EDIE raw to staging stage"
spark2-submit --master yarn --deploy-mode client --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" /opt/software/edif/current/dpp-etl.jar file:/home/edie.service/automation-poc/customer_profile_raw_to_staging_init.yaml 2> edie-staging.log
if [ $? -ne 0 ] ; then echo "EDIE raw to staging failure" ; exit 1; fi;

## Step 2: To retrive HIVE query output into the staging_hive_result_source.txt file ----- Raw table count
echo "Staging table count and comparison" 
hive -f staging_hive_query_count1.sql 1> staging_hive_query_count1.txt 2>> hive.log
if [ $? -ne 0  ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;

diff raw_hive_query_count.txt staging_hive_query_count1.txt
if [ $? -ne 0  ]; then echo " Raw to Staging comparison failed" ; exit 1; fi;

## Step3: To retrive HIVE query output into the staging_hive_result_source.txt file -----
hive -f staging_hive_query_count2.sql 1> staging_hive_query_count2.txt 2>> hive.log
if [ $? -ne 0 ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;

diff raw_hive_query_count.txt staging_hive_query_count2.txt
if [ $? -ne 0 ]; then echo " Raw to Staging comparison failed" ; exit 1; fi;

hive -f staging_hive_query_count3.sql 1> staging_hive_query_count3.txt 2>> hive.log
if [ $? -ne 0 ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;

diff raw_hive_query_count.txt staging_hive_query_count3.txt
if [ $? -ne 0 ]; then echo " Raw to Staging comparison failed" ; exit 1; fi;

## ---------------- Staging layer to Infomation layer test  -------------------
## Spark job should be executed to load data from staging layer to infomation layer.
## Create a file  that contains the hive query to return the data from staging layer tables information_hive_query_source.sql.
## Create a file  that contains the hive query to return the data from information layer tables information_hive_query_target.sql .
## Write a command to execute that staging layer hive query and store result set in file for comparison.
## Write a command to execute that infomation layer hive query and store result set in file for comparison.
## Use diff command to compare the result.
##-----------------------------------------------------------------------------

## Step 1: Run spark job for customer_profile_staging_to_information_init.yaml to load the data into information layer tables.
echo "Running EDIE staging to information stage"
spark2-submit --master yarn --deploy-mode client --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" /opt/software/edif/current/dpp-etl.jar file:/home/edie.service/automation-poc/customer_profile_staging_to_information_init.yaml 2> edie-information.log

## Step2: To retrive HIVE query output into the information_hive_result_source.txt file
echo "Information tables count and comparison"
hive -f information_hive_query_count1.sql 1> information_hive_result_count1.txt 2>> hive.log
if [ $? -ne 0 ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;

diff raw_hive_query_count.txt information_hive_result_count1.txt

## Step3: To retrive HIVE query output into the information_hive_result_target.txt file
hive -f information_hive_query_count2.sql 1> information_hive_result_count2.txt 2>> hive.log
if [ $? -ne 0 ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;


## Step4: To compare the staging layer table data with Infomation layer table data set.
diff raw_hive_query_count.txt information_hive_result_count2.txt
if [ $? -ne 0 ]; then echo "Staging to Information test failed" ; exit 1; fi;


echo " Test Completed and Passed"


## ------------------------- END of SCRIPT-----------------------

