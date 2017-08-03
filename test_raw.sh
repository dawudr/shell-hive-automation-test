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
#sudo -su edie.service

#kinit edie.service
#edie
# Export the home directory to ubuntu's home
##export HOME=/home/ubuntu/automation-poc/

##cd

## ------------------- JSConnect Test ----------------------------
now=$(date +"%T")
echo "Current time : $now"

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
echo "Copying file to EDWSFTP location"
filename="`date +%Y%m%d_%H%M%S`_customer_nectar_initial_test.dat"
scp -i /home/edie.service/automation-poc/b2bgw -P 2224 sftp_file.dat edwsftp@dsproxy.mgt.data-platform.io:/data/abinitio/EDW/prod/serial/common/output_data/edwbatch/DPP/Initial/Nectar/$filename 2> sftp.log
if [ $? -ne 0 ] ; then echo "File is not copied" ; exit 1; fi;

echo "Waiting for JS-Connect about 2 minutes"
sleep 130 # runs on interval 1 minute, we leave an overhead of 10 extra seconds over the maximum waiting time of 2 minutes

echo "Getting list of files currently in secondary source"
aws s3 ls  s3://js-dpp-sit-data-customer-uvc-nectar-app/initial/ | sed 's/ \+/ /g' | cut -d ' ' -f 4 > secondary_source_after
if [ $? -ne 0 ]; then echo "AWS ls failed" ; exit 1; fi;


echo "Downloading file from S3 to verify JS-Connect"
#rm -f /home/edie.service/automation-poc/aws_file.dat
#aws s3 cp  s3://js-dpp-sit-data-customer-uvc-nectar-app/initial/$filename /home/edie.service/automation-poc/aws_file.dat
#if [ $? -ne 0 ] ; then echo "File could not be downloaded" ; exit 1; fi;

## Verifying that nonsensitive fields are read and written same as in source file. 
##diff <(cut -c 1-14,16-23,99-100,102-111 aws_file.dat) <(cut -c 1-14,26-33,116-117,121-130 sftp_file.dat)
##if [ $? -ne 0 ]; then echo "Nonsensitive check failed" ; echo "JSConnect Test Failed" ; exit 1; fi;

##wc=$(echo `diff <(cut -c 25-60,62-97 aws_file.dat) <(cut -c 51-61,76-83 sftp_file.dat)| wc -l`)
##if [ $wc -eq 0 ]; then echo "Obfuscation failed" ; echo "JSConnect Test Failed" ; exit 1; fi;

## Assertions on JSConnect
echo "Asserting JSConnect output" 
#wc=`wc -l < /home/edie.service/automation-poc/aws_file.dat`
op=`wc -l < /home/edie.service/automation-poc/sftp_file.dat`
#if [ $wc -ne $op ]; then echo "JSConnect failed" ; exit 1; fi;

now=$(date +"%T")

echo "Running EDIE s3 to raw stage at $now"
spark2-submit --master yarn --deploy-mode client --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/opt/software/edif/current/conf/jaas.conf -Djavax.net.ssl.trustStore=/usr/lib/jvm/java-8-oracle/jre/lib/security/cacerts" /opt/software/edif/current/dpp-etl.jar file:/home/edie.service/automation-poc/customer_profile_nectar_dat_initial_csv.yaml 2> edie-log.log
if [ $? -ne 0 ] ; then echo "EDIE S3 to raw stage failure" ; exit 1; fi;

now=$(date +"%T")
echo "Edie finished at : $now"

## Created file raw_hive_query.sql that contains the hive query to return the data from raw.nectar_dat_initial table.
## Following query is present in raw_hive_query.sql
## Select distinct nectar_dat_initial.ic_key,nectar_dat_initial.sty_rcnum,nectar_dat_initial.lms_party_account_no,nectar_dat_initial.lms_status_code,nectar_dat_initial.lms_customer_create_datefrom raw.nectar_dat_initial where created_ts = (Select max(created_ts) from raw.nectar_dat_initial) order by nectar_dat_initial.ic_key 
## This query retrives the data from the hive raw.nectar_dat_initial table and copy into the output_raw.txt file for comparison.
## But the result set is tab delimited and hence required to be changed to csv format for comparison. 
## AWS file data needs to be sorted for comparison.
## Expected output of comparison is that all the data in source file and target file matches.

## Step2: To retrive HIVE query output into the output_raw.txt file
now=$(date +"%T")

echo "Retrieving data from Raw.nectar_dat_initial1 table $now"
hive -f raw_hive_query.sql 2>> hive.log 1> output_raw.txt 
if [ $? -ne 0 ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;

now=$(date +"%T")
echo "hive query end time: $now"

wc=`wc -l < /home/edie.service/automation-poc/output_raw.txt`
if [ $wc -ne $op ]; then echo "data count failed" ; exit 1; fi;
## Step3: To convert tab delimited output into csv format
##sed -i 's/\t/,/g' output_raw.txt 

## Step4: To sort the AWS S3 file data
##sort aws_file.dat> aws_file1.txt

## Step5: To compare the AWS file data with Raw layer table result set. raw_hive_query_count.txt
##echo "S3 to Raw comparison"
##diff output_raw.txt <(cut -c 1-14,24-105,107,108,110,111 aws_file1.txt)
##if [ $? -ne 0 ]; then echo " S3 to Raw comparison failed " ; exit 1; fi;

##raw_count=`wc -l < /home/edie.service/automation-poc/output_raw.txt` 
echo "Source to raw Comparison"
sed -i 's/\t/,/g' output_raw.txt 

sort sftp_file.dat> sftp_file1.txt

sed -i 's/\-//g' sftp_file1.txt

## checking 
diff -w  <(cut -c 1-14,90-91,93-100 output_raw.txt ) <(cut -c 1-14,116-117,121-130 sftp_file1.txt|sort)
if [ $? -ne 0 ]; then echo " S3 to Raw comparison failed " ; exit 1; fi;

wc=$(echo `diff -w  <(cut -c 16-51,53-88 output_raw.txt) <(cut -c 51-61,76-83 sftp_file1.txt)| wc -l`)
if [ $wc -eq 0 ]; then echo "Obfuscation failed" ; echo "JSConnect Test Failed" ; exit 1; fi;


##echo "$raw_count" > "$raw_hive_query_count.txt"
hive -f raw_hive_query_count.sql 1> raw_hive_query_count.txt 2>> hive.log
if [ $? -ne 0  ]; then echo " The hive query is not executed successfully. Check the query for correctness" ; exit 1; fi;
echo "Comparison completed"

now=$(date +"%T")
echo "Current time : $now"

echo "Test Passed"
echo "Raw Layer Completed"
