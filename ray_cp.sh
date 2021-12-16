#!/bin/bash

ray_head_ip=`ray get-head-ip cluster.yaml 2> /dev/null`
if [ "$?" -ne 0 ]; then
   echo "Your Ray cluster doesn't seem to be up."
   exit 1
fi

if [ -z ${INPUT_ACCESS_KEY+x} ]; then
   echo "(set variable INPUT_ACCESS_KEY to avoid this interactive prompt)"
   while [ -z ${INPUT_ACCESS_KEY} ]; do
      read -p "Access key for input: " INPUT_ACCESS_KEY
   done
else
   echo "Using input access key found in variable INPUT_ACCESS_KEY."
fi
if [ -z ${INPUT_SECRET_KEY+x} ]; then
   echo "(set variable INPUT_SECRET_KEY to avoid this interactive prompt)"
   while [ -z ${INPUT_SECRET_KEY} ]; do
      read -p "Secret key for input: " INPUT_SECRET_KEY
   done
else
   echo "Using input access key found in variable INPUT_SECRET_KEY."
fi
if [ -z ${INPUT_ENDPOINT+x} ]; then
   echo "(set variable INPUT_ENDPOINT to avoid this interactive prompt)"
   while [ -z ${INPUT_ENDPOINT} ]; do
      read -p "Endpoint (without 'https://') for input: " INPUT_ENDPOINT
   done
else
   echo "Using endpoint $INPUT_ENDPOINT found in variable INPUT_ENDPOINT for input data."
fi
if [ -z ${INPUT_BUCKET+x} ]; then
   echo "(set variable INPUT_BUCKET to avoid this interactive prompt)"
   while [ -z ${INPUT_BUCKET} ]; do
      read -p "Bucket name for input: " INPUT_BUCKET
   done
else
   echo "Using bucket $INPUT_BUCKET found in variable INPUT_BUCKET for input data."
fi
if [ -z ${INPUT_PREFIX+x} ]; then
   echo "(set variable INPUT_PREFIX to avoid this interactive prompt)"
   read -p "Prefix for input: " INPUT_PREFIX
   if [ -z ${INPUT_PREFIX} ]; then
      echo Empty prefix. Will copy entire.
   fi
else
   echo "Using prefix $INPUT_PREFIX found in variable INPUT_PREFIX for input data."
fi
if [ -z ${OUTPUT_ACCESS_KEY+x} ]; then
   echo "(set variable OUTPUT_ACCESS_KEY to avoid this interactive prompt)"
   read -p "Access key for output (default: same value as input access key): " OUTPUT_ACCESS_KEY
   if [ -z ${OUTPUT_ACCESS_KEY} ]; then
      OUTPUT_ACCESS_KEY=$INPUT_SECRET_KEY
   fi
else
   echo "Using output access key found in variable OUTPUT_ACCESS_KEY."
fi
if [ -z ${OUTPUT_SECRET_KEY+x} ]; then
   echo "(set variable OUTPUT_SECRET_KEY to avoid this interactive prompt)"
   read -p "Secret key for output (default: same value as input secret key): "
   if [ -z ${OUTPUT_SECRET_KEY} ]; then
      OUTPUT_SECRET_KEY=$INPUT_SECRET_KEY
   fi
else
   echo "Using output secret key found in variable OUTPUT_SECRET_KEY."
fi
if [ -z ${OUTPUT_ENDPOINT+x} ]; then
   echo "(set variable OUTPUT_ENDPOINT to avoid this interactive prompt)"
   read -p "Secret key for output (default: $INPUT_ENDPOINT): " 
   if [ -z ${OUTPUT_ENDPOINT} ]; then
      OUTPUT_ENDPOINT=$INPUT_ENDPOINT
   fi
else
   echo "Using endpoint $OUTPUT_ENDPOINT found in variable OUTPUT_ENDPOINT for input data."
fi
if [ -z ${OUTPUT_BUCKET+x} ]; then
   echo "(set variable OUTPUT_BUCKET to avoid this interactive prompt)"
   while [ -z ${OUTPUT_BUCKET} ]; do
      read -p "Bucket name for output: " OUTPUT_BUCKET
   done
else
   echo "Using bucket $OUTPUT_BUCKET found in variable OUTPUT_BUCKET for input data."
fi
if [ -z ${OUTPUT_PREFIX+x} ]; then
   echo "(set variable OUTPUT_PREFIX to avoid this interactive prompt)"
   read -p "Prefix for output (default: $INPUT_PREFIX): " OUTPUT_PREFIX
   if [ -z ${OUTPUT_PREFIX} ]; then
      OUTPUT_PREFIX=$INPUT_PREFIX
   fi
else
   echo "Using prefix '$OUTPUT_PREFIX' found in variable OUTPUT_PREFIX for input data."
fi

echo ""
echo "Getting list of objects and preparing copy task for each object..."
mc alias set source https://$INPUT_ENDPOINT $INPUT_ACCESS_KEY $INPUT_SECRET_KEY > /dev/null
mc alias set target https://$OUTPUT_ENDPOINT $OUTPUT_ACCESS_KEY $OUTPUT_SECRET_KEY > /dev/null
rm final_commands.txt 2> /dev/null
object_listing=`mc ls --recursive source/$INPUT_BUCKET/$INPUT_PREFIX`
echo "object_name,size" > object_2_size.csv
echo "$object_listing" \
    | tr -s " " \
    | cut -d " " -f 5,4 \
    | while read -r col1 col2; do echo $col2 $col1; done \
    | tr " " "," \
    >> object_2_size.csv
echo "$object_listing" \
    | tr -s " " \
    | cut -d " " -f 5 \
    | while read line; do \
        echo "mc cp source/$INPUT_BUCKET/$INPUT_PREFIX/$line target/$OUTPUT_BUCKET/$OUTPUT_PREFIX/$line" \
        >>final_commands.txt; done
number_of_objects=`cat final_commands.txt | wc -l`
echo "Prepared $number_of_objects copy tasks."

echo "Uploading copy tasks to ray cluster header at $ray_head_ip ..."
ray rsync-up cluster.yaml final_commands.txt /root/final_commands.txt
ray rsync-up cluster.yaml object_2_size.csv /root/object_2_size.csv
echo "Done"

ray submit cluster.yaml cli_command_execution.py -- \
$INPUT_ENDPOINT \
$OUTPUT_ENDPOINT \
$INPUT_ACCESS_KEY \
$INPUT_SECRET_KEY \
$OUTPUT_ACCESS_KEY \
$OUTPUT_SECRET_KEY

