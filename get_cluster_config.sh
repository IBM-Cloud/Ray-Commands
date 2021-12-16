#!/bin/bash

if [ "$#" -ne 1 ]; then
   echo "Usage: $0 <ray cluster config yaml generated with lithopscloud>"
   exit 1
fi
grep -q "upscaling_speed: " $1
if [ "$?" -ne 0 ]; then
   echo "$1 doesn't seem to be a valid ray cluster config yaml generated with lithopscloud"
   exit 1
fi
grep -q " wget https://dl.min.io/client/mc/release/linux-amd64/mc" $1
if [ "$?" -eq 0 ]; then
   echo "$1 seems to be already processed by $0 beforehand"
   exit 1
fi

sed '/upscaling_speed: /i\
- wget https://dl.min.io/client/mc/release/linux-amd64/mc\
- cp mc /usr/local/bin/\
- chmod 700  /usr/local/bin/mc\
- alias mc=/usr/local/bin/mc\
- pip install humanfriendly\
- pip install pandas\
' $1 > cluster.yaml

echo "Your ray cluster yaml is now stored as 'cluster.yaml' ready to be used."
echo ""
echo "You can bring up your ray cluster next with 'ray up cluster.yaml'"
