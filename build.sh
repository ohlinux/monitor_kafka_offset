#!/bin/bash

cd src && go build monitor_kafka_offset.go && mv monitor_kafka_offset ../bin
ask=$?
if [ $ask -eq 0 ]; then 
    echo "$ask build success!!!"
else 
    echo "$ask build failed!!!"
fi 
