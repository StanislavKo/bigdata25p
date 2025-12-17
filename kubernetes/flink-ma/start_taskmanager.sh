#!/bin/bash

echo "Starting taskmanager"

#rm /var/lib/dpkg/info/libc-bin.*
#apt clean
#apt update
#dpkg --remove --force-remove-reinstreq libc-bin
#dpkg --purge libc-bin
#apt install libc-bin
#apt --reinstall install libc-bin

#apt install python3 -y
#python3 -m http.server 8000 &

#apt install libcgi-session-perl
#apt install libproc-daemon-perl
#apt install libconfig-simple-perl
#perl /data/http.pl

#apt update
#apt install netcat -y
#/data/simple_server.sh &

./bin/taskmanager.sh start-foreground

