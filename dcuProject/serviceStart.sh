#!/bin/sh
sleep 5
cd /home/ubuntu/dcuProject/
sudo python3 spuReceiver.py &
sleep 2
sudo python3 guiDash.py & 
