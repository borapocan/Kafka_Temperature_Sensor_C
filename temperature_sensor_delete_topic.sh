#!/bin/sh

#
# Author: Merih Bora Poçan
# Mail: mborapocan@gmail.com
# Github: @borapocan <git@borapocan.com>
# Creation Date: 12-04-2023
# Last Modified:
#
/opt/kafka_2.13-3.4.0/bin/kafka-topics.sh --bootstrap-server="192.168.212.40:9092" --topic="temperature_sensor" --delete
