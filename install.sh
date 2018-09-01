#!/usr/bin/env bash

./gradlew clean build

source ./gradle.properties
mkdir -p ~/lib/kafkaless
mkdir -p ~/bin/
tar -xf ./build/distributions/kafkaless-shadow-${VERSION}.tar --strip 1 -C ~/lib/kafkaless
ln -Fs ~/lib/kafkaless/bin/kafkaless ~/bin/kafkaless

#Recommended .profile additions:
#export PATH=$PATH:$HOME/bin
#alias kl="kafkaless -b kafka03-prod02.messagehub.services.us-south.bluemix.net:9093 -s -u xxxx -p xxxx"
