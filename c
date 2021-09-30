#!/bin/sh

export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home

#mvn clean install -s ~/.m2/settings-hdp.xml -nsu

mvn clean install -Pdist -s ~/.m2/settings-hdp.xml -nsu dependency:tree -Ddetail=true 


