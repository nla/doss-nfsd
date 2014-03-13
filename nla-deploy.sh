#!/bin/bash
mvn package
cp target/doss-nfsd-*-jar-with-dependencies.jar $1/doss-nfsd.jar
