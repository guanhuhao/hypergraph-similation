#!/bin/bash
rm -rf ghh-* && mvn -DskipTests clean package && spark-submit --class GraphXExample  target/graphx-example-1.0-SNAPSHOT.jar
