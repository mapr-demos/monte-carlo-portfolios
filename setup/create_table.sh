#!/bin/bash

TABLE_PATH=/mapr/demo.mapr.com/tables
TABLE_NAME=test

maprcli table delete -path ${TABLE_PATH}/${TABLE_NAME}

maprcli table create -path ${TABLE_PATH}/${TABLE_NAME} -tabletype json -audit false -insertionorder false

maprcli table info -path ${TABLE_PATH}/${TABLE_NAME} -json
maprcli table cf list -path ${TABLE_PATH}/${TABLE_NAME} -json

maprdb importJSON -idField "id" -src /mapr/demo.mapr.com/tmp/*.json -dst ${TABLE_PATH}/${TABLE_NAME}

