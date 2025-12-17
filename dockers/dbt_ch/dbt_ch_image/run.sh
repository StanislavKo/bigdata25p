#!/bin/bash

while : ; do
  echo -e $(date) " DBT task "

  dbt run

  sleep 15 
done






