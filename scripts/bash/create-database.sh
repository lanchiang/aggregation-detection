#!/bin/sh

port=$1

#echo "SELECT 'CREATE DATABASE aggrdet' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'aggrdet')\gexec" | psql

psql -p $port -U $USER -f ../sql/create-database.sql