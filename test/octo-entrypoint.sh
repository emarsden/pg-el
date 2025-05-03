#!/bin/bash

# Adapted from the entrypoint.sh script in the official Docker image, which does
# not allow the user to set commandline options for rocto.
#
#   https://gitlab.com/YottaDB/DBMS/YDBOcto/-/blob/master/tools/entrypoint.sh?ref_type=heads


# If /data/octo.conf doesn't exist, it means that user passed in their own database
# Therefore, do the set-up that was previously done in the docker file
if [ ! -f /data/octo.conf ]; then
	cp /opt/yottadb/current/plugin/octo/octo.conf /data/octo.conf
	sed -i 's/address = "127.0.0.1"/address = "0.0.0.0"/' /data/octo.conf
	source /opt/yottadb/current/ydb_env_set
	octo -f $ydb_dist/plugin/octo/northwind.sql
	mupip load $ydb_dist/plugin/octo/northwind.zwr
	printf "ydbrocks\nydbrocks" | "$ydb_dist/yottadb" -r %ydboctoAdmin add user ydb
	source /opt/yottadb/current/ydb_env_unset
fi

# Set environment variables
source /opt/yottadb/current/ydb_env_set


printf "pgeltest\npgeltest" | yottadb -r %ydboctoAdmin add user pgeltestuser --readwrite --allowschemachanges

# Run the rocto service (Must use exec to that CTRL-C goes to Rocto, not the bash script)
exec rocto -v --allowschemachanges --readwrite
