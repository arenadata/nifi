#!/bin/bash
IS_MASTER_OR_STANDBY=$1
PRIMARY_SEGMENTS_COUNT=$2
WITH_MIRRORS=${3:-false}

if "$IS_MASTER_OR_STANDBY" ; then
    is_master_started=$(ps aux | grep "/usr/local/greenplum-db-devel/bin/postgres -D /data/gpdata/master/gpseg-1 -p 6000 -E" | grep -v grep -oc)
    if [ "$is_master_started" -ne 1 ]; then
        echo "There is no running postgres process on the master host"
        exit 1
    fi
else
    started_segments=$(ps aux | grep "/usr/local/greenplum-db-devel/bin/postgres -D /data/gpdata/primary*" | grep -v grep -oc)
    echo "Started primary segments count $started_segments"
    if [ "$started_segments" -ne "$PRIMARY_SEGMENTS_COUNT" ]; then
        echo "Started primary segments count expected $PRIMARY_SEGMENTS_COUNT but actual is $started_segments"
        exit 1
    fi
    if "$WITH_MIRRORS" ; then
        started_mirror_segments=$(ps aux | grep "/usr/local/greenplum-db-devel/bin/postgres -D /data/gpdata/mirror*" | grep -v grep -oc)
        echo "Started mirror segments count $started_mirror_segments"
        if [ "$started_mirror_segments" -ne "$PRIMARY_SEGMENTS_COUNT" ]; then
            echo "Started mirror segments count expected $PRIMARY_SEGMENTS_COUNT but actual is $started_mirror_segments"
            exit 1
        fi
    fi
fi
