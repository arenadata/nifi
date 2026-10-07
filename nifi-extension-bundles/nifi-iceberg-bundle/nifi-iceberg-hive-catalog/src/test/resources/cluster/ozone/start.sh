#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts one Storage Container Manager, one Ozone Manager and one DataNode in one container and reports readiness
# after the SCM left safe mode and an open RATIS/ONE pipeline exists.

set -u

ready_message="Ozone cluster ready"
failed_message="Ozone cluster failed"
conf_directory="${OZONE_CONF_DIR:-/etc/hadoop}"
metadata_directory="${OZONE_METADATA_DIRECTORY:-/data/metadata}"
data_directory="${OZONE_DATA_DIRECTORY:-/data/hdds}"
ready_timeout_seconds="${OZONE_READY_TIMEOUT_SECONDS:-540}"
init_attempts="${OZONE_INIT_ATTEMPTS:-30}"
check_principal="${OZONE_CHECK_PRINCIPAL}"
check_keytab="${OZONE_CHECK_KEYTAB}"
kerberos_options="-Dsun.security.krb5.rcache=none"

export KRB5CCNAME=/tmp/krb5cc_ozone_cluster
export OZONE_SCM_OPTS="-Xmx${OZONE_SCM_HEAP:-512m} ${kerberos_options}"
export OZONE_OM_OPTS="-Xmx${OZONE_OM_HEAP:-512m} ${kerberos_options}"
export OZONE_DATANODE_OPTS="-Xmx${OZONE_DATANODE_HEAP:-768m} ${kerberos_options}"
export OZONE_ADMIN_OPTS="-Xmx256m ${kerberos_options}"

scm_pid=""
om_pid=""
datanode_pid=""

log() {
    echo "[ozone-cluster] $*"
}

prefixed() {
    sed -u "s/^/[$1] /"
}

stop_daemons() {
    for pid in ${datanode_pid} ${om_pid} ${scm_pid}; do
        kill "${pid}" 2>/dev/null
    done
    for pid in ${datanode_pid} ${om_pid} ${scm_pid}; do
        wait "${pid}" 2>/dev/null
    done
}

fail() {
    log "$*"
    log "${failed_message}"
    stop_daemons
    exit 1
}

check_running() {
    local name=$1
    local pid=$2
    if [ -n "${pid}" ] && ! kill -0 "${pid}" 2>/dev/null; then
        fail "${name} stopped"
    fi
}

trap 'stop_daemons; exit 0' TERM INT

mkdir -p "${metadata_directory}" "${data_directory}" || fail "Data directories not created"
for properties in /opt/hadoop/etc/hadoop/*.properties; do
    if [ -f "${properties}" ] && [ ! -e "${conf_directory}/$(basename "${properties}")" ]; then
        cp "${properties}" "${conf_directory}/"
    fi
done

if [ ! -f "${metadata_directory}/scm/current/VERSION" ]; then
    log "Initializing Storage Container Manager"
    ozone scm --init > >(prefixed scm-init) 2>&1 || fail "Storage Container Manager initialization failed"
fi

log "Starting Storage Container Manager"
ozone scm > >(prefixed scm) 2>&1 &
scm_pid=$!

attempt=1
log "Initializing Ozone Manager"
until ozone om --init > >(prefixed om-init) 2>&1; do
    check_running "Storage Container Manager" "${scm_pid}"
    if [ "${attempt}" -ge "${init_attempts}" ]; then
        fail "Ozone Manager initialization failed after ${attempt} attempts"
    fi
    attempt=$((attempt + 1))
    log "Ozone Manager initialization attempt ${attempt}"
    sleep 5
done

log "Starting Ozone Manager"
ozone om > >(prefixed om) 2>&1 &
om_pid=$!

log "Starting DataNode"
ozone datanode > >(prefixed datanode) 2>&1 &
datanode_pid=$!

deadline=$((SECONDS + ready_timeout_seconds))
until kinit -kt "${check_keytab}" "${check_principal}"; do
    if [ "${SECONDS}" -ge "${deadline}" ]; then
        fail "Kerberos login of ${check_principal} failed"
    fi
    sleep 2
done

log "Waiting for safe mode exit and an open RATIS/ONE pipeline"
while true; do
    check_running "Storage Container Manager" "${scm_pid}"
    check_running "Ozone Manager" "${om_pid}"
    check_running "DataNode" "${datanode_pid}"
    safe_mode=$(ozone admin safemode status 2>&1)
    if [[ "${safe_mode}" == *"SCM is out of safe mode"* ]]; then
        pipelines=$(ozone admin pipeline list -t RATIS -r ONE -s OPEN 2>&1)
        if [[ "${pipelines}" == *"Pipeline{"* ]]; then
            break
        fi
    fi
    if [ "${SECONDS}" -ge "${deadline}" ]; then
        log "Safe mode status: ${safe_mode}"
        fail "Ozone not ready after ${ready_timeout_seconds} seconds"
    fi
    sleep 3
done

log "${ready_message}"

wait -n "${scm_pid}" "${om_pid}" "${datanode_pid}"
log "Ozone daemon exited"
stop_daemons
exit 1
