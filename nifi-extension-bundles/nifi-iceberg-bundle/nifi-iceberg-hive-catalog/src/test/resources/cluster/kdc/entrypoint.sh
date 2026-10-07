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

# Creates the realm database on first start, starts krb5kdc listening on TCP only, and reports readiness after a
# Kerberos authentication through the configured TCP port succeeds.

set -euo pipefail

realm="${KRB5_REALM}"
port="${KDC_PORT}"
state_directory=/var/lib/krb5kdc
database="${state_directory}/principal"
probe_principal="kdc-probe@${realm}"
probe_keytab="${state_directory}/kdc-probe.keytab"
probe_cache="FILE:/tmp/kdc-probe.cc"
ready_attempts=240

cat > "${KRB5_KDC_PROFILE}" <<CONFIGURATION
[kdcdefaults]
    kdc_listen = ""
    kdc_tcp_listen = ${port}

[realms]
    ${realm} = {
        database_name = ${database}
        key_stash_file = ${state_directory}/.k5.${realm}
        kdc_listen = ""
        kdc_tcp_listen = ${port}
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
        master_key_type = aes256-cts-hmac-sha1-96
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
        default_principal_flags = +renewable
    }

[logging]
    kdc = STDERR
    default = STDERR
CONFIGURATION

if [ ! -f "${database}" ]; then
    master_password="$(head -c 32 /dev/urandom | base64)"
    kdb5_util -r "${realm}" -P "${master_password}" create -s
    echo "Created realm database [${realm}]"
fi

/bin/bash /usr/local/bin/create-keytab.sh "${probe_keytab}" "${probe_principal}"

krb5kdc -n -r "${realm}" &
kdc_pid=$!
trap 'kill -TERM "${kdc_pid}" 2>/dev/null || true' TERM INT

ready=false
for _ in $(seq 1 "${ready_attempts}"); do
    if kinit -k -t "${probe_keytab}" -c "${probe_cache}" "${probe_principal}" 2>/dev/null; then
        kdestroy -c "${probe_cache}" 2>/dev/null || true
        ready=true
        break
    fi
    if ! kill -0 "${kdc_pid}" 2>/dev/null; then
        echo "krb5kdc exited before becoming ready" >&2
        exit 1
    fi
    sleep 0.5
done

if [ "${ready}" != "true" ]; then
    echo "krb5kdc not ready after ${ready_attempts} attempts" >&2
    exit 1
fi

echo "Key Distribution Center ready realm [${realm}] port [${port}]"

wait "${kdc_pid}"
