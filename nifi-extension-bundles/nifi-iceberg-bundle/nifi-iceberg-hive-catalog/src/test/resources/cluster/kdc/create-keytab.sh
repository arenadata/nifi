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

# Usage: create-keytab.sh <keytab path> <principal>...
# Creates each principal with a random key when missing and exports the current keys without randomizing them, so that
# a principal shared by several keytabs, such as HTTP/localhost, keeps one key version valid in every keytab.

set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "Usage: create-keytab.sh <keytab path> <principal>..." >&2
    exit 2
fi

keytab="$1"
shift

mkdir -p "$(dirname "${keytab}")"
rm -f "${keytab}"

for principal in "$@"; do
    principal_status="$(kadmin.local -q "getprinc ${principal}" 2>&1 || true)"
    if ! grep -F -x -q "Principal: ${principal}" <<< "${principal_status}"; then
        kadmin.local -q "addprinc -randkey ${principal}"
    fi
    kadmin.local -q "ktadd -k ${keytab} -norandkey ${principal}"
done

chmod 644 "${keytab}"

keytab_principals="$(klist -k "${keytab}" | awk 'NR > 3 { print $2 }')"
for principal in "$@"; do
    if ! grep -F -x -q "${principal}" <<< "${keytab_principals}"; then
        echo "Principal [${principal}] not found in keytab [${keytab}]" >&2
        exit 1
    fi
done

echo "Created keytab [${keytab}] principals [$*]"
