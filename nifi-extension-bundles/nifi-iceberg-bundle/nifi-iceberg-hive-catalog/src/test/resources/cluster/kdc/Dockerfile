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

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    KRB5_KDC_PROFILE=/etc/krb5kdc/kdc.conf \
    KRB5_REALM=NIFI.COM \
    KDC_PORT=10088 \
    KEYTAB_DIRECTORY=/var/lib/krb5kdc/keytabs

RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server krb5-user \
    && rm -rf /var/lib/apt/lists/* \
    && for tool in krb5kdc kdb5_util kadmin.local kinit klist; do command -v "${tool}" || exit 1; done \
    && mkdir -p /etc/krb5kdc /var/lib/krb5kdc "${KEYTAB_DIRECTORY}" \
    && rm -f /etc/krb5kdc/kdc.conf

COPY entrypoint.sh /usr/local/bin/kdc-entrypoint.sh
COPY create-keytab.sh /usr/local/bin/create-keytab.sh

RUN chmod 755 /usr/local/bin/kdc-entrypoint.sh /usr/local/bin/create-keytab.sh

ENTRYPOINT ["/bin/bash", "/usr/local/bin/kdc-entrypoint.sh"]
