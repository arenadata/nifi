#!/bin/bash

#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

set -e

info () {
    echo "[INFO] $@"
}

info "Running setup"

[ -f /var/lib/samba/.setup ] && info "Already setup..." && exit 0

info "Provisioning domain controller..."

info "Given admin password: ${SMB_ADMIN_PASSWORD}"

rm -f /etc/samba/smb.conf

samba-tool domain provision\
 --server-role=dc\
 --use-rfc2307\
 --dns-backend=SAMBA_INTERNAL\
 --realm=nifi.test\
 --domain=nifi\
 --adminpass=${SMB_ADMIN_PASSWORD}

sed -i '/^\[global\]/a client ldap sasl wrapping = sign\nldap server require strong auth = no\n' /etc/samba/smb.conf
sed -i '/^\[global\]/a client ldap sasl wrapping = sign\nldap server require strong auth = no\n' /etc/samba/smb.conf

cp /etc/samba/smb.conf /var/lib/samba/private/smb.conf
rm -f /etc/samba/smb.conf

touch /var/lib/samba/.setup
