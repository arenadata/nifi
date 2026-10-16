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

[ -f /var/lib/samba/.setup ] || {
    >&2 echo "[ERROR] Samba is not setup yet, which should happen automatically. Look for errors!"
    exit 127
}

samba -i -s /var/lib/samba/private/smb.conf &

# Update the password policy settings
samba-tool domain passwordsettings set --complexity=off
samba-tool domain passwordsettings set --min-pwd-length=4
samba-tool domain passwordsettings set --history-length=0

sleep 10

set +e
ldapadd -x -H ldap://samba:389 -D "cn=Administrator,CN=Users,DC=nifi,DC=test" -w "$SMB_ADMIN_PASSWORD" -f /opt/ad-scripts/people.ldif
LDAPADD_STATUS=$?
set -e

if [ $LDAPADD_STATUS -ne 0 ]; then
    >&2 echo "[ERROR] ldapadd failed with exit code $LDAPADD_STATUS"
fi

tail -f /dev/null
