# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

# Copy bootstrap configs first
cp -a /opt/nifi/nifi-current/bootstrap_conf/. /opt/nifi/nifi-current/conf/

# Now copy custom configs from /tmp/custom-configs (mounted as volumes)
CUSTOM_CONFIGS_DIR="/tmp/custom-configs"

if [ -f "$CUSTOM_CONFIGS_DIR/login-identity-providers.xml" ]; then
    cp "$CUSTOM_CONFIGS_DIR/login-identity-providers.xml" /opt/nifi/nifi-current/conf/
    echo "Copied login-identity-providers.xml from custom configs"
fi

if [ -f "$CUSTOM_CONFIGS_DIR/authorizers.xml" ]; then
    cp "$CUSTOM_CONFIGS_DIR/authorizers.xml" /opt/nifi/nifi-current/conf/
    echo "Copied authorizers.xml from custom configs"
fi

if [ -f "$CUSTOM_CONFIGS_DIR/ranger-nifi-security.xml" ]; then
    cp "$CUSTOM_CONFIGS_DIR/ranger-nifi-security.xml" /opt/nifi/nifi-current/conf/
    echo "Copied ranger-nifi-security.xml from custom configs"
fi

if [ -f "$CUSTOM_CONFIGS_DIR/ranger-nifi-audit.xml" ]; then
    cp "$CUSTOM_CONFIGS_DIR/ranger-nifi-audit.xml" /opt/nifi/nifi-current/conf/
    echo "Copied ranger-nifi-audit.xml from custom configs"
fi

# Set permissions
chown -R nifi:nifi /opt/nifi/nifi-current/conf && \
chmod -R 755 /opt/nifi/nifi-current/conf

# Apply environment variables to nifi.properties
NIFI_PROPS="/opt/nifi/nifi-current/conf/nifi.properties"

if [ -n "$NIFI_SECURITY_USER_AUTHORIZER" ]; then
    echo "Setting nifi.security.user.authorizer to $NIFI_SECURITY_USER_AUTHORIZER"
    sed -i "s|^nifi.security.user.authorizer=.*|nifi.security.user.authorizer=$NIFI_SECURITY_USER_AUTHORIZER|" "$NIFI_PROPS"
fi

if [ -n "$NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER" ]; then
    echo "Setting nifi.security.user.login.identity.provider to $NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER"
    sed -i "s|^nifi.security.user.login.identity.provider=.*|nifi.security.user.login.identity.provider=$NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER|" "$NIFI_PROPS"
fi

# Start NiFi
source "../scripts/start.sh"