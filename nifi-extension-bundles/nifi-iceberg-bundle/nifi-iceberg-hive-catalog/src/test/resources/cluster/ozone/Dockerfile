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

# Apache Ozone image with the Apache Ranger Ozone plugin in the Ozone Manager library directory. The plugin
# uses Jersey 1 and JAX-RS from the Ozone Manager classpath, so its implementation directory must not contain JAX-RS jars.

ARG OZONE_IMAGE=apache/ozone:2.1.2-slim

FROM ${OZONE_IMAGE}

ARG RANGER_PLUGIN_URLS

USER root

RUN mkdir -p /etc/ranger /var/log/ranger /opt/ozone-cluster \
    && chown hadoop:hadoop /etc/ranger /var/log/ranger /opt/ozone-cluster \
    && chmod 1777 /etc/ranger /var/log/ranger \
    && for tool in kinit ozone sed; do command -v "${tool}" || exit 1; done \
    && test -n "${RANGER_PLUGIN_URLS}" \
    && manager_lib=/opt/hadoop/share/ozone/lib/ozone-manager \
    && tenancy_classpath=/opt/hadoop/share/ozone/classpath/ozone-multitenancy-ranger.classpath \
    && mkdir -p /tmp/ranger-plugin "${manager_lib}" \
    && downloaded=false \
    && for url in ${RANGER_PLUGIN_URLS}; do \
        if curl -fsSL --retry 3 --retry-delay 5 --connect-timeout 30 -o /tmp/ranger-plugin.tar.gz "${url}" \
            && gzip -t /tmp/ranger-plugin.tar.gz; then downloaded=true; break; fi; \
        echo "Download failed: ${url}"; \
    done \
    && test "${downloaded}" = true \
    && tar -xzf /tmp/ranger-plugin.tar.gz -C /tmp/ranger-plugin --strip-components=1 \
    && test -d /tmp/ranger-plugin/lib/libext/ranger-ozone-plugin-impl \
    && cp -R /tmp/ranger-plugin/lib/libext/. "${manager_lib}/" \
    && ls "${manager_lib}"/ranger-ozone-plugin-shim-*.jar "${manager_lib}"/ranger-plugin-classloader-*.jar \
        "${manager_lib}"/ranger-ozone-plugin-impl/ranger-ozone-plugin-*.jar \
    && if ls "${manager_lib}/ranger-ozone-plugin-impl" | grep -E '^(jersey|jsr311|javax\.ws\.rs|jakarta\.ws\.rs)'; then \
        echo "JAX-RS jars found in ${manager_lib}/ranger-ozone-plugin-impl"; exit 1; fi \
    && grep -q 'jersey-client-1\.' "${tenancy_classpath}" \
    && grep -q 'jackson-jaxrs-json-provider' "${tenancy_classpath}" \
    && grep -q 'ozone-multitenancy-ranger' /opt/hadoop/libexec/shellprofile.d/hadoop-ozone-manager.sh \
    && chmod -R a+rX "${manager_lib}" \
    && rm -rf /tmp/ranger-plugin /tmp/ranger-plugin.tar.gz

USER hadoop
