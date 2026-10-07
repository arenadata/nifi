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

# Apache Ozone image with the optional Apache Ranger Ozone plugin in the Ozone Manager classpath directory. Jersey 1
# for the plugin REST client is copied there too, as loading it in the plugin class loader fails with a LinkageError.

ARG OZONE_IMAGE=apache/ozone:2.1.2-slim

FROM ${OZONE_IMAGE}

ARG RANGER_PLUGIN_URLS=

USER root

RUN mkdir -p /etc/ranger /var/log/ranger /opt/ozone-cluster \
    && chown hadoop:hadoop /etc/ranger /var/log/ranger /opt/ozone-cluster \
    && chmod 1777 /etc/ranger /var/log/ranger \
    && for tool in kinit ozone sed; do command -v "${tool}" || exit 1; done \
    && if [ -n "${RANGER_PLUGIN_URLS}" ]; then \
        manager_lib=/opt/hadoop/share/ozone/lib/ozone-manager \
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
        && for pattern in 'jersey-client-1.*.jar' 'jersey-core-1.*.jar'; do \
            found=false; \
            for jar in /opt/hadoop/share/ozone/lib/${pattern}; do \
                if [ -f "${jar}" ]; then cp "${jar}" "${manager_lib}/" && found=true; fi; \
            done; \
            if [ "${found}" != true ]; then echo "Missing Jersey 1 jar: ${pattern}"; exit 1; fi; \
        done \
        && for jar in /opt/hadoop/share/ozone/lib/javax.annotation-api-*.jar; do \
            if [ -f "${jar}" ]; then cp "${jar}" "${manager_lib}/"; fi; \
        done \
        && chmod -R a+rX "${manager_lib}" \
        && rm -rf /tmp/ranger-plugin /tmp/ranger-plugin.tar.gz; \
    fi

USER hadoop
