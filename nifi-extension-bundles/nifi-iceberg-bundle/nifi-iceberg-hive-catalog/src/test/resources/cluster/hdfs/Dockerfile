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

# Hadoop image with the Apache Ranger HDFS plugin: the shim and class loader jars and the ranger-hdfs-plugin-impl
# directory are linked into the HDFS library directory on the NameNode classpath, as enable-hdfs-plugin.sh does.

ARG HADOOP_IMAGE=apache/hadoop:3.4.1
FROM ${HADOOP_IMAGE}

ARG HADOOP_LIBRARY_DIRECTORY=/opt/hadoop/share/hadoop/hdfs/lib
ARG PLUGIN_DIRECTORY=/opt/ranger

USER root

ADD ranger-hdfs-plugin.tar.gz ${PLUGIN_DIRECTORY}/

RUN plugin_home="$(find "${PLUGIN_DIRECTORY}" -mindepth 1 -maxdepth 1 -type d -name 'ranger-*-hdfs-plugin' | head -n 1)" \
    && test -n "${plugin_home}" \
    && chmod -R a+rX "${PLUGIN_DIRECTORY}" \
    && for library in "${plugin_home}"/lib/*; do ln -sfn "${library}" "${HADOOP_LIBRARY_DIRECTORY}/$(basename "${library}")"; done \
    && ls "${HADOOP_LIBRARY_DIRECTORY}"/ranger-hdfs-plugin-shim-*.jar "${HADOOP_LIBRARY_DIRECTORY}"/ranger-plugin-classloader-*.jar \
    && ls "${HADOOP_LIBRARY_DIRECTORY}"/ranger-hdfs-plugin-impl/ranger-hdfs-plugin-*.jar

USER hadoop
