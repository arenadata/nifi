/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.gpfdist.service.cluster;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.gpfdist.service.NodeIndexService;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterStateNodeIndexService implements NodeIndexService {
    private static final String HOSTS_KEY = "hosts";
    private final int nodeCount;
    private final String hostName;
    private final StateManager stateManager;
    private final ComponentLog logger;

    public ClusterStateNodeIndexService(int nodeCount,
                                        String hostName,
                                        StateManager stateManager,
                                        ComponentLog logger) {
        this.nodeCount = nodeCount;
        this.hostName = hostName;
        this.stateManager = stateManager;
        this.logger = logger;
    }

    @Override
    public int getNodeIndex() throws Exception {
        final StateMap state = stateManager.getState(Scope.CLUSTER);
        final String hostsStr = state.get(HOSTS_KEY);
        final Set<String> hostSet = getHosts(hostsStr);
        final List<String> sorted = new ArrayList<>(hostSet);
        Collections.sort(sorted);
        final int index = sorted.indexOf(hostName);
        if (index < 0) {
            throw new IllegalStateException("Hostname " + hostName + " is not found in sorted hosts: " + sorted);
        }
        logger.info("Node {} got index {} out of {} nodes (hosts={})", hostName, index, nodeCount, sorted);
        return index;
    }

    private Set<String> getHosts(String hostsStr) {
        if (hostsStr == null || hostsStr.trim().isEmpty()) {
            throw new IllegalStateException("Cluster hosts state is empty when getting node index for " + hostName);
        }
        final Set<String> hosts = Arrays.stream(hostsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        if (!hosts.contains(hostName)) {
            throw new IllegalStateException("Hostname " + hostName + " is not found in cluster hosts: " + hostsStr);
        }
        if (hosts.size() < nodeCount) {
            throw new IllegalStateException("Not all nodes are registered yet: currentSize=" +
                    hosts.size() + ", expected=" + nodeCount + ", hosts=" + hostsStr);
        }
        return hosts;
    }

    @Override
    public int getNodeCount() {
        return nodeCount;
    }
}
