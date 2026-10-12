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
package org.apache.nifi.tests.system.arenadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Service for querying Ranger audit events from Solr
 */
@Slf4j
public class SolrAuditService {
    private final String solrUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public SolrAuditService(String solrUrl) {
        this.solrUrl = solrUrl;
        this.httpClient = HttpClient.newBuilder().build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Query audit events with custom Solr query
     */
    public List<Map<String, Object>> query(String solrQuery, int maxResults) throws IOException, InterruptedException {
        String url = String.format("%s/solr/ranger_audits/select?q=%s&rows=%d&sort=evtTime+desc&wt=json",
                solrUrl,
                URLEncoder.encode(solrQuery, StandardCharsets.UTF_8),
                maxResults);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
            Map<String, Object> responseData = (Map<String, Object>) result.get("response");
            List<Map<String, Object>> documents = (List<Map<String, Object>>) responseData.get("docs");
            return normalizeDocuments(documents);
        } else {
            throw new IOException("Failed to query Solr: " + response.statusCode() + " - " + response.body());
        }
    }

    private List<Map<String, Object>> normalizeDocuments(List<Map<String, Object>> documents) {
        return documents.stream()
                .map(this::normalizeDocument)
                .toList();
    }

    private Map<String, Object> normalizeDocument(Map<String, Object> document) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        document.forEach((key, value) -> normalized.put(key, normalizeValue(value)));
        if (!normalized.containsKey("repoName") && normalized.containsKey("repo")) {
            normalized.put("repoName", normalized.get("repo"));
        }
        return normalized;
    }

    private Object normalizeValue(Object value) {
        if (value instanceof List<?> values && values.size() == 1) {
            return normalizeValue(values.getFirst());
        }
        if (value instanceof Double doubleValue && doubleValue == Math.rint(doubleValue)) {
            long longValue = doubleValue.longValue();
            if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                return (int) longValue;
            }
            return longValue;
        }
        if (value instanceof Float floatValue && floatValue == Math.rint(floatValue)) {
            long longValue = floatValue.longValue();
            if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                return (int) longValue;
            }
            return longValue;
        }
        return value;
    }

    /**
     * Wait for audit events matching criteria to appear in Solr
     * @param user Username to filter by (null for any)
     * @param resource Resource path to filter by (null for any)
     * @param action Action type to filter by (null for any)
     * @param result Access result to filter by (null for any, 1=allowed, 0=denied)
     * @return List of matching audit events
     */
    public List<Map<String, Object>> waitForAuditEvents(String user, String resource, String action, Integer result) {
        StringBuilder queryBuilder = new StringBuilder("*:*");
        if (user != null) {
            queryBuilder.append(" AND reqUser:\"").append(user).append("\"");
        }
        if (resource != null) {
            // Escape quotes and backslashes in resource path for Solr query
            String escapedResource = resource.replace("\\", "\\\\").replace("\"", "\\\"");
            queryBuilder.append(" AND resource:\"").append(escapedResource).append("\"");
        }
        if (action != null) {
            queryBuilder.append(" AND action:\"").append(action).append("\"");
        }
        if (result != null) {
            queryBuilder.append(" AND result:").append(result);
        }
        String queryStr = queryBuilder.toString();
        log.info("Waiting for an audit event matching: {}", queryStr);
        AtomicReference<List<Map<String, Object>>> events = new AtomicReference<>(List.of());
        Awaitility.waitAtMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    List<Map<String, Object>> currentEvents = query(queryStr, 100);
                    events.set(currentEvents);
                    return !currentEvents.isEmpty();
                });
        log.info("Found {} audit events", events.get().size());
        return events.get();
    }
}
