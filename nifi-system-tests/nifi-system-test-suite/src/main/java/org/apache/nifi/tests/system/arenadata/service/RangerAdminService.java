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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.nifi.tests.system.arenadata.util.ConfigUtil.getTestConfig;

/**
 * Service for interacting with Ranger Admin REST API
 */
@Slf4j
public final class RangerAdminService implements AutoCloseable {
    private static final String API_V2 = "/service/public/v2/api";
    private static final String ENDPOINT_SERVICES = API_V2 + "/service";
    private static final String ENDPOINT_POLICIES = API_V2 + "/policy";
    private static final String ENDPOINT_SERVICE_DEF = API_V2 + "/servicedef";
    private static final String ENDPOINT_LOGIN = "/login.jsp";

    private static final MediaType JSON = MediaType.parse("application/json");

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final String authorizationHeader;

    public RangerAdminService() {
        this.baseUrl = getTestConfig().getRanger().getUrl();
        String username = getTestConfig().getRanger().getUsername();
        String password = getTestConfig().getRanger().getPassword();
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(2, TimeUnit.MINUTES)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
        this.objectMapper = new ObjectMapper();
        this.authorizationHeader = Credentials.basic(username, password);
    }

    /**
     * Create Ranger policy if it doesn't exist
     */
    public void createPolicyIfAbsent(JsonNode policy) {
        String serviceName = requireNonNull(policy.get("service"), "policy service is null").asText();
        String policyName = requireNonNull(policy.get("name"), "policy name is null").asText();
        if (policyExists(serviceName, policyName)) {
            log.info("Ranger policy '{}' on service '{}' already exists", policyName, serviceName);
            return;
        }
        ObjectNode body = policy.deepCopy();
        body.remove("id");
        body.remove("guid");
        body.remove("version");
        try {
            postJson(ENDPOINT_POLICIES, body);
            log.info("Created Ranger policy '{}' on service '{}'", policyName, serviceName);
        } catch (IllegalStateException e) {
            // Only treat as success when OUR named policy is present
            if (e.getMessage() != null && e.getMessage().contains("already exists for matching resource")
                    && policyExists(serviceName, policyName)) {
                log.info("Ranger policy '{}' on service '{}' already exists (collision)", policyName, serviceName);
                return;
            }
            throw e;
        }
    }

    /**
     * List all policies for a service
     */
    public JsonNode listPolicies(String serviceName) {
        Request request = authorizedRequest(ENDPOINT_SERVICES + "/" + serviceName + "/policy")
                .get()
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                String responseBody = response.body() == null ? "" : response.body().string();
                throw new IllegalStateException(format(
                        "Failed to list Ranger policies for service %s: HTTP %s: %s",
                        serviceName,
                        response.code(),
                        responseBody));
            }
            JsonNode body = objectMapper.readTree(requireNonNull(response.body(), "response body is null").byteStream());
            if (body.isArray()) {
                return body;
            }
            if (body.has("policies") && body.get("policies").isArray()) {
                return body.get("policies");
            }
            throw new IllegalStateException(format("Unexpected Ranger policies response for service %s: %s", serviceName, body));
        } catch (IOException e) {
            throw new UncheckedIOException(format("Failed to list Ranger policies for service %s", serviceName), e);
        }
    }

    /**
     * Ensure that the first allow policy item of the named policy includes the provided user.
     */
    public void ensurePolicyItemUser(String serviceName, String policyName, String user) {
        for (JsonNode policy : listPolicies(serviceName)) {
            if (!policyName.equals(policy.path("name").asText())) {
                continue;
            }
            ObjectNode updatedPolicy = policy.deepCopy();
            ArrayNode policyItems = getOrCreateArray(updatedPolicy, "policyItems");
            ObjectNode policyItem;
            if (policyItems.isEmpty()) {
                policyItem = policyItems.addObject();
                policyItem.putArray("groups");
                policyItem.putArray("accesses");
                policyItem.put("delegateAdmin", false);
            } else {
                policyItem = (ObjectNode) policyItems.get(0);
            }
            ArrayNode users = getOrCreateArray(policyItem, "users");
            for (JsonNode existingUser : users) {
                if (user.equals(existingUser.asText())) {
                    log.info("Ranger policy '{}' on service '{}' already includes user '{}'", policyName, serviceName, user);
                    return;
                }
            }
            users.add(user);
            putJson(ENDPOINT_POLICIES + "/" + updatedPolicy.path("id").asInt(), updatedPolicy);
            log.info("Updated Ranger policy '{}' on service '{}' to include user '{}'", policyName, serviceName, user);
            return;
        }
        throw new IllegalStateException(format("Ranger policy %s on service %s was not found", policyName, serviceName));
    }

    private ArrayNode getOrCreateArray(ObjectNode objectNode, String fieldName) {
        JsonNode array = objectNode.get(fieldName);
        if (array instanceof ArrayNode arrayNode) {
            return arrayNode;
        }
        return objectNode.putArray(fieldName);
    }

    /**
     * Delete a policy by ID
     */
    public void deletePolicyById(int policyId, String policyName, String serviceName) {
        Request deleteRequest = authorizedRequest(ENDPOINT_POLICIES + "/" + policyId)
                .delete()
                .build();
        try (Response deleteResponse = httpClient.newCall(deleteRequest).execute()) {
            if (deleteResponse.code() != 200 && deleteResponse.code() != 204) {
                String responseBody = deleteResponse.body() == null ? "" : deleteResponse.body().string();
                throw new IllegalStateException(format(
                        "Failed to delete Ranger policy %s on service %s: HTTP %s: %s",
                        policyName,
                        serviceName,
                        deleteResponse.code(),
                        responseBody));
            }
            log.info("Deleted Ranger policy '{}' on service '{}'", policyName, serviceName);
        } catch (IOException e) {
            throw new UncheckedIOException(format("Failed to delete Ranger policy %s on service %s", policyName, serviceName), e);
        }
    }

    /**
     * Check if policy exists
     */
    private boolean policyExists(String serviceName, String policyName) {
        // List policies and check by name (more reliable than lookup API)
        for (JsonNode policy : listPolicies(serviceName)) {
            if (policyName.equals(policy.path("name").asText())) {
                return true;
            }
        }
        return false;
    }

    /**
     * POST JSON to Ranger API
     */
    private void postJson(String path, JsonNode body) {
        Request request = authorizedRequest(path)
                .header("Content-Type", JSON.toString())
                .post(RequestBody.create(body.toString().getBytes(UTF_8), JSON))
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() == 200 || response.code() == 201) {
                return;
            }
            String responseBody = response.body() == null ? "" : response.body().string();
            throw new IllegalStateException(format("Ranger Admin request to %s failed with HTTP %s: %s",
                    path, response.code(), responseBody));
        } catch (IOException e) {
            throw new UncheckedIOException(format("Ranger Admin request to %s failed", path), e);
        }
    }

    /**
     * PUT JSON to Ranger API
     */
    private void putJson(String path, JsonNode body) {
        Request request = authorizedRequest(path)
                .header("Content-Type", JSON.toString())
                .put(RequestBody.create(body.toString().getBytes(UTF_8), JSON))
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() == 200) {
                return;
            }
            String responseBody = response.body() == null ? "" : response.body().string();
            throw new IllegalStateException(format("Ranger Admin request to %s failed with HTTP %s: %s",
                    path, response.code(), responseBody));
        } catch (IOException e) {
            throw new UncheckedIOException(format("Ranger Admin request to %s failed", path), e);
        }
    }

    /**
     * Create authorized request builder
     */
    private Request.Builder authorizedRequest(String pathOrUrl) {
        String url = pathOrUrl.startsWith("http") ? pathOrUrl : baseUrl + pathOrUrl;
        return new Request.Builder()
                .url(url)
                .header("Authorization", authorizationHeader);
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }
}
