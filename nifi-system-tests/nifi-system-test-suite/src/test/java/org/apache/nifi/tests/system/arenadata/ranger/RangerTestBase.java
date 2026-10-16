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
package org.apache.nifi.tests.system.arenadata.ranger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.qameta.allure.Step;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.tests.system.arenadata.NifiSystemContainerizedIT;
import org.apache.nifi.tests.system.arenadata.service.RangerAdminService;
import org.apache.nifi.tests.system.arenadata.service.SolrAuditService;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.ProcessorClient;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.toolkit.client.impl.request.BearerTokenRequestConfig;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.tests.system.arenadata.util.ConfigUtil.getTestConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for Ranger system tests with common setup
 */
@Slf4j
public abstract class RangerTestBase extends NifiSystemContainerizedIT {

    protected static final String NIFI_SERVICE_NAME = "nifi";
    protected static final String TEST_PROCESSOR = "org.apache.nifi.processors.standard.GenerateFlowFile";
    protected static final String ARTIFACT_ID = "nifi-standard-nar";
    private static final String NIFI_ADMIN_BOOTSTRAP_POLICY_NAME = "nifi-admin-bootstrap";
    private static final String NIFI_CLIENT_CERTIFICATE_IDENTITY = "CN=nifi01.demo, O=NiFi Test, L=Santa Monica, ST=CA, C=US";
    protected static final String TEST_USER1 = "test-user1";
    protected static final String TEST_USER1_PASSWORD = "test-pass1";
    protected static final String TEST_USER2 = "test-user2";
    protected static final String TEST_USER2_PASSWORD = "test-pass2";
    protected static final String TEST_GROUP1 = "test-group1";

    protected static RangerAdminService rangerAdminService;
    protected static SolrAuditService solrService;
    protected static ObjectMapper objectMapper;

    protected final List<String> createdPolicyNames = new ArrayList<>();

    @BeforeAll
    public static void setupRangerInfrastructure() {
        objectMapper = new ObjectMapper();
        rangerAdminService = new RangerAdminService();
        rangerAdminService.ensurePolicyItemUser(NIFI_SERVICE_NAME, NIFI_ADMIN_BOOTSTRAP_POLICY_NAME, NIFI_CLIENT_CERTIFICATE_IDENTITY);
        String solrUrl = getTestConfig().getSolrUrl();
        solrService = new SolrAuditService(solrUrl);
        log.info("Ranger infrastructure is ready");
    }

    @AfterEach
    public void cleanupPolicies() {
        for (String policyName : createdPolicyNames) {
            try {
                deletePolicy(policyName);
            } catch (Exception e) {
                log.warn("Failed to delete policy {}: {}", policyName, e.getMessage());
            }
        }
        createdPolicyNames.clear();
    }

    @Step("Delete policy")
    public void deletePolicy(String policyName) {
        log.info("Delete policy {}", policyName);
        JsonNode policies = rangerAdminService.listPolicies(NIFI_SERVICE_NAME);
        for (JsonNode policy : policies) {
            if (policyName.equals(policy.get("name").asText())) {
                int policyId = policy.get("id").asInt();
                rangerAdminService.deletePolicyById(policyId, policyName, NIFI_SERVICE_NAME);
                log.info("Deleted test policy: {} (id={})", policyName, policyId);
                break;
            }
        }
    }

    @Step("Create Ranger policy")
    public void createPolicy(PolicyBuilder builder, String policyName) {
        ObjectNode policy = builder.build();
        log.info("Policy JSON: {}", policy.toPrettyString());
        rangerAdminService.createPolicyIfAbsent(policy);
        createdPolicyNames.add(policyName);
    }

    @Step("Create NiFi processor")
    public ProcessorEntity createProcessor() throws Exception {
        ProcessorEntity processor = getClientUtil().createProcessor(
                TEST_PROCESSOR,
                NIFI_GROUP_ID,
                ARTIFACT_ID,
                getNiFiVersion()
        );
        processor = getClientUtil().setAutoTerminatedRelationships(processor, "success");
        getClientUtil().waitForValidProcessor(processor.getId());
        log.info("Created test processor id={}", processor.getId());
        return processor;
    }

    public RequestConfig createNifiRequestConfigForUser(String username, String password) throws Exception {
        try (NiFiClient loginClient = createClient(getClientApiPort())) {
            String token = loginClient.getAccessClient().getToken(username, password);
            return new BearerTokenRequestConfig(token);
        }
    }

    @Step("Perform read operation with wait")
    public void performReadOperationWithWait(String user,
                                                String password,
                                                ProcessorEntity processor,
                                                boolean canRead) {
        assertWithPooling(() -> performReadOperation(user,  password, processor, canRead));
    }

    @Step("Perform read operation")
    public void performReadOperation(String user, String password, ProcessorEntity processor, boolean canRead)
            throws Exception {
        ProcessorClient processorClient =
                getNifiClient().getProcessorClient(createNifiRequestConfigForUser(user, password));
        ProcessorEntity readEntity = null;
        try {
            readEntity = processorClient.getProcessor(processor.getId());
            log.info("User {} performed READ on processor {}", user, processor.getId());
        } catch (Exception e) {
            log.info("User {} READ denied for processor {}: {}", user, processor.getId(), e.getMessage());
        }
        if (canRead) {
            assertNotNull(readEntity, user + " should be able to READ processor");
        } else {
            assertNull(readEntity, user + " should not be able to READ processor");
        }
    }

    @Step("Perform write operation with wait")
    public void performWriteOperationWithWait(String user,
                                                 String password,
                                                 ProcessorEntity processor,
                                                 boolean canWrite) {
        assertWithPooling(() -> performWriteOperation(user,  password, processor, canWrite));
    }

    @Step("Perform write operation")
    public void performWriteOperation(String user, String password, ProcessorEntity processor, boolean canWrite)
            throws Exception {
        ProcessorClient processorClient =
                getNifiClient().getProcessorClient(createNifiRequestConfigForUser(user, password));
        boolean writeSucceeded;
        try {
            ProcessorEntity readEntity = processorClient.getProcessor(processor.getId());
            PositionDTO pos = readEntity.getComponent().getPosition() == null
                              ? new PositionDTO(0.0, 0.0)
                              : readEntity.getComponent().getPosition();
            readEntity.getComponent().setPosition(new PositionDTO(pos.getX() + 1.0, pos.getY()));
            processorClient.updateProcessor(readEntity);
            writeSucceeded = true;
            log.info("User {} performed WRITE on processor {}", user, processor.getId());
        } catch (Exception e) {
            writeSucceeded = false;
            log.info("User {} WRITE denied for processor {}: {}", user, processor.getId(), e.getMessage());
        }
        if (canWrite) {
            assertTrue(writeSucceeded, user + " should be able to WRITE processor");
        } else {
            assertFalse(writeSucceeded, user + " should NOT be able to WRITE processor");
        }
    }

    @Step("Perform delete operation with wait")
    public void performDeleteOperationWithWait(String user,
                                                  String password,
                                                  ProcessorEntity processor,
                                                  boolean canDelete) {
        assertWithPooling(() -> performDeleteOperation(user,  password, processor, canDelete));
    }

    @Step("Perform delete operation")
    public void performDeleteOperation(String user, String password, ProcessorEntity processor, boolean canDelete)
            throws Exception {
        ProcessorClient processorClient =
                getNifiClient().getProcessorClient(createNifiRequestConfigForUser(user, password));
        boolean deleteSucceeded;
        try {
            processorClient.deleteProcessor(processor);
            deleteSucceeded = true;
            log.info("Processor {} deleted by {}", processor.getId(), user);
        } catch (Exception e) {
            deleteSucceeded = false;
            log.info("DELETE denied for {}: {}", user, e.getMessage());
        }
        assertEquals(canDelete, deleteSucceeded,
                "%s should %s be able to DELETE processor".formatted(user, canDelete ? "" : "NOT"));
    }

    public PolicyBuilder createPolicyBuilder(String policyName, String resourcePath) {
        return new PolicyBuilder(objectMapper, NIFI_SERVICE_NAME, policyName, resourcePath);
    }

    /**
     * Builder for creating Ranger policies with ALLOW and DENY items
     */
    public static class PolicyBuilder {
        private final ObjectNode policy;
        private final ArrayNode policyItems;
        private final ArrayNode denyPolicyItems;
        private final ArrayNode allowExceptions;
        private final ArrayNode denyExceptions;

        public PolicyBuilder(ObjectMapper objectMapper, String serviceName, String policyName, String resourcePath) {
            this.policy = objectMapper.createObjectNode();

            // Basic policy structure
            policy.put("service", serviceName);
            policy.put("name", policyName);
            policy.put("policyType", 0); // Always 0 for policies with allow/deny items
            policy.put("description", "Test policy created by system tests");
            policy.put("isEnabled", true);
            policy.put("isAuditEnabled", true);

            // Resources - NiFi does not support isRecursive or isExcludes
            ObjectNode resources = policy.putObject("resources");
            ObjectNode resource = resources.putObject("nifi-resource");
            resource.putArray("values").add(resourcePath);
            resource.put("isExcludes", false);
            resource.put("isRecursive", false);

            // Initialize arrays
            this.policyItems = policy.putArray("policyItems");
            this.allowExceptions = policy.putArray("allowExceptions");
            this.denyPolicyItems = policy.putArray("denyPolicyItems");
            this.denyExceptions = policy.putArray("denyExceptions");
            policy.putArray("dataMaskPolicyItems");
            policy.putArray("rowFilterPolicyItems");
        }

        /**
         * Add ALLOW policy item
         */
        public PolicyBuilder addAllowItem(String[] users, String[] groups, AccessType[] accesses) {
            ObjectNode policyItem = policyItems.addObject();
            addPolicyItemContent(policyItem, users, groups, accesses);
            return this;
        }

        /**
         * Add DENY policy item
         */
        public PolicyBuilder addDenyItem(String[] users, String[] groups, AccessType[] accesses) {
            ObjectNode denyPolicyItem = denyPolicyItems.addObject();
            addPolicyItemContent(denyPolicyItem, users, groups, accesses);
            return this;
        }

        /**
         * Add ALLOW exception - users/groups that are excluded from deny rules
         */
        public PolicyBuilder addAllowException(String[] users, String[] groups, AccessType[] accesses) {
            ObjectNode allowException = allowExceptions.addObject();
            addPolicyItemContent(allowException, users, groups, accesses);
            return this;
        }

        /**
         * Add DENY exception - users/groups that are excluded from allow rules
         */
        public PolicyBuilder addDenyException(String[] users, String[] groups, AccessType[] accesses) {
            ObjectNode denyException = denyExceptions.addObject();
            addPolicyItemContent(denyException, users, groups, accesses);
            return this;
        }

        private void addPolicyItemContent(ObjectNode policyItem,
                                          String[] users,
                                          String[] groups,
                                          AccessType[] accesses) {
            ArrayNode usersArray = policyItem.putArray("users");
            if (users != null) {
                for (String user : users) {
                    usersArray.add(user);
                }
            }
            ArrayNode groupsArray = policyItem.putArray("groups");
            if (groups != null) {
                for (String group : groups) {
                    groupsArray.add(group);
                }
            }
            ArrayNode accessesArray = policyItem.putArray("accesses");
            if (accesses != null) {
                for (AccessType access : accesses) {
                    accessesArray.addObject()
                            .put("type", access.toString())
                            .put("isAllowed", true);
                }
            }
            policyItem.put("delegateAdmin", false);
        }

        /**
         * Build the final policy JSON
         */
        public ObjectNode build() {
            return policy;
        }
    }

    public enum AccessType {
        READ,
        WRITE
    }

    public enum AuditResult {
        ALLOWED(1),
        DENIED(0);

        @Getter
        private final int value;

        AuditResult(int value) {
            this.value = value;
        }
    }
}
