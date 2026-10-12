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

import io.qameta.allure.Feature;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Feature("Ranger Authorization")
public class RangerAuthorizationIT extends RangerTestBase {

    @Test
    public void testReadAuthorization() {
        String flowPolicyName = "test-read-flow-policy-user1";
        String flowResourcePath = "/flow";
        verifyReadPermissionForRootProcessGroup(TEST_USER1, TEST_USER1_PASSWORD, false);
        PolicyBuilder userPolicyBuilder = createPolicyBuilder(flowPolicyName, flowResourcePath);
        userPolicyBuilder.addAllowItem(new String[] {TEST_USER1}, null, new String[] {"READ"});
        createPolicy(userPolicyBuilder, flowPolicyName);
        verifyReadPermissionForRootProcessGroup(TEST_USER1, TEST_USER1_PASSWORD, true);
        deletePolicy(flowPolicyName);
        verifyReadPermissionForRootProcessGroup(TEST_USER1, TEST_USER1_PASSWORD, false);
        PolicyBuilder groupPolicyBuilder = createPolicyBuilder(flowPolicyName, flowResourcePath);
        groupPolicyBuilder.addAllowItem(null, new String[] {TEST_GROUP1}, new String[] {"READ"});
        createPolicy(groupPolicyBuilder, flowPolicyName);
        verifyReadPermissionForRootProcessGroup(TEST_USER1, TEST_USER1_PASSWORD, true);
    }

    @Test
    public void testDenyPolicyPriority() throws Exception {
        var processor = createProcessor();
        String policyName = "test-policy-processor-deny-priority";
        String resourcePath = "/processors/" + processor.getId();
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(null, new String[] {TEST_GROUP1}, new String[] {"READ", "WRITE"});
        builder.addDenyItem(new String[] {TEST_USER1}, null, new String[] {"WRITE"});
        createPolicy(builder, policyName);
        verifyReadAndWriteProcessorPermission(TEST_USER1, TEST_USER1_PASSWORD, processor, true, false);
    }

    @Test
    public void testPolicyAggregation() {
        String policyName = "test-aggregation-policy-flow";
        String resourcePath = "/flow";
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(new String[]{TEST_USER2}, new String[]{TEST_GROUP1}, new String[]{"READ"});
        createPolicy(builder, policyName);
        verifyReadPermissionForRootProcessGroup(TEST_USER1, TEST_USER1_PASSWORD, true);
        verifyReadPermissionForRootProcessGroup(TEST_USER2, TEST_USER2_PASSWORD, true);
    }

    @Test
    public void testWildcardPolicy() throws Exception {
        var processor1 = createProcessor();
        var processor2 = createProcessor();
        String policyName = "test-policy-wildcard-processors";
        String wildcardResourcePath = "/processors/*";
        PolicyBuilder builder = createPolicyBuilder(policyName, wildcardResourcePath);
        builder.addAllowItem(new String[] {TEST_USER1}, null, new String[] {"READ"});
        createPolicy(builder, policyName);
        verifyReadAndWriteProcessorPermission(TEST_USER1, TEST_USER1_PASSWORD, processor1, true, false);
        verifyReadAndWriteProcessorPermission(TEST_USER1, TEST_USER1_PASSWORD, processor2, true, false);
    }

    @Test
    public void testAllowExceptionPolicy() throws Exception {
        var processor = createProcessor();
        String policyName = "test-policy-allow-exception";
        String resourcePath = "/processors/" + processor.getId();
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(new String[] {TEST_USER1, TEST_USER2}, null, new String[] {"READ", "WRITE"});
        builder.addAllowException(new String[] {TEST_USER1}, null, new String[] {"WRITE"});
        createPolicy(builder, policyName);
        verifyReadAndWriteProcessorPermission(TEST_USER1, TEST_USER1_PASSWORD, processor, true, false);
        verifyReadAndWriteProcessorPermission(TEST_USER2, TEST_USER2_PASSWORD, processor, true, true);
    }

    @Test
    public void testDenyExceptionPolicy() throws Exception {
        var processor = createProcessor();
        String policyName = "test-policy-allow-exception";
        String resourcePath = "/processors/" + processor.getId();
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(new String[] {TEST_USER1, TEST_USER2}, null, new String[] {"READ", "WRITE"});
        builder.addDenyItem(null, new String[] {TEST_GROUP1}, new String[] {"WRITE"});
        builder.addDenyException(new String[] {TEST_USER1}, null, new String[] {"WRITE"});
        createPolicy(builder, policyName);
        verifyReadAndWriteProcessorPermission(TEST_USER1, TEST_USER1_PASSWORD, processor, true, true);
        verifyReadAndWriteProcessorPermission(TEST_USER2, TEST_USER2_PASSWORD, processor, true, true);
    }

    @Test
    public void testDeleteAuthorization() throws Exception {
        var processor = createProcessor();
        String parentGroupId = processor.getComponent().getParentGroupId();
        log.info("Parent process group id={}", parentGroupId);
        String processorPolicyName = "test-policy-processor-delete";
        String processorResourcePath = "/processors/" + processor.getId();
        PolicyBuilder processorPolicyBuilder = createPolicyBuilder(processorPolicyName, processorResourcePath);
        processorPolicyBuilder.addAllowItem(new String[] {TEST_USER1}, null, new String[] {"READ"});
        processorPolicyBuilder.addAllowItem(new String[] {TEST_USER2}, null, new String[] {"READ", "WRITE"});
        createPolicy(processorPolicyBuilder, processorPolicyName);
        String groupPolicyName = "test-policy-group-delete";
        String groupResourcePath = "/process-groups/" + parentGroupId;
        PolicyBuilder groupPolicyBuilder = createPolicyBuilder(groupPolicyName, groupResourcePath);
        groupPolicyBuilder.addAllowItem(new String[] {TEST_USER2}, null, new String[] {"READ", "WRITE"});
        createPolicy(groupPolicyBuilder, groupPolicyName);
        verifyDeletePermissionForProcessor(TEST_USER1, TEST_USER1_PASSWORD, processor, false);
        verifyDeletePermissionForProcessor(TEST_USER2, TEST_USER2_PASSWORD, processor, true);
    }

    private void verifyReadPermissionForRootProcessGroup(String user, String password, boolean hasPermission) {
        log.info("Verify {} read root process group permission", user);
        assertWithPooling(() -> {
            String rootGroupId = null;
            try {
                rootGroupId = getNifiClient()
                        .getFlowClient(createNifiRequestConfigForUser(user, password))
                        .getRootGroupId();
            } catch (NiFiClientException e) {
                log.warn("Failed to get root group id from NiFi: {}", e.getMessage());
            }
            if (hasPermission) {
                assertNotNull(rootGroupId, user + " should be able to read root process group");
            } else {
                assertNull(rootGroupId, user + " should not be able to read root process group");
            }
        });
    }

    private void verifyReadAndWriteProcessorPermission(String user,
                                                       String password,
                                                       ProcessorEntity processor,
                                                       boolean canRead,
                                                       boolean canWrite) {
        log.info("Verify {} permissions for processor {}: canRead={}, canWrite={}",
                user, processor.getId(), canRead, canWrite);
        assertWithPooling(() -> {
            performReadOperation(user,  password, processor, canRead);
            performWriteOperation(user,  password, processor, canWrite);
        });
    }

    private void verifyDeletePermissionForProcessor(String user,
                                                    String password,
                                                    ProcessorEntity processor,
                                                    boolean canDelete) {
        log.info("Verify {} delete processor permission", user);
        assertWithPooling(() -> {
            performReadOperation(user,  password, processor, true);
            performDeleteOperation(user,  password, processor, canDelete);
        });
    }
}
