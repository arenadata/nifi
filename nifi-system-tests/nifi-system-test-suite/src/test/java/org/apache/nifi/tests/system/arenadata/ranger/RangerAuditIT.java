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
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Feature("Ranger Audit")
public class RangerAuditIT extends RangerTestBase {

    @Test
    public void testReadEventsAudited() throws Exception {
        ProcessorEntity processor = createProcessor();
        String policyName = "test-audit-read";
        String resourcePath = "/processors/" + processor.getId();
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(new String[]{TEST_USER1}, null, new String[]{"READ"});
        createPolicy(builder, policyName);
        performReadOperationWithWait(TEST_USER1, TEST_USER1_PASSWORD, processor, true);
        performReadOperationWithWait(TEST_USER2, TEST_USER2_PASSWORD, processor, false);
        verifyAuditEventInSolr(TEST_USER1, resourcePath, "READ", 1);
        verifyAuditEventInSolr(TEST_USER2, resourcePath, "READ", 0);
    }

    @Test
    public void testWriteEventsAudited() throws Exception {
        ProcessorEntity processor = createProcessor();
        String policyName = "test-audit-write";
        String resourcePath = "/processors/" + processor.getId();
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(new String[]{TEST_USER1}, null, new String[]{"READ", "WRITE"});
        builder.addAllowItem(new String[]{TEST_USER2}, null, new String[]{"READ"});
        createPolicy(builder, policyName);
        performWriteOperationWithWait(TEST_USER1, TEST_USER1_PASSWORD, processor, true);
        performWriteOperationWithWait(TEST_USER2, TEST_USER2_PASSWORD, processor, false);
        verifyAuditEventInSolr(TEST_USER1, resourcePath, "WRITE", 1);
        verifyAuditEventInSolr(TEST_USER2, resourcePath, "WRITE", 0);
    }

    @Test
    public void testDeleteEventsAudited() throws Exception {
        ProcessorEntity processor = createProcessor();
        String parentGroupId = processor.getComponent().getParentGroupId();
        String processorPolicyName = "test-audit-delete";
        String processorResourcePath = "/processors/" + processor.getId();
        PolicyBuilder processorPolicyBuilder = createPolicyBuilder(processorPolicyName, processorResourcePath);
        processorPolicyBuilder.addAllowItem(new String[] {TEST_USER1}, null, new String[] {"READ", "WRITE"});
        processorPolicyBuilder.addAllowItem(new String[] {TEST_USER2}, null, new String[] {"READ"});
        createPolicy(processorPolicyBuilder, processorPolicyName);
        String groupPolicyName = "test-policy-group-delete";
        String groupResourcePath = "/process-groups/" + parentGroupId;
        PolicyBuilder groupPolicyBuilder = createPolicyBuilder(groupPolicyName, groupResourcePath);
        groupPolicyBuilder.addAllowItem(new String[] {TEST_USER1}, null, new String[] {"READ", "WRITE"});
        groupPolicyBuilder.addAllowItem(new String[] {TEST_USER2}, null, new String[] {"READ"});
        createPolicy(groupPolicyBuilder, groupPolicyName);
        performDeleteOperationWithWait(TEST_USER2, TEST_USER2_PASSWORD, processor, false);
        performDeleteOperationWithWait(TEST_USER1, TEST_USER1_PASSWORD, processor, true);
        verifyAuditEventInSolr(TEST_USER2, processorResourcePath, "WRITE", 0);
        verifyAuditEventInSolr(TEST_USER1, processorResourcePath, "WRITE", 1);
    }

    private static void verifyAuditEventInSolr(String user, String resourcePath, String action, int result) {
        List<Map<String, Object>> events = solrService.waitForAuditEvents(user, resourcePath, action, result);
        assertFalse(events.isEmpty(), "Should have at least one audit event with specified parameters");
        Map<String, Object> event = events.getFirst();
        log.info("Verify audit event: {}", event);
        assertEquals(user, event.get("reqUser"), "Event user should be " + user);
        assertEquals(resourcePath, event.get("resource"), "Event resource should match");
        assertEquals(action, event.get("action"), "Event action should be " + action);
        assertEquals(result, event.get("result"), "Event result should be " + result);
        assertNotNull(event.get("evtTime"), "Event should have evtTime");
        assertNotNull(event.get("repoName"), "Event should have repoName (service name)");
        assertNotNull(event.get("repoType"), "Event should have repoType");
    }
}
