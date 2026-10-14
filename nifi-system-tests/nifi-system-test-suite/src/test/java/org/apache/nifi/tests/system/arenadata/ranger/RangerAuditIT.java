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
import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.tests.system.arenadata.ranger.RangerTestBase.AccessType.READ;
import static org.apache.nifi.tests.system.arenadata.ranger.RangerTestBase.AccessType.WRITE;
import static org.apache.nifi.tests.system.arenadata.ranger.RangerTestBase.AuditResult.ALLOWED;
import static org.apache.nifi.tests.system.arenadata.ranger.RangerTestBase.AuditResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@Feature("Ranger Audit")
public class RangerAuditIT extends RangerTestBase {

    @Test
    public void testReadEventsAudited() throws Exception {
        ProcessorEntity processor = createProcessor();
        String policyName = "test-audit-read";
        String resourcePath = "/processors/" + processor.getId();
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(new String[]{TEST_USER1}, null, new AccessType[]{READ});
        createPolicy(builder, policyName);
        performReadOperationWithWait(TEST_USER1, TEST_USER1_PASSWORD, processor, true);
        performReadOperationWithWait(TEST_USER2, TEST_USER2_PASSWORD, processor, false);
        verifyAuditEventInSolr(TEST_USER1, resourcePath, READ, ALLOWED);
        verifyAuditEventInSolr(TEST_USER2, resourcePath, READ, DENIED);
    }

    @Test
    public void testWriteEventsAudited() throws Exception {
        ProcessorEntity processor = createProcessor();
        String policyName = "test-audit-write";
        String resourcePath = "/processors/" + processor.getId();
        PolicyBuilder builder = createPolicyBuilder(policyName, resourcePath);
        builder.addAllowItem(new String[]{TEST_USER1}, null, new AccessType[]{READ, WRITE});
        builder.addAllowItem(new String[]{TEST_USER2}, null, new AccessType[]{READ});
        createPolicy(builder, policyName);
        performWriteOperationWithWait(TEST_USER1, TEST_USER1_PASSWORD, processor, true);
        performWriteOperationWithWait(TEST_USER2, TEST_USER2_PASSWORD, processor, false);
        verifyAuditEventInSolr(TEST_USER1, resourcePath, WRITE, ALLOWED);
        verifyAuditEventInSolr(TEST_USER2, resourcePath, WRITE, DENIED);
    }

    @Test
    public void testDeleteEventsAudited() throws Exception {
        ProcessorEntity processor = createProcessor();
        String parentGroupId = processor.getComponent().getParentGroupId();
        String processorPolicyName = "test-audit-delete";
        String processorResourcePath = "/processors/" + processor.getId();
        PolicyBuilder processorPolicyBuilder = createPolicyBuilder(processorPolicyName, processorResourcePath);
        processorPolicyBuilder.addAllowItem(new String[] {TEST_USER1}, null, new AccessType[] {READ, WRITE});
        processorPolicyBuilder.addAllowItem(new String[] {TEST_USER2}, null, new AccessType[] {READ});
        createPolicy(processorPolicyBuilder, processorPolicyName);
        String groupPolicyName = "test-policy-group-delete";
        String groupResourcePath = "/process-groups/" + parentGroupId;
        PolicyBuilder groupPolicyBuilder = createPolicyBuilder(groupPolicyName, groupResourcePath);
        groupPolicyBuilder.addAllowItem(new String[] {TEST_USER1}, null, new AccessType[] {READ, WRITE});
        groupPolicyBuilder.addAllowItem(new String[] {TEST_USER2}, null, new AccessType[] {READ});
        createPolicy(groupPolicyBuilder, groupPolicyName);
        performDeleteOperationWithWait(TEST_USER2, TEST_USER2_PASSWORD, processor, false);
        performDeleteOperationWithWait(TEST_USER1, TEST_USER1_PASSWORD, processor, true);
        verifyAuditEventInSolr(TEST_USER2, processorResourcePath, WRITE, DENIED);
        verifyAuditEventInSolr(TEST_USER1, processorResourcePath, WRITE, ALLOWED);
    }

    @Step("Verify audit event in Solr")
    private static void verifyAuditEventInSolr(String user,
                                               String resourcePath,
                                               AccessType action,
                                               AuditResult result) {
        List<Map<String, Object>> events =
                solrService.waitForAuditEvents(user, resourcePath, action.toString(), result.getValue());
        assertFalse(events.isEmpty(), "Should have at least one audit event with specified parameters");
        Map<String, Object> event = events.getFirst();
        log.info("Verify audit event: {}", event);
        assertEquals(user, event.get("reqUser"), "Event user should be " + user);
        assertEquals(resourcePath, event.get("resource"), "Event resource should match");
        assertEquals(action.toString(), event.get("action"), "Event action should be " + action);
        assertEquals(result.getValue(), event.get("result"), "Event result should be "
                + "%s(%d)".formatted(result, result.getValue()));
        assertNotNull(event.get("evtTime"), "Event should have evtTime");
        assertNotNull(event.get("repoName"), "Event should have repoName (service name)");
        assertNotNull(event.get("repoType"), "Event should have repoType");
    }
}
