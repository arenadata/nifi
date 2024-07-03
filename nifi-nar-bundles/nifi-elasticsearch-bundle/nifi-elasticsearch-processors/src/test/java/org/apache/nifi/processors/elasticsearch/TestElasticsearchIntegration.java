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
package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.processors.elasticsearch.docker.ElasticsearchDockerInitializer;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Before;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestElasticsearchIntegration extends ElasticsearchDockerInitializer {
    private TestRunner runner;
    private static byte[] docExample;
    private String esUrl = "http://127.0.0.1:" + elasticsearchDockerComposeContainer.getServicePort("es01",9200);

    @Before
    public void once() throws IOException {
        docExample = TestPutElasticsearchHttp.getDocExample();
    }

    @After
    public void teardown() {
        runner = null;
    }

    @BeforeClass
    public static void startElasticSearchDockerContainer(){
        elasticsearchDockerComposeContainer.start();
    }
    @AfterClass
    public static void stopElasticSearchDockerContainer(){
        elasticsearchDockerComposeContainer.stop();
        elasticsearchDockerComposeContainer.close();
    }

    @Test
    public void testPutElasticSearchHttpRecordBasic() throws InitializationException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecord());
        MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField("id", RecordFieldType.INT);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("code", RecordFieldType.INT);
        recordReader.addSchemaField("date", RecordFieldType.DATE);
        recordReader.addSchemaField("time", RecordFieldType.TIME);
        recordReader.addSchemaField("ts",RecordFieldType.TIMESTAMP);
        recordReader.addSchemaField("routing",RecordFieldType.STRING);

        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutElasticsearchHttpRecord.RECORD_READER, "reader");
        runner.setProperty(PutElasticsearchHttpRecord.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.ROUTING_RECORD_PATH,"/routing");
        runner.assertValid();
        Date date = new Date(new java.util.Date().getTime());
        Time time = new Time(System.currentTimeMillis());
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        recordReader.addRecord(0,"rec",100,date,time,timestamp,"user_0");

        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(new byte[0]);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertNotNull(provEvents);
        assertEquals(1, provEvents.size());
        assertEquals(ProvenanceEventType.SEND, provEvents.get(0).getEventType());
    }

    @Test
    public void testPutElasticSearchHttpRecordBatch() throws IOException, InitializationException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecord());
        MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField("id", RecordFieldType.INT);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("code", RecordFieldType.INT);
        recordReader.addSchemaField("date", RecordFieldType.DATE);
        recordReader.addSchemaField("time", RecordFieldType.TIME);
        recordReader.addSchemaField("ts",RecordFieldType.TIMESTAMP);
        recordReader.addSchemaField("routing",RecordFieldType.STRING);

        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutElasticsearchHttpRecord.RECORD_READER, "reader");
        runner.setProperty(PutElasticsearchHttpRecord.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.ROUTING_RECORD_PATH,"/routing");
        runner.assertValid();


        for (int i = 1; i < 101; i++) {
            Date date = new Date(new java.util.Date().getTime());
            Time time = new Time(System.currentTimeMillis());
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            recordReader.addRecord(i,"rec"+i,100+i,date,time,timestamp,"user_"+i);
            String newStrId = Integer.toString(i);
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});
            runner.run(1,true,true);
        }
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 100);
    }
    @Test
    public void testPutElasticSearchHttpBasic() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner  runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.ROUTING_ATTRIBUTE, "doc_routing");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
            put("doc_routing", "new_user");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testPutElasticSearchHttpBatch() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner  runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        byte[] docExample = TestPutElasticsearchHttp.getDocExample();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.ROUTING_ATTRIBUTE, "doc_routing");

        runner.assertValid();

        for (int i = 0; i < 100; i++) {
            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            int finalI = i;
            runner.enqueue(docExample, new HashMap<String, String>() {{
                put("doc_id", newStrId);
                put("doc_routing", "new_user" + finalI);
            }});
        }
        runner.run();
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 100);
    }
    @Test
    public void testFetchElasticsearchBasic() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        testPutElasticSearchHttpBasic();
        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchElasticsearchBatch() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttp());
        testPutElasticSearchHttpBatch();
        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, esUrl);
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.assertValid();

        for (int i = 0; i < 100; i++) {
            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            runner.enqueue(docExample, new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});
        }
        runner.run(100);
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 100);
    }

}
