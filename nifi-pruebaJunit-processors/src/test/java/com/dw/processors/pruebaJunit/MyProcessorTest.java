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
package com.dw.processors.pruebaJunit;

import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static junit.framework.TestCase.assertTrue;


public class MyProcessorTest {

    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(MyProcessor.class);
    }

    @Test
    public void testProcessor() throws InitializationException, IOException {
        final String schema = new String(Files.readAllBytes(Paths.get("src/test/resources/charly.json")), "UTF-8");
        final CSVReader csvReader = new CSVReader();

        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_TEXT, schema);
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "false");
        runner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(csvReader);

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("writer", csvWriter);
        runner.setProperty(csvWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(csvWriter, CSVUtils.INCLUDE_HEADER_LINE, "false");
        runner.enableControllerService(csvWriter);

        runner.setProperty(MyProcessor.MY_PROPERTY, "hola mundo charly");
        runner.setProperty(MyProcessor.RECORD_READER, "reader");
        runner.setProperty(MyProcessor.RECORD_WRITER, "writer");

        String content = "744320,5491137726450,3457845455454\n744320,5491137726450,3457845455454\n";
        runner.enqueue(content);
        content = "722310,5491137726450,3457845455454\n722310,5491137726450,3457845455454\n";
        runner.enqueue(content);

        runner.run(3);

        // If you need to read or do additional tests on results you can access the content
        //List<MockFlowFile> results = runner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS);
        //assertTrue("2 match", results.size() == 2);
        runner.assertTransferCount(MyProcessor.REL_ERRORS, 1);
        runner.assertTransferCount(MyProcessor.REL_SUCCESS, 1);
        runner.assertQueueEmpty();
        //runner.assertAllFlowFilesTransferred(MyProcessor.REL_FAILURE, 1);
        //runner.assertAllFlowFilesTransferred(MyProcessor.REL_SUCCESS, 1);
        //runner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS).get(0).assertContentEquals(content);
    }

}
