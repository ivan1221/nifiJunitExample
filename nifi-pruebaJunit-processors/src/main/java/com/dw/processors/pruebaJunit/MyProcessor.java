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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("MY_RELATIONSHIP")
            .description("Example relationship")
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-group-mod-reader")
            .displayName("Record Reader")
            .description("A record reader to use for reading the records.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully inserted into cache will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("REL_FAILURE")
            .description("Any FlowFile that is successfully inserted into cache will be routed to this relationship")
            .build();

    public static final Relationship REL_ERRORS = new Relationship.Builder()
            .name("REL_ERRORS")
            .description("Any FlowFile that is successfully inserted into cache will be routed to this relationship")
            .build();

    public static final Relationship REL_DISCARD = new Relationship.Builder()
            .name("REL_DISCARD")
            .description("Any FlowFile that is successfully inserted into cache will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MY_PROPERTY);
        descriptors.add(RECORD_READER);
        descriptors.add(RECORD_WRITER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_DISCARD);
        relationships.add(REL_ERRORS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        // TODO implement

        final RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final ComponentLog logger = getLogger();
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        final FlowFile outFlowFile = session.create(flowFile);

        try {
            session.read(flowFile, in -> {
                try (RecordReader reader = factory.createRecordReader(flowFile, in, logger)) {

                    final RecordSchema schema = writerFactory.getSchema(null, reader.getSchema());

                    if (!schema.getField("imsi").isPresent()) {
                        session.transfer(outFlowFile, REL_FAILURE);
                        return;
                    }

                    Record record;
                    final OutputStream out = session.write(outFlowFile);
                    int error = 0;
                    try (final RecordSetWriter recordSetWriter = writerFactory.createWriter(getLogger(), schema, out)) {
                        recordSetWriter.beginRecordSet();

                        while ((record = reader.nextRecord()) != null) {
                            if (record.getAsString("imsi").equals("722310")) {
                                error++;
                            }
                            recordSetWriter.write(record);
                        }

                        recordSetWriter.finishRecordSet();
                    }

                    if (error > 0) {
                        session.transfer(outFlowFile, REL_ERRORS);
                    } else {
                        session.transfer(outFlowFile, REL_SUCCESS);
                    }

                } catch (final SchemaNotFoundException | MalformedRecordException e) {
                    getLogger().error("SchemaNotFound or MalformedRecordException \n" + e.toString());
                    throw new ProcessException(e);
                } catch (final Exception e) {
                    getLogger().error(e.toString());
                    throw new ProcessException(e);
                }
            });
        } catch (final Exception e) {
            session.remove(outFlowFile);
            session.transfer(flowFile, REL_DISCARD);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e});
            return;
        }

        session.remove(flowFile);
    }
}
