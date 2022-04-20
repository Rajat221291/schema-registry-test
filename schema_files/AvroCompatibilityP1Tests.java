package qe;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.*;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.avro.impl.AvroSerializerFactory;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.shared.NameUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.*;

public class AvroCompatibilityP1Tests {
    private static String schemaRegistryURI = "http://10.243.41.92:9092";
    private static String controllerURI = "tcp://10.243.41.62:9090";
    private static ClientConfig clientConfig;
    private static SchemaRegistryClient schemaRegistryClient;
    private static String resourceURl;
    private static Client client;
    private WebTarget webTarget;
    private static List<String> groupNames =  new ArrayList<>();

    @BeforeClass
    public static void setUp() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        schemaRegistryClient = SchemaRegistryClientFactory.withDefaultNamespace(SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(schemaRegistryURI)).build());

        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        client = ClientBuilder.newClient(clientConfig);
    }

    @AfterClass
    public static void tearDown(){
        // Delete all groups created
        for(String groupName: groupNames){
            resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName).toString();
            Response response = client.target(resourceURl).request().delete();
            assertEquals("Delete group status for "+groupName, NO_CONTENT.getStatusCode(), response.getStatus());
        }
    }

    @Test
    public void verifyBackwardCompatibilityByRunningIO(){
        final String scope = "scope" + System.currentTimeMillis();
        final String stream = "stream";
        final String groupId = scope+"-"+stream;

        createGroup(groupId, Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.backward()),false);
        groupNames.add(groupId);

        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        Response response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        AvroSchema<Object> schema0 = AvroSchema.from(schemaRegistryClient.getSchemaVersions(groupId,null).get(0).getSchemaInfo());
        EventStreamWriter<Object> writer1 = createWriter(scope,stream,groupId,schema0);
        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(schema0.getSchema());
            record.put("a","writer1-"+i);
            record.put("b", i);
            produce(writer1,record).join();
            System.out.println("Written event: "+record);
        }

        AvroSchema<Object> latestSchema = AvroSchema.from(schemaRegistryClient.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        EventStreamWriter<Object> writer2 = createWriter(scope,stream,groupId,latestSchema);

        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(latestSchema.getSchema());
            record.put("a","writer2-"+i);
            produce(writer2,record).join();
            System.out.println("Written event: "+record);
        }

        assertTrue(schemaRegistryClient.canReadUsing(groupId,latestSchema.getSchemaInfo()));
        EventStreamReader<Object> reader1 = createReader(scope,stream,groupId,latestSchema);
        List<Object> objectList = new ArrayList<>();
        EventRead<Object> event = reader1.readNextEvent(10000);
        while (event.getEvent() != null || event.isCheckpoint()) {
            Object obj = event.getEvent();
            System.out.println("event read:" + obj);
            objectList.add(obj);
            event = reader1.readNextEvent(10000);
        }
        assertEquals(10, objectList.size());
        GenericRecord record0 = new GenericData.Record(latestSchema.getSchema());
        record0.put("a","writer1-"+0);
        assertEquals(record0.toString(), objectList.get(0).toString());
        GenericRecord record9 = new GenericData.Record(latestSchema.getSchema());
        record9.put("a","writer2-"+4);
        assertEquals(record9.toString(), objectList.get(9).toString());

        assertFalse(schemaRegistryClient.canReadUsing(groupId,schema0.getSchemaInfo()));
    }

    @Test
    public void verifyBackwardTransitiveCompatibilityByRunningIO(){
        final String scope = "scope" + System.currentTimeMillis();
        final String stream = "stream";
        final String groupId = scope+"-"+stream;

        createGroup(groupId, Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.backwardTransitive()),false);
        groupNames.add(groupId);

        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"},{\"name\":\"c\",\"type\":\"int\"}]}";
        Response response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        AvroSchema<Object> schema0 = AvroSchema.from(schemaRegistryClient.getSchemaVersions(groupId,null).get(0).getSchemaInfo());
        EventStreamWriter<Object> writer1 = createWriter(scope,stream,groupId,schema0);
        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(schema0.getSchema());
            record.put("a","writer1-"+i);
            record.put("b", i);
            record.put("c", i);
            produce(writer1,record).join();
            System.out.println("Written event: "+record);
        }

        AvroSchema<Object> schema1 = AvroSchema.from(schemaRegistryClient.getSchemaVersions(groupId,null).get(1).getSchemaInfo());
        EventStreamWriter<Object> writer2 = createWriter(scope,stream,groupId,schema1);
        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(schema1.getSchema());
            record.put("a","writer2-"+i);
            record.put("b", i);
            produce(writer2,record).join();
            System.out.println("Written event: "+record);
        }

        AvroSchema<Object> latestSchema = AvroSchema.from(schemaRegistryClient.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        EventStreamWriter<Object> writer3 = createWriter(scope,stream,groupId,latestSchema);

        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(latestSchema.getSchema());
            record.put("a","writer3-"+i);
            produce(writer3,record).join();
            System.out.println("Written event: "+record);
        }

        assertTrue(schemaRegistryClient.canReadUsing(groupId,latestSchema.getSchemaInfo()));
        EventStreamReader<Object> reader1 = createReader(scope,stream,groupId,latestSchema);
        List<Object> objectList = new ArrayList<>();
        EventRead<Object> event = reader1.readNextEvent(10000);
        while (event.getEvent() != null || event.isCheckpoint()) {
            Object obj = event.getEvent();
            System.out.println("event read:" + obj);
            objectList.add(obj);
            event = reader1.readNextEvent(10000);
        }
        assertEquals(15, objectList.size());
        GenericRecord record0 = new GenericData.Record(latestSchema.getSchema());
        record0.put("a","writer1-"+0);
        assertEquals(record0.toString(), objectList.get(0).toString());
        GenericRecord record14 = new GenericData.Record(latestSchema.getSchema());
        record14.put("a","writer3-"+4);
        assertEquals(record14.toString(), objectList.get(14).toString());

        assertFalse(schemaRegistryClient.canReadUsing(groupId,schema0.getSchemaInfo()));
        assertFalse(schemaRegistryClient.canReadUsing(groupId,schema1.getSchemaInfo()));
    }

    @Test
    public void verifyForwardCompatibilityByRunningIO(){
        final String scope = "scope" + System.currentTimeMillis();
        final String stream = "stream";
        final String groupId = scope+"-"+stream;

        createGroup(groupId, Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forward()),false);
        groupNames.add(groupId);

        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        Response response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        AvroSchema<Object> latestSchema = AvroSchema.from(schemaRegistryClient.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        EventStreamWriter<Object> writer = createWriter(scope,stream,groupId,latestSchema);
        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(latestSchema.getSchema());
            record.put("a","writer-"+i);
            record.put("b", i);
            produce(writer,record).join();
            System.out.println("Written event: "+record);
        }
        assertTrue(schemaRegistryClient.canReadUsing(groupId,latestSchema.getSchemaInfo()));
        EventStreamReader<Object> reader1 = createReader(scope,stream,groupId,latestSchema);
        List<Object> objectList = new ArrayList<>();
        EventRead<Object> event = reader1.readNextEvent(10000);
        while (event.getEvent() != null || event.isCheckpoint()) {
            Object obj = event.getEvent();
            System.out.println("event read:" + obj);
            objectList.add(obj);
            event = reader1.readNextEvent(10000);
        }
        assertEquals(5, objectList.size());
        GenericRecord record0 = new GenericData.Record(latestSchema.getSchema());
        record0.put("a","writer-"+0);
        record0.put("b", 0);
        assertEquals(record0.toString(), objectList.get(0).toString());

        AvroSchema<Object> schema0 = AvroSchema.from(schemaRegistryClient.getSchemaVersions(groupId,null).get(0).getSchemaInfo());
        assertTrue(schemaRegistryClient.canReadUsing(groupId,schema0.getSchemaInfo()));
        EventStreamReader<Object> reader2 = createReader(scope,stream,groupId,schema0);
        List<Object> objectList2 = new ArrayList<>();
        EventRead<Object> event2 = reader2.readNextEvent(10000);
        while (event2.getEvent() != null || event2.isCheckpoint()) {
            Object obj = event2.getEvent();
            System.out.println("event read:" + obj);
            objectList2.add(obj);
            event2 = reader2.readNextEvent(10000);
        }
        assertEquals(5, objectList2.size());
        record0 = new GenericData.Record(schema0.getSchema());
        record0.put("a","writer-"+0);
        assertEquals(record0.toString(), objectList2.get(0).toString());
    }

    @Test
    public void verifyForwardTransitiveCompatibilityByRunningIO(){
        final String scope = "scope" + System.currentTimeMillis();
        final String stream = "stream";
        final String groupId = scope+"-"+stream;

        createGroup(groupId, Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forwardTransitive()),false);
        groupNames.add(groupId);

        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        Response response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"},{\"name\":\"c\",\"type\":\"int\"}]}";
        response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        AvroSchema<Object> latestSchema = AvroSchema.from(schemaRegistryClient.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        EventStreamWriter<Object> writer = createWriter(scope,stream,groupId,latestSchema);
        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(latestSchema.getSchema());
            record.put("a","writer-"+i);
            record.put("b", i);
            record.put("c", i);
            produce(writer,record).join();
            System.out.println("Written event: "+record);
        }
        assertTrue(schemaRegistryClient.canReadUsing(groupId,latestSchema.getSchemaInfo()));
        EventStreamReader<Object> reader1 = createReader(scope,stream,groupId,latestSchema);
        List<Object> objectList = new ArrayList<>();
        EventRead<Object> event = reader1.readNextEvent(10000);
        while (event.getEvent() != null || event.isCheckpoint()) {
            Object obj = event.getEvent();
            System.out.println("event read:" + obj);
            objectList.add(obj);
            event = reader1.readNextEvent(10000);
        }
        assertEquals(5, objectList.size());
        GenericRecord record0 = new GenericData.Record(latestSchema.getSchema());
        record0.put("a","writer-"+0);
        record0.put("b", 0);
        record0.put("c", 0);
        assertEquals(record0.toString(), objectList.get(0).toString());

        AvroSchema<Object> schema1 = AvroSchema.from(schemaRegistryClient.getSchemaVersions(groupId,null).get(1).getSchemaInfo());
        assertTrue(schemaRegistryClient.canReadUsing(groupId,schema1.getSchemaInfo()));
        EventStreamReader<Object> reader2 = createReader(scope,stream,groupId,schema1);
        List<Object> objectList2 = new ArrayList<>();
        EventRead<Object> event2 = reader2.readNextEvent(10000);
        while (event2.getEvent() != null || event2.isCheckpoint()) {
            Object obj = event2.getEvent();
            System.out.println("event read:" + obj);
            objectList2.add(obj);
            event2 = reader2.readNextEvent(10000);
        }
        assertEquals(5, objectList2.size());
        record0 = new GenericData.Record(schema1.getSchema());
        record0.put("a","writer-"+0);
        record0.put("b", 0);
        assertEquals(record0.toString(), objectList2.get(0).toString());

        AvroSchema<Object> schema0 = AvroSchema.from(schemaRegistryClient.getSchemaVersions(groupId,null).get(0).getSchemaInfo());
        assertTrue(schemaRegistryClient.canReadUsing(groupId,schema0.getSchemaInfo()));
        EventStreamReader<Object> reader3 = createReader(scope,stream,groupId,schema0);
        List<Object> objectList3 = new ArrayList<>();
        EventRead<Object> event3 = reader3.readNextEvent(10000);
        while (event3.getEvent() != null || event3.isCheckpoint()) {
            Object obj = event3.getEvent();
            System.out.println("event read:" + obj);
            objectList3.add(obj);
            event3 = reader3.readNextEvent(10000);
        }
        assertEquals(5, objectList3.size());
        record0 = new GenericData.Record(schema0.getSchema());
        record0.put("a","writer-"+0);
        assertEquals(record0.toString(), objectList3.get(0).toString());
    }

    @Test
    public void verifyFullCompatibilityByRunningIO(){
        final String scope = "scope" + System.currentTimeMillis();
        final String stream = "stream";
        final String groupId = scope+"-"+stream;

        createGroup(groupId, Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.full()),false);
        groupNames.add(groupId);

        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"string\",\"default\":\"green\"}]}";
        Response response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"c\",\"type\":\"string\",\"default\":\"red\"}]}";
        response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        AvroSchema<Object> schema0 = AvroSchema.from(schemaRegistryClient.getSchemaVersions(groupId,null).get(0).getSchemaInfo());
        EventStreamWriter<Object> writer1 = createWriter(scope,stream,groupId,schema0);
        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(schema0.getSchema());
            record.put("a","writer1-"+i);
            record.put("b", "bb"+i);
            produce(writer1,record).join();
            System.out.println("Written event: "+record);
        }

        AvroSchema<Object> latestSchema = AvroSchema.from(schemaRegistryClient.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        EventStreamWriter<Object> writer2 = createWriter(scope,stream,groupId,latestSchema);

        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(latestSchema.getSchema());
            record.put("a","writer2-"+i);
            record.put("c", "cc"+i);
            produce(writer2,record).join();
            System.out.println("Written event: "+record);
        }

        assertTrue(schemaRegistryClient.canReadUsing(groupId,latestSchema.getSchemaInfo()));
        EventStreamReader<Object> reader1 = createReader(scope,stream,groupId,latestSchema);
        List<Object> objectList = new ArrayList<>();
        EventRead<Object> event = reader1.readNextEvent(10000);
        while (event.getEvent() != null || event.isCheckpoint()) {
            Object obj = event.getEvent();
            System.out.println("event read:" + obj);
            objectList.add(obj);
            event = reader1.readNextEvent(10000);
        }
        assertEquals(10, objectList.size());
        GenericRecord record0 = new GenericData.Record(latestSchema.getSchema());
        record0.put("a","writer1-"+0);
        record0.put("c","red");
        assertEquals(record0.toString(), objectList.get(0).toString());
        GenericRecord record9 = new GenericData.Record(latestSchema.getSchema());
        record9.put("a","writer2-"+4);
        record9.put("c","cc"+4);
        assertEquals(record9.toString(), objectList.get(9).toString());

        assertTrue(schemaRegistryClient.canReadUsing(groupId,schema0.getSchemaInfo()));
        EventStreamReader<Object> reader2 = createReader(scope,stream,groupId,schema0);
        List<Object> objectList2 = new ArrayList<>();
        EventRead<Object> event2 = reader2.readNextEvent(10000);
        while (event2.getEvent() != null || event2.isCheckpoint()) {
            Object obj = event2.getEvent();
            System.out.println("event read:" + obj);
            objectList2.add(obj);
            event2 = reader2.readNextEvent(10000);
        }
        assertEquals(10, objectList2.size());
        record0 = new GenericData.Record(schema0.getSchema());
        record0.put("a","writer1-"+0);
        record0.put("b","bb"+0);
        assertEquals(record0.toString(), objectList2.get(0).toString());
        record9 = new GenericData.Record(schema0.getSchema());
        record9.put("a","writer2-"+4);
        record9.put("b","green");
        assertEquals(record9.toString(), objectList2.get(9).toString());
    }

    private EventStreamReader<Object> createReader(String scope, String stream, String groupId, AvroSchema<Object> schema) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .groupId(groupId)
                .registryClient(schemaRegistryClient)
                .build();

        Serializer<Object> deserializer = AvroSerializerFactory.genericDeserializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
    }

    private EventStreamWriter<Object> createWriter(String scope, String stream, String groupId, AvroSchema<Object> schema) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .groupId(groupId)
                .registryClient(schemaRegistryClient)
                .build();

        Serializer<Object> serializer = AvroSerializerFactory.serializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private CompletableFuture<Void> produce(EventStreamWriter<Object> writer, GenericRecord record) {
        return writer.writeEvent(record);
    }

    private void createGroup(String groupName, Map<String,String> properties, SerializationFormat.SerializationFormatEnum serializationFormatEnum,
                             Compatibility compatibility, boolean allowMultipleTypes){
        CreateGroupRequest createGroupRequest = new CreateGroupRequest();
        createGroupRequest.setGroupName(groupName);
        GroupProperties mygroup = new GroupProperties().properties(properties)
                .serializationFormat(new io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat()
                        .serializationFormat(serializationFormatEnum))
                .compatibility(compatibility)
                .allowMultipleTypes(allowMultipleTypes);
        createGroupRequest.setGroupProperties(mygroup);

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createGroupRequest));
        assertEquals("Create Group status for "+groupName, CREATED.getStatusCode(), response.getStatus());
    }

    private Response createSchema(String groupName, String schemaType, SerializationFormat serializationFormat,
                                  String schemaData, Map<String,String> properties){
        SchemaInfo schemaInfo = new SchemaInfo()
                .type(schemaType)
                .serializationFormat(serializationFormat)
                .schemaData(schemaData.getBytes())
                .properties(properties);
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName+"/schemas").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(schemaInfo));
        return response;
    }



}
