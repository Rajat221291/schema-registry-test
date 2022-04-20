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
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import org.apache.avro.AvroTypeException;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroCompatibilityBasicTests {
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
    public void verifyAllowAnyCompatibilityByRunningIO(){
        final String scope = "scope" + System.currentTimeMillis();
        final String stream = "stream";
        final String groupId = scope+"-"+stream+"Raj";

        createGroup(groupId, Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.allowAny()),false);
        groupNames.add(groupId);

        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        Response response = createSchema(groupId, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"}]}";
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
            record.put("a","writerA-"+i);
            produce(writer1,record).join();
            System.out.println("Written event: "+record);
        }

        AvroSchema<Object> latestSchema = AvroSchema.from(schemaRegistryClient.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        EventStreamWriter<Object> writer2 = createWriter(scope,stream,groupId,latestSchema);

        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(latestSchema.getSchema());
            record.put("b","writerB-"+i);
            produce(writer2,record).join();
            System.out.println("Written event: "+record);
        }

        boolean avroExceptionFound = false;
        EventStreamReader<Object> reader1 = createReader(scope,stream,"r1",groupId,schema0);
        List<Object> objectList = new ArrayList<>();
        EventRead<Object> event = reader1.readNextEvent(10000);
        while (event.getEvent() != null || event.isCheckpoint()) {
            Object obj = event.getEvent();
            System.out.println("event read:" + obj);
            objectList.add(obj);
            try {
                event = reader1.readNextEvent(10000);
            } catch(AvroTypeException e){
                avroExceptionFound = true;
                break;
            }
        }
        assertEquals(5, objectList.size());
        assertTrue(avroExceptionFound);
        GenericRecord record = new GenericData.Record(schema0.getSchema());
        record.put("a","writerA-"+0);
        assertEquals(record.toString(), objectList.get(0).toString());
    }

    @Test
    public void verifyDenyAllCompatibilityByRunningIO(){
        final String scope = "scope" + System.currentTimeMillis();
        final String stream = "stream";
        final String groupId = scope+"-"+stream;

        createGroup(groupId, Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.denyAll()),false);
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
        assertEquals("addSchemasToGroup status", CONFLICT.getStatusCode(), response.getStatus());

        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        AvroSchema<Object> latestSchema = AvroSchema.from(schemaRegistryClient.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        EventStreamWriter<Object> writer = createWriter(scope,stream,groupId,latestSchema);

        for(int i=0; i <5; i++){
            GenericRecord record = new GenericData.Record(latestSchema.getSchema());
            record.put("a","writerA-"+i);
            produce(writer,record).join();
            System.out.println("Written event: "+record);
        }

        EventStreamReader<Object> reader = createReader(scope,stream,"r1",groupId,latestSchema);
        List<Object> objectList = new ArrayList<>();
        EventRead<Object> event = reader.readNextEvent(10000);
        while (event.getEvent() != null || event.isCheckpoint()) {
            Object obj = event.getEvent();
            System.out.println("event read:" + obj);
            objectList.add(obj);
            event = reader.readNextEvent(10000);
        }
        assertEquals(5, objectList.size());
        GenericRecord record = new GenericData.Record(latestSchema.getSchema());
        record.put("a","writerA-"+4);
        assertEquals(record.toString(), objectList.get(4).toString());
    }

    private EventStreamReader<Object> createReader(String scope, String stream, String readerId, String groupId, AvroSchema<Object> schema) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .groupId(groupId)
                .registryClient(schemaRegistryClient)
                .build();

        Serializer<Object> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String rg = "rg" + scope + stream;
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        return clientFactory.createReader(readerId, rg, deserializer, ReaderConfig.builder().build());
    }

    private EventStreamWriter<Object> createWriter(String scope, String stream, String groupId, AvroSchema<Object> schema) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .groupId(groupId)
                .registryClient(schemaRegistryClient)
                .build();

        Serializer<Object> serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
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
