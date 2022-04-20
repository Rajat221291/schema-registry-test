package qe;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.shared.NameUtils;
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
import java.util.concurrent.atomic.AtomicInteger;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.assertEquals;

public class SchemaRegistryP1Tests {
    private static String schemaRegistryURI = "http://10.243.41.92:9092";
    private static String controllerRestURI = "http://10.243.41.62:10080";
    private static String controllerURI = "tcp://10.243.41.62:9090";
    private static String resourceURl;
    private static Client client;
    private static String groupName1 = "grp"+ System.currentTimeMillis();
    private AtomicInteger startIndex = new AtomicInteger(0);


    @BeforeClass
    public static void setUp(){
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        client = ClientBuilder.newClient(clientConfig);
    }

    @AfterClass
    public static void tearDown(){
        // Delete all groups created
        for(int i=0; i < 10; i++){
            resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+"concurrentGrp"+i).toString();
            Response response = client.target(resourceURl).request().delete();
            assertEquals("Delete group status", NO_CONTENT.getStatusCode(), response.getStatus());
        }
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1).toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("Delete group status", NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @Test
    public void verifySchemaRegistryIsPingable(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/ping").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Ping status", OK.getStatusCode(), response.getStatus());

        resourceURl = new StringBuilder(controllerRestURI).append("/v1/scopes/_schemaregistry").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("_schemaregistry scope status", OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void verifyConcurrentCreationOfGroups(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get all Groups status", OK.getStatusCode(), response.getStatus());
        int initialNumberOfGroups = response.readEntity(ListGroupsResponse.class).getGroups().size();

        List<CompletableFuture> futures = new ArrayList();
        int numberOfGroupsToAdd = 10;
        for (int i=0; i < numberOfGroupsToAdd; i++){
            futures.add(CompletableFuture.runAsync(()->createGroup()));
        }
        futures.forEach(CompletableFuture::join);

        response = client.target(resourceURl).request().get();
        assertEquals("Get all Groups status", OK.getStatusCode(), response.getStatus());
        assertEquals((initialNumberOfGroups+numberOfGroupsToAdd),response.readEntity(ListGroupsResponse.class).getGroups().size());

    }

    @Test
    public void verifyConcurrentCreationOfSchemas(){
        CreateGroupRequest createGroupRequest = new CreateGroupRequest();
        createGroupRequest.setGroupName(groupName1);
        GroupProperties mygroup = new GroupProperties().properties(Collections.emptyMap())
                .serializationFormat(new io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat()
                        .serializationFormat(SerializationFormat.SerializationFormatEnum.AVRO))
                .compatibility(ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forward()))
                .allowMultipleTypes(true);
        createGroupRequest.setGroupProperties(mygroup);

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups").toString();
        WebTarget webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createGroupRequest));
        assertEquals("Create Group status", CREATED.getStatusCode(), response.getStatus());

        for(int i=0; i <10; i++){
            String schemaType = "Type"+i;
            String schemaData = "{\"type\":\"record\",\"name\":\"$SCHEMA_TYPE\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
            schemaData = schemaData.replace("$SCHEMA_TYPE",schemaType);
            response = createSchema(groupName1, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                    schemaData, Collections.emptyMap());
            assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        }
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get schemas status", OK.getStatusCode(), response.getStatus());
        SchemaVersionsList schemaVersionsList = response.readEntity(SchemaVersionsList.class);
        assertEquals(10,schemaVersionsList.getSchemas().size());
    }

    @Test
    public void verifyRunningIOWithoutUsingSchemaRegistry(){
        String scope = "scopeNoSchema" + System.currentTimeMillis();
        String stream = "stream";
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        JavaSerializer<String> javaSerializer = new JavaSerializer<>();
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, javaSerializer, EventWriterConfig.builder().build());
        writer.writeEvent("Hello : I am not using schema registry ");

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        EventStreamReader<String> reader = clientFactory.createReader("r1", rg, javaSerializer, ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(10000);
        int numberOfEvents = 0;
        while (event.getEvent() != null || event.isCheckpoint()) {
            System.out.println("event read: " + event.getEvent());
            event = reader.readNextEvent(10000);
            numberOfEvents++;
        }
        assertEquals(1, numberOfEvents);
    }

    private void createGroup(){
        CreateGroupRequest createGroupRequest = new CreateGroupRequest();
        createGroupRequest.setGroupName("concurrentGrp"+ startIndex.getAndIncrement());
        GroupProperties mygroup = new GroupProperties().properties(Collections.emptyMap())
                .serializationFormat(new io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat()
                        .serializationFormat(SerializationFormat.SerializationFormatEnum.AVRO))
                .compatibility(ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forward()))
                .allowMultipleTypes(false);
        createGroupRequest.setGroupProperties(mygroup);
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups").toString();
        WebTarget webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        builder.post(Entity.json(createGroupRequest));
    }

    private Response createSchema(String groupName, String schemaType, SerializationFormat serializationFormat,
                                  String schemaData, Map<String,String> properties){
        SchemaInfo schemaInfo = new SchemaInfo()
                .type(schemaType)
                .serializationFormat(serializationFormat)
                .schemaData(schemaData.getBytes())
                .properties(properties);
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName+"/schemas").toString();
        WebTarget webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(schemaInfo));
        return response;
    }

}
