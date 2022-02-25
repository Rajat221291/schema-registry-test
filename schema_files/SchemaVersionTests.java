package qe;

import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.assertEquals;

public class SchemaVersionTests {
    private static String schemaRegistryURI = "http://10.243.41.97:9092";
    private static String resourceURl;
    private static Client client;
    private WebTarget webTarget;
    private static List<String> groupNames =  new ArrayList<>();

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
        for(String groupName: groupNames){
            resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName).toString();
            Response response = client.target(resourceURl).request().delete();
            assertEquals("Delete group status for "+groupName, NO_CONTENT.getStatusCode(), response.getStatus());
        }
    }

    @Test
    public void verifySupportForMultipleTypesOfSchema(){
        // Support for multiple types of schemas
        String groupName = "grpMultiFormatSupport";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.ANY,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.allowAny()),true);
        groupNames.add(groupName);

        String schemaType = "Type1";
        String schemaData = "{\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaType = "Json1";
        schemaData = "{\"type\": \"object\",\"id\": \"id1\",\"properties\": {\"age\":{\"type\": \"integer\"}}}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Json),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        VersionInfo versionInfo = response.readEntity(VersionInfo.class);
        assertEquals(schemaType,versionInfo.getType());
        assertEquals(new Integer(0),versionInfo.getVersion());
        assertEquals(new Integer(1),versionInfo.getId());

        schemaType = "Custom1";
        schemaData = "INT,string";
        SerializationFormat serializationFormat = new SerializationFormat().serializationFormat(SerializationFormat.SerializationFormatEnum.CUSTOM);
        serializationFormat.setFullTypeName("custom1");
        response = createSchema(groupName, schemaType, serializationFormat,
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        versionInfo = response.readEntity(VersionInfo.class);
        assertEquals(schemaType,versionInfo.getType());
        assertEquals(new Integer(0),versionInfo.getVersion());
        assertEquals(new Integer(2),versionInfo.getId());

        schemaType = "Type2";
        schemaData = "{\"type\":\"record\",\"name\":\"Type2\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
    }

    @Test
    public void verifySchemaEvolution(){
        // Allow evolution of schema: Verify by changing the schema used in a stream.
        testCompatibilityAllowAny();
        testCompatibilityDenyAll();
        testCompatibilityBackward();
        testCompatibilityForward();
        testCompatibilityBackwardTransitive();
        testCompatibilityForwardTransitive();
        testCompatibilityFull();
    }

    private void testCompatibilityAllowAny(){
        String groupName = "grpAllowAny";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.allowAny()),false);
        groupNames.add(groupName);
        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"z\",\"type\":\"string\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        VersionInfo versionInfo = response.readEntity(VersionInfo.class);
        assertEquals(schemaType,versionInfo.getType());
        assertEquals(new Integer(1),versionInfo.getVersion());
        assertEquals(new Integer(1),versionInfo.getId());
        System.out.println("test Compatibility AllowAny successful");
    }

    private void testCompatibilityDenyAll(){
        String groupName = "grpDenyAll";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.denyAll()),false);
        groupNames.add(groupName);
        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CONFLICT.getStatusCode(), response.getStatus());
        System.out.println("test Compatibility DenyAll successful");
    }

    private void testCompatibilityBackward(){
        String groupName = "grpBackward";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.backward()),false);
        groupNames.add(groupName);
        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"},{\"name\":\"c\",\"type\":\"int\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CONFLICT.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        System.out.println("test Compatibility Backward successful");
    }

    private void testCompatibilityBackwardTransitive(){
        String groupName = "grpBackwardTransitive";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.backwardTransitive()),false);
        groupNames.add(groupName);
        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"},{\"name\":\"c\",\"type\":\"int\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CONFLICT.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        System.out.println("test Compatibility BackwardTransitive successful");
    }

    private void testCompatibilityForward(){
        String groupName = "grpForward";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forward()),false);
        groupNames.add(groupName);
        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CONFLICT.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"},{\"name\":\"c\",\"type\":\"int\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        System.out.println("test Compatibility Forward successful");
    }

    private void testCompatibilityForwardTransitive(){
        String groupName = "grpForwardTransitive";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forwardTransitive()),false);
        groupNames.add(groupName);
        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CONFLICT.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\"},{\"name\":\"c\",\"type\":\"int\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        System.out.println("test Compatibility ForwardTransitive successful");
    }

    private void testCompatibilityFull(){
        String groupName = "grpAllowFull";
        createGroup(groupName,Collections.emptyMap(),SerializationFormat.SerializationFormatEnum.AVRO,
                ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.full()),false);
        groupNames.add(groupName);
        String schemaType = "io.pravega.schemaregistry.test.integrationtest.generated.Type1";
        String schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        Response response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"string\",\"default\":\"green\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        schemaData = "{\"namespace\":\"io.pravega.schemaregistry.test.integrationtest.generated\",\"type\":\"record\",\"name\":\"Type1\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"c\",\"type\":\"string\",\"default\":\"red\"}]}";
        response = createSchema(groupName, schemaType, ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro),
                schemaData, Collections.emptyMap());
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());

        VersionInfo versionInfo = response.readEntity(VersionInfo.class);
        assertEquals(schemaType,versionInfo.getType());
        assertEquals(new Integer(2),versionInfo.getVersion());
        assertEquals(new Integer(2),versionInfo.getId());
        System.out.println("test Compatibility Full successful");
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
