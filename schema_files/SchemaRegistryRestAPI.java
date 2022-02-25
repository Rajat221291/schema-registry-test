package qe;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SchemaRegistryRestAPI {
    private static String schemaRegistryURI = "http://10.243.41.97:9092";
    private static String resourceURl;
    private static Client client;
    private WebTarget webTarget;
    private String groupName1 = "a1";
    private boolean namespacePresent = true;
    private String schemaType1FullName = "io.pravega.schemaregistry.test.integrationtest.generated"+"."+"Type1";
    private String schemaType1 = namespacePresent ? schemaType1FullName: "Type1";


    @BeforeClass
    public static void setUp(){
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        client = ClientBuilder.newClient(clientConfig);
    }

    @Test
    public void test01_createGroup(){
        CreateGroupRequest createGroupRequest = new CreateGroupRequest();
        createGroupRequest.setGroupName(groupName1);
        GroupProperties mygroup = new GroupProperties().properties(Collections.emptyMap())
                .serializationFormat(new io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat()
                        .serializationFormat(SerializationFormat.SerializationFormatEnum.AVRO))
                .compatibility(ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forward()))
                .allowMultipleTypes(false);
        createGroupRequest.setGroupProperties(mygroup);

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(createGroupRequest));
        assertEquals("Create Group status", CREATED.getStatusCode(), response.getStatus());
    }

    @Test
    public void test02_listAllGroups(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get all Groups status", OK.getStatusCode(), response.getStatus());
        //System.out.println(response.readEntity(String.class));
        assertTrue(response.readEntity(ListGroupsResponse.class).getGroups().containsKey(groupName1));

    }

    @Test
    public void test03_fetchDetailsOfGroup(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1).toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("fetchDetailsOfGroup status", OK.getStatusCode(), response.getStatus());
        assertEquals("Avro",response.readEntity(GroupProperties.class).getSerializationFormat().getSerializationFormat().toString());

    }

    @Test
    public void test04_updateSchemaCompatibility(){
        UpdateCompatibilityRequest updateCompatibilityRequest = new UpdateCompatibilityRequest()
                .compatibility(ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.backward()))
                .previousCompatibility(ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.forward()));
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/compatibility").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.put(Entity.json(updateCompatibilityRequest));

        assertEquals("Update schema status", OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void test05_addSchemasToGroup(){
        Map<String,String> properties = new HashMap<>();
        properties.put("name","raghu");

        JsonObject schemaData = new JsonObject();
        if (namespacePresent) schemaData.addProperty("namespace","io.pravega.schemaregistry.test.integrationtest.generated");
        schemaData.addProperty("type","record");
        schemaData.addProperty("name","Type1");

        JsonObject field1 = new JsonObject();
        field1.addProperty("name","a");
        field1.addProperty("type","string");
        JsonObject field2 = new JsonObject();
        field2.addProperty("name","b");
        field2.addProperty("type","int");
        JsonArray fields = new JsonArray();
        fields.add(field1);
        fields.add(field2);

        schemaData.add("fields", fields);

        // create schema of type Type1, version:0 and ordinal:0
        SchemaInfo schemaInfo = new SchemaInfo()
                .type(schemaType1)
                .serializationFormat(ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro))
                .schemaData(schemaData.toString().getBytes())
                .properties(properties);
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(schemaInfo));
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        VersionInfo versionInfo = response.readEntity(VersionInfo.class);
        assertEquals(schemaType1,versionInfo.getType());
        assertEquals(new Integer(0),versionInfo.getVersion());
        assertEquals(new Integer(0),versionInfo.getId());

        // create schema of type Type1, version:1 and ordinal:1
        schemaData.remove("fields");
        fields.remove(1);
        schemaData.add("fields", fields);

        schemaInfo = new SchemaInfo()
                .type(schemaType1)
                .serializationFormat(ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro))
                .schemaData(schemaData.toString().getBytes())
                .properties(properties);
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas").toString();
        webTarget = client.target(resourceURl);
        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(schemaInfo));
        assertEquals("addSchemasToGroup status", CREATED.getStatusCode(), response.getStatus());
        versionInfo = response.readEntity(VersionInfo.class);
        assertEquals(schemaType1,versionInfo.getType());
        assertEquals(new Integer(1),versionInfo.getVersion());
        assertEquals(new Integer(1),versionInfo.getId());
    }

    @Test
    public void test06_getHistoryOfGroup(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/history").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get History of Group status", OK.getStatusCode(), response.getStatus());
        GroupHistory groupHistory = response.readEntity(GroupHistory.class);
        assertEquals(2,groupHistory.getHistory().size());
        assertEquals(schemaType1,groupHistory.getHistory().get(0).getSchemaInfo().getType());
        assertEquals(new Integer(0),groupHistory.getHistory().get(0).getVersionInfo().getVersion());
    }

    @Test
    public void test07_getSchemas(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get schemas status", OK.getStatusCode(), response.getStatus());
        SchemaVersionsList schemaVersionsList = response.readEntity(SchemaVersionsList.class);
        assertEquals(1,schemaVersionsList.getSchemas().size());
        SchemaWithVersion schemaWithVersion = schemaVersionsList.getSchemas().get(0);
        assertEquals("Avro",schemaWithVersion.getSchemaInfo().getSerializationFormat().getSerializationFormat().toString());
        assertEquals(schemaType1,schemaWithVersion.getSchemaInfo().getType());
        assertEquals(new Integer(1),schemaWithVersion.getVersionInfo().getVersion());
        assertEquals(new Integer(1),schemaWithVersion.getVersionInfo().getId());
    }

    @Test
    public void test08_getSchemaVersions(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/versions").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get schemas status", OK.getStatusCode(), response.getStatus());
        SchemaVersionsList schemaVersionsList = response.readEntity(SchemaVersionsList.class);
        assertEquals(2,schemaVersionsList.getSchemas().size());
        SchemaWithVersion schemaWithVersion0 = schemaVersionsList.getSchemas().get(0);
        assertEquals("Avro",schemaWithVersion0.getSchemaInfo().getSerializationFormat().getSerializationFormat().toString());
        assertEquals(schemaType1,schemaWithVersion0.getVersionInfo().getType());
        assertEquals(new Integer(0),schemaWithVersion0.getVersionInfo().getVersion());
        assertEquals(new Integer(0),schemaWithVersion0.getVersionInfo().getId());
        SchemaWithVersion schemaWithVersion1 = schemaVersionsList.getSchemas().get(1);
        assertEquals(schemaType1,schemaWithVersion1.getVersionInfo().getType());
        assertEquals(new Integer(1),schemaWithVersion1.getVersionInfo().getVersion());
        assertEquals(new Integer(1),schemaWithVersion1.getVersionInfo().getId());
    }

    @Test
    public void test09_findSchemaVersionInGroup(){
        Map<String,String> properties = new HashMap<>();
        properties.put("name","raghu");

        JsonObject schemaData = new JsonObject();
        if(namespacePresent) schemaData.addProperty("namespace","io.pravega.schemaregistry.test.integrationtest.generated");
        schemaData.addProperty("type","record");
        schemaData.addProperty("name","Type1");

        JsonObject field1 = new JsonObject();
        field1.addProperty("name","a");
        field1.addProperty("type","string");
        JsonObject field2 = new JsonObject();
        field2.addProperty("name","b");
        field2.addProperty("type","int");
        JsonArray fields = new JsonArray();
        fields.add(field1);
        fields.add(field2);

        schemaData.add("fields", fields);

        // find schema of type Type1, version:0 and ordinal:0
        SchemaInfo schemaInfo = new SchemaInfo()
                .type(schemaType1)
                .serializationFormat(ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro))
                .schemaData(schemaData.toString().getBytes())
                .properties(properties);
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/versions/find").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(schemaInfo));
        assertEquals("Find schema version status", OK.getStatusCode(), response.getStatus());
        VersionInfo versionInfo = response.readEntity(VersionInfo.class);
        assertEquals(schemaType1,versionInfo.getType());
        assertEquals(new Integer(0),versionInfo.getVersion());
        assertEquals(new Integer(0),versionInfo.getId());
    }

    @Test
    public void test10_getSchemaFromSchemaId(){
        // GET /groups/{groupName}/schemas/schema/{schemaId}
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/schema/0").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get schemasFromSchemaId status", OK.getStatusCode(), response.getStatus());
        SchemaInfo schemaInfo = response.readEntity(SchemaInfo.class);
        assertEquals(schemaType1,schemaInfo.getType());
        assertEquals("Avro",schemaInfo.getSerializationFormat().getSerializationFormat().toString());
    }

    @Test
    public void test11_getSchemaFromVersionNumber(){
        // GET /groups/{groupName}/schemas/format/{serializationFormat}/type/{type}/versions/{version}
        String schemaType = schemaType1;
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/format/Avro/type/"+schemaType+"/versions/1").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get schemasFromVersionNumber status", OK.getStatusCode(), response.getStatus());
        SchemaInfo schemaInfo = response.readEntity(SchemaInfo.class);
        assertEquals(schemaType,schemaInfo.getType());
        assertEquals("Avro",schemaInfo.getSerializationFormat().getSerializationFormat().toString());
    }

    @Test
    public void test12_validateSchemaVersions(){
        // POST /groups/{groupName}/schemas/versions/validate
        JsonObject schemaData = new JsonObject();
        if(namespacePresent) schemaData.addProperty("namespace","io.pravega.schemaregistry.test.integrationtest.generated");
        schemaData.addProperty("type","record");
        schemaData.addProperty("name","Type1");
        JsonObject field1 = new JsonObject();
        field1.addProperty("name","a");
        field1.addProperty("type","string");
        JsonArray fields = new JsonArray();
        fields.add(field1);
        schemaData.add("fields", fields);
        SchemaInfo schemaInfo = new SchemaInfo()
                .type(schemaType1)
                .serializationFormat(ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro))
                .schemaData(schemaData.toString().getBytes())
                .properties(Collections.emptyMap());

        ValidateRequest validateRequest = new ValidateRequest().schemaInfo(schemaInfo)
                .compatibility(ModelHelper.encode(io.pravega.schemaregistry.contract.data.Compatibility.backward()));
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/versions/validate").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(validateRequest));
        assertEquals("validateSchemaVersions status", OK.getStatusCode(), response.getStatus());
        assertEquals(true, response.readEntity(Valid.class).isValid());
    }

    @Test
    public void test13_canReadSchemaVersions(){
        // POST /groups/{groupName}/schemas/versions/canRead
        JsonObject schemaData = new JsonObject();
        if (namespacePresent) schemaData.addProperty("namespace","io.pravega.schemaregistry.test.integrationtest.generated");
        schemaData.addProperty("type","record");
        schemaData.addProperty("name","Type1");
        JsonObject field1 = new JsonObject();
        field1.addProperty("name","a");
        field1.addProperty("type","string");
        JsonArray fields = new JsonArray();
        fields.add(field1);
        schemaData.add("fields", fields);
        SchemaInfo schemaInfo = new SchemaInfo()
                .type(schemaType1)
                .serializationFormat(ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro))
                .schemaData(schemaData.toString().getBytes())
                .properties(Collections.emptyMap());

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/versions/canRead").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(schemaInfo));
        assertEquals("canReadSchemaVersions", OK.getStatusCode(), response.getStatus());
        assertEquals(true, response.readEntity(CanRead.class).isCompatible());
    }

    @Test
    public void test14_getEncodingId(){
        // PUT /groups/{groupName}/encodings
        GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest()
                .versionInfo(new VersionInfo().type(schemaType1).version(0).id(0).serializationFormat("Avro"))
                .codecType("");

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/encodings").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.put(Entity.json(getEncodingIdRequest));
        assertEquals("Get encodingId status", OK.getStatusCode(), response.getStatus());
        assertEquals(new Integer(0), response.readEntity(EncodingId.class).getEncodingId());

        getEncodingIdRequest = new GetEncodingIdRequest()
                .versionInfo(new VersionInfo().type(schemaType1).version(1).id(1).serializationFormat("Avro"))
                .codecType("");
        response = builder.put(Entity.json(getEncodingIdRequest));
        assertEquals("Get encodingId status", OK.getStatusCode(), response.getStatus());
        assertEquals(new Integer(1), response.readEntity(EncodingId.class).getEncodingId());
    }

    @Test
    public void test15_getEncodingInfo(){
        // GET /groups/{groupName}/encodings/{encodingId}
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/encodings/0").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get encodingInfo status", OK.getStatusCode(), response.getStatus());
        EncodingInfo encodingInfo0 = response.readEntity(EncodingInfo.class);
        assertEquals("Avro", encodingInfo0.getSchemaInfo().getSerializationFormat().getSerializationFormat().toString());
        assertEquals(new Integer(0), encodingInfo0.getVersionInfo().getVersion());
        assertEquals(new Integer(0), encodingInfo0.getVersionInfo().getId());
        assertEquals(schemaType1, encodingInfo0.getVersionInfo().getType());
        assertEquals("", encodingInfo0.getCodecType().getName());

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/encodings/1").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get encodingInfo status", OK.getStatusCode(), response.getStatus());
        EncodingInfo encodingInfo1 = response.readEntity(EncodingInfo.class);
        assertEquals(new Integer(1), encodingInfo1.getVersionInfo().getVersion());
        assertEquals(new Integer(1), encodingInfo1.getVersionInfo().getId());
        assertEquals(schemaType1, encodingInfo1.getVersionInfo().getType());
        assertEquals("", encodingInfo1.getCodecType().getName());
    }

    @Test
    public void test16_addNewCodecToGroup(){
        //POST /groups/{groupName}/codecTypes
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/codecTypes").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        CodecType codecType = new CodecType().name("Snappy");
        Response response = builder.post(Entity.json(codecType));
        assertEquals("Add new codec to Group status", CREATED.getStatusCode(), response.getStatus());

        codecType = new CodecType().name("Custom");
        response = builder.post(Entity.json(codecType));
        assertEquals("Add new codec to Group status", CREATED.getStatusCode(), response.getStatus());
    }

    @Test
    public void test17_getCodecTypesForGroup(){
        // GET /groups/{groupName}/codecTypes
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/codecTypes").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get CodecTypesForGroup status", OK.getStatusCode(), response.getStatus());
        CodecTypes CodecTypes = response.readEntity(CodecTypes.class);
        assertEquals(2, CodecTypes.getCodecTypes().size());
        assertEquals("Snappy", CodecTypes.getCodecTypes().get(0).getName());
        assertEquals("Custom", CodecTypes.getCodecTypes().get(1).getName());
    }

    @Test
    public void test18_getGroupsWhereSchemaRegistered(){
        // POST /schemas/addedTo
        JsonObject schemaData = new JsonObject();
        if(namespacePresent) schemaData.addProperty("namespace","io.pravega.schemaregistry.test.integrationtest.generated");
        schemaData.addProperty("type","record");
        schemaData.addProperty("name","Type1");
        JsonObject field1 = new JsonObject();
        field1.addProperty("name","a");
        field1.addProperty("type","string");
        JsonArray fields = new JsonArray();
        fields.add(field1);
        schemaData.add("fields", fields);
        SchemaInfo schemaInfo = new SchemaInfo()
                .type(schemaType1)
                .serializationFormat(ModelHelper.encode(io.pravega.schemaregistry.contract.data.SerializationFormat.Avro))
                .schemaData(schemaData.toString().getBytes())
                .properties(Collections.emptyMap());

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/schemas/addedTo").toString();
        webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        Response response = builder.post(Entity.json(schemaInfo));
        assertEquals("getGroupsWhereSchemaRegistered status", OK.getStatusCode(), response.getStatus());
        String response_str = response.readEntity(String.class);
        JsonObject responseJson = new Gson().fromJson(response_str, JsonObject.class);
        JsonObject group1 = responseJson.getAsJsonObject("groups").getAsJsonObject(groupName1);
        assertEquals( schemaType1, group1.get("type").getAsString());
        assertEquals( 1, group1.get("version").getAsInt());
        assertEquals( 1, group1.get("id").getAsInt());
    }

    @Test
    public void test19_deleteSchemaUsingSchemaId(){
        // DELETE /groups/{groupName}/schemas/schema/{schemaId}
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/schema/0").toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("deleteSchemaUsingSchemaId", NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @Test
    public void test20_deleteSchemaUsingVersionNumber(){
        // DELETE /groups/{groupName}/schemas/format/{serializationFormat}/type/{type}/versions/{version}
        String schemaType = schemaType1;
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas/format/Avro/type/"+schemaType+"/versions/1").toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("deleteSchemaUsingVersionNumber", NO_CONTENT.getStatusCode(), response.getStatus());

        // verify schemas are deleted
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1+"/schemas").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get schemas status", OK.getStatusCode(), response.getStatus());
        SchemaVersionsList schemaVersionsList = response.readEntity(SchemaVersionsList.class);
        assertEquals(0,schemaVersionsList.getSchemas().size());
    }

    @Test
    public void test3_deleteGroup(){
        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupName1).toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("Delete group status", NO_CONTENT.getStatusCode(), response.getStatus());
        response = client.target(resourceURl).request().get();
        assertEquals("deleteGroup status", NOT_FOUND.getStatusCode(), response.getStatus());
        System.out.println("Delete group successful");

    }

}
