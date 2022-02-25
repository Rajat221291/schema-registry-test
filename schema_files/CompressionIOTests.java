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
import io.pravega.schemaregistry.serializer.shared.codec.Codec;
import io.pravega.schemaregistry.serializer.shared.codec.Codecs;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypes;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

import static javax.ws.rs.core.Response.Status.*;
import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CompressionIOTests {
    private static String schemaRegistryURI = "http://10.243.41.97:9092";
    private static String controllerURI = "tcp://10.243.41.69:9090";
    private static ClientConfig clientConfig;
    private static SchemaRegistryClient schemaRegistryClient;
    private static final String scope = "scope" + System.currentTimeMillis();
    private static final String stream = "stream";
    private static final String groupId = scope+"-"+stream;
    private final CodecType MYCOMPRESSION = new CodecType("mycompression");
    private final Random RANDOM = new Random();
    private final Schema SCHEMA1 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();
    private final AvroSchema<GenericRecord> schema1 = AvroSchema.ofRecord(SCHEMA1);


    @BeforeClass
    public static void setUp() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        schemaRegistryClient = SchemaRegistryClientFactory.withDefaultNamespace(SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(schemaRegistryURI)).build());
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SerializationFormat serializationFormat = SerializationFormat.Avro;
        schemaRegistryClient.addGroup(groupId, new GroupProperties(serializationFormat,
                Compatibility.backward(),
                false));
    }

    @AfterClass
    public static void tearDown(){
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);

        String resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupId).toString();
        Response response = client.target(resourceURl).request().delete();
        assertEquals("Delete group status", NO_CONTENT.getStatusCode(), response.getStatus());
        response = client.target(resourceURl).request().get();
        assertEquals("deleteGroup status", NOT_FOUND.getStatusCode(), response.getStatus());
        System.out.println("Delete group successful");
    }

    @Test
    public void test1_verifySerializerWithAvroForWriteWithCompression(){
        // Verify PravegaSerializer implementation for avro schema for serialization
        int size = 10; // This is size in kb
        writeUsingCompression(Codecs.GzipCompressor.getCodec(),generateBigString(size));
        writeUsingCompression(Codecs.SnappyCompressor.getCodec(),generateBigString(size));
        writeUsingCompression(createCustomCodec(),generateBigString(size));
    }

    @Test
    public void test2_verifySerializerWithAvroForReadWithCompression(){
        // Verify PravegaSerializer implementation for avro schema for deserialization
        Codec MY_CODEC = createCustomCodec();
        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                .groupId(groupId)
                .decoder(MY_CODEC.getName(), MY_CODEC)
                .registryClient(schemaRegistryClient)
                .build();

        Serializer<Object> readerDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig2, null);

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String readerGroup = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(readerGroup,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        EventStreamReader<Object> reader = clientFactory.createReader("r2", readerGroup, readerDeserializer, ReaderConfig.builder().build());

        List<Object> eList = new ArrayList<>();
        EventRead<Object> event = reader.readNextEvent(10000);
        while (event.isCheckpoint() || event.getEvent() != null) {
            Object e = event.getEvent();
            System.out.println("event read = " + e.toString());
            eList.add(e);
            event = reader.readNextEvent(10000);
        }
        assertEquals(3, eList.size());
    }

    @Test
    public void test3_verifyCodecTypes(){
        // Verify codecs type using different compression formats
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);

        String resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupId+"/codecTypes").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get CodecTypesForGroup status", OK.getStatusCode(), response.getStatus());
        CodecTypes CodecTypes = response.readEntity(CodecTypes.class);
        assertEquals(3, CodecTypes.getCodecTypes().size());
        assertEquals("application/x-gzip", CodecTypes.getCodecTypes().get(0).getName());
        assertEquals("application/x-snappy-framed", CodecTypes.getCodecTypes().get(1).getName());
        assertEquals("mycompression", CodecTypes.getCodecTypes().get(2).getName());
    }

    @Test
    public void test4_verifyEncodingId(){
        // Register schema in the registry and use registry client to encode schema Id with payload
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);

        String resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupId+"/encodings/0").toString();
        Response response = client.target(resourceURl).request().get();
        assertEquals("Get encodingInfo status", OK.getStatusCode(), response.getStatus());
        EncodingInfo encodingInfo0 = response.readEntity(EncodingInfo.class);
        assertEquals("Avro", encodingInfo0.getSchemaInfo().getSerializationFormat().getSerializationFormat().toString());
        assertEquals(new Integer(0), encodingInfo0.getVersionInfo().getVersion());
        assertEquals(new Integer(0), encodingInfo0.getVersionInfo().getId());
        assertEquals("MyTest", encodingInfo0.getVersionInfo().getType());
        assertEquals("application/x-gzip", encodingInfo0.getCodecType().getName());

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupId+"/encodings/1").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get encodingInfo status", OK.getStatusCode(), response.getStatus());
        EncodingInfo encodingInfo1 = response.readEntity(EncodingInfo.class);
        assertEquals("Avro", encodingInfo1.getSchemaInfo().getSerializationFormat().getSerializationFormat().toString());
        assertEquals(new Integer(0), encodingInfo1.getVersionInfo().getVersion());
        assertEquals(new Integer(0), encodingInfo1.getVersionInfo().getId());
        assertEquals("MyTest", encodingInfo1.getVersionInfo().getType());
        assertEquals("application/x-snappy-framed", encodingInfo1.getCodecType().getName());

        resourceURl = new StringBuilder(schemaRegistryURI).append("/v1/groups/"+groupId+"/encodings/2").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get encodingInfo status", OK.getStatusCode(), response.getStatus());
        EncodingInfo encodingInfo2 = response.readEntity(EncodingInfo.class);
        assertEquals("Avro", encodingInfo2.getSchemaInfo().getSerializationFormat().getSerializationFormat().toString());
        assertEquals(new Integer(0), encodingInfo2.getVersionInfo().getVersion());
        assertEquals(new Integer(0), encodingInfo2.getVersionInfo().getId());
        assertEquals("MyTest", encodingInfo2.getVersionInfo().getType());
        assertEquals("mycompression", encodingInfo2.getCodecType().getName());
    }

    private void writeUsingCompression(Codec codecType, String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .groupId(groupId)
                .registerSchema(true)
                .registerCodec(true)
                .encoder(codecType)
                .registryClient(schemaRegistryClient)
                .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();
    }

    private String generateBigString(int sizeInKb) {
        byte[] array = new byte[1024 * sizeInKb];
        RANDOM.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }


    private Codec createCustomCodec(){
        Codec MY_CODEC = new Codec() {
            @Override
            public String getName() {
                return MYCOMPRESSION.getName();
            }

            @Override
            public CodecType getCodecType() {
                return MYCOMPRESSION;
            }

            @Override
            public void encode(ByteBuffer data, OutputStream bos) throws IOException {
                // left rotate by 1 byte
                byte[] array = new byte[data.remaining()];
                data.get(array);

                int i;
                byte temp = array[0];
                for (i = 0; i < array.length - 1; i++) {
                    array[i] = array[i + 1];
                }
                array[array.length - 1] = temp;
                bos.write(array, 0, array.length);
            }

            @Override
            public ByteBuffer decode(ByteBuffer data, Map<String, String> properties) throws IOException {
                byte[] array = new byte[data.remaining()];
                data.get(array);

                int i;
                byte temp = array[array.length - 1];
                for (i = array.length - 1; i > 0; i--) {
                    array[i] = array[i - 1];
                }
                array[0] = temp;
                return ByteBuffer.wrap(array);
            }
        };
        return MY_CODEC;
    }



}
