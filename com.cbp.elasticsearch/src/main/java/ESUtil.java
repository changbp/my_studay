import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * @Author changbp
 * @Date 2021/2/19 15:40
 * @Version 1.0
 */
public class ESUtil {
    public static void main(String[] args) throws IOException {
        TransportClient esRestClient = getEsRestClient();
        System.out.println(esRestClient);
//        testAddIndexJSON(esRestClient);
//        testGet(esRestClient);
        testUpdate(esRestClient);
//        testDelete(esRestClient);
    }

    //增加索引
    public static void testAddIndexJSON(TransportClient esRestClient) {
        String source = "{\"username\":\"李四\",\"age\":11}";
        IndexResponse response = esRestClient
                .prepareIndex("user", "_doc", "1").setSource(source, XContentType.JSON).get();
        String index = response.getIndex();
        String id = response.getId();
        System.out.println("version: " + response.getVersion());
        System.out.println(index + "=========="+id);
    }

    //查询
    public static void testGet(TransportClient esRestClient) {
        GetResponse response = esRestClient.prepareGet("user", "_doc", "1").get();
        Map<String, Object> map = response.getSource();
        System.out.println("version: " + response.getVersion());
        for (Map.Entry<String, Object> me : map.entrySet()) {
            System.out.println(me.getKey() + "=" + me.getValue());
        }
    }

    //更新
    public static void testUpdate(TransportClient esRestClient) throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "flink")
                .field("author", "CDH")
                .field("version", 2.9)
                .endObject();
        esRestClient.prepareUpdate("user", "_doc", "1").setDoc(source).get();
        testGet(esRestClient);
    }

    //删除
    public static void testDelete(TransportClient esRestClient) {
        DeleteResponse response = esRestClient.prepareDelete("user", "_doc", "1").get();
        System.out.println("version: " + response.getVersion());
        testAddIndexJSON(esRestClient);
        testGet(esRestClient);
    }

//    static String esHost = "192.168.2.101,192.168.2.102,192.168.2.103";
    static String esHost = "192.168.130.216";

    //获取连接
    public static TransportClient getEsRestClient() throws UnknownHostException {
        TransportClient transportClient = new PreBuiltXPackTransportClient(Settings.builder()
                .put("cluster.name", "es-cluster")
                .put("xpack.security.user", "elastic:123456")
                .put("xpack.security.transport.ssl.keystore.path", "/elastic-certificates.p12")
                .put("xpack.security.transport.ssl.truststore.path", "/elastic-certificates.p12")
                .put("xpack.security.transport.ssl.verification_mode", "certificate")
                .put("xpack.security.transport.ssl.enabled", true)
                .build());
        String[] hosts = esHost.split(",");
        for (String host : hosts) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host), Integer.parseInt("9300")));
        }
        return transportClient;
    }
}
