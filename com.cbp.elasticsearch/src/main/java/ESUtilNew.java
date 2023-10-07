import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.*;

import java.io.IOException;


/**
 * @Author changbp
 * @Date 2021/2/22 11:10
 * @Version 1.0
 */
public class ESUtilNew {

    public static RestClient getRestClient() {
        UsernamePasswordCredentials usernamePassword = new UsernamePasswordCredentials("elstic", "123456");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, usernamePassword);
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("", 9200))
                .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider));
        return builder.build();
    }

    public static void get() throws IOException {
        UsernamePasswordCredentials usernamePassword = new UsernamePasswordCredentials("elstic", "123456");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, usernamePassword);
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http"))
                        .setHttpClientConfigCallback(httpAsyncClientBuilder ->httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));
        RestClient lowLevelClient = client.getLowLevelClient();
        lowLevelClient.close();
        client.close();
    }

    private static final RequestOptions COMMON_OPTIONS;
    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Authorization", "Bearer " + "");
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(30 * 1024 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }


}
