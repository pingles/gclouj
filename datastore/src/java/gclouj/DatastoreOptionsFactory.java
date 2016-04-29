package gclouj;

import com.google.cloud.AuthCredentials;
import com.google.cloud.datastore.DatastoreOptions;

public class DatastoreOptionsFactory {
    public static DatastoreOptions create(String projectId, String namespace, AuthCredentials credentials) {
        DatastoreOptions.Builder builder = DatastoreOptions.builder()
            .projectId(projectId)
            .namespace(namespace)
            .authCredentials(credentials);
        return builder.build();

    }

    public static DatastoreOptions createTestOptions(String projectId, Integer port) {
        String host = "localhost:"+port;

        DatastoreOptions.Builder builder = DatastoreOptions.builder()
            .projectId(projectId)
            .host(host);

        return builder.build();
    }
}
