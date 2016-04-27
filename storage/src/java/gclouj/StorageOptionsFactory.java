package gclouj;

import com.google.cloud.AuthCredentials;
import com.google.cloud.storage.StorageOptions;

public class StorageOptionsFactory {
    public static StorageOptions create(String projectId, AuthCredentials credentials) {
        StorageOptions.Builder builder = StorageOptions.builder()
            .projectId(projectId)
            .authCredentials(credentials);
        return builder.build();
    }
}
