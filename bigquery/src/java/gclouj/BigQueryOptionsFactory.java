package gclouj;

import com.google.cloud.bigquery.BigQueryOptions;

public class BigQueryOptionsFactory {
  public static BigQueryOptions create(String projectId) {
    BigQueryOptions.Builder builder = BigQueryOptions.builder()
      .projectId(projectId);
    return builder.build();
  }
}