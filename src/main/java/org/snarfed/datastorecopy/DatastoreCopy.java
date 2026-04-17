package org.snarfed.datastorecopy;

import com.google.cloud.NoCredentials;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies Firestore in Datastore mode entities of the specified kinds from one
 * project to another, with an optional WHERE clause applied to all kinds.
 *
 * <p>Local (DirectRunner):
 * <pre>
 *   mvn compile exec:java -Dexec.mainClass=org.snarfed.datastorecopy.DatastoreCopy \
 *     -Dexec.args="--sourceProject=my-src-project \
 *                  --kinds=Kind1,Kind2,Kind3 \
 *                  --targetProject=my-dst-project"
 * </pre>
 *
 * <p>Dataflow:
 * <pre>
 *   mvn package
 *   java -jar target/datastore-copy-1.0-SNAPSHOT.jar \
 *     --runner=DataflowRunner \
 *     --project=my-dst-project \
 *     --region=us-central1 \
 *     --tempLocation=gs://my-bucket/tmp \
 *     --sourceProject=my-src-project \
 *     --kinds=Kind1,Kind2,Kind3 \
 *     --targetProject=my-dst-project
 * </pre>
 */
public class DatastoreCopy {

  static final Logger log = LoggerFactory.getLogger(DatastoreCopy.class);

  /**
   * Writes entities to Datastore in batches of up to 500 (the API maximum).
   *
   * <p>Uses {@code google-cloud-datastore} v2 {@code HttpDatastoreRpc} directly, which correctly
   * routes named databases via the {@code x-goog-request-params: database_id=...} HTTP header.
   * {@code DatastoreIO.v1().write().withDatabaseId()} cannot be used because Beam receives the
   * database ID but never passes it to the underlying {@code DatastoreOptions}, so it silently
   * writes to the default database.
   */
  static class WriteEntityFn extends DoFn<Entity, Void> {
    static final int BATCH_SIZE = 500;

    private final String projectId;
    private final String databaseId;
    private final String localhost;

    private transient com.google.cloud.datastore.spi.v1.DatastoreRpc datastoreRpc;
    private transient List<Mutation> batch;

    WriteEntityFn(String projectId, String databaseId, String localhost) {
      this.projectId = projectId;
      this.databaseId = databaseId;
      this.localhost = localhost;
    }

    @Setup
    public void setup() throws Exception {
      com.google.cloud.datastore.DatastoreOptions.Builder builder =
          com.google.cloud.datastore.DatastoreOptions.newBuilder()
              .setProjectId(projectId)
              .setDatabaseId(databaseId);
      if (!localhost.isEmpty()) {
        builder.setHost("http://" + localhost)
               .setCredentials(NoCredentials.getInstance());
      }
      datastoreRpc = new com.google.cloud.datastore.spi.v1.HttpDatastoreRpc(builder.build());
      batch = new ArrayList<>();
    }

    @ProcessElement
    public void processElement(@Element Entity entity) throws Exception {
      batch.add(Mutation.newBuilder().setUpsert(entity).build());
      if (batch.size() >= BATCH_SIZE) {
        flush();
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (!batch.isEmpty()) {
        flush();
      }
    }

    private void flush() throws Exception {
      log.info("Writing batch of {} entities to project={} database={}",
          batch.size(), projectId, databaseId.isEmpty() ? "(default)" : databaseId);
      CommitRequest request = CommitRequest.newBuilder()
          .setProjectId(projectId)
          .setDatabaseId(databaseId)
          .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
          .addAllMutations(batch)
          .build();
      datastoreRpc.commit(request);
      batch.clear();
    }
  }

  public interface Options extends PipelineOptions {
    @Description("Source Firestore in Datastore mode project ID")
    @Required
    String getSourceProject();
    void setSourceProject(String value);

    @Description("Comma-separated list of Datastore kinds to copy, e.g. \"Kind1,Kind2,Kind3\"")
    @Required
    String getKinds();
    void setKinds(String value);

    @Description("Optional GQL WHERE clause applied to all kinds, e.g. \"status = 'active'\". "
        + "Do not include the WHERE keyword.")
    @Default.String("")
    String getWhere();
    void setWhere(String value);

    @Description("Source database ID (empty string for the default database)")
    @Default.String("")
    String getSourceDatabaseId();
    void setSourceDatabaseId(String value);

    @Description("Source namespace (empty string for default namespace)")
    @Default.String("")
    String getSourceNamespace();
    void setSourceNamespace(String value);

    @Description("Target Firestore in Datastore mode project ID (defaults to source project)")
    @Default.String("")
    String getTargetProject();
    void setTargetProject(String value);

    @Description("Target database ID (empty string for the default database; defaults to source database ID)")
    @Default.String("")
    String getTargetDatabaseId();
    void setTargetDatabaseId(String value);

    @Description("Target namespace (empty string for default namespace; defaults to source namespace)")
    @Default.String("")
    String getTargetNamespace();
    void setTargetNamespace(String value);

    @Description("Number of query splits per kind for parallel reads. Defaults to 0 (Beam "
        + "chooses automatically). Set to 1 if your --where clause uses an inequality filter "
        + "(e.g. >, <, >=, <=), which the query splitter does not support.")
    @Default.Integer(0)
    int getNumQuerySplits();
    void setNumQuerySplits(int value);

    @Description("Datastore emulator host, e.g. \"localhost:8089\". If set, reads and writes go to "
        + "the emulator instead of Cloud Datastore. Primarily a test seam; DatastoreIO does not "
        + "read DATASTORE_EMULATOR_HOST itself.")
    @Default.String("")
    String getLocalhost();
    void setLocalhost(String value);
  }

  /** Rewrites an entity's keys (top-level and embedded) to use the target project, database, and namespace. */
  static class RekeyEntity extends SimpleFunction<Entity, Entity> {
    private final String targetProject;
    private final String targetDatabaseId;
    private final String targetNamespace;

    RekeyEntity(String targetProject, String targetDatabaseId, String targetNamespace) {
      this.targetProject = targetProject;
      this.targetDatabaseId = targetDatabaseId;
      this.targetNamespace = targetNamespace;
    }

    @Override
    public Entity apply(Entity entity) {
      PartitionId newPartition = PartitionId.newBuilder()
          .setProjectId(targetProject)
          .setDatabaseId(targetDatabaseId)
          .setNamespaceId(targetNamespace)
          .build();
      Entity.Builder builder = entity.toBuilder()
          .setKey(entity.getKey().toBuilder().setPartitionId(newPartition).build());
      builder.clearProperties();
      for (Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
        builder.putProperties(entry.getKey(), rewriteValue(entry.getValue(), newPartition));
      }
      return builder.build();
    }

    private static Value rewriteValue(Value v, PartitionId partition) {
      switch (v.getValueTypeCase()) {
        case KEY_VALUE:
          return v.toBuilder()
              .setKeyValue(v.getKeyValue().toBuilder().setPartitionId(partition).build())
              .build();
        case ENTITY_VALUE: {
          Entity embedded = v.getEntityValue();
          Entity.Builder eb = embedded.toBuilder();
          if (embedded.hasKey()) {
            eb.setKey(embedded.getKey().toBuilder().setPartitionId(partition).build());
          }
          eb.clearProperties();
          for (Map.Entry<String, Value> entry : embedded.getPropertiesMap().entrySet()) {
            eb.putProperties(entry.getKey(), rewriteValue(entry.getValue(), partition));
          }
          return v.toBuilder().setEntityValue(eb.build()).build();
        }
        case ARRAY_VALUE: {
          ArrayValue.Builder ab = ArrayValue.newBuilder();
          for (Value element : v.getArrayValue().getValuesList()) {
            ab.addValues(rewriteValue(element, partition));
          }
          return v.toBuilder().setArrayValue(ab.build()).build();
        }
        default:
          return v;
      }
    }
  }

  static String buildQuery(String kind, String where) {
    String query = "SELECT * FROM " + kind;
    if (!where.isEmpty()) {
      query += " WHERE " + where;
    }
    return query;
  }

  static void buildPipeline(Pipeline p, Options options) {
    String targetProject = options.getTargetProject().isEmpty()
        ? options.getSourceProject()
        : options.getTargetProject();
    String targetDatabaseId = options.getTargetDatabaseId().isEmpty()
        ? options.getSourceDatabaseId()
        : options.getTargetDatabaseId();
    String targetNamespace = options.getTargetNamespace().isEmpty()
        ? options.getSourceNamespace()
        : options.getTargetNamespace();

    List<String> kinds = Arrays.asList(options.getKinds().split(","));
    String localhost = options.getLocalhost();

    List<PCollection<Entity>> reads = new ArrayList<>();
    for (String kind : kinds) {
      String query = buildQuery(kind.trim(), options.getWhere());
      DatastoreV1.Read read = DatastoreIO.v1().read()
          .withProjectId(options.getSourceProject())
          .withLiteralGqlQuery(query)
          .withNamespace(options.getSourceNamespace())
          .withNumQuerySplits(options.getNumQuerySplits());
      if (!options.getSourceDatabaseId().isEmpty()) {
        read = read.withDatabaseId(options.getSourceDatabaseId());
      }
      if (!localhost.isEmpty()) {
        read = read.withLocalhost(localhost);
      }
      reads.add(p.apply("Read-" + kind, read));
    }

    PCollectionList.of(reads)
        .apply("FlattenKinds", Flatten.pCollections())
        .apply("RekeyEntities",
            MapElements.via(new RekeyEntity(targetProject, targetDatabaseId, targetNamespace)))
        .apply("WriteToDatastore",
            ParDo.of(new WriteEntityFn(targetProject, targetDatabaseId, localhost)));
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);
    buildPipeline(p, options);
    p.run().waitUntilFinish();
  }
}
