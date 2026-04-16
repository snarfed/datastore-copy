package org.snarfed.datastorecopy;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

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

    @Description("Source namespace (empty string for default namespace)")
    @Default.String("")
    String getSourceNamespace();
    void setSourceNamespace(String value);

    @Description("Target Firestore in Datastore mode project ID")
    @Required
    String getTargetProject();
    void setTargetProject(String value);

    @Description("Target namespace (empty string for default namespace; defaults to source namespace)")
    @Default.String("")
    String getTargetNamespace();
    void setTargetNamespace(String value);

    @Description("Datastore emulator host, e.g. \"localhost:8089\". If set, reads and writes go to "
        + "the emulator instead of Cloud Datastore. Primarily a test seam; DatastoreIO does not "
        + "read DATASTORE_EMULATOR_HOST itself.")
    @Default.String("")
    String getLocalhost();
    void setLocalhost(String value);
  }

  /** Rewrites an entity's key to use the target project and namespace. */
  static class RekeyEntity extends SimpleFunction<Entity, Entity> {
    private final String targetProject;
    private final String targetNamespace;

    RekeyEntity(String targetProject, String targetNamespace) {
      this.targetProject = targetProject;
      this.targetNamespace = targetNamespace;
    }

    @Override
    public Entity apply(Entity entity) {
      PartitionId newPartition = PartitionId.newBuilder()
          .setProjectId(targetProject)
          .setNamespaceId(targetNamespace)
          .build();
      Key newKey = entity.getKey().toBuilder()
          .setPartitionId(newPartition)
          .build();
      return entity.toBuilder()
          .setKey(newKey)
          .build();
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
          .withNamespace(options.getSourceNamespace());
      if (!localhost.isEmpty()) {
        read = read.withLocalhost(localhost);
      }
      reads.add(p.apply("Read-" + kind, read));
    }

    DatastoreV1.Write write = DatastoreIO.v1().write()
        .withProjectId(options.getTargetProject());
    if (!localhost.isEmpty()) {
      write = write.withLocalhost(localhost);
    }

    PCollectionList.of(reads)
        .apply("FlattenKinds", Flatten.pCollections())
        .apply("RekeyEntities",
            MapElements.via(new RekeyEntity(options.getTargetProject(), targetNamespace)))
        .apply("WriteToDatastore", write);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);
    buildPipeline(p, options);
    p.run().waitUntilFinish();
  }
}
