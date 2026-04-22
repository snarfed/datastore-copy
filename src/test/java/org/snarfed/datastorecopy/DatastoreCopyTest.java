package org.snarfed.datastorecopy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for DatastoreCopy.
 *
 * <p>Integration tests require the Datastore emulator running on localhost:8089 (or
 * DATASTORE_EMULATOR_HOST if set). Start it with:
 * <pre>
 *   gcloud beta emulators datastore start --host-port=localhost:8089
 * </pre>
 */
public class DatastoreCopyTest {

  static final String SOURCE_PROJECT = "test-source";
  static final String TARGET_PROJECT = "test-target";
  static final String TEST_KIND = "TestKind";
  static final String OTHER_KIND = "OtherKind";

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  private String emulatorHost;
  private Datastore sourceDs;
  private Datastore targetDs;

  // --- Unit tests (no emulator needed) ---

  @Test
  public void testBuildQuery_noWhere() {
    assertEquals("SELECT * FROM `MyKind`", DatastoreCopy.buildQuery("MyKind", ""));
  }

  @Test
  public void testBuildQuery_withWhere() {
    assertEquals("SELECT * FROM `MyKind` WHERE status = 'active'",
        DatastoreCopy.buildQuery("MyKind", "status = 'active'"));
  }

  @Test
  public void testRekeyEntity() {
    com.google.datastore.v1.Entity entity = com.google.datastore.v1.Entity.newBuilder()
        .setKey(com.google.datastore.v1.Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder()
                .setProjectId("source-project")
                .setNamespaceId("source-ns"))
            .addPath(com.google.datastore.v1.Key.PathElement.newBuilder()
                .setKind("MyKind").setName("my-entity")))
        .build();

    com.google.datastore.v1.Entity rekeyed =
        new DatastoreCopy.RekeyEntity("target-project", "target-db", "target-ns").apply(entity);

    assertEquals(
        com.google.datastore.v1.Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder()
                .setProjectId("target-project")
                .setDatabaseId("target-db")
                .setNamespaceId("target-ns"))
            .addPath(com.google.datastore.v1.Key.PathElement.newBuilder()
                .setKind("MyKind").setName("my-entity"))
            .build(),
        rekeyed.getKey());
  }

  @Test
  public void testRekeyEntity_preservesProperties() {
    com.google.datastore.v1.Entity entity = com.google.datastore.v1.Entity.newBuilder()
        .setKey(com.google.datastore.v1.Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder().setProjectId("source-project"))
            .addPath(com.google.datastore.v1.Key.PathElement.newBuilder()
                .setKind("MyKind").setId(42)))
        .putProperties("name", Value.newBuilder().setStringValue("Alice").build())
        .build();

    com.google.datastore.v1.Entity rekeyed =
        new DatastoreCopy.RekeyEntity("target-project", "", "").apply(entity);

    assertEquals("Alice", rekeyed.getPropertiesOrThrow("name").getStringValue());
    assertEquals("target-project", rekeyed.getKey().getPartitionId().getProjectId());
    assertEquals(42, rekeyed.getKey().getPath(0).getId());
  }

  // --- Integration tests (require DATASTORE_EMULATOR_HOST) ---

  @Before
  public void setUp() {
    emulatorHost = System.getenv("DATASTORE_EMULATOR_HOST");
    if (emulatorHost == null) {
      emulatorHost = "localhost:8089";
    }
    sourceDs = DatastoreOptions.newBuilder()
        .setProjectId(SOURCE_PROJECT)
        .setHost("http://" + emulatorHost)
        .setCredentials(NoCredentials.getInstance())
        .build().getService();
    targetDs = DatastoreOptions.newBuilder()
        .setProjectId(TARGET_PROJECT)
        .setHost("http://" + emulatorHost)
        .setCredentials(NoCredentials.getInstance())
        .build().getService();
    deleteAll(sourceDs, TEST_KIND);
    deleteAll(sourceDs, OTHER_KIND);
    deleteAll(targetDs, TEST_KIND);
    deleteAll(targetDs, OTHER_KIND);
  }

  @After
  public void tearDown() {
    if (sourceDs != null) {
      deleteAll(sourceDs, TEST_KIND);
      deleteAll(sourceDs, OTHER_KIND);
    }
    if (targetDs != null) {
      deleteAll(targetDs, TEST_KIND);
      deleteAll(targetDs, OTHER_KIND);
    }
  }

  @Test
  public void testCopyOneKind() {
    sourceDs.put(
        entity(sourceDs, TEST_KIND, "alice", "name", "Alice"),
        entity(sourceDs, TEST_KIND, "bob", "name", "Bob"));

    runPipeline(SOURCE_PROJECT, TEST_KIND, "", TARGET_PROJECT);

    List<Entity> results = queryAll(targetDs, TEST_KIND);
    assertEquals(2, results.size());
    List<String> names = new ArrayList<>();
    for (Entity e : results) {
      names.add(e.getString("name"));
    }
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Bob"));
  }

  @Test
  public void testCopyMultipleKinds() {
    sourceDs.put(entity(sourceDs, TEST_KIND, "a", "val", "1"));
    sourceDs.put(entity(sourceDs, OTHER_KIND, "b", "val", "2"));

    runPipeline(SOURCE_PROJECT, TEST_KIND + "," + OTHER_KIND, "", TARGET_PROJECT);

    assertEquals(1, queryAll(targetDs, TEST_KIND).size());
    assertEquals(1, queryAll(targetDs, OTHER_KIND).size());
  }

  @Test
  public void testCopyWithWhere() {
    sourceDs.put(
        entity(sourceDs, TEST_KIND, "active1", "status", "active"),
        entity(sourceDs, TEST_KIND, "inactive1", "status", "inactive"),
        entity(sourceDs, TEST_KIND, "active2", "status", "active"));

    runPipeline(SOURCE_PROJECT, TEST_KIND, "status = 'active'", TARGET_PROJECT);

    List<Entity> results = queryAll(targetDs, TEST_KIND);
    assertEquals(2, results.size());
    for (Entity e : results) {
      assertEquals("active", e.getString("status"));
    }
  }

  @Test
  public void testRekeyUsesTargetProject() {
    sourceDs.put(entity(sourceDs, TEST_KIND, "k1", "x", "y"));

    runPipeline(SOURCE_PROJECT, TEST_KIND, "", TARGET_PROJECT);

    List<Entity> results = queryAll(targetDs, TEST_KIND);
    assertEquals(1, results.size());
    assertEquals(TARGET_PROJECT, results.get(0).getKey().getProjectId());
  }

  // --- Helpers ---

  private void runPipeline(String sourceProject, String kinds, String where,
      String targetProject) {
    DatastoreCopy.Options options =
        PipelineOptionsFactory.create().as(DatastoreCopy.Options.class);
    options.setSourceProject(sourceProject);
    options.setKinds(kinds);
    options.setWhere(where);
    options.setTargetProject(targetProject);
    options.setLocalhost(emulatorHost);
    DatastoreCopy.buildPipeline(pipeline, options);
    pipeline.run().waitUntilFinish();
  }

  private static Entity entity(Datastore ds, String kind, String name,
      String propKey, String propVal) {
    Key key = ds.newKeyFactory().setKind(kind).newKey(name);
    return Entity.newBuilder(key).set(propKey, propVal).build();
  }

  private static List<Entity> queryAll(Datastore ds, String kind) {
    QueryResults<Entity> results = ds.run(
        Query.newEntityQueryBuilder().setKind(kind).build());
    List<Entity> entities = new ArrayList<>();
    while (results.hasNext()) {
      entities.add(results.next());
    }
    return entities;
  }

  private static void deleteAll(Datastore ds, String kind) {
    QueryResults<Key> keys = ds.run(
        Query.newKeyQueryBuilder().setKind(kind).build());
    List<Key> toDelete = new ArrayList<>();
    while (keys.hasNext()) {
      toDelete.add(keys.next());
    }
    if (!toDelete.isEmpty()) {
      ds.delete(toDelete.toArray(new Key[0]));
    }
  }
}
