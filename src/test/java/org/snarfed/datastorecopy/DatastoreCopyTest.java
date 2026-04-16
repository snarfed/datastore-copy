package org.snarfed.datastorecopy;

import static org.junit.Assert.assertEquals;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.PartitionId;
import org.junit.Test;

public class DatastoreCopyTest {

  @Test
  public void testBuildQuery_noWhere() {
    assertEquals("SELECT * FROM MyKind", DatastoreCopy.buildQuery("MyKind", ""));
  }

  @Test
  public void testBuildQuery_withWhere() {
    assertEquals("SELECT * FROM MyKind WHERE status = 'active'",
        DatastoreCopy.buildQuery("MyKind", "status = 'active'"));
  }

  @Test
  public void testRekeyEntity() {
    Entity entity = Entity.newBuilder()
        .setKey(Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder()
                .setProjectId("source-project")
                .setNamespaceId("source-ns"))
            .addPath(PathElement.newBuilder().setKind("MyKind").setName("my-entity")))
        .build();

    Entity rekeyed = new DatastoreCopy.RekeyEntity("target-project", "target-ns").apply(entity);

    assertEquals(
        Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder()
                .setProjectId("target-project")
                .setNamespaceId("target-ns"))
            .addPath(PathElement.newBuilder().setKind("MyKind").setName("my-entity"))
            .build(),
        rekeyed.getKey());
  }

  @Test
  public void testRekeyEntity_preservesProperties() {
    Entity entity = Entity.newBuilder()
        .setKey(Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder().setProjectId("source-project"))
            .addPath(PathElement.newBuilder().setKind("MyKind").setId(42)))
        .putProperties("name",
            com.google.datastore.v1.Value.newBuilder().setStringValue("Alice").build())
        .build();

    Entity rekeyed = new DatastoreCopy.RekeyEntity("target-project", "").apply(entity);

    assertEquals("Alice", rekeyed.getPropertiesOrThrow("name").getStringValue());
    assertEquals("target-project", rekeyed.getKey().getPartitionId().getProjectId());
    assertEquals(42, rekeyed.getKey().getPath(0).getId());
  }
}
