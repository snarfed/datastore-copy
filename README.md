# datastore-copy

An [Apache Beam](https://beam.apache.org/) pipeline that copies [Firestore in Datastore mode](https://cloud.google.com/datastore/docs) entities of one or more kinds from one Google Cloud project to another, with an optional filter. Runs locally via the DirectRunner or on [Google Cloud Dataflow](https://cloud.google.com/dataflow).

## Background

Google Cloud provides a set of [provided Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates) for common data movement tasks, including:

- [Firestore to Cloud Storage Text](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Firestore_to_GCS_Text.md) — reads entities via GQL and writes JSON to GCS
- [Cloud Storage Text to Firestore](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Text_to_Firestore.md) — reads JSON from GCS and writes entities

There is no provided template for copying entities directly from one Datastore project to another. This pipeline fills that gap by combining the Datastore source from the first template with the Datastore sink from the second, skipping the GCS intermediate step, and rewriting entity keys to use the target project.

For general Beam SDK usage on Dataflow, see [Use Apache Beam with Dataflow](https://cloud.google.com/dataflow/docs/guides/use-beam).

## Parameters

| Flag | Required | Description |
|------|----------|-------------|
| `--sourceProject` | yes | Source Firestore in Datastore mode project ID |
| `--kinds` | yes | Comma-separated list of Datastore kinds to copy, e.g. `Kind1,Kind2,Kind3` |
| `--where` | no | GQL WHERE clause applied to all kinds, e.g. `status = 'active'`. Do not include the `WHERE` keyword. |
| `--sourceNamespace` | no | Source namespace (default: empty string, i.e. the default namespace) |
| `--targetProject` | yes | Target Firestore in Datastore mode project ID |
| `--targetNamespace` | no | Target namespace (default: same as source namespace) |

When running on Dataflow, also pass the standard runner flags: `--runner`, `--project`, `--region`, `--tempLocation`.

## Prerequisites

- Java 11+
- [Maven](https://maven.apache.org/)
- [Google Cloud SDK](https://cloud.google.com/sdk) (`gcloud`) authenticated with credentials that have Datastore read access on the source project and write access on the target project

## Building

```sh
mvn package
```

This produces a fat jar at `target/datastore-copy-1.0-SNAPSHOT.jar` containing all dependencies.

## Running locally (DirectRunner)

Uses application default credentials. Run `gcloud auth application-default login` first if needed.

```sh
mvn compile exec:java \
  -Dexec.mainClass=org.snarfed.datastorecopy.DatastoreCopy \
  -Dexec.args="--sourceProject=my-src-project \
               --kinds=Kind1,Kind2,Kind3 \
               --targetProject=my-dst-project"
```

With an optional filter and namespace:

```sh
mvn compile exec:java \
  -Dexec.mainClass=org.snarfed.datastorecopy.DatastoreCopy \
  -Dexec.args="--sourceProject=my-src-project \
               --kinds=Kind1,Kind2,Kind3 \
               --where=\"status = 'active'\" \
               --sourceNamespace=my-namespace \
               --targetProject=my-dst-project \
               --targetNamespace=other-namespace"
```

## Running tests

```sh
mvn test
```

The tests cover `buildQuery` and `RekeyEntity` — all custom logic — using pure in-memory data with no external dependencies.

## Running on Dataflow

Build the fat jar first (`mvn package`), then:

```sh
java -jar target/datastore-copy-1.0-SNAPSHOT.jar \
  --runner=DataflowRunner \
  --project=my-dst-project \
  --region=us-central1 \
  --tempLocation=gs://my-bucket/tmp \
  --sourceProject=my-src-project \
  --kinds=Kind1,Kind2,Kind3 \
  --targetProject=my-dst-project
```

The `--project` and `--region` flags are Dataflow runner options (which project/region hosts the job itself). `--sourceProject` and `--targetProject` are independent and can be any projects your credentials can access.

## Implementation notes

- **Multiple kinds**: each kind is read as a separate `SELECT * FROM {kind}` query (with `WHERE {where}` appended if `--where` is set). The results are flattened into a single collection before being written to the target. Reads for each kind run in parallel.

- **Key rewriting**: Datastore entity keys contain the source project ID (and namespace) in their `PartitionId`. Before writing to the target, `RekeyEntity` rewrites each key's `PartitionId` to use the target project and namespace. Without this step, writes would fail or land in the wrong project.

- **Namespace defaulting**: if `--targetNamespace` is not specified, it defaults to `--sourceNamespace`, so entities land in the same namespace in the target project.

- **No GCS intermediate**: unlike chaining the provided Firestore↔GCS templates, this pipeline keeps entities in their native protobuf form in memory and on Dataflow workers, avoiding serialization to JSON and a GCS staging step.

- **Authentication**: the pipeline uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). The credentials must have `datastore.entities.get` / `datastore.databases.get` on the source project and `datastore.entities.create` / `datastore.entities.update` on the target project.
