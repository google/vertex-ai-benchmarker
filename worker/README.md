# Vertex AI Benchmarker Worker

This Java app servers as a load generator for benchmarking a Vertex AI Feature
Store.

Generally, the worker should not be invoked directly but should be used through
the provided Python CLI utilities.

## Usage

The Gradle Wrapper can be used to execute the worker.

```sh
./gradlew run
```

## Dependency Installation

### Gradle

If not using the Gradle Wrapper (`gradlew`), you'll need to install
[gradle](https://gradle.org/).

For linux:

```
sudo apt install gradle
```

For mac:

Follow directions at https://gradle.org/install/

## Details

`gradle run` by default will run a gradle task that runs
`featurestoreloadtest/app/src/main/java/featurestoreloadtest/app/App.java`

App.java accesses GCP using default application credentials.

Before running App.java, ensure gcloud is installed and set default application
credentials.
