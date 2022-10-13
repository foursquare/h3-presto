# H3-Presto

[![tests](https://github.com/foursquare/h3-presto/workflows/tests/badge.svg)](https://github.com/foursquare/h3-presto/actions)
[![Coverage Status](https://coveralls.io/repos/github/foursquare/h3-presto/badge.svg?branch=main)](https://coveralls.io/github/foursquare/h3-presto?branch=main)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.foursquare.presto/h3-presto/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.foursquare.presto/h3-presto)
[![H3 Version](https://img.shields.io/badge/h3-v4.0.1-blue.svg)](https://github.com/uber/h3/releases/tag/v4.0.1)

This library provides Presto bindings for the [H3 Core Library](https://github.com/uber/h3) via a Presto plugin. For API reference, please see the [H3 Documentation](https://h3geo.org/).

# Usage

You will need to [install the plugin](https://prestodb.io/docs/current/develop/spi-overview.html#deploying-a-custom-plugin) on all nodes of your Presto cluster. For convenience of deployment, the plugin is distributed as a shaded Jar with the dependencies such as H3-Java and Guava packaged in.

Once installed, Presto will automatically load the `H3Plugin` at startup and the functions will then be available from SQL, e.g.:

```sql
SELECT h3_latlng_to_cell(lat, lng, 9) AS hex FROM my_table;
```

# Development

Building the library requires a JDK and Maven. To install to your local Maven cache, run:

```sh
mvn install
```

To build the library, run:

```sh
mvn package
```

To build the documentation site, run:

```sh
mvn site
```

To format source code as required by CI, run:

```sh
mvn com.spotify.fmt:fmt-maven-plugin:format
```

## Release

Releasing uses a process along the lines of [h3-java's release process](https://github.com/uber/h3-java/blob/master/docs/releasing.md).

```sh
mvn release:prepare
mvn release:perform
```

(As this library does not have native build steps, we plan to automate this in the future.)

# Licensing

H3-Presto is licensed under the [Apache 2.0 License](./LICENSE).
Copyright 2022 Foursquare Labs, Inc.
