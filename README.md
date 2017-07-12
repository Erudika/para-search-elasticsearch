![Logo](https://s3-eu-west-1.amazonaws.com/org.paraio/para.png)
============================

> ### Elasticsearch plugin for Para

[![Build Status](https://travis-ci.org/Erudika/para-search-elasticsearch.svg?branch=master)](https://travis-ci.org/Erudika/para-search-elasticsearch)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.erudika/para-search-elasticsearch/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.erudika/para-search-elasticsearch)
[![Join the chat at https://gitter.im/Erudika/para](https://badges.gitter.im/Erudika/para.svg)](https://gitter.im/Erudika/para?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## What is this?

**Para** was designed as a simple and modular back-end framework for object persistence and retrieval.
It enables your application to store objects directly to a data store (NoSQL) or any relational database (RDBMS)
and it also automatically indexes those objects and makes them searchable.

This plugin allows you to use Elasticsearch as the search engine for Para.

## Documentation

### [Read the Docs](https://paraio.org/docs)

## Getting started

The plugin is on Maven Central. Here's the Maven snippet to include in your `pom.xml`:

```xml
<dependency>
  <groupId>com.erudika</groupId>
  <artifactId>para-search-elasticsearch</artifactId>
  <version>{VERSION}</version>
</dependency>
```

### Configuration

Here are all the configuration properties for this plugin (these go inside your `application.conf`):
```
# enable this to bypass the DB and read all data straight from ES
para.read_from_index = false
para.es.async_enabled = false
para.es.cors_enabled = false
para.es.cors_allow_origin = "localhost"
para.es.discovery_type = "ec2"
para.es.discovery_group = "elasticsearch"
para.es.local_node = true
para.es.data_node = true
para.es.shards = 5
para.es.replicas = 0
para.es.dir = "data"
para.es.auto_expand_replicas = "0-1"
para.es.use_transportclient = false
para.es.transportclient_host = "localhost"
para.es.transportclient_port = 9300
```

Add the project as dependency through Maven and set the config property
```
para.search = "ElasticSearch"
```
This could be a Java system property or part of a `application.conf` file on the classpath.
This tells Para to use the Elasticsearch implementation instead of the default.


Alternatively you can build the project with `mvn clean install` and unzip the file `target/para-search-elasticsearch.zip`
into a `lib` folder alongside the server WAR file `para-server.war`. Para will look for plugins inside `lib`
and pick up the Elasticsearch plugin.

### Requirements

- Elasticsearch official Java client
- [Para Core](https://github.com/Erudika/para)

## License
[Apache 2.0](LICENSE)
