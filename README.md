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

## Features

- Implements `Search` interface using `TransportClient`
- Implements `DAO` interface so you can use Elasticsearch as a database (avoid in production!)
- Full pagination support for both "search-after" and "from-size" modes
- Proxy endpoint `/v1/_elasticsearch` - relays all requests directly to Elasticsearch (disabled by default)

## Documentation

### [Read the Docs](https://paraio.org/docs)

## Getting started

The plugin is on Maven Central. Here's the Maven snippet to include in your `pom.xml`:

```xml
<dependency>
  <groupId>com.erudika</groupId>
  <artifactId>para-search-elasticsearch</artifactId>
  <version>{see_green_version_badge_above}</version>
</dependency>
```
Alternatively you can download the JAR from the "Releases" tab above put it in a `lib` folder alongside the server
WAR file `para-x.y.z.war`. Para will look for plugins inside `lib` and pick up the Elasticsearch plugin.

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
para.es.shards = 5
para.es.replicas = 0
para.es.dir = "data"
para.es.auto_expand_replicas = "0-1"
para.es.use_transportclient = false
para.es.transportclient_host = "localhost"
para.es.transportclient_port = 9300
# proxy settings
para.es.proxy_enabled = false
para.es.proxy_path = "_elasticsearch"
para.es.restclient_scheme = "http"
para.es.restclient_host = "localhost"
para.es.restclient_port = 9200
```

Finally, set the config property:
```
para.search = "ElasticSearch"
```
This could be a Java system property or part of a `application.conf` file on the classpath.
This tells Para to use the Elasticsearch implementation instead of the default (Lucene).

### Calling Elasticsearch through the proxy endpoint

You can directly call the Elasticsearch API through `/v1/_elasticsearch`. To enable it set `para.es.proxy_enabled = true` first.
Then you must specify the `path` parameter corresponds to the Elasticsearch API resource path. This is done for every
`GET`, `PUT`, `POST`, `PATCH` or `DELETE` request to Elasticsearch:

```
GET /v1/_elasticsearch?path=_search
DELETE /v1/_elasticsearch/tweet%2f1
```
`ParaClient` example:

```java
Response res = paraClient.invokePost("_elasticsearch/_count",
				Entity.json(Collections.singletonMap("query",
										Collections.singletonMap("term",
										Collections.singletonMap("type", "cat")))));
```
If the `path` parameter is omitted, it defaults to `_search`.
The endpoint accepts request to either `/v1/_elasticsearch` or /v1/_elasticsearch/{path}` where `path` is a URL-encoded
path parameter, which can also be supplied as query string (e.g. `/v1/_elasticsearch?path=...`)

**Note:** This endpoint requires authentication and unsigned requests are not allowed. Keep in mind that all requests
to Elasticsearch are prefixed with the app identifier. For example if the app id is "app:myapp, then Para will proxy
requests to Elasticsearch at `http://eshost:9200/myapp/{path}`.

### Requirements

- Elasticsearch official Java client
- [Para Core](https://github.com/Erudika/para)

## License
[Apache 2.0](LICENSE)
