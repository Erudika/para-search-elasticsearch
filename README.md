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
- Index sharing and multitenancy support through alias routing and filtering
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
```ini
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

### Indexing modes

This plugin has two indexing modes: **normal** and **nested**. The nested mode was added after v1.28 to protect against
a possible [mapping explosion](https://discuss.elastic.co/t/can-nested-fields-prevent-mapping-explosion/95464) which
happens when there are lots of objects with lots of different custom properties in them. This overloads the Elasticsearch
index metadata and can crash the whole cluster. This indexing mode affects only custom properties in `Sysprop` objects.

The old "normal" mode is suitable for most Para deployments, with just a few tenants or a single tenant
(one app per server). In this mode, Para objects are indexed without modification (all data types are preserved)
but this could lead to a mapping explosion.

The nested data structure for these two indexing modes is shown below:
```
// NORMAL MODE                   // NESTED MODE
{                                {
  "id": "123",                     "id": "123",
  "appid": "para",                 "appid": "para",
  "type": "custom",                "type": "custom",
  "properties": {                  "properties": [
    "key1": "value1",                {"k": "key1",         "v": "value1"},
    "key2": {                        {"k": "key2-subkey1", "v": "subValue1"},
      "subkey1": "subValue1"         {"k": "numericKey3",  "vn": 5}
    },                             ],
    "numericKey3": 5               "_properties": "{\"key1\":\"value1\"}..."
  }                              }
}
```

Switching to the new nested indexing mode is done with the configuration property:
```
para.es.es.use_nested_custom_fields = true
```

Another benefit, when using the "nested" mode, is the support for nested queries in query strings.
This is a really useful feature which, at the time of writing this, has not yet been implemented in Elasticsearch
(issue [elastic/elasticsearch#11322](https://github.com/elastic/elasticsearch/issues/11322)). Even better, you can
query objects within nested arrays with pinpoint precision, e.g. `?q=properties.nestedArray[2].key:value`.
A nested query string query is detected if it contains a field with prefix `properties.*`.
Examples of query string queries:
```
/v1/search?q=term AND properties.owner.age:[* TO 34]
/v1/search?q=properties.owner.name:alice OR properties.owner.pets[1].name=whiskers
```

### Calling Elasticsearch through the proxy endpoint

You can directly call the Elasticsearch API through `/v1/_elasticsearch`. To enable it set `para.es.proxy_enabled = true` first.
Then you must specify the `path` parameter corresponds to the Elasticsearch API resource path. This is done for every
`GET`, `PUT`, `POST`, `PATCH` or `DELETE` request to Elasticsearch. The endpoint accepts request to either
`/v1/_elasticsearch` or `/v1/_elasticsearch/{path}` where `path` is a URL-encoded path parameter.
**Do not add query parameters to the request path with `?`, instead, pass them as a parameter map.**

```
GET /v1/_elasticsearch/_search
GET /v1/_elasticsearch/mytype%2f_search
DELETE /v1/_elasticsearch/tweet%2f1
```
`ParaClient` example:

```java
Response get = paraClient.invokeGet("_elasticsearch/" + Utils.urlEncode("tweet/_search"), params);

Response post = paraClient.invokePost("_elasticsearch/_count",
   Entity.json(Collections.singletonMap("query",
               Collections.singletonMap("term",
               Collections.singletonMap("type", "cat")))));
```
If the `path` parameter is omitted, it defaults to `_search`.

The response object will be transformed to be compatible with Para clients an looks like this:

```js
{
	"page":0,
	"totalHits":3,
	"items":[{...}]
}
```

If you wish to get the raw query response from Elasticsearch, add the parameter `getRawResponse=true` to the requst
path and also URL-encode it:
```
GET /v1/_elasticsearch/mytype%2f_search%3FgetRawResponse%3Dtrue
```
Equivalently, the same can be done by adding the query parameter using `ParaClient`:
```
MultivaluedHashMap<String, String> params = new MultivaluedHashMap<>();
params.putSingle("getRawRequest", "true");
paraClient.invokeGet("_elasticsearch/" + Utils.urlEncode("mytype/_search"), params);
```

**Note:** This endpoint requires authentication and unsigned requests are not allowed. Keep in mind that all requests
to Elasticsearch are prefixed with the app identifier. For example if the app id is "app:myapp, then Para will proxy
requests to Elasticsearch at `http://eshost:9200/myapp/{path}`.

### Rebuilding indices through the Elasticsearch proxy endpoint

You can rebuild the whole app index from scratch by calling `POST /v1/_elasticsearch/reindex`. To enable it set
`para.es.proxy_reindexing_enabled = true` first. This operation executes `ElasticSearchUtils.rebuildIndex()` internally,
and returns a response indicating the number of reindexed objects and the elapsed time:

```
{
   "reindexed": 154,
   "tookMillis": 365
}
```

### Shared indices with alias routing

The plugin also supports index sharing, whereby the root app index is shared with other apps which are created with the
flag `app.isSharingIndex = true`. This feature is enabled with `para.es.root_index_sharing_enabled = true` and it is off
by default. When the root index is created with sharing enabled, a special alias is created for it that contains a
routing field which sends all documents of a child app to a particular shard, while providing total isolation between
apps. This is useful when there are lots of smaller apps with just a few hundred documents each and we want to avoid the
overhead of one index per app.

### Requirements

- Elasticsearch official Java client
- [Para Core](https://github.com/Erudika/para)

## License
[Apache 2.0](LICENSE)
