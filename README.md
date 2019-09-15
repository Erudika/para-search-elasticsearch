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

- Implements the Para `Search` interface using the high level REST client (HLRC)
- Index sharing and multitenancy support through alias routing and filtering
- Supports both asynchronous and synchronous document operations
- Full pagination support for both "search-after" and "from-size" modes
- Proxy endpoint `/v1/_elasticsearch` - relays all requests directly to Elasticsearch (disabled by default)
- Supports AWS Elasticsearch Service with request signing

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
para.es.restclient_scheme = "http"
para.es.restclient_host = "localhost"
para.es.restclient_port = 9200
# context path of elasticsearch e.g. /es
para.es.restclient_context_path = ""
para.es.sign_requests_to_aws = false
para.es.aws_region = "eu-west-1"
para.es.fail_on_indexing_errors = false
para.es.track_total_hits = 10000
# if login and password are filled then add for each request
# the Authorization header with basic auth
para.es.basic_auth_login = ""
para.es.basic_auth_password = ""

# asynchronous settings
para.es.async_enabled = false
para.es.bulk.size_limit_mb = 5
para.es.bulk.action_limit = 1000
para.es.bulk.concurrent_requests = 1
para.es.bulk.flush_interval_ms = 5000
para.es.bulk.backoff_initial_delay_ms = 50
para.es.bulk.max_num_retries = 8
para.es.bulk.flush_immediately = false

# proxy settings
para.es.proxy_enabled = false
para.es.proxy_path = "_elasticsearch"
para.es.proxy_reindexing_enabled = false
```

Finally, set the config property:
```
para.search = "ElasticSearch"
```
This could be a Java system property or part of a `application.conf` file on the classpath.
This tells Para to use the Elasticsearch implementation instead of the default (Lucene).

### Synchronous versus Asynchronous Indexing
The Elasticsearch plugin supports both synchronous (default) and asynchronous indexing modes.
For synchronous indexing, the Elasticsearch plugin will make a single, blocking request through the client
and wait for a response. This means each document operation (index, reindex, or delete) invokes
a new client request. For certain applications, this can induce heavy load on the Elasticsearch cluster.
The advantage of synchronous indexing, however, is the result of the request can be communicated back
to the client application. If the setting `para.es.fail_on_indexing_errors` is set to `true`, synchronous
requests that result in an error will propagate back to the client application with an HTTP error code.

The asynchronous indexing mode uses the Elasticsearch BulkProcessor for batching all requests to the Elasticsearch
cluster. If the asynchronous mode is enabled, all document requests will be fed into the BulkProcessor, which
will flush the requests to the cluster on occasion. There are several configurable parameters to control the
flush frequency based on document count, total document size (MB), and total duration (ms). Since Elasticsearch
is designed as a near real-time search engine, the asynchronous mode is highly recommended. Making occasional,
larger batches of document requests will help reduce the load on the Elasticsearch cluster.

The asynchronous indexing mode also offers an appealing feature to automatically retry failed indexing requests. If
your Elasticsearch cluster is under heavy load, it's possible a request to index new documents may be rejected. With
synchronous indexing, the burden falls on the client application to try the indexing request again. The Elasticsearch
BulkProcessor, however, offers a useful feature to automatically retry indexing requests with exponential
backoff between retries. If the index request fails with a `EsRejectedExecutionException`, the request
will be retried up to `para.es.bulk.max_num_retries` times. Even if your use case demands a high degree
of confidence with respect to data consistency between your database (`DAO`) and index (`Search`), it's still
recommended to use asynchronous indexing with retries enabled. If you'd prefer to use asynchronous indexing but have
the BulkProcessor flushed upon every invocation of index/unindex/indexAll/unindexAll, simply enabled
`para.es.bulk.flush_immediately`. When this option is enabled, the BulkProcessor's flush method will be called
immediately after adding the documents in the request. This option is also useful for writing unit tests where you
want ensure the documents flush promptly.

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
**Note:** Sorting on nested fields works only with numeric data. For example, sorting on a field `properties.year` will
work, but sorting on `properties.month` won't (applicable only to the "nested" mode).

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

Additionally, you can specify the destination index to reindex into, which must have been created beforehand:
```
POST /v1/_elasticsearch/reindex?destinationIndex=yourCustomIndex
```

### Shared indices with alias routing

The plugin also supports index sharing, whereby the root app index is shared with other apps which are created with the
flag `app.isSharingIndex = true`. This feature is enabled with `para.es.root_index_sharing_enabled = true` and it is off
by default. When the root index is created with sharing enabled, a special alias is created for it that contains a
routing field which sends all documents of a child app to a particular shard, while providing total isolation between
apps. This is useful when there are lots of smaller apps with just a few hundred documents each and we want to avoid the
overhead of one index per app.

### Deprecation notice

- Support for the ES Transport client has been removed because it is now deprecated and removed in ES 7.0.
- The previously bundled `IndexBasedDAO` has been removed because it had lots of issues.

### Requirements

- Elasticsearch official Java client
- [Para Core](https://github.com/Erudika/para)

## License
[Apache 2.0](LICENSE)
