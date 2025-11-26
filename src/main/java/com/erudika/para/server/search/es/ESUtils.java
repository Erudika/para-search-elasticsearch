/*
 * Copyright 2013-2022 Erudika. http://erudika.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For issues and patches go to: https://github.com/erudika
 */
package com.erudika.para.server.search.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.HealthStatus;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.TrackHits;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.elasticsearch.indices.update_aliases.AddAction;
import co.elastic.clients.elasticsearch.indices.update_aliases.RemoveAction;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import co.elastic.clients.util.ObjectBuilder;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Sysprop;
import com.erudika.para.core.listeners.DestroyListener;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.rest.Signer;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.core.utils.Utils;
import com.github.davidmoten.aws.lw.client.Credentials;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class ESUtils {

	private static final Logger logger = LoggerFactory.getLogger(ESUtils.class);
	private static ElasticsearchClient restClient;
	private static ElasticsearchAsyncClient restClientAsync;
	private static final int MAX_QUERY_DEPTH = 10; // recursive depth for compound queries - bool, boost
	private static final String DATE_FORMAT = "epoch_millis||epoch_second||yyyy-MM-dd HH:mm:ss||"
			+ "yyyy-MM-dd||yyyy/MM/dd||yyyyMMdd||yyyy";

	static final String PROPS_FIELD = "properties";
	static final String PROPS_PREFIX = PROPS_FIELD + ".";
	static final String PROPS_JSON = "_" + PROPS_FIELD;
	static final String PROPS_REGEX = "(^|.*\\W)" + PROPS_FIELD + "[\\.\\:].+";

	/**
	 * Switches between normal indexing and indexing with nested key/value objects for Sysprop.properties.
	 * When this is 'false' (normal mode), Para objects will be indexed without modification but this could lead to
	 * a field mapping explosion and crash the ES cluster.
	 *
	 * When set to 'true' (nested mode), Para objects will be indexed with all custom fields flattened to an array of
	 * key/value properties: properties: [{"k": "field", "v": "value"},...]. This is done for Sysprop objects with
	 * containing custom properties. This mode prevents an eventual field mapping explosion.
	 */
	static boolean nestedMode() {
		return Para.getConfig().elasticsearchNestedModeEnabled();
	}

	/**
	 * @return true if asynchronous indexing/unindexing is enabled.
	 */
	static boolean asyncEnabled() {
		return Para.getConfig().elasticsearchAsyncModeEnabled();
	}

	/**
	 * @return true if we want the bulk processor to flush immediately after each bulk request.
	 */
	static boolean flushImmediately() {
		return Para.getConfig().elasticsearchBulkFlushEnabled();
	}

	/**
	 * A list of default mappings that are defined upon index creation.
	 * @return the default mapping properties map
	 */
	public static Map<String, Property> getDefaultMapping() {
		Map<String, Property> props = new HashMap<String, Property>();
		props.put("nstd", Property.of(p -> p.nested(n -> n.enabled(true))));
		props.put("properties", Property.of(p -> {
				if (nestedMode()) {
					p.nested(n -> n.enabled(true));
				} else {
					p.object(n -> n.enabled(true));
				}
				return p;
			}
		));
		props.put("latlng", Property.of(p -> p.geoPoint(n -> n.nullValue(v -> v.text("0,0")))));
		props.put("_docid", Property.of(p -> p.long_(n -> n.index(false))));
		props.put("updated", Property.of(p -> p.date(n -> n.format(DATE_FORMAT))));
		props.put("timestamp", Property.of(p -> p.date(n -> n.format(DATE_FORMAT))));

		props.put("tag", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("id", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("key", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("name", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("type", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("tags", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("token", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("email", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("appid", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("groups", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("password", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("parentid", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("creatorid", Property.of(p -> p.keyword(n -> n.index(true))));
		props.put("identifier", Property.of(p -> p.keyword(n -> n.index(true))));
		return props;
	}

	/**
	 * These fields are not indexed.
	 */
	private static final String[] IGNORED_FIELDS = new String[] {
		"settings", // App
		"datatypes", // App
		"deviceState", // Thing
		"deviceMetadata", // Thing
		"resourcePermissions", // App
		"validationConstraints" // App
	};

	private ESUtils() { }

	/**
	 * Creates an instance of the high-level REST client that talks to Elasticsearch.
	 * @return a RestHighLevelClient instance
	 */
	public static ElasticsearchClient getRESTClient() {
		if (restClient != null) {
			return restClient;
		}
		String esScheme = Para.getConfig().elasticsearchRestClientScheme();
		String esHost = Para.getConfig().elasticsearchRestClientHost();
		int esPort = Para.getConfig().elasticsearchRestClientPort();
		boolean signRequests = Para.getConfig().elasticsearchSignRequestsForAwsEnabled();

		HttpHost host = new HttpHost(esScheme, esHost, esPort);
		Rest5ClientBuilder clientBuilder = Rest5Client.builder(host);

		String esPrefix = Para.getConfig().elasticsearchRestClientContextPath();
		if (StringUtils.isNotEmpty(esPrefix)) {
			clientBuilder.setPathPrefix(esPrefix);
		}

		// register all customizations
		clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
			if (signRequests) {
				addAWSRequestSigningInterceptor(httpClientBuilder, host.getSchemeName() + "://" + host.getHostName());
			}
			addAuthenticationCallback(httpClientBuilder);

//			if (esHost.startsWith("localhost") || !Para.getConfig().inProduction()) {
//				httpClientBuilder.setSSLHostnameVerifier((hostname, session) -> true);
//			}
		});

		// Create the transport with a Jackson mapper
		Rest5ClientTransport transport = new Rest5ClientTransport(clientBuilder.build(), new JacksonJsonpMapper());
		restClient = new ElasticsearchClient(transport);
		restClientAsync = new ElasticsearchAsyncClient(transport);

		Para.addDestroyListener(new DestroyListener() {
			public void onDestroy() {
				shutdownClient();
			}
		});
		if (!existsIndex(Para.getConfig().getRootAppIdentifier())) {
			createIndex(Para.getConfig().getRootAppIdentifier());
		}
		return restClient;
	}

	/**
	 * Stops the client instance and releases resources.
	 */
	protected static void shutdownClient() {
		if (restClient != null) {
			try {
				restClient._transport().close();
				restClientAsync._transport().close();
			} catch (IOException ex) {
				logger.error(null, ex);
			}
		}
	}

	private static ElasticsearchAsyncClient getAsyncRESTClient() {
		if (restClientAsync == null) {
			getRESTClient();
		}
		return restClientAsync;
	}

	private static boolean createIndexWithoutAlias(String name, int shards, int replicas) {
		if (StringUtils.isBlank(name) || StringUtils.containsWhitespace(name) || existsIndex(name)) {
			return false;
		}
		try {
			if (shards <= 0) {
				shards = Para.getConfig().elasticsearchRootIndexShards();
			}
			if (replicas < 0) {
				replicas = Para.getConfig().elasticsearchRootIndexReplicas();
			}
			final int numShards = shards;
			final int numReplicas = replicas;
			IndexSettings settings = IndexSettings.of(b -> {
				b.numberOfShards(Integer.toString(numShards));
				b.numberOfReplicas(Integer.toString(numReplicas));
				b.autoExpandReplicas(Para.getConfig().elasticsearchAutoExpandReplicas());
				return b;
			});
			// create index with default system mappings; ES allows only one type per index
			getRESTClient().indices().create(b -> b.index(name).settings(settings).
					mappings(TypeMapping.of(t -> t.properties(getDefaultMapping()))));
			logger.info("Created a new index '{}' with {} shards, {} replicas.", name, shards, replicas);
		} catch (Exception e) {
			logger.warn(null, e);
			return false;
		}
		return true;
	}

	/**
	 * Creates a new search index.
	 * @param appid the index name (alias)
	 * @return true if created
	 */
	public static boolean createIndex(String appid) {
		return createIndex(appid, Para.getConfig().elasticsearchRootIndexShards(), Para.getConfig().elasticsearchRootIndexReplicas());
	}

	/**
	 * Creates a new search index.
	 * @param appid the index name (alias)
	 * @param shards number of shards
	 * @param replicas number of replicas
	 * @return true if created
	 */
	public static boolean createIndex(String appid, int shards, int replicas) {
		if (StringUtils.isBlank(appid)) {
			return false;
		}
		String indexName = appid.trim() + "_1";
		boolean created = createIndexWithoutAlias(indexName, shards, replicas);
		if (created) {
			boolean withAliasRouting = App.isRoot(appid) && Para.getConfig().elasticsearchRootIndexSharingEnabled();
			boolean aliased = addIndexAlias(indexName, appid, withAliasRouting);
			if (!aliased) {
				logger.info("Created ES index '{}' without an alias '{}'.", indexName, appid);
			} else {
				logger.info("Created ES index '{}' with alias '{}'.", indexName, appid);
			}
		}
		return created;
	}

	/**
	 * Deletes an existing search index.
	 * @param appid the index name (alias)
	 * @return true if deleted
	 */
	public static boolean deleteIndex(String appid) {
		if (StringUtils.isBlank(appid) || !existsIndex(appid)) {
			return false;
		}
		try {
			// wildcard deletion might fail if "action.destructive_requires_name" is "true"
			String indexName = getIndexNameForAlias(appid.trim());
			getRESTClient().indices().delete(b -> b.index(indexName));
			logger.info("Deleted ES index '{}'.", indexName);
		} catch (Exception e) {
			logger.warn(null, e);
			return false;
		}
		return true;
	}

	/**
	 * Checks if the index exists.
	 * @param appid the index name (alias)
	 * @return true if exists
	 */
	public static boolean existsIndex(String appid) {
		if (StringUtils.isBlank(appid)) {
			return false;
		}
		// don't assume false, might be distructive!
		boolean exists;
		try {
			String indexName = appid.trim();
			exists = getRESTClient().indices().exists(b -> b.index(indexName)).value();
		} catch (Exception e) {
			logger.warn(null, e);
			exists = false;
		}
		return exists;
	}

	/**
	 * Rebuilds an index.
	 * Reads objects from the data store and indexes them in batches.
	 * Works on one DB table and index only.
	 * @param dao DAO for connecting to the DB - the primary data source
	 * @param app an app
	 * @param destinationIndex the new index where data will be reindexed to
	 * @param pager a Pager instance
	 * @return true if successful, false if index doesn't exist or failed.
	 */
	public static boolean rebuildIndex(DAO dao, App app, String destinationIndex, Pager... pager) {
		Objects.requireNonNull(dao, "DAO object cannot be null!");
		Objects.requireNonNull(app, "App object cannot be null!");
		if (StringUtils.isBlank(app.getAppIdentifier())) {
			return false;
		}
		try {
			String indexName = app.getAppIdentifier().trim();
			if (!existsIndex(indexName)) {
				if (app.isSharingIndex()) {
					// add alias pointing to the root index
					addIndexAliasWithRouting(getIndexName(Para.getConfig().getRootAppIdentifier()), app.getAppIdentifier());
				} else {
					logger.info("Creating '{}' index because it doesn't exist.", indexName);
					createIndex(indexName);
				}
			}
			String oldName = getIndexNameForAlias(indexName);
			String newName = indexName;

			if (!app.isSharingIndex()) {
				if (StringUtils.isBlank(destinationIndex)) {
					newName = getNewIndexName(indexName, oldName);
					createIndexWithoutAlias(newName, -1, -1); // use defaults
				} else {
					newName = destinationIndex;
				}
			}

			List<BulkOperation> batch = new LinkedList<>();
			Pager p = getPager(pager);
			int batchSize = Para.getConfig().reindexBatchSize(p.getLimit());
			if (batchSize <= 0) {
				batchSize = p.getLimit();
			}
			long reindexedCount = 0;
			List<ParaObject> list;
			final String newIndex = newName;
			do {
				list = dao.readPage(app.getAppIdentifier(), p); // use app identifier without trimming it
				logger.debug("rebuildIndex(): Read {} objects from table {}.", list.size(), indexName);
				for (final ParaObject obj : list) {
					if (obj != null) {
						// put objects from DB into the newly created index
						batch.add(BulkOperation.of(b -> b.index(i -> i.
								index(newIndex).id(obj.getId()).
								document(getSourceFromParaObject(obj)))));
						// index in batches of ${queueSize} objects
						if (batch.size() >= batchSize) {
							reindexedCount += batch.size();
							executeRequests(batch);
							logger.debug("rebuildIndex(): indexed {}", batch.size());
							batch.clear();
						}
					}
				}
			} while (!list.isEmpty());

			// anything left after loop? index that too
			if (!batch.isEmpty()) {
				reindexedCount += batch.size();
				executeRequests(batch);
				logger.debug("rebuildIndex(): indexed {}", batch.size());
			}

			if (!app.isSharingIndex()) {
				// switch to alias NEW_INDEX -> ALIAS, OLD_INDEX -> DELETE old index
				switchIndexToAlias(oldName, newName, indexName, true);
			}
			logger.info("rebuildIndex(): {} objects reindexed in '{}' [shared: {}].",
					reindexedCount, indexName, app.isSharingIndex());
		} catch (Exception e) {
			logger.warn(null, e);
			return false;
		}
		return true;
	}

	/**
	 * Executes a synchronous index refresh request. Also flushes
	 * @param appid the appid / index alias
	 * @throws IOException exception
	 */
	public static void refreshIndex(String appid) throws IOException {
		if (!StringUtils.isBlank(appid)) {
//			if (asyncEnabled()) {
//				bulkProcessor(getRESTClient()).flush();
//			}
			getRESTClient().indices().refresh(b -> b.index(getIndexName(appid)));
		}
	}

	/**
	 * Executes a delete_by_query request to ES and refreshes the index.
	 * @param appid the appid / index alias
	 * @param fb query
	 * @return number of unindexed documents.
	 */
	public static long deleteByQuery(String appid, QueryVariant fb) {
		return deleteByQuery(appid, fb, asyncEnabled() ? (res) -> {
			logger.debug("Unindexed {}", res.total());
			if (!res.failures().isEmpty()) {
				logger.error("Delete by query reqest failed for app '" + appid + "' - {}",
						res.failures().iterator().next().cause().reason());
			}
		} : null);
	}

	/**
	 * Executes a delete_by_query request to ES and refreshes the index.
	 * @param appid the appid / index alias
	 * @param fb query
	 * @param cb callback
	 * @return number of unindexed documents.
	 */
	public static long deleteByQuery(String appid, QueryVariant fb, Consumer<DeleteByQueryResponse> cb) {
		int batchSize = 1000;
		boolean isSharingIndex = !App.isRoot(appid) && Strings.CS.startsWith(appid, " ");
		String indexName = getIndexName(appid);
		DeleteByQueryRequest.Builder deleteByQueryReq = new DeleteByQueryRequest.Builder();
		deleteByQueryReq.index(indexName);
		deleteByQueryReq.conflicts(Conflicts.Proceed);
		deleteByQueryReq.query(fb._toQuery());
//		deleteByQueryReq.BatchSize(batchSize);
		deleteByQueryReq.slices(s -> s.value(1)); // parallelize operation?
		deleteByQueryReq.scroll(Time.of(t -> t.time("10m")));
		deleteByQueryReq.refresh(true);
		if (isSharingIndex) {
			deleteByQueryReq.routing(indexName);
		}
		if (cb != null) {
//			getRESTClient().deleteByQueryAsync(deleteByQueryReq, RequestOptions.DEFAULT, cb);
			getAsyncRESTClient().deleteByQuery(deleteByQueryReq.build()).thenAccept(cb);
		} else {
			DeleteByQueryResponse res;
			try {
				res = getRESTClient().deleteByQuery(deleteByQueryReq.build());
				if (!res.failures().isEmpty()) {
					logger.warn("Failures in deleteByQuery() - {}", res.failures().iterator().next().cause().reason());
				}
				return res.total();
			} catch (IOException ex) {
				logger.error(null, ex);
			}
		}
		return 0L;
	}

	/**
	 * @param pager an array of optional Pagers
	 * @return the first {@link Pager} object in the array or a new Pager
	 */
	protected static Pager getPager(Pager[] pager) {
		return (pager != null && pager.length > 0) ? pager[0] : new Pager();
	}

	/**
	 * The {@code pager.sortBy} can contain comma-separated sort fields. For example "name,timestamp".
	 * It can also contain sort orders for each field, for example: "name:asc,timestamp:desc".
	 * @param pager a {@link Pager} object
	 * @return a list of ES SortBuilder objects for sorting the results of a search request
	 */
	protected static List<SortOptions> getSortFieldsFromPager(Pager pager) {
		if (pager == null) {
			pager = new Pager();
		}
		SortOrder defaultOrder = pager.isDesc() ? SortOrder.Desc : SortOrder.Asc;
		if (pager.getSortby().contains(",")) {
			String[] fields = pager.getSortby().split(",");
			ArrayList<SortOptions> sortFields = new ArrayList<>(fields.length);
			for (String field : fields) {
				SortOrder order;
				String fieldName;
				if (field.endsWith(":asc")) {
					order = SortOrder.Asc;
					fieldName = field.substring(0, field.indexOf(":asc")).trim();
				} else if (field.endsWith(":desc")) {
					order = SortOrder.Desc;
					fieldName = field.substring(0, field.indexOf(":desc")).trim();
				} else {
					order = defaultOrder;
					fieldName = field.trim();
				}
				if (nestedMode() && fieldName.startsWith(PROPS_PREFIX)) {
					sortFields.add(getNestedFieldSort(fieldName, order));
				} else {
					sortFields.add(SortOptions.of(b -> b.field(f -> f.field(fieldName).order(order))));
				}
			}
			return sortFields;
		} else if (StringUtils.isBlank(pager.getSortby())) {
			return Collections.singletonList(SortOptions.of(b -> b.score(s -> s.order(defaultOrder))));
		} else {
			String fieldName = pager.getSortby();
			if (nestedMode() && fieldName.startsWith(PROPS_PREFIX)) {
				return Collections.singletonList(getNestedFieldSort(fieldName, defaultOrder));
			} else {
				return Collections.singletonList(SortOptions.of(b -> b.field(f -> f.field(fieldName).order(defaultOrder))));
			}
		}
	}

	private static SortOptions getNestedFieldSort(String fieldName, SortOrder order) {
		// nested sorting works only on numeric fields (sorting on properties.v requires fielddata enabled)
		return SortOptions.of(b -> b.field(f -> f.field(PROPS_FIELD + ".vn").
						order(order).
						nested(n -> n.path(PROPS_FIELD).
						filter(nf -> nf.term(t -> t.field(PROPS_FIELD + ".k").
						value(fv -> fv.stringValue(Strings.CS.removeStart(fieldName, PROPS_FIELD + "."))))))));
	}

	/**
	 * Adds a new alias to an existing index with routing and filtering by appid.
	 * @param indexName the index name
	 * @param aliasName the alias
	 * @return true if acknowledged
	 */
	public static boolean addIndexAliasWithRouting(String indexName, String aliasName) {
		return addIndexAlias(indexName, aliasName, true);
	}

	/**
	 * Adds a new alias to an existing index.
	 * @param indexName the index name
	 * @param aliasName the alias
	 * @param withAliasRouting enables alias routing for index with filtering by appid
	 * @return true if acknowledged
	 */
	public static boolean addIndexAlias(String indexName, String aliasName, boolean withAliasRouting) {
		if (StringUtils.isBlank(aliasName) || !existsIndex(indexName)) {
			return false;
		}
		try {
			String alias = aliasName.trim();
			String index = getIndexNameWithWildcard(indexName.trim());
			return getRESTClient().indices().updateAliases(b -> b.actions(a -> {
				if (withAliasRouting) {
					return a.add(aa -> aa.index(index).alias(alias).
							searchRouting(alias).indexRouting(alias).
							filter(QueryBuilders.term().
									field(Config._APPID).
									value(FieldValue.of(aliasName)).build()._toQuery())); // DO NOT trim filter query!
				}
				return a.add(aa -> aa.index(index).alias(alias));
			})).acknowledged();
		} catch (Exception e) {
			logger.error(null, e);
			return false;
		}
	}

	/**
	 * Removes an alias from an index.
	 * @param indexName the index name
	 * @param aliasName the alias
	 * @return true if acknowledged
	 */
	public static boolean removeIndexAlias(String indexName, String aliasName) {
		if (StringUtils.isBlank(aliasName) || !existsIndex(indexName)) {
			return false;
		}
		String alias = aliasName.trim();
		try {
			String index = getIndexNameWithWildcard(indexName.trim());
			return getRESTClient().indices().updateAliases(b -> b.
					actions(a -> a.remove(r -> r.index(index).alias(alias)))).acknowledged();
		} catch (Exception e) {
			logger.warn("Failed to remove index alias '" + alias + "' for index " + indexName + ": {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Checks if an index has a registered alias.
	 * @param indexName the index name
	 * @param aliasName the alias
	 * @return true if alias is set on index
	 */
	public static boolean existsIndexAlias(String indexName, String aliasName) {
		if (StringUtils.isBlank(indexName) || StringUtils.isBlank(aliasName)) {
			return false;
		}
		try {
			String alias = aliasName.trim();
			String index = getIndexNameWithWildcard(indexName.trim());
			return getRESTClient().indices().existsAlias(b -> b.index(index).name(alias)).value();
		} catch (Exception e) {
			logger.error(null, e);
			return false;
		}
	}

	/**
	 * Replaces the index to which an alias points with another index.
	 * @param oldIndex the index name to be replaced
	 * @param newIndex the new index name to switch to
	 * @param alias the alias (unchanged)
	 * @param deleteOld if true will delete the old index completely
	 */
	public static void switchIndexToAlias(String oldIndex, String newIndex, String alias, boolean deleteOld) {
		if (StringUtils.isBlank(oldIndex) || StringUtils.isBlank(newIndex) || StringUtils.isBlank(alias)) {
			return;
		}
		try {
			String aliaz = alias.trim();
			String oldName = oldIndex.trim();
			String newName = newIndex.trim();
			logger.info("Switching index aliases {}->{}, deleting '{}': {}", aliaz, newIndex, oldIndex, deleteOld);
			Action removeAction = RemoveAction.of(b -> b.index(oldName).alias(aliaz))._toAction();
			Action addAction = AddAction.of(b -> b.index(newName).alias(aliaz))._toAction();
			getRESTClient().indices().updateAliases(b -> b.actions(removeAction, addAction));
			// delete the old index
			if (deleteOld) {
				deleteIndex(oldName);
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
	}

	/**
	 * Returns the real index name for a given alias.
	 * @param appid the index name (alias)
	 * @return the real index name (not alias)
	 */
	public static String getIndexNameForAlias(String appid) {
		if (StringUtils.isBlank(appid)) {
			return appid;
		}
		try {
			Map<String, IndexAliases> aliases = getRESTClient().indices().getAlias(b -> b.index(appid)).aliases();
			if (!aliases.isEmpty()) {
				return aliases.keySet().iterator().next();
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
		return appid;
	}

	/**
	 * @param appid the index name (alias)
	 * @param oldName old index name
	 * @return a new index name, e.g. "app_15698795757"
	 */
	static String getNewIndexName(String appid, String oldName) {
		if (StringUtils.isBlank(appid)) {
			return appid;
		}
		return (oldName.contains("_") ? oldName.substring(0, oldName.indexOf('_')) : appid) + "_" + Utils.timestamp();
	}

	/**
	 * Executes a batch of write requests.
	 * @param requests a list of index/delete requests,
	 */
	public static void executeRequests(List<BulkOperation> requests) {
		if (requests == null || requests.isEmpty()) {
			return;
		}
		try {
			if (asyncEnabled()) {
				getAsyncRESTClient().bulk(b -> b.operations(requests).
						refresh(flushImmediately() ? Refresh.True : Refresh.False)).thenAccept(b -> {
					if (b.errors()) {
						b.items().stream().filter(i -> i.status() != 200).forEach(item -> {
							//FUTURE: Increment counter metric for failed document indexing
							logger.error("Failed to execute async {} operation for index '{}', document id '{}': {}",
									item.operationType(), item.index(), item.id(), item.error().reason());
						});
					}
				});
			} else {
				BulkResponse res = getRESTClient().bulk(b -> b.operations(requests).
						refresh(flushImmediately() ? Refresh.True : Refresh.False));
				if (res.errors()) {
					res.items().stream().filter(i -> i.status() != 200).forEach(item -> {
						//FUTURE: Increment counter metric for failed document indexing
						logger.error("Failed to execute sync {} operation for index '{}', document id '{}': {}",
								item.operationType(), item.index(), item.id(), item.error().reason());
					});
					handleFailedRequests();
				}
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
	}

	private static void handleFailedRequests() {
		if (Para.getConfig().exceptionOnWriteErrorsEnabled()) {
			throw new RuntimeException("Synchronous indexing operation failed!");
		}
	}

	/**
	 * Check if cluster status is green or yellow.
	 * @return false if status is red
	 */
	public static boolean isClusterOK() {
		try {
			HealthStatus status = getRESTClient().cluster().health().status();
			return !HealthStatus.Red.equals(status);
		} catch (Exception e) {
			logger.error(null, e);
		}
		return false;
	}

	/**
	 * Creates a term filter for a set of terms.
	 * @param terms some terms
	 * @param mustMatchAll if true all terms must match ('AND' operation)
	 * @return the filter
	 */
	static QueryVariant getTermsQuery(Map<String, ?> terms, boolean mustMatchAll) {
		BoolQuery.Builder fb = QueryBuilders.bool();
		int addedTerms = 0;
		boolean noop = true;
		QueryVariant bfb = null;

		for (Map.Entry<String, ?> term : terms.entrySet()) {
			Object val = term.getValue();
			if (!StringUtils.isBlank(term.getKey()) && val != null && Utils.isBasicType(val.getClass())) {
				String stringValue = val.toString();
				if (StringUtils.isBlank(stringValue)) {
					continue;
				}
				Matcher matcher = Pattern.compile(".*(<|>|<=|>=)$").matcher(term.getKey().trim());
				if (matcher.matches()) {
					bfb = range(matcher.group(1), term.getKey(), stringValue);
				} else {
					if (nestedMode()) {
						bfb = (QueryVariant) term(new org.apache.lucene.search.
								TermQuery(new Term(term.getKey(), stringValue))).build();
					} else {
						bfb = QueryBuilders.term().field(term.getKey()).value(v -> v.stringValue(stringValue)).build();
					}
				}
				if (mustMatchAll) {
					fb.must(bfb._toQuery());
				} else {
					fb.should(bfb._toQuery());
				}
				addedTerms++;
				noop = false;
			}
		}
		if (addedTerms == 1 && bfb != null) {
			return bfb;
		}
		return noop ? null : fb.build();
	}

	/**
	 * Tries to parse a query string in order to check if it is valid.
	 * @param query a Lucene query string
	 * @return the query if valid, or '*' if invalid
	 */
	static String qs(String query) {
		if (StringUtils.isBlank(query) || "*".equals(query.trim())) {
			return "*";
		}
		query = query.trim();
		if (query.length() > 1 && query.startsWith("*")) {
			query = query.substring(1);
		}
		try {
			StandardQueryParser parser = new StandardQueryParser();
			parser.setAllowLeadingWildcard(false);
			parser.parse(query, "");
		} catch (Exception ex) {
			logger.warn("Failed to parse query string '{}'.", query);
			query = "*";
		}
		return query.trim();
	}

	static Query qsParsed(String query) {
		if (StringUtils.isBlank(query) || "*".equals(query.trim())) {
			return null;
		}
		try {
			StandardQueryParser parser = new StandardQueryParser();
			parser.setAllowLeadingWildcard(false);
			return parser.parse(query, "");
		} catch (Exception ex) {
			logger.warn("Failed to parse query string '{}'.", query);
		}
		return null;
	}

	public static boolean isValidQueryString(String query) {
		if (StringUtils.isBlank(query)) {
			return false;
		}
		if ("*".equals(query.trim())) {
			return true;
		}
		try {
			StandardQueryParser parser = new StandardQueryParser();
			parser.setAllowLeadingWildcard(false);
			parser.parse(query, "");
			return true;
		} catch (QueryNodeException ex) {
			return false;
		}
	}

	/**
	 * Converts a {@link ParaObject} to a map of fields and values.
	 * @param po an object
	 * @return a map of keys and values
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getSourceFromParaObject(ParaObject po) {
		if (po == null) {
			return Collections.emptyMap();
		}
		Map<String, Object> data = ParaObjectUtils.getAnnotatedFields(po, null, false);
		Map<String, Object> source = new HashMap<>(data.size() + 1);
		source.putAll(data);
		if (nestedMode() && po instanceof Sysprop) {
			try {
				Map<String, Object> props = (Map<String, Object>) data.get(PROPS_FIELD);
				// flatten properites object to array of keys/values, to prevent field mapping explosion
				List<Map<String, Object>> keysAndValues = getNestedProperties(props);
				source.put(PROPS_FIELD, keysAndValues); // overwrite properties object with flattened array
				// special field for holding the original sysprop.properties map as JSON string
				source.put(PROPS_JSON, ParaObjectUtils.getJsonWriterNoIdent().writeValueAsString(props));
			} catch (Exception e) {
				logger.error(null, e);
			}
		}
		for (String field : IGNORED_FIELDS) {
			source.remove(field);
		}
		// special DOC ID field used in "search after"
		source.put("_docid", NumberUtils.toLong(Utils.getNewId()));
		return source;
	}

	static TrackHits getTrackTotalHits() {
		String trackTotalHits = Para.getConfig().elasticsearchTrackTotalHits();
		return TrackHits.of(t -> {
			if (NumberUtils.isDigits(trackTotalHits)) {
				return t.count(NumberUtils.toInt(trackTotalHits, Config.DEFAULT_LIMIT));
			} else if (!Boolean.valueOf(trackTotalHits)) {
				return t.enabled(false);
			}
			return t.enabled(true);
		});
	}

	/**
	 * Flattens a complex object like a property Map ({@code Sysprop.getProperties()}) to a list of key/value pairs.
	 * Rearranges properites to prevent field mapping explosion, for example:
	 * properties: [{k: key1, v: value1}, {k: key2, v: value2}...]
	 * @param objectData original object properties
	 * @param keysAndValues a list of key/value objects, each containing one property
	 * @param fieldPrefix a field prefix, e.g. "properties.key"
	 */
	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> getNestedProperties(Map<String, Object> objectData) {
		if (objectData == null || objectData.isEmpty()) {
			return Collections.emptyList();
		}
		List<Map<String, Object>> keysAndValues = new LinkedList<>();
		LinkedList<Map<String, Object>> stack = new LinkedList<>();
		stack.add(Collections.singletonMap("", objectData));
		while (!stack.isEmpty()) {
			Map<String, Object> singletonMap = stack.pop();
			String prefix = singletonMap.keySet().iterator().next();
			Object value = singletonMap.get(prefix);
			if (value != null) {
				if (value instanceof Map) {
					String pre = (StringUtils.isBlank(prefix) ? "" : prefix + "-");
					for (Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
						addFieldToStack(pre + entry.getKey(), entry.getValue(), stack, keysAndValues);
					}
				} else {
					addFieldToStack(prefix, value, stack, keysAndValues);
				}
			}
		}
		return keysAndValues;
	}

	private static void addFieldToStack(String prefix, Object val, LinkedList<Map<String, Object>> stack,
			List<Map<String, Object>> keysAndValues) {
		if (val instanceof Map) {
			// flatten all nested objects
			stack.push(Collections.singletonMap(prefix, val));
		} else if (val instanceof List) {
			// input array: key: [value1, value2] - [{k: key-0, v: value1}, {k: key-1, v: value2}]
			for (int i = 0; i < ((List) val).size(); i++) {
				stack.push(Collections.singletonMap(prefix + "-" + String.valueOf(i), ((List) val).get(i)));
			}
		} else {
			keysAndValues.add(getKeyValueField(prefix, val));
		}
	}

	private static Map<String, Object> getKeyValueField(String field, Object value) {
		Map<String, Object> propMap = new HashMap<String, Object>(2);
		propMap.put("k", field);
		if (value instanceof Number) {
			propMap.put("vn", value);
		} else {
			// boolean and Date data types are ommited for simplicity
			propMap.put("v", String.valueOf(value));
		}
		return propMap;
	}

	/**
	 * Converts the source of an ES document to {@link ParaObject}.
	 * @param <P> object type
	 * @param source a map of keys and values coming from ES
	 * @return a new ParaObject
	 */
	static <P extends ParaObject> P getParaObjectFromSource(Map<String, Object> source) {
		if (source == null) {
			return null;
		}
		Map<String, Object> data = new HashMap<>(source.size());
		data.putAll(source);
		// retrieve the JSON for the original properties field and deserialize it
		if (nestedMode() && data.containsKey(PROPS_JSON)) {
			try {
				Map<String, Object> props = ParaObjectUtils.getJsonReader(Map.class).
						readValue((String) data.get(PROPS_JSON));
				data.put(PROPS_FIELD, props);
			} catch (Exception e) {
				logger.error(null, e);
			}
			data.remove(PROPS_JSON);
		}
		data.remove("_docid");
		return ParaObjectUtils.setAnnotatedFields(data);
	}

	/**
	 * @param operator operator <,>,<=,>=
	 * @param field field name
	 * @param stringValue field value
	 * @return a range query
	 */
	static QueryVariant range(String operator, String field, String stringValue) {
		Objects.requireNonNull(field);
		String key = field.replaceAll("[<>=\\s]+$", "");
		boolean nestedMode = nestedMode() && field.startsWith(PROPS_PREFIX);
		RangeQuery rq = QueryBuilders.range().untyped(qb -> {
			qb.field(nestedMode ? getValueFieldName(stringValue) : key);
			if (">".equals(operator)) {
				qb.gt(getNumericValue(stringValue));
			} else if ("<".equals(operator)) {
				qb.lt(getNumericValue(stringValue));
			} else if (">=".equals(operator)) {
				qb.gte(getNumericValue(stringValue));
			} else if ("<=".equals(operator)) {
				qb.lte(getNumericValue(stringValue));
			}
			return qb;
		}).build();
		if (nestedMode) {
			return (QueryVariant) nestedPropsQuery(keyValueBoolQuery(key, rq)).build();
		} else {
			return rq;
		}
	}

	/**
	 * Convert a normal query string query to one which supports nested fields.
	 * Reference: https://github.com/elastic/elasticsearch/issues/11322
	 * @param query query string
	 * @return a list of composite queries for matching nested objects
	 */
	static QueryVariant convertQueryStringToNestedQuery(String query) {
		String queryStr = StringUtils.trimToEmpty(query).replaceAll("\\[(\\d+)\\]", "-$1"); // nested array syntax
		Query q = qsParsed(queryStr);
		if (q == null) {
			return QueryBuilders.matchAll().build();
		}
		try {
			return rewriteQuery(q, 0);
		} catch (Exception e) {
			logger.warn(e.getMessage() + " - query: " + StringUtils.abbreviate(query, 500));
			return null;
		}
	}

	/**
	 * @param q parsed Lucene query string query
	 * @return a rewritten query with nested queries for custom properties (when in nested mode)
	 */
	private static QueryVariant rewriteQuery(Query q, int depth) throws IllegalAccessException {
		if (depth > MAX_QUERY_DEPTH) {
			throw new IllegalArgumentException("Query depth exceeded! Max depth: " + MAX_QUERY_DEPTH + " - " + q.toString());
		}
		ObjectBuilder<?> qb = null;
		if (q instanceof BooleanQuery) {
			qb = QueryBuilders.bool();
			for (BooleanClause clause : ((BooleanQuery) q).clauses()) {
				switch (clause.occur()) {
					case MUST:
						((BoolQuery.Builder) qb).must(rewriteQuery(clause.query(), depth++)._toQuery());
						break;
					case MUST_NOT:
						((BoolQuery.Builder) qb).mustNot(rewriteQuery(clause.query(), depth++)._toQuery());
						break;
					case FILTER:
						((BoolQuery.Builder) qb).filter(rewriteQuery(clause.query(), depth++)._toQuery());
						break;
					case SHOULD:
					default:
						((BoolQuery.Builder) qb).should(rewriteQuery(clause.query(), depth++)._toQuery());
				}
			}
		} else if (q instanceof TermRangeQuery) {
			qb = termRange(q);
		} else if (q instanceof BoostQuery) {
			qb = QueryBuilders.boosting().positive(rewriteQuery(((BoostQuery) q).getQuery(), depth++)._toQuery()).
					boost(((BoostQuery) q).getBoost());
		} else if (q instanceof org.apache.lucene.search.TermQuery) {
			qb = term(q);
		} else if (q instanceof FuzzyQuery) {
			qb = fuzzy(q);
		} else if (q instanceof PrefixQuery) {
			qb = prefix(q);
		} else if (q instanceof WildcardQuery) {
			qb = wildcard(q);
		} else {
			logger.warn("Unknown query type in nested mode query syntax: {}", q);
		}
		return (qb == null) ? QueryBuilders.matchAll().build() : (QueryVariant) qb.build();
	}

	private static ObjectBuilder<?> termRange(Query q) {
		ObjectBuilder<?> qb = null;
		TermRangeQuery trq = (TermRangeQuery) q;
		if (!StringUtils.isBlank(trq.getField())) {
			String from = trq.getLowerTerm() != null ? Term.toString(trq.getLowerTerm()) : "*";
			String to = trq.getUpperTerm() != null ? Term.toString(trq.getUpperTerm()) : "*";
			boolean isNestedField = trq.getField().matches(PROPS_REGEX);
			if ("*".equals(from) && "*".equals(to)) {
				qb = QueryBuilders.matchAll();
			} else {
				qb = QueryBuilders.range().untyped(b -> {
					b.field(isNestedField ? getValueFieldNameFromRange(from, to) : trq.getField());
					if (!"*".equals(from)) {
						if (trq.includesLower()) {
							b.gte(getNumericValue(from));
						} else {
							b.gt(getNumericValue(from));
						}
					}
					if (!"*".equals(to)) {
						if (trq.includesUpper()) {
							b.lte(getNumericValue(to));
						} else {
							b.lt(getNumericValue(to));
						}
					}
					return b;
				});
			}
			if (isNestedField) {
				qb = nestedPropsQuery(keyValueBoolQuery(trq.getField(), (QueryVariant) qb.build()));
			}
		}
		return qb;
	}

	private static ObjectBuilder<?> term(Query q) {
		ObjectBuilder<?> qb;
		String field = ((org.apache.lucene.search.TermQuery) q).getTerm().field();
		String value = ((org.apache.lucene.search.TermQuery) q).getTerm().text();
		if (StringUtils.isBlank(field)) {
			QueryVariant kQuery = QueryBuilders.matchAll().build();
			QueryVariant vQuery = QueryBuilders.multiMatch().query(value).build();
			QueryVariant nested = (QueryVariant) nestedPropsQuery(QueryBuilders.bool().
					must(kQuery._toQuery(), vQuery._toQuery()).build()).build();
			qb = QueryBuilders.bool().should(nested._toQuery(), QueryBuilders.multiMatch().query(value).build()._toQuery());
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, value));
		} else {
			qb = QueryBuilders.term().field(field).value(v -> v.stringValue(value));
		}
		return qb;
	}

	private static ObjectBuilder<?> fuzzy(Query q) {
		ObjectBuilder<?> qb;
		String field = ((FuzzyQuery) q).getTerm().field();
		String value = ((FuzzyQuery) q).getTerm().text();
		if (StringUtils.isBlank(field)) {
			QueryVariant kQuery = QueryBuilders.matchAll().build();
			QueryVariant vQuery = QueryBuilders.fuzzy().field(getValueFieldName(value)).value(v -> v.stringValue(value)).build();
			QueryVariant nested = (QueryVariant) nestedPropsQuery(QueryBuilders.bool().
					must(kQuery._toQuery(), vQuery._toQuery()).build());
			qb = QueryBuilders.bool().should(nested._toQuery(), QueryBuilders.multiMatch().query(value).build()._toQuery());
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, QueryBuilders.
					fuzzy().field(getValueFieldName(value)).value(v -> v.stringValue(value)).build()));
		} else {
			qb = QueryBuilders.fuzzy().field(field).value(v -> v.stringValue(value));
		}
		return qb;
	}

	private static ObjectBuilder<?> prefix(Query q) {
		ObjectBuilder<?> qb;
		String field = ((PrefixQuery) q).getPrefix().field();
		String value = ((PrefixQuery) q).getPrefix().text();
		if (StringUtils.isBlank(field)) {
			QueryVariant kQuery = QueryBuilders.matchAll().build();
			QueryVariant vQuery = QueryBuilders.prefix().field(getValueFieldName(value)).value(value).build();
			QueryVariant nested = (QueryVariant) nestedPropsQuery(QueryBuilders.bool().
					must(kQuery._toQuery(), vQuery._toQuery()).build());
			qb = QueryBuilders.bool().should(nested._toQuery(), QueryBuilders.multiMatch().query(value).build()._toQuery());
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, QueryBuilders.
					prefix().field(getValueFieldName(value)).value(value).build()));
		} else {
			qb = QueryBuilders.prefix().field(field).value(value);
		}
		return qb;
	}

	private static ObjectBuilder<?> wildcard(Query q) {
		ObjectBuilder<?> qb;
		String field = ((WildcardQuery) q).getTerm().field();
		String value = ((WildcardQuery) q).getTerm().text();
		if (StringUtils.isBlank(field)) {
			QueryVariant kQuery = QueryBuilders.matchAll().build();
			QueryVariant vQuery = QueryBuilders.wildcard().field(getValueFieldName(value)).value(value).build();
			QueryVariant nested = (QueryVariant) nestedPropsQuery(QueryBuilders.bool().
					must(kQuery._toQuery(), vQuery._toQuery()).build());
			qb = QueryBuilders.bool().should(nested._toQuery(), QueryBuilders.multiMatch().query(value).build()._toQuery());
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, QueryBuilders.
					wildcard().field(getValueFieldName(value)).value(value).build()));
		} else {
			qb = QueryBuilders.wildcard().field(field).value(value);
		}
		return qb;
	}

	/**
	 * @param k field name
	 * @param query query object
	 * @return a composite query: bool(match(key) AND match(value))
	 */
	static QueryVariant keyValueBoolQuery(String k, QueryVariant query) {
		return keyValueBoolQuery(k, null, query);
	}

	/**
	 * @param k field name
	 * @param v field value
	 * @return a composite query: bool(match(key) AND match(value))
	 */
	static QueryVariant keyValueBoolQuery(String k, String v) {
		return keyValueBoolQuery(k, v, null);
	}

	/**
	 * @param k field name
	 * @param v field value
	 * @param query query object
	 * @return a composite query: bool(match(key) AND match(value))
	 */
	static QueryVariant keyValueBoolQuery(String k, String v, QueryVariant query) {
		if (StringUtils.isBlank(k) || (query == null && StringUtils.isBlank(v))) {
			return QueryBuilders.matchAll().build();
		}
		QueryVariant kQuery = QueryBuilders.match().field(PROPS_PREFIX + "k").
				query(b -> b.stringValue(getNestedKey(k))).operator(Operator.And).build();
		QueryVariant vQuery = (query == null) ? QueryBuilders.match().field(getValueFieldName(v)).
				query(b -> b.stringValue(v)).operator(Operator.And).build() : query;
		if ("*".equals(v) || query instanceof MatchAllQuery) {
			return QueryBuilders.bool().must(kQuery._toQuery()).build();
		}
		return QueryBuilders.bool().must(kQuery._toQuery(), vQuery._toQuery()).build();
	}

	/**
	 * @param query query
	 * @return a nested query
	 */
	static ObjectBuilder<?> nestedPropsQuery(QueryVariant query) {
		return QueryBuilders.nested().path(PROPS_FIELD).query(query._toQuery()).scoreMode(ChildScoreMode.Avg);
	}

	/**
	 * @param key dotted field path
	 * @return translate "properties.path.to.key" to "properties.path-to-key"
	 */
	static String getNestedKey(String key) {
		if (Strings.CS.startsWith(key, PROPS_PREFIX)) {
			return Strings.CS.removeStart(key, PROPS_PREFIX).replaceAll("\\[(\\d+)\\]", "-$1").replaceAll("\\.", "-");
		}
		return key;
	}

	/**
	 * @param v search term
	 * @return the name of the value property inside a nested object, e.g. "properties.v"
	 */
	static String getValueFieldName(String v) {
		return PROPS_PREFIX + (NumberUtils.isDigits(v) ? "vn" : "v");
	}

	/**
	 * @param from from value
	 * @param to to value
	 * @return either "properties.vn" if one of the range limits is a number, or "properties.v" otherwise.
	 */
	static String getValueFieldNameFromRange(String from, String to) {
		if (("*".equals(from) && "*".equals(to)) || NumberUtils.isDigits(from) || NumberUtils.isDigits(to)) {
			return PROPS_PREFIX + "vn";
		}
		return PROPS_PREFIX + "v";
	}

	/**
	 * @param v search term
	 * @return the long value of v if it is a number
	 */
	static JsonData getNumericValue(String v) {
		return JsonData.of(NumberUtils.isDigits(v) ? NumberUtils.toLong(v, 0) : v);
	}

	/**
	 * A method reserved for future use. It allows to have indexes with different names than the appid.
	 *
	 * @param appid an app identifer
	 * @return the correct index name
	 */
	static String getIndexName(String appid) {
		return appid.trim();
	}

	/**
	 * @param indexName index name or alias
	 * @return e.g. "index-name_*"
	 */
	static String getIndexNameWithWildcard(String indexName) {
		return Strings.CS.contains(indexName, "_") ? indexName : indexName + "_*"; // ES v6
	}

	/**
	 * Intercepts and signs requests to AWS Elasticsearch endpoints.
	 * @param endpoint the ES endpoint URI
	 */
	public static void addAWSRequestSigningInterceptor(HttpAsyncClientBuilder httpClientBuilder, String endpoint) {
		httpClientBuilder.addRequestInterceptorLast((HttpRequest request, EntityDetails ed, HttpContext context) -> {
			Signer s = new Signer();
			Credentials cred = Credentials.fromEnvironment();
			URIBuilder uriBuilder;
			String resourcePath;
			InputStream body = null;
			Map<String, String> params = new HashMap<>();
			Map<String, String> headers = new HashMap<>();

			try {
				uriBuilder = new URIBuilder(request.getUri());
				resourcePath = uriBuilder.getPath();

				for (NameValuePair param : uriBuilder.getQueryParams()) {
					params.put(param.getName(), param.getValue());
				}

				if (request instanceof HttpEntityEnclosingRequest) {
					HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
					if (entity != null) {
						body = entity.getContent();
					}
				}

				for (Header header : request.getHeaders()) {
					headers.put(header.getName(), header.getValue());
				}

				s.sign(request.getMethod(), endpoint, resourcePath, headers, params, body,
						cred.accessKey(), cred.secretKey(), "es", Para.getConfig().elasticsearchAwsRegion(), true);

				for (String header : headers.keySet()) {
					request.setHeader(header, headers.get(header));
				}
			} catch (Exception ex) {
				logger.error("Failed to sign request to AWS Elasticsearch:", ex);
			}
		});
	}

	static void addAuthenticationCallback(HttpAsyncClientBuilder httpClientBuilder) {
		final String basicAuthLogin = Para.getConfig().elasticsearchAuthUser();
		final String basicAuthPassword = Para.getConfig().elasticsearchAuthPassword();

		if (!StringUtils.isAnyEmpty(basicAuthLogin, basicAuthPassword)) {
			// basic auth as documented by Elastic
			final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(new AuthScope(null, -1),
					new UsernamePasswordCredentials(basicAuthLogin, basicAuthPassword.toCharArray()));
			httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		}
	}
}
