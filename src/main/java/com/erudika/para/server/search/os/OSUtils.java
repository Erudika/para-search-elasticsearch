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
package com.erudika.para.server.search.os;

import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Sysprop;
import com.erudika.para.core.listeners.DestroyListener;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.core.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
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
import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.MUST_NOT;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import static org.apache.lucene.search.join.ScoreMode.Avg;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.fuzzyQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.multiMatchQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.prefixQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.QueryBuilders.wildcardQuery;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class OSUtils {

	private static final Logger logger = LoggerFactory.getLogger(OSUtils.class);
	private static RestHighLevelClient restClient;
	private static BulkProcessor bulkProcessor;
	private static ActionListener<BulkResponse> syncListener;
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
	 * @return default mapping JSON
	 */
	public static String getDefaultMapping() {
		return "{\n" +
			"  \"properties\": {\n" +
			"    \"nstd\": {\"type\": \"nested\"},\n" +
			"    \"properties\": {\"type\": \"" + (nestedMode() ? "nested" : "object") + "\"},\n" +
			"    \"latlng\": {\"type\": \"geo_point\"},\n" +
			"    \"_docid\": {\"type\": \"long\", \"index\": false},\n" +
			"    \"updated\": {\"type\": \"date\", \"format\" : \"" + DATE_FORMAT + "\"},\n" +
			"    \"timestamp\": {\"type\": \"date\", \"format\" : \"" + DATE_FORMAT + "\"},\n" +

			"    \"tag\": {\"type\": \"keyword\"},\n" +
			"    \"id\": {\"type\": \"keyword\"},\n" +
			"    \"key\": {\"type\": \"keyword\"},\n" +
			"    \"name\": {\"type\": \"keyword\"},\n" +
			"    \"type\": {\"type\": \"keyword\"},\n" +
			"    \"tags\": {\"type\": \"keyword\"},\n" +
			"    \"token\": {\"type\": \"keyword\"},\n" +
			"    \"email\": {\"type\": \"keyword\"},\n" +
			"    \"appid\": {\"type\": \"keyword\"},\n" +
			"    \"groups\": {\"type\": \"keyword\"},\n" +
			"    \"password\": {\"type\": \"keyword\"},\n" +
			"    \"parentid\": {\"type\": \"keyword\"},\n" +
			"    \"creatorid\": {\"type\": \"keyword\"},\n" +
			"    \"identifier\": {\"type\": \"keyword\"}\n" +
			"  }\n" +
			"}";
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

	private OSUtils() { }

	static void initClient() {
		getRESTClient();
	}

	/**
	 * Creates an instance of the high-level REST client that talks to Elasticsearch.
	 * @return a RestHighLevelClient instance
	 */
	public static RestHighLevelClient getRESTClient() {
		if (restClient != null) {
			return restClient;
		}
		String esScheme = Para.getConfig().elasticsearchRestClientScheme();
		String esHost = Para.getConfig().elasticsearchRestClientHost();
		int esPort = Para.getConfig().elasticsearchRestClientPort();
		boolean signRequests = Para.getConfig().elasticsearchSignRequestsForAwsEnabled();
		HttpHost host = new HttpHost(esScheme, esHost, esPort);
		RestClientBuilder clientBuilder = RestClient.builder(host);

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
			return httpClientBuilder;
		});

		restClient = new RestHighLevelClient(clientBuilder);

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
				restClient.close();
			} catch (IOException ex) {
				logger.error(null, ex);
			}
		}
		if (bulkProcessor != null) {
			boolean closed = false;
			try {
				closed = bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
			} catch (InterruptedException ex) {
				logger.warn("Interrupted waiting for BulkProcessor to close.", ex);
				Thread.currentThread().interrupt();
			} finally {
				if (!closed) {
					bulkProcessor.close();
				}
				bulkProcessor = null;
			}
		}
	}

	private static boolean createIndexWithoutAlias(String name, int shards, int replicas) {
		if (StringUtils.isBlank(name) || StringUtils.containsWhitespace(name) || existsIndex(name)) {
			return false;
		}
		if (shards <= 0) {
			shards = Para.getConfig().elasticsearchRootIndexShards();
		}
		if (replicas < 0) {
			replicas = Para.getConfig().elasticsearchRootIndexReplicas();
		}
		try {
			Settings.Builder settings = Settings.builder();
			settings.put("number_of_shards", Integer.toString(shards));
			settings.put("number_of_replicas", Integer.toString(replicas));
			settings.put("auto_expand_replicas", Para.getConfig().elasticsearchAutoExpandReplicas());
			settings.put("analysis.analyzer.default.type", "standard");
			settings.putList("analysis.analyzer.default.stopwords",
					"arabic", "armenian", "basque", "brazilian", "bulgarian", "catalan",
					"czech", "danish", "dutch", "english", "finnish", "french", "galician",
					"german", "greek", "hindi", "hungarian", "indonesian", "italian",
					"norwegian", "persian", "portuguese", "romanian", "russian", "spanish",
					"swedish", "turkish");

			// create index with default system mappings; ES allows only one type per index
			CreateIndexRequest create = new CreateIndexRequest(name).settings(settings.build()).
					mapping(getDefaultMapping(), XContentType.JSON);
			getRESTClient().indices().create(create, RequestOptions.DEFAULT);
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
			DeleteIndexRequest delete = new DeleteIndexRequest(indexName);
			getRESTClient().indices().delete(delete, RequestOptions.DEFAULT);
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
			GetIndexRequest get = new GetIndexRequest(indexName);
			exists = getRESTClient().indices().exists(get, RequestOptions.DEFAULT);
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

			List<DocWriteRequest<?>> batch = new LinkedList<>();
			Pager p = getPager(pager);
			int batchSize = Para.getConfig().reindexBatchSize(p.getLimit());
			long reindexedCount = 0;

			List<ParaObject> list;
			do {
				list = dao.readPage(app.getAppIdentifier(), p); // use appid!
				logger.debug("rebuildIndex(): Read {} objects from table {}.", list.size(), indexName);
				for (ParaObject obj : list) {
					if (obj != null) {
						// put objects from DB into the newly created index
						batch.add(new IndexRequest(newName).id(obj.getId()).source(getSourceFromParaObject(obj)));
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
			if (batch.size() > 0) {
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
			if (asyncEnabled()) {
				bulkProcessor(getRESTClient()).flush();
			}
			getRESTClient().indices().refresh(new RefreshRequest(getIndexName(appid)), RequestOptions.DEFAULT);
		}
	}

	/**
	 * Executes a delete_by_query request to ES and refreshes the index.
	 * @param appid the appid / index alias
	 * @param fb query
	 * @return number of unindexed documents.
	 */
	public static long deleteByQuery(String appid, QueryBuilder fb) {
		return deleteByQuery(appid, fb, asyncEnabled() ? new ActionListener<BulkByScrollResponse>() {
			public void onResponse(BulkByScrollResponse res) {
				logger.debug(res.getStatus().toString());
			}
			public void onFailure(Exception ex) {
				logger.error("Delete by query reqest failed for app '" + appid + "'", ex);
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
	public static long deleteByQuery(String appid, QueryBuilder fb, ActionListener<BulkByScrollResponse> cb) {
		int batchSize = 1000;
		boolean isSharingIndex = !App.isRoot(appid) && StringUtils.startsWith(appid, " ");
		String indexName = getIndexName(appid);
		DeleteByQueryRequest deleteByQueryReq = new DeleteByQueryRequest(indexName);
		deleteByQueryReq.setConflicts("proceed");
		deleteByQueryReq.setQuery(fb);
		deleteByQueryReq.setBatchSize(batchSize);
		deleteByQueryReq.setSlices(1); // parallelize operation?
		deleteByQueryReq.setScroll(TimeValue.timeValueMinutes(10));
		deleteByQueryReq.setRefresh(true);
		if (isSharingIndex) {
			deleteByQueryReq.setRouting(indexName);
		}
		if (cb != null) {
			getRESTClient().deleteByQueryAsync(deleteByQueryReq, RequestOptions.DEFAULT, cb);
		} else {
			BulkByScrollResponse res;
			try {
				res = getRESTClient().deleteByQuery(deleteByQueryReq, RequestOptions.DEFAULT);
				if (!res.getBulkFailures().isEmpty()) {
					logger.warn("Bulk failure in deleteByQuery()!", res.getBulkFailures().iterator().next().getCause());
				}
				if (!res.getSearchFailures().isEmpty()) {
					logger.warn("Search failure in deleteByQuery()!", res.getSearchFailures().iterator().next().getReason());
				}
				return res.getTotal();
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
	protected static List<SortBuilder<?>> getSortFieldsFromPager(Pager pager) {
		if (pager == null) {
			pager = new Pager();
		}
		SortOrder defaultOrder = pager.isDesc() ? SortOrder.DESC : SortOrder.ASC;
		if (pager.getSortby().contains(",")) {
			String[] fields = pager.getSortby().split(",");
			ArrayList<SortBuilder<?>> sortFields = new ArrayList<>(fields.length);
			for (String field : fields) {
				SortOrder order;
				String fieldName;
				if (field.endsWith(":asc")) {
					order = SortOrder.ASC;
					fieldName = field.substring(0, field.indexOf(":asc")).trim();
				} else if (field.endsWith(":desc")) {
					order = SortOrder.DESC;
					fieldName = field.substring(0, field.indexOf(":desc")).trim();
				} else {
					order = defaultOrder;
					fieldName = field.trim();
				}
				if (nestedMode() && fieldName.startsWith(PROPS_PREFIX)) {
					sortFields.add(getNestedFieldSort(fieldName, order));
				} else {
					sortFields.add(SortBuilders.fieldSort(fieldName).order(order));
				}
			}
			return sortFields;
		} else if (StringUtils.isBlank(pager.getSortby())) {
			return Collections.singletonList(SortBuilders.scoreSort());
		} else {
			String fieldName = pager.getSortby();
			if (nestedMode() && fieldName.startsWith(PROPS_PREFIX)) {
				return Collections.singletonList(getNestedFieldSort(fieldName, defaultOrder));
			} else {
				return Collections.singletonList(SortBuilders.fieldSort(fieldName).order(defaultOrder));
			}
		}
	}

	private static FieldSortBuilder getNestedFieldSort(String fieldName, SortOrder order) {
		// nested sorting works only on numeric fields (sorting on properties.v requires fielddata enabled)
		return SortBuilders.fieldSort(PROPS_FIELD + ".vn").order(order).
							setNestedSort(new NestedSortBuilder(PROPS_FIELD).
									setFilter(QueryBuilders.termQuery(PROPS_FIELD + ".k",
											StringUtils.removeStart(fieldName, PROPS_FIELD + "."))));
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
			AliasActions addAction;
			if (withAliasRouting) {
				addAction = AliasActions.add().index(index).alias(alias).
						searchRouting(alias).indexRouting(alias).
						filter(termQuery(Config._APPID, aliasName)); // DO NOT trim filter query!
			} else {
				addAction = AliasActions.add().index(index).alias(alias);
			}
			IndicesAliasesRequest actions = new IndicesAliasesRequest().addAliasAction(addAction);
			return getRESTClient().indices().updateAliases(actions, RequestOptions.DEFAULT).isAcknowledged();
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
			AliasActions removeAction = AliasActions.remove().index(index).alias(alias);
			IndicesAliasesRequest actions = new IndicesAliasesRequest().addAliasAction(removeAction);
			return getRESTClient().indices().updateAliases(actions, RequestOptions.DEFAULT).isAcknowledged();
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
			GetAliasesRequest getAlias = new GetAliasesRequest().indices(index).aliases(alias);
			return getRESTClient().indices().existsAlias(getAlias, RequestOptions.DEFAULT);
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
			AliasActions removeAction = AliasActions.remove().index(oldName).alias(aliaz);
			AliasActions addAction = AliasActions.add().index(newName).alias(aliaz);
			IndicesAliasesRequest actions = new IndicesAliasesRequest().
					addAliasAction(removeAction).addAliasAction(addAction);
			getRESTClient().indices().updateAliases(actions, RequestOptions.DEFAULT);
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
			Map<String, Set<AliasMetadata>> aliases = getRESTClient().indices().
					getAlias(new GetAliasesRequest().indices(appid), RequestOptions.DEFAULT).getAliases();
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
	public static void executeRequests(List<DocWriteRequest<?>> requests) {
		if (requests == null || requests.isEmpty()) {
			return;
		}
		ActionListener<BulkResponse> listener = getSyncRequestListener();
		try {
			if (asyncEnabled()) {
				BulkProcessor bp = bulkProcessor(getRESTClient());
				requests.forEach(bp::add);
				if (flushImmediately()) {
					bp.flush();
				}
			} else {
				BulkRequest bulk = new BulkRequest();
				requests.forEach(bulk::add);
				listener.onResponse(getRESTClient().bulk(bulk, RequestOptions.DEFAULT));
			}
		} catch (Exception e) {
			listener.onFailure(e);
		}
	}

	private static BulkProcessor bulkProcessor(RestHighLevelClient client) {
		if (bulkProcessor == null) {
			bulkProcessor = configureBulkProcessor(BulkProcessor.builder((request, bulkListener) ->
					client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), getAsyncRequestListener()));
		}
		return bulkProcessor;
	}

	private static BulkProcessor configureBulkProcessor(BulkProcessor.Builder builder) {
		final int sizeLimit = Para.getConfig().elasticsearchBulkSizeLimitMb();
		final int actionLimit = Para.getConfig().elasticsearchBulkActionLimit();
		final int concurrentRequests = Para.getConfig().elasticsearchBulkConcurrentRequests();
		final int flushInterval = Para.getConfig().elasticsearchBulkFlushIntervalSec();
		final int backoffInitialDelayMs = Para.getConfig().elasticsearchBulkBackoffDelayMs();
		final int backoffNumRetries = Para.getConfig().elasticsearchBulkBackoffRetries();
		builder.setBulkSize(new ByteSizeValue(sizeLimit, ByteSizeUnit.MB));
		builder.setBulkActions(actionLimit);
		builder.setConcurrentRequests(concurrentRequests);
		if (flushInterval > 0) {
			builder.setFlushInterval(TimeValue.timeValueMillis(flushInterval));
		}
		if (backoffNumRetries > 0) {
			builder.setBackoffPolicy(BackoffPolicy.
					exponentialBackoff(TimeValue.timeValueMillis(backoffInitialDelayMs), backoffNumRetries));
		} else {
			builder.setBackoffPolicy(BackoffPolicy.noBackoff());
		}
		return builder.build();
	}

	private static BulkProcessor.Listener getAsyncRequestListener() {
		return new BulkProcessor.Listener() {
			public void beforeBulk(long executionId, BulkRequest request) { }

			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				if (response != null && response.hasFailures()) {
					Arrays.stream(response.getItems()).filter(BulkItemResponse::isFailed).forEach(item -> {
						//FUTURE: Increment counter metric for failed document indexing
						logger.error("Failed to execute async {} operation for index '{}', document id '{}': {}",
								item.getOpType(), item.getIndex(), item.getId(), item.getFailure().getMessage());
					});
				}
			}

			public void afterBulk(long executionId, BulkRequest request, Throwable throwable) {
				//FUTURE: Increment counter metric for failed indexing requests
				logger.error("Asynchronous indexing operation failed.", throwable);
			}
		};
	};

	private static ActionListener<BulkResponse> getSyncRequestListener() {
		if (syncListener != null) {
			return syncListener;
		}
		syncListener = new ActionListener<BulkResponse>() {
			public void onResponse(BulkResponse response) {
				if (response != null && response.hasFailures()) {
					Arrays.stream(response.getItems()).
							filter(BulkItemResponse::isFailed).
							forEach(item -> {
								logger.error("Failed to execute {} operation for index '{}', document id '{}': {}",
										item.getOpType(), item.getIndex(), item.getId(), item.getFailure().getMessage());
							});

					handleFailedRequests(Arrays.stream(response.getItems()).
							filter(BulkItemResponse::isFailed).
							map(BulkItemResponse::getFailure).
							map(BulkItemResponse.Failure::getCause).
							filter(Objects::nonNull).
							findFirst().orElse(null));
				}
			}
			public void onFailure(Exception e) {
				logger.error("Synchronous indexing operation failed!", e);
				handleFailedRequests(e);
			}
		};
		return syncListener;
	}

	private static void handleFailedRequests(Throwable t) {
		if (t != null && Para.getConfig().exceptionOnWriteErrorsEnabled()) {
			throw new RuntimeException("Synchronous indexing operation failed!", t);
		}
	}

	/**
	 * Check if cluster status is green or yellow.
	 * @return false if status is red
	 */
	public static boolean isClusterOK() {
		try {
			ClusterHealthStatus status = getRESTClient().cluster().
					health(new ClusterHealthRequest(), RequestOptions.DEFAULT).getStatus();
			return !ClusterHealthStatus.RED.equals(status);
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
	static QueryBuilder getTermsQuery(Map<String, ?> terms, boolean mustMatchAll) {
		BoolQueryBuilder fb = boolQuery();
		int addedTerms = 0;
		boolean noop = true;
		QueryBuilder bfb = null;

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
						bfb = term(new TermQuery(new Term(term.getKey(), stringValue)));
					} else {
						bfb = termQuery(term.getKey(), stringValue);
					}
				}
				if (mustMatchAll) {
					fb.must(bfb);
				} else {
					fb.should(bfb);
				}
				addedTerms++;
				noop = false;
			}
		}
		if (addedTerms == 1 && bfb != null) {
			return bfb;
		}
		return noop ? null : fb;
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

	/**
	 * Helper method which creates a new instance of the source builder object.
	 * @param query a query
	 * @param max max hits
	 * @return source builder instance
	 */
	static SearchSourceBuilder getSourceBuilder(QueryBuilder query, int max) {
		String trackTotalHits = Para.getConfig().elasticsearchTrackTotalHits();
		SearchSourceBuilder source = new SearchSourceBuilder().query(query).size(max);
		if (NumberUtils.isDigits(trackTotalHits)) {
			source.trackTotalHitsUpTo(NumberUtils.toInt(trackTotalHits, Config.DEFAULT_LIMIT));
		} else if (Boolean.valueOf(trackTotalHits)) {
			source.trackTotalHits(true);
		}
		return source;
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
	static QueryBuilder range(String operator, String field, String stringValue) {
		Objects.requireNonNull(field);
		String key = field.replaceAll("[<>=\\s]+$", "");
		boolean nestedMode = nestedMode() && field.startsWith(PROPS_PREFIX);
		RangeQueryBuilder rfb = rangeQuery(nestedMode ? getValueFieldName(stringValue) : key);
		if (">".equals(operator)) {
			rfb.gt(getNumericValue(stringValue));
		} else if ("<".equals(operator)) {
			rfb.lt(getNumericValue(stringValue));
		} else if (">=".equals(operator)) {
			rfb.gte(getNumericValue(stringValue));
		} else if ("<=".equals(operator)) {
			rfb.lte(getNumericValue(stringValue));
		}
		if (nestedMode) {
			return nestedPropsQuery(keyValueBoolQuery(key, rfb));
		} else {
			return rfb;
		}
	}

	/**
	 * Convert a normal query string query to one which supports nested fields.
	 * Reference: https://github.com/elastic/elasticsearch/issues/11322
	 * @param query query string
	 * @return a list of composite queries for matching nested objects
	 */
	static QueryBuilder convertQueryStringToNestedQuery(String query) {
		String queryStr = StringUtils.trimToEmpty(query).replaceAll("\\[(\\d+)\\]", "-$1"); // nested array syntax
		Query q = qsParsed(queryStr);
		if (q == null) {
			return matchAllQuery();
		}
		try {
			return rewriteQuery(q, 0);
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	/**
	 * @param q parsed Lucene query string query
	 * @return a rewritten query with nested queries for custom properties (when in nested mode)
	 */
	private static QueryBuilder rewriteQuery(Query q, int depth) throws IllegalAccessException {
		if (depth > MAX_QUERY_DEPTH) {
			throw new IllegalArgumentException("Query depth exceeded! Max depth: " + MAX_QUERY_DEPTH + " - " + q.toString());
		}
		QueryBuilder qb = null;
		if (q instanceof BooleanQuery) {
			qb = boolQuery();
			for (BooleanClause clause : ((BooleanQuery) q).clauses()) {
				switch (clause.occur()) {
					case MUST:
						((BoolQueryBuilder) qb).must(rewriteQuery(clause.query(), depth++));
						break;
					case MUST_NOT:
						((BoolQueryBuilder) qb).mustNot(rewriteQuery(clause.query(), depth++));
						break;
					case FILTER:
						((BoolQueryBuilder) qb).filter(rewriteQuery(clause.query(), depth++));
						break;
					case SHOULD:
					default:
						((BoolQueryBuilder) qb).should(rewriteQuery(clause.query(), depth++));
				}
			}
		} else if (q instanceof TermRangeQuery) {
			qb = termRange(q);
		} else if (q instanceof BoostQuery) {
			qb = rewriteQuery(((BoostQuery) q).getQuery(), depth++).boost(((BoostQuery) q).getBoost());
		} else if (q instanceof TermQuery) {
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
		return (qb == null) ? matchAllQuery() : qb;
	}

	private static QueryBuilder termRange(Query q) {
		QueryBuilder qb = null;
		TermRangeQuery trq = (TermRangeQuery) q;
		if (!StringUtils.isBlank(trq.getField())) {
			String from = trq.getLowerTerm() != null ? Term.toString(trq.getLowerTerm()) : "*";
			String to = trq.getUpperTerm() != null ? Term.toString(trq.getUpperTerm()) : "*";
			boolean isNestedField = trq.getField().matches(PROPS_REGEX);
			qb = rangeQuery(isNestedField ? getValueFieldNameFromRange(from, to) : trq.getField());
			if ("*".equals(from) && "*".equals(to)) {
				qb = matchAllQuery();
			}
			if (!"*".equals(from)) {
				((RangeQueryBuilder) qb).from(getNumericValue(from)).includeLower(trq.includesLower());
			}
			if (!"*".equals(to)) {
				((RangeQueryBuilder) qb).to(getNumericValue(to)).includeUpper(trq.includesUpper());
			}
			if (isNestedField) {
				qb = nestedPropsQuery(keyValueBoolQuery(trq.getField(), qb));
			}
		}
		return qb;
	}

	private static QueryBuilder term(Query q) {
		QueryBuilder qb;
		String field = ((TermQuery) q).getTerm().field();
		String value = ((TermQuery) q).getTerm().text();
		if (StringUtils.isBlank(field)) {
			QueryBuilder kQuery = matchAllQuery();
			QueryBuilder vQuery = multiMatchQuery(value);
			qb = boolQuery().should(nestedPropsQuery(boolQuery().must(kQuery).must(vQuery))).
					should(multiMatchQuery(value));
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, value));
		} else {
			qb = termQuery(field, value);
		}
		return qb;
	}

	private static QueryBuilder fuzzy(Query q) {
		QueryBuilder qb;
		String field = ((FuzzyQuery) q).getTerm().field();
		String value = ((FuzzyQuery) q).getTerm().text();
		if (StringUtils.isBlank(field)) {
			QueryBuilder kQuery = matchAllQuery();
			QueryBuilder vQuery = fuzzyQuery(getValueFieldName(value), value);
			qb = boolQuery().should(nestedPropsQuery(boolQuery().must(kQuery).must(vQuery))).
					should(multiMatchQuery(value));
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, fuzzyQuery(getValueFieldName(value), value)));
		} else {
			qb = fuzzyQuery(field, value);
		}
		return qb;
	}

	private static QueryBuilder prefix(Query q) {
		QueryBuilder qb;
		String field = ((PrefixQuery) q).getPrefix().field();
		String value = ((PrefixQuery) q).getPrefix().text();
		if (StringUtils.isBlank(field)) {
			QueryBuilder kQuery = matchAllQuery();
			QueryBuilder vQuery = prefixQuery(getValueFieldName(value), value);
			qb = boolQuery().should(nestedPropsQuery(boolQuery().must(kQuery).must(vQuery))).
					should(multiMatchQuery(value));
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, prefixQuery(getValueFieldName(value), value)));
		} else {
			qb = prefixQuery(field, value);
		}
		return qb;
	}

	private static QueryBuilder wildcard(Query q) {
		QueryBuilder qb;
		String field = ((WildcardQuery) q).getTerm().field();
		String value = ((WildcardQuery) q).getTerm().text();
		if (StringUtils.isBlank(field)) {
			QueryBuilder kQuery = matchAllQuery();
			QueryBuilder vQuery = wildcardQuery(getValueFieldName(value), value);
			qb = boolQuery().should(nestedPropsQuery(boolQuery().must(kQuery).must(vQuery))).
					should(multiMatchQuery(value));
		} else if (field.matches(PROPS_REGEX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, wildcardQuery(getValueFieldName(value), value)));
		} else {
			qb = wildcardQuery(field, value);
		}
		return qb;
	}

	/**
	 * @param k field name
	 * @param query query object
	 * @return a composite query: bool(match(key) AND match(value))
	 */
	static QueryBuilder keyValueBoolQuery(String k, QueryBuilder query) {
		return keyValueBoolQuery(k, null, query);
	}

	/**
	 * @param k field name
	 * @param v field value
	 * @return a composite query: bool(match(key) AND match(value))
	 */
	static QueryBuilder keyValueBoolQuery(String k, String v) {
		return keyValueBoolQuery(k, v, null);
	}

	/**
	 * @param k field name
	 * @param v field value
	 * @param query query object
	 * @return a composite query: bool(match(key) AND match(value))
	 */
	static QueryBuilder keyValueBoolQuery(String k, String v, QueryBuilder query) {
		if (StringUtils.isBlank(k) || (query == null && StringUtils.isBlank(v))) {
			return matchAllQuery();
		}
		QueryBuilder kQuery = matchQuery(PROPS_PREFIX + "k", getNestedKey(k)).operator(Operator.AND);
		QueryBuilder vQuery = (query == null) ? matchQuery(getValueFieldName(v), v).operator(Operator.AND) : query;
		if ("*".equals(v) || matchAllQuery().equals(query)) {
			return boolQuery().must(kQuery);
		}
		return boolQuery().must(kQuery).must(vQuery);
	}

	/**
	 * @param query query
	 * @return a nested query
	 */
	static NestedQueryBuilder nestedPropsQuery(QueryBuilder query) {
		return nestedQuery(PROPS_FIELD, query, Avg);
	}

	/**
	 * @param key dotted field path
	 * @return translate "properties.path.to.key" to "properties.path-to-key"
	 */
	static String getNestedKey(String key) {
		if (StringUtils.startsWith(key, PROPS_PREFIX)) {
			return StringUtils.removeStart(key, PROPS_PREFIX).replaceAll("\\[(\\d+)\\]", "-$1").replaceAll("\\.", "-");
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
	static Object getNumericValue(String v) {
		return NumberUtils.isDigits(v) ? NumberUtils.toLong(v, 0) : v;
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
		return StringUtils.contains(indexName, "_") ? indexName : indexName + "_*"; // ES v6
	}

	/**
	 * Intercepts and signs requests to AWS Elasticsearch endpoints.
	 * @param endpoint the ES endpoint URI
	 * @return a client callback containing the interceptor
	 */
	static void addAWSRequestSigningInterceptor(HttpAsyncClientBuilder httpClientBuilder, String endpoint) {
		httpClientBuilder.addRequestInterceptorLast((HttpRequest request, EntityDetails ed, HttpContext context) -> {
			Aws4Signer signer = Aws4Signer.create();
			AwsCredentials creds = DefaultCredentialsProvider.create().resolveCredentials();
			Aws4SignerParams.Builder<?> signerParams = Aws4SignerParams.builder().
					awsCredentials(creds).
					doubleUrlEncode(true).
					signingName("es").
					signingRegion(Region.of(Para.getConfig().elasticsearchAwsRegion()));
			URIBuilder uriBuilder;
			String httpMethod = request.getMethod();
			String resourcePath;
			Map<String, String> params = new HashMap<>();

			try {
				SdkHttpFullRequest.Builder r = SdkHttpFullRequest.builder();
				if (!StringUtils.isBlank(httpMethod)) {
					r.method(SdkHttpMethod.valueOf(httpMethod));
				}

				if (!StringUtils.isBlank(endpoint)) {
					if (endpoint.startsWith("https://")) {
						r.protocol("HTTPS");
						r.host(StringUtils.removeStart(endpoint, "https://"));
					} else if (endpoint.startsWith("http://")) {
						r.protocol("HTTP");
						r.host(StringUtils.removeStart(endpoint, "http://"));
					}
				}

				uriBuilder = new URIBuilder(request.getUri());
				resourcePath = uriBuilder.getPath();
				if (!StringUtils.isBlank(resourcePath)) {
					r.encodedPath(resourcePath);
				}

				for (NameValuePair param : uriBuilder.getQueryParams()) {
					r.appendRawQueryParameter(param.getName(), param.getValue());
				}

				if (request instanceof HttpEntityEnclosingRequest) {
					HttpEntity body = ((HttpEntityEnclosingRequest) request).getEntity();
					if (body != null) {
						InputStream is = body.getContent();
						r.contentStreamProvider(() -> is);
					}
				}
				if (r.contentStreamProvider() == null) {
					request.removeHeaders("Content-Length");
				}

				for (Header header : request.getHeaders()) {
					r.putHeader(header.getName(), header.getValue());
				}

				SdkHttpFullRequest signedReq = signer.sign(r.build(), signerParams.build());

				for (String header : signedReq.headers().keySet()) {
					request.setHeader(header, signedReq.firstMatchingHeader(header).orElse(""));
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
