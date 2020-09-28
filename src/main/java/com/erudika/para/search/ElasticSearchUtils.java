/*
 * Copyright 2013-2020 Erudika. https://erudika.com
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
package com.erudika.para.search;

import com.erudika.para.DestroyListener;
import com.erudika.para.Para;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Sysprop;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.persistence.DAO;
import com.erudika.para.utils.Config;
import com.erudika.para.utils.Pager;
import com.erudika.para.utils.Utils;
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
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import static org.apache.lucene.search.join.ScoreMode.Avg;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
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
 * Helper utilities for connecting to an Elasticsearch cluster.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class ElasticSearchUtils {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUtils.class);
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
		return Config.getConfigBoolean("es.use_nested_custom_fields", false);
	}

	/**
	 * @return true if asynchronous indexing/unindexing is enabled.
	 */
	static boolean asyncEnabled() {
		return Config.getConfigBoolean("es.async_enabled", false);
	}

	/**
	 * @return true if we want the bulk processor to flush immediately after each bulk request.
	 */
	static boolean flushImmediately() {
		return Config.getConfigBoolean("es.bulk.flush_immediately", true);
	}

	/**
	 * A list of default mappings that are defined upon index creation.
	 */
	static String getDefaultMapping() {
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

	private ElasticSearchUtils() { }

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
		String esScheme = Config.getConfigParam("es.restclient_scheme", Config.IN_PRODUCTION ? "https" : "http");
		String esHost = Config.getConfigParam("es.restclient_host", "localhost");
		int esPort = Config.getConfigInt("es.restclient_port", 9200);
		boolean signRequests = Config.getConfigBoolean("es.sign_requests_to_aws", esHost.contains("amazonaws.com"));
		HttpHost host = new HttpHost(esHost, esPort, esScheme);
		RestClientBuilder clientBuilder = RestClient.builder(host);

		String esPrefix = Config.getConfigParam("es.restclient_context_path", "");
		if (StringUtils.isNotEmpty(esPrefix)) {
			clientBuilder.setPathPrefix(esPrefix);
		}

		List<RestClientBuilder.HttpClientConfigCallback> configurationCallbacks = new ArrayList<>();

		if (signRequests) {
			configurationCallbacks.add(getAWSRequestSigningInterceptor(host.toURI()));
		}
		configurationCallbacks.add(getAuthenticationCallback());

		// register all customizations
		clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
			configurationCallbacks.forEach(c -> c.customizeHttpClient(httpClientBuilder));
			return httpClientBuilder;
		});

		restClient = new RestHighLevelClient(clientBuilder);

		Para.addDestroyListener(new DestroyListener() {
			public void onDestroy() {
				shutdownClient();
			}
		});
		if (!existsIndex(Config.getRootAppIdentifier())) {
			createIndex(Config.getRootAppIdentifier());
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
			shards = Config.getConfigInt("es.shards", 5);
		}
		if (replicas < 0) {
			replicas = Config.getConfigInt("es.replicas", 0);
		}
		try {
			Settings.Builder settings = Settings.builder();
			settings.put("number_of_shards", Integer.toString(shards));
			settings.put("number_of_replicas", Integer.toString(replicas));
			settings.put("auto_expand_replicas", Config.getConfigParam("es.auto_expand_replicas", "0-1"));
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
		return createIndex(appid, Config.getConfigInt("es.shards", 5), Config.getConfigInt("es.replicas", 0));
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
			boolean withAliasRouting = App.isRoot(appid) && Config.getConfigBoolean("es.root_index_sharing_enabled", false);
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
					addIndexAliasWithRouting(getIndexName(Config.getRootAppIdentifier()), app.getAppIdentifier());
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
			int batchSize = Config.getConfigInt("reindex_batch_size", p.getLimit());
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
		try {
			String alias = aliasName.trim();
			String index = getIndexNameWithWildcard(indexName.trim());
			AliasActions removeAction = AliasActions.remove().index(index).alias(alias);
			IndicesAliasesRequest actions = new IndicesAliasesRequest().addAliasAction(removeAction);
			return getRESTClient().indices().updateAliases(actions, RequestOptions.DEFAULT).isAcknowledged();
		} catch (Exception e) {
			logger.error(null, e);
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
			Map<String, Set<AliasMetaData>> aliases = getRESTClient().indices().
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
		final int sizeLimit = Config.getConfigInt("es.bulk.size_limit_mb", 5);
		final int actionLimit = Config.getConfigInt("es.bulk.action_limit", 1000);
		final int concurrentRequests = Config.getConfigInt("es.bulk.concurrent_requests", 1);
		final int flushInterval = Config.getConfigInt("es.bulk.flush_interval_ms", 5000);
		final int backoffInitialDelayMs = Config.getConfigInt("es.bulk.backoff_initial_delay_ms", 50);
		final int backoffNumRetries = Config.getConfigInt("es.bulk.max_num_retries", 8);
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
						logger.error("Failed to execute async {} operation for index '{}', document id '{}': ",
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
								logger.error("Failed to execute {} operation for index '{}', document id '{}': ",
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
		if (t != null && Config.getConfigBoolean("es.fail_on_indexing_errors", false)) {
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

	static boolean isValidQueryString(String query) {
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
		String trackTotalHits = Config.getConfigParam("es.track_total_hits", "");
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
			throw new IllegalArgumentException("`Query depth exceeded! Max depth: " + MAX_QUERY_DEPTH);
		}
		QueryBuilder qb = null;
		if (q instanceof BooleanQuery) {
			qb = boolQuery();
			for (BooleanClause clause : ((BooleanQuery) q).clauses()) {
				switch (clause.getOccur()) {
					case MUST:
						((BoolQueryBuilder) qb).must(rewriteQuery(clause.getQuery(), depth++));
						break;
					case MUST_NOT:
						((BoolQueryBuilder) qb).mustNot(rewriteQuery(clause.getQuery(), depth++));
						break;
					case FILTER:
						((BoolQueryBuilder) qb).filter(rewriteQuery(clause.getQuery(), depth++));
						break;
					case SHOULD:
					default:
						((BoolQueryBuilder) qb).should(rewriteQuery(clause.getQuery(), depth++));
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
		QueryBuilder kQuery = matchQuery(PROPS_PREFIX + "k", getNestedKey(k));
		QueryBuilder vQuery = (query == null) ? matchQuery(getValueFieldName(v), v) : query;
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
	static RestClientBuilder.HttpClientConfigCallback getAWSRequestSigningInterceptor(String endpoint) {
		return (HttpAsyncClientBuilder httpClientBuilder) -> {
			httpClientBuilder.addInterceptorLast((HttpRequest request, HttpContext context) -> {
				Aws4Signer signer = Aws4Signer.create();
				AwsCredentials creds = DefaultCredentialsProvider.create().resolveCredentials();
				Aws4SignerParams.Builder<?> signerParams = Aws4SignerParams.builder().
						awsCredentials(creds).
						doubleUrlEncode(false).
						signingName("es").
						signingRegion(Region.of(Config.getConfigParam("es.aws_region", "eu-west-1")));
				URIBuilder uriBuilder;
				String httpMethod = request.getRequestLine().getMethod();
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

					uriBuilder = new URIBuilder(request.getRequestLine().getUri());
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

					for (Header header : request.getAllHeaders()) {
						r.putHeader(header.getName(), header.getValue());
					}

					signer.sign(r.build(), signerParams.build());

					for (String header : r.headers().keySet()) {
						request.setHeader(header, r.firstMatchingHeader(header).orElse(""));
					}
				} catch (Exception ex) {
					logger.error("Failed to sign request to AWS Elasticsearch:", ex);
				}
			});
			return httpClientBuilder;
		};
	}

	static RestClientBuilder.HttpClientConfigCallback getAuthenticationCallback() {
		final String basicAuthLogin = Config.getConfigParam("es.basic_auth_login", "");
		final String basicAuthPassword = Config.getConfigParam("es.basic_auth_password", "");

		if (StringUtils.isAnyEmpty(basicAuthLogin, basicAuthPassword)) {
			// no authentication
			return (HttpAsyncClientBuilder httpClientBuilder) -> httpClientBuilder;
		} else {
			// basic auth as documented by Elastic
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,
					new UsernamePasswordCredentials(basicAuthLogin, basicAuthPassword));
			return (HttpAsyncClientBuilder httpClientBuilder) -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		}
	}
}
