/*
 * Copyright 2013-2018 Erudika. https://erudika.com
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

import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Sysprop;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.persistence.DAO;
import com.erudika.para.utils.Config;
import com.erudika.para.utils.Pager;
import com.erudika.para.utils.Utils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.index.Term;
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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper utilities for connecting to an Elasticsearch cluster.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class ElasticSearchUtils {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUtils.class);
	private static TransportClient searchClient;
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
	 * A list of default mappings that are defined upon index creation.
	 */
	private static String getDefaultMapping() {
		return "{\n" +
			"  \"paraobject\": {\n" +
			"    \"properties\": {\n" +
			"      \"nstd\": {\"type\": \"nested\"},\n" +
			"      \"properties\": {\"type\": \"" + (nestedMode() ? "nested" : "object") + "\"},\n" +
			"      \"latlng\": {\"type\": \"geo_point\"},\n" +
			"      \"_docid\": {\"type\": \"long\", \"index\": false},\n" +
			"      \"updated\": {\"type\": \"date\", \"format\" : \"" + DATE_FORMAT + "\"},\n" +
			"      \"timestamp\": {\"type\": \"date\", \"format\" : \"" + DATE_FORMAT + "\"},\n" +

			"      \"tag\": {\"type\": \"keyword\"},\n" +
			"      \"id\": {\"type\": \"keyword\"},\n" +
			"      \"key\": {\"type\": \"keyword\"},\n" +
			"      \"name\": {\"type\": \"keyword\"},\n" +
			"      \"type\": {\"type\": \"keyword\"},\n" +
			"      \"tags\": {\"type\": \"keyword\"},\n" +
			"      \"token\": {\"type\": \"keyword\"},\n" +
			"      \"email\": {\"type\": \"keyword\"},\n" +
			"      \"appid\": {\"type\": \"keyword\"},\n" +
			"      \"groups\": {\"type\": \"keyword\"},\n" +
			"      \"password\": {\"type\": \"keyword\"},\n" +
			"      \"parentid\": {\"type\": \"keyword\"},\n" +
			"      \"creatorid\": {\"type\": \"keyword\"},\n" +
			"      \"identifier\": {\"type\": \"keyword\"}\n" +
			"    }\n" +
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

	/**
	 * Creates an instance of the client that talks to Elasticsearch.
	 * @return a client instance
	 */
	public static Client getClient() {
		if (searchClient != null) {
			return searchClient;
		}
		String esHost = Config.getConfigParam("es.transportclient_host", "localhost");
		int esPort = Config.getConfigInt("es.transportclient_port", 9300);
		boolean useTransportClient = Config.getConfigBoolean("es.use_transportclient", true);

		Settings.Builder settings = Settings.builder();
		settings.put("client.transport.sniff", true);
		settings.put("cluster.name", Config.CLUSTER_NAME);

		if (useTransportClient) {
			searchClient = new PreBuiltTransportClient(settings.build());
			TransportAddress addr;
			try {
				addr = new TransportAddress(InetAddress.getByName(esHost), esPort);
			} catch (UnknownHostException ex) {
				addr = new TransportAddress(InetAddress.getLoopbackAddress(), esPort);
				logger.warn("Unknown host: " + esHost, ex);
			}
			searchClient.addTransportAddress(addr);
		} else {
			throw new UnsupportedOperationException("REST client is yet to be supported in Elasticsearch v6.");
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdownClient();
			}
		});
		// wait for the shards to initialize - prevents NoShardAvailableActionException!
		String timeout = Config.IN_PRODUCTION ? "1m" : "5s";
		searchClient.admin().cluster().prepareHealth(Config.getRootAppIdentifier()).
				setWaitForGreenStatus().setTimeout(timeout).execute().actionGet();

		if (!existsIndex(Config.getRootAppIdentifier())) {
			createIndex(Config.getRootAppIdentifier());
		}
		return searchClient;
	}

	/**
	 * Stops the client instance and releases resources.
	 */
	protected static void shutdownClient() {
		if (searchClient != null) {
			searchClient.close();
			searchClient = null;
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

			CreateIndexRequestBuilder create = getClient().admin().indices().prepareCreate(name).
					setSettings(settings.build());

			// default system mapping (all the rest are dynamic)
			create.addMapping("paraobject", getDefaultMapping(), XContentType.JSON);
			create.execute().actionGet();
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
			String indexName = getIndexNameWithWildcard(appid.trim());
			logger.info("Deleted ES index '{}'.", indexName);
			getClient().admin().indices().prepareDelete(indexName).execute().actionGet();
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
		boolean exists = true;
		try {
			String indexName = appid.trim();
			exists = getClient().admin().indices().prepareExists(indexName).execute().
					actionGet().isExists();
		} catch (Exception e) {
			logger.warn(null, e);
		}
		return exists;
	}

	/**
	 * Rebuilds an index.
	 * Reads objects from the data store and indexes them in batches.
	 * Works on one DB table and index only.
	 * @param dao DAO for connecting to the DB - the primary data source
	 * @param appid the index name (alias)
	 * @param isShared is the app shared, controls index aliases and index switching
	 * @param pager a Pager instance
	 * @return true if successful, false if index doesn't exist or failed.
	 */
	public static boolean rebuildIndex(DAO dao, String appid, boolean isShared, Pager... pager) {
		if (StringUtils.isBlank(appid) || dao == null) {
			return false;
		}
		try {
			String indexName = appid.trim();
			if (!isShared && !existsIndex(indexName)) {
				logger.info("Creating '{}' index because it doesn't exist.", indexName);
				createIndex(indexName);
			}
			String oldName = getIndexNameForAlias(indexName);
			String newName = indexName;

			if (!isShared) {
				newName = getNewIndexName(indexName, oldName);
				createIndexWithoutAlias(newName, -1, -1); // use defaults
			}

			logger.info("rebuildIndex(): {}", indexName);

			BulkRequestBuilder brb = getClient().prepareBulk();
			BulkResponse resp;
			Pager p = getPager(pager);
			int batchSize = Config.getConfigInt("reindex_batch_size", p.getLimit());
			long reindexedCount = 0;

			List<ParaObject> list;
			do {
				list = dao.readPage(appid, p); // use appid!
				logger.debug("rebuildIndex(): Read {} objects from table {}.", list.size(), indexName);
				for (ParaObject obj : list) {
					if (obj != null) {
						// put objects from DB into the newly created index
						brb.add(getClient().prepareIndex(newName, getType(), obj.getId()).
								setSource(getSourceFromParaObject(obj)).request());
						// index in batches of ${queueSize} objects
						if (brb.numberOfActions() >= batchSize) {
							reindexedCount += brb.numberOfActions();
							resp = brb.execute().actionGet();
							logger.info("rebuildIndex(): indexed {}, failures: {}",
									brb.numberOfActions(), resp.hasFailures() ? resp.buildFailureMessage() : "false");
							brb = getClient().prepareBulk();
						}
					}
				}
			} while (!list.isEmpty());

			// anything left after loop? index that too
			if (brb.numberOfActions() > 0) {
				reindexedCount += brb.numberOfActions();
				resp = brb.execute().actionGet();
				logger.info("rebuildIndex(): indexed {}, failures: {}",
						brb.numberOfActions(), resp.hasFailures() ? resp.buildFailureMessage() : "false");
			}

			if (!isShared) {
				// switch to alias NEW_INDEX -> ALIAS, OLD_INDEX -> DELETE old index
				switchIndexToAlias(oldName, newName, indexName, true);
			}
			logger.info("rebuildIndex(): Done. {} objects reindexed.", reindexedCount);
		} catch (Exception e) {
			logger.warn(null, e);
			return false;
		}
		return true;
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
				sortFields.add(SortBuilders.fieldSort(fieldName).order(order));
			}
			return sortFields;
		} else {
			return Collections.singletonList(StringUtils.isBlank(pager.getSortby()) ?
					SortBuilders.scoreSort() : SortBuilders.fieldSort(pager.getSortby()).order(defaultOrder));
		}
	}

	/**
	 * Returns information about a cluster.
	 * @return a map of key value pairs containing cluster information
	 */
	public static Map<String, NodeInfo> getSearchClusterInfo() {
		NodesInfoResponse res = getClient().admin().cluster().nodesInfo(new NodesInfoRequest().all()).actionGet();
		return res.getNodesMap();
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
			AliasActions aliasBuilder;
			if (withAliasRouting) {
				aliasBuilder = AliasActions.add().index(index).alias(alias).
						searchRouting(alias).indexRouting(alias).
						filter(termQuery(Config._APPID, aliasName)); // DO NOT trim filter query!
			} else {
				aliasBuilder = AliasActions.add().index(index).alias(alias);
			}
			return getClient().admin().indices().prepareAliases().addAliasAction(aliasBuilder).
					execute().actionGet().isAcknowledged();
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
		String index = getIndexNameWithWildcard(indexName.trim());
		return getClient().admin().indices().prepareAliases().removeAlias(index, alias).
				execute().actionGet().isAcknowledged();
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
		String alias = aliasName.trim();
		String index = getIndexNameWithWildcard(indexName.trim());
		return getClient().admin().indices().prepareAliasesExist(index).addAliases(alias).
				execute().actionGet().exists();
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
			getClient().admin().indices().prepareAliases().
					addAlias(newName, aliaz).
					removeAlias(oldName, aliaz).
					execute().actionGet();
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
			GetIndexResponse result = getClient().admin().indices().prepareGetIndex().
					setIndices(appid).execute().actionGet();
			if (result.indices() != null && result.indices().length > 0) {
				return result.indices()[0];
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
	 * @param <T> type of ES Response
	 * @return a callback for handling ES response errors
	 */
	public static <T extends DocWriteResponse> ActionListener<T> getIndexResponseHandler() {
		return new ActionListener<T>() {
			public void onResponse(T response) {
				int status = response.status().getStatus();
				if (status >= 400) {
					logger.warn("Indexing object {}/{} might have failed - status {}.",
							response.getIndex(), response.getId(), status);
				}
			}
			public void onFailure(Exception e) {
				logger.error("Indexing failure: {}", e);
			}
		};
	}

	/**
	 * @return a callback for handling ES response errors for bulk requests
	 */
	public static ActionListener<BulkResponse> getBulkIndexResponseHandler() {
		return new ActionListener<BulkResponse>() {
			public void onResponse(BulkResponse response) {
				int status = response.status().getStatus();
				if (response.hasFailures() || status >= 400) {
					logger.warn("Indexing objects in bulk might have failed - status {}. Reason: {}",
							status, response.buildFailureMessage());
				}
			}
			public void onFailure(Exception e) {
				logger.error("Bulk indexing failure: {}", e);
			}
		};
	}

	/**
	 * Check if cluster status is green or yellow.
	 * @return false if status is red
	 */
	public static boolean isClusterOK() {
		return !getClient().admin().cluster().prepareClusterStats().execute().actionGet().
				getStatus().equals(ClusterHealthStatus.RED);
	}

	/**
	 * @return true if asynchronous indexing/unindexing is enabled.
	 */
	static boolean isAsyncEnabled() {
		return Config.getConfigBoolean("es.async_enabled", false);
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
					if (nestedMode() && term.getKey().startsWith(PROPS_PREFIX)) {
						bfb = nestedPropsQuery(keyValueBoolQuery(term.getKey(), stringValue));
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

	/**
	 * Converts a {@link ParaObject} to a map of fields and values.
	 * @param po an object
	 * @return a map of keys and values
	 */
	@SuppressWarnings("unchecked")
	static Map<String, Object> getSourceFromParaObject(ParaObject po) {
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
		String key = StringUtils.replaceAll(field, "[<>=\\s]+$", "");
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
			return nestedPropsQuery(keyValueBoolQuery(key, stringValue, rfb));
		} else {
			return rfb;
		}
	}

	/**
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
			logger.warn("Unknown query type in nested mode query syntax: {}", q.getClass());
		}
		return (qb == null) ? matchAllQuery() : qb;
	}

	private static QueryBuilder termRange(Query q) {
		QueryBuilder qb = null;
		TermRangeQuery trq = (TermRangeQuery) q;
		if (!StringUtils.isBlank(trq.getField())) {
			String from = trq.getLowerTerm() != null ? Term.toString(trq.getLowerTerm()) : "*";
			String to = trq.getUpperTerm() != null ? Term.toString(trq.getUpperTerm()) : "*";
			boolean nestedMode = nestedMode() && trq.getField().matches(PROPS_REGEX);
			qb = rangeQuery(nestedMode ? getValueFieldNameFromRange(from, to) : trq.getField());
			if ("*".equals(from) && "*".equals(to)) {
				qb = matchAllQuery();
			}
			if (!"*".equals(from)) {
				((RangeQueryBuilder) qb).from(getNumericValue(from)).includeLower(trq.includesLower());
			}
			if (!"*".equals(to)) {
				((RangeQueryBuilder) qb).to(getNumericValue(to)).includeUpper(trq.includesUpper());
			}
			if (nestedMode) {
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
			qb = multiMatchQuery(value);
		} else if (nestedMode() && field.matches(PROPS_REGEX)) {
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
			qb = multiMatchQuery(value);
		} else if (nestedMode() && field.matches(PROPS_REGEX)) {
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
			qb = multiMatchQuery(value);
		} else if (nestedMode() && field.matches(PROPS_REGEX)) {
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
			qb = multiMatchQuery(value);
		} else if (nestedMode() && field.matches(PROPS_REGEX)) {
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
	 * Para indices have 1 type only - "paraobject". From v6 onwards, ES allows only 1 type per index.
	 * @return "paraobject"
	 */
	static String getType() {
		return ParaObject.class.getSimpleName().toLowerCase();
	}

	/**
	 * @param indexName index name or alias
	 * @return e.g. "index-name_*"
	 */
	static String getIndexNameWithWildcard(String indexName) {
		return StringUtils.contains(indexName, "_") ? indexName : indexName + "_*"; // ES v6
	}
}
