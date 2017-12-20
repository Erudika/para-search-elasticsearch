/*
 * Copyright 2013-2017 Erudika. https://erudika.com
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

import com.erudika.para.core.ParaObject;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
	private static final String DATE_FORMAT = "epoch_millis||epoch_second||yyyy-MM-dd HH:mm:ss||"
			+ "yyyy-MM-dd||yyyy/MM/dd||yyyyMMdd||yyyy";
	/**
	 * A list of default mappings that are defined upon index creation.
	 */
	private static final String DEFAULT_MAPPING =
			"{\n" +
			"  \"_default_\": {\n" +
			"    \"properties\": {\n" +
			"      \"nstd\": {\"type\": \"nested\"},\n" +
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
			create.addMapping("_default_", DEFAULT_MAPPING, XContentType.JSON);
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
			boolean aliased = addIndexAlias(indexName, appid);
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
			int queueSize = 50;
			int count = 0;
			Pager p = getPager(pager);
			p.setLimit(100);

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
						if (brb.numberOfActions() >= queueSize) {
							count += brb.numberOfActions();
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
				count += brb.numberOfActions();
				resp = brb.execute().actionGet();
				logger.info("rebuildIndex(): indexed {}, failures: {}",
						brb.numberOfActions(), resp.hasFailures() ? resp.buildFailureMessage() : "false");
			}

			if (!isShared) {
				// switch to alias NEW_INDEX -> ALIAS, OLD_INDEX -> DELETE old index
				switchIndexToAlias(oldName, newName, indexName, true);
			}
			logger.info("rebuildIndex(): Done. {} objects reindexed.", count);
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
	 * Adds a new alias to an existing index.
	 * @param indexName the index name
	 * @param aliasName the alias
	 * @return true if acknowledged
	 */
	public static boolean addIndexAlias(String indexName, String aliasName) {
		if (StringUtils.isBlank(aliasName) || !existsIndex(indexName)) {
			return false;
		}
		try {
			String alias = aliasName.trim();
			String index = getIndexNameWithWildcard(indexName.trim());
			return getClient().admin().indices().prepareAliases().addAliasAction(AliasActions.add().
					index(index).alias(alias).searchRouting(alias).indexRouting(alias).
					filter(QueryBuilders.termQuery(Config._APPID, aliasName))).// DO NOT trim filter query!
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
		BoolQueryBuilder fb = QueryBuilders.boolQuery();
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
				bfb = QueryBuilders.termQuery(term.getKey(), stringValue);
				if (matcher.matches()) {
					String key = term.getKey().replaceAll("[<>=\\s]+$", "");
					RangeQueryBuilder rfb = QueryBuilders.rangeQuery(key);
					if (">".equals(matcher.group(1))) {
						bfb = rfb.gt(stringValue);
					} else if ("<".equals(matcher.group(1))) {
						bfb = rfb.lt(stringValue);
					} else if (">=".equals(matcher.group(1))) {
						bfb = rfb.gte(stringValue);
					} else if ("<=".equals(matcher.group(1))) {
						bfb = rfb.lte(stringValue);
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

	/**
	 * Converts a {@link ParaObject} to a map of fields and values.
	 * @param po an object
	 * @return a map of keys and values
	 */
	static Map<String, Object> getSourceFromParaObject(ParaObject po) {
		if (po == null) {
			return Collections.emptyMap();
		}
		Map<String, Object> data = ParaObjectUtils.getAnnotatedFields(po, null, false);
		Map<String, Object> source = new HashMap<>(data.size() + 1);
		source.putAll(data);
		// special DOC ID field used in "search after"
		source.put("_docid", NumberUtils.toLong(Utils.getNewId()));
		return source;
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
		data.remove("_docid");
		return ParaObjectUtils.setAnnotatedFields(data);
	}

	/**
	 * A method reserved for future use. It allows to have indexes with different names than the appid.
	 *
	 * @param appid an app identifer
	 * @return the correct index name
	 */
	protected static String getIndexName(String appid) {
		return appid.trim();
	}

	/**
	 * Para indices have 1 type only - "paraobject". From v6 onwards, ES allows only 1 type per index.
	 * @return "paraobject"
	 */
	protected static String getType() {
		return ParaObject.class.getSimpleName().toLowerCase();
	}

	/**
	 * @param indexName index name or alias
	 * @return e.g. "index-name_*"
	 */
	protected static String getIndexNameWithWildcard(String indexName) {
		return StringUtils.contains(indexName, "_") ? indexName : indexName + "_*"; // ES v6
	}
}
