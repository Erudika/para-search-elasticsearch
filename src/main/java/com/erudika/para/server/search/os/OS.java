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

import com.erudika.para.core.Address;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Tag;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.core.utils.Utils;
import static com.erudika.para.server.search.os.OSUtils.PROPS_PREFIX;
import static com.erudika.para.server.search.os.OSUtils.convertQueryStringToNestedQuery;
import static com.erudika.para.server.search.os.OSUtils.executeRequests;
import static com.erudika.para.server.search.os.OSUtils.getIndexName;
import static com.erudika.para.server.search.os.OSUtils.getNestedKey;
import static com.erudika.para.server.search.os.OSUtils.getPager;
import static com.erudika.para.server.search.os.OSUtils.getRESTClient;
import static com.erudika.para.server.search.os.OSUtils.getTermsQuery;
import static com.erudika.para.server.search.os.OSUtils.getValueFieldName;
import static com.erudika.para.server.search.os.OSUtils.keyValueBoolQuery;
import static com.erudika.para.server.search.os.OSUtils.nestedMode;
import static com.erudika.para.server.search.os.OSUtils.nestedPropsQuery;
import static com.erudika.para.server.search.os.OSUtils.qs;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import static org.apache.lucene.search.join.ScoreMode.Avg;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.opensearch.index.query.QueryBuilder;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.opensearch.index.query.QueryBuilders.idsQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.prefixQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.QueryBuilders.termsQuery;
import static org.opensearch.index.query.QueryBuilders.wildcardQuery;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class OS {

	private static final Logger logger = LoggerFactory.getLogger(OS.class);
	private static DAO dao;

	/**
	 * No-args constructor.
	 */
	private OS() { }

	public static void setDao(DAO dao) {
		OS.dao = dao;
	}

	public static <P extends ParaObject> void indexAllInternal(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		try {
			executeRequests(objects.stream().
					filter(Objects::nonNull).
					map(obj -> new IndexRequest(getIndexName(appid)).id(obj.getId()).
						source(OSUtils.getSourceFromParaObject(obj))).
					collect(Collectors.toList()));
			logger.debug("Search.indexAll() {}", objects.size());
		} catch (Exception e) {
			logger.warn(null, e);
		}
	}

	public static <P extends ParaObject> void unindexAllInternal(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		try {
			executeRequests(objects.stream().
					filter(Objects::nonNull).
					map(obj -> new DeleteRequest(getIndexName(appid)).id(obj.getId())).
					collect(Collectors.toList()));
			logger.debug("Search.unindexAll() {}", objects.size());
		} catch (Exception e) {
			logger.warn(null, e);
		}
	}

	public static void unindexAllInternal(String appid, Map<String, ?> terms, boolean matchAll) {
		if (StringUtils.isBlank(appid)) {
			return;
		}
		try {
			long time = System.nanoTime();
			long unindexedCount = OSUtils.deleteByQuery(appid,
					(terms == null || terms.isEmpty()) ? matchAllQuery() : getTermsQuery(terms, matchAll));
			time = System.nanoTime() - time;
			logger.info("Unindexed {} documents without failures, took {}s.",
					unindexedCount, TimeUnit.NANOSECONDS.toSeconds(time));
		} catch (Exception e) {
			logger.warn(null, e);
		}
	}

	public static <P extends ParaObject> P findByIdInternal(String appid, String id) {
		try {
			return OSUtils.getParaObjectFromSource(getSource(appid, id));
		} catch (Exception e) {
			logger.warn(null, e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <P extends ParaObject> List<P> findByIdsInternal(String appid, List<String> ids) {
		List<P> list = new LinkedList<P>();
		if (ids == null || ids.isEmpty()) {
			return list;
		}
		try {
			QueryBuilder qb = termsQuery(Config._ID, ids);
			return searchQuery(appid, null, qb);
		} catch (Exception e) {
			logger.warn(null, e);
		}
		return list;
	}

	public static <P extends ParaObject> List<P> findTermInListInternal(String appid, String type,
			String field, List<?> terms, Pager... pager) {
		if (StringUtils.isBlank(field) || terms == null) {
			return Collections.emptyList();
		}
		QueryBuilder qb;
		if (nestedMode() && field.startsWith(PROPS_PREFIX)) {
			QueryBuilder bfb = null;
			BoolQueryBuilder fb = boolQuery();
			for (Object term : terms) {
				bfb = keyValueBoolQuery(field, String.valueOf(term));
				fb.should(bfb);
			}
			qb = nestedPropsQuery(terms.size() > 1 ? fb : bfb);
		} else {
			qb = termsQuery(field, terms);
		}
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findPrefixInternal(String appid, String type,
			String field, String prefix, Pager... pager) {
		if (StringUtils.isBlank(field) || StringUtils.isBlank(prefix)) {
			return Collections.emptyList();
		}
		QueryBuilder qb;
		if (nestedMode() && field.startsWith(PROPS_PREFIX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, prefixQuery(getValueFieldName(prefix), prefix)));
		} else {
			qb = prefixQuery(field, prefix);
		}
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findQueryInternal(String appid, String type,
			String query, Pager... pager) {
		if (StringUtils.isBlank(query)) {
			return Collections.emptyList();
		}
		// a basic implementation of support for nested queries in query string
		// https://github.com/elastic/elasticsearch/issues/11322
		QueryBuilder qb;
		if (nestedMode()) {
			qb = convertQueryStringToNestedQuery(query);
			if (qb == null) {
				return Collections.emptyList();
			}
		} else {
			qb = queryStringQuery(qs(query)).allowLeadingWildcard(false);
		}
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findNestedQueryInternal(String appid, String type, String field,
			String query, Pager... pager) {
		if (StringUtils.isBlank(query) || StringUtils.isBlank(field)) {
			return Collections.emptyList();
		}
		String queryString = "nstd." + field + ":" + query;
		QueryBuilder qb = nestedQuery("nstd", queryStringQuery(qs(queryString)), Avg);
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findWildcardInternal(String appid, String type,
			String field, String wildcard, Pager... pager) {
		if (StringUtils.isBlank(field) || StringUtils.isBlank(wildcard)) {
			return Collections.emptyList();
		}
		QueryBuilder qb;
		if (nestedMode() && field.startsWith(PROPS_PREFIX)) {
			qb = nestedPropsQuery(keyValueBoolQuery(field, wildcardQuery(getValueFieldName(wildcard), wildcard)));
		} else {
			qb = wildcardQuery(field, wildcard);
		}
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findTaggedInternal(String appid, String type,
			String[] tags, Pager... pager) {
		if (tags == null || tags.length == 0 || StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}

		BoolQueryBuilder tagFilter = boolQuery();
		//assuming clean & safe tags here
		for (String tag : tags) {
			tagFilter.must(termQuery(Config._TAGS, tag));
		}
		// The filter looks like this: ("tag1" OR "tag2" OR "tag3") AND "type"
		return searchQuery(appid, type, tagFilter, pager);
	}

	@SuppressWarnings("unchecked")
	public static <P extends ParaObject> List<P> findTermsInternal(String appid, String type,
			Map<String, ?> terms, boolean mustMatchAll, Pager... pager) {
		if (terms == null || terms.isEmpty()) {
			return Collections.emptyList();
		}

		QueryBuilder fb = getTermsQuery(terms, mustMatchAll);

		if (fb == null) {
			return Collections.emptyList();
		} else {
			return searchQuery(appid, type, fb, pager);
		}
	}

	public static <P extends ParaObject> List<P> findSimilarInternal(String appid, String type, String filterKey,
			String[] fields, String liketext, Pager... pager) {
		if (StringUtils.isBlank(liketext)) {
			return Collections.emptyList();
		}
		QueryBuilder qb;

		String matchPercent = "70%";
		if (fields == null || fields.length == 0) {
			qb = moreLikeThisQuery(new String[]{liketext}).minDocFreq(1).minTermFreq(1).minimumShouldMatch(matchPercent);
		} else {
			boolean containsNestedProps = Arrays.stream(fields).anyMatch((f) -> StringUtils.startsWith(f, PROPS_PREFIX));
			if (nestedMode() && containsNestedProps) {
				BoolQueryBuilder bqb = boolQuery();
				for (String field : fields) {
					QueryBuilder kQuery = matchQuery(PROPS_PREFIX + "k", getNestedKey(field));
					QueryBuilder vQuery = moreLikeThisQuery(new String[]{PROPS_PREFIX + "v"},
							new String[]{liketext}, Item.EMPTY_ARRAY).minDocFreq(1).minTermFreq(1).
							minimumShouldMatch(matchPercent);
					bqb.should(nestedPropsQuery(boolQuery().must(kQuery).must(vQuery)));
				}
				qb = bqb;
			} else {
				qb = moreLikeThisQuery(fields, new String[]{liketext}, Item.EMPTY_ARRAY).
						minDocFreq(1).minTermFreq(1).minimumShouldMatch(matchPercent);
			}
		}

		if (!StringUtils.isBlank(filterKey)) {
			qb = boolQuery().mustNot(termQuery(Config._ID, filterKey)).filter(qb);
		}
		return searchQuery(appid, searchQueryRaw(appid, type, qb, pager));
	}

	public static <P extends ParaObject> List<P> findTagsInternal(String appid, String keyword, Pager... pager) {
		if (StringUtils.isBlank(keyword)) {
			return Collections.emptyList();
		}
		QueryBuilder qb = wildcardQuery("tag", keyword.concat("*"));
		return searchQuery(appid, Utils.type(Tag.class), qb, pager);
	}

	public static <P extends ParaObject> List<P> findNearbyInternal(String appid, String type,
		String query, int radius, double lat, double lng, Pager... pager) {

		if (StringUtils.isBlank(type) || StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}
		if (StringUtils.isBlank(query)) {
			query = "*";
		}
		// find nearby Address objects
		Pager page = getPager(pager);
		QueryBuilder qb1 = geoDistanceQuery("latlng").point(lat, lng).distance(radius, DistanceUnit.KILOMETERS);
		SearchHits hits1 = searchQueryRaw(appid, Utils.type(Address.class), qb1, page);
		page.setLastKey(null); // will cause problems if not cleared

		if (hits1 == null) {
			return Collections.emptyList();
		}

		if (type.equals(Utils.type(Address.class))) {
			return searchQuery(appid, hits1);
		}

		// then find their parent objects
		String[] parentids = new String[hits1.getHits().length];
		for (int i = 0; i < hits1.getHits().length; i++) {
			Object pid = hits1.getAt(i).getSourceAsMap().get(Config._PARENTID);
			if (pid != null) {
				parentids[i] = (String) pid;
			}
		}

		QueryBuilder qb2 = boolQuery().must(queryStringQuery(qs(query))).filter(idsQuery().addIds(parentids));
		SearchHits hits2 = searchQueryRaw(appid, type, qb2, page);
		return searchQuery(appid, hits2);
	}

	private static <P extends ParaObject> List<P> searchQuery(String appid, String type,
			QueryBuilder query, Pager... pager) {
		return searchQuery(appid, searchQueryRaw(appid, type, query, pager));
	}

	/**
	 * Processes the results of searchQueryRaw() and fetches the results from the data store (can be disabled).
	 * @param <P> type of object
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @param hits the search results from a query
	 * @return the list of object found
	 */
	protected static <P extends ParaObject> List<P> searchQuery(final String appid, SearchHits hits) {
		if (hits == null) {
			return Collections.emptyList();
		}
		List<P> results = new ArrayList<P>(hits.getHits().length);
		List<String> keys = new LinkedList<String>();
		boolean readFromIndex = Para.getConfig().getConfigBoolean("read_from_index", false);
		boolean cleanupIndex = Para.getConfig().getConfigBoolean("sync_index_with_db", true);
		try {
			for (SearchHit hit : hits) {
				if (readFromIndex) {
					P pobj = OSUtils.getParaObjectFromSource(hit.getSourceAsMap());
					results.add(pobj);
				} else {
					keys.add(hit.getId());
				}
				logger.debug("Search result: appid={}, {}->{}", appid, hit.getSourceAsMap().get(Config._APPID), hit.getId());
			}

			if (!readFromIndex && !keys.isEmpty()) {
				List<P> objectsMissingFromDB = new ArrayList<>(results.size());
				Map<String, P> fromDB = dao.readAll(appid, keys, true);
				for (int i = 0; i < keys.size(); i++) {
					String key = keys.get(i);
					P pobj = fromDB.get(key);
					if (pobj == null) {
						pobj = OSUtils.getParaObjectFromSource(hits.getAt(i).getSourceAsMap());
						// show warning that object is still in index but not in DB
						if (pobj != null && appid.equals(pobj.getAppid()) && pobj.getStored()) {
							objectsMissingFromDB.add(pobj);
						}
					}
					if (pobj != null) {
						results.add(pobj);
					}
				}

				if (!objectsMissingFromDB.isEmpty()) {
					handleMissingObjects(appid, objectsMissingFromDB, cleanupIndex);
				}
			}
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.warn("Search query failed for app '{}': {}", appid, msg);
		}
		return results;
	}

	private static <P extends ParaObject> void handleMissingObjects(String appid, List<P> objectsMissingFromDB, boolean cleanupIndex) {
		if (cleanupIndex) {
			unindexAllInternal(appid, objectsMissingFromDB);
			logger.debug("Removed {} objects from index in app '{}' that were not found in database: {}.",
					objectsMissingFromDB.size(), appid,
					objectsMissingFromDB.stream().map(o -> o.getId()).collect(Collectors.toList()));
		} else {
			logger.warn("Found {} objects in app '{}' that are still indexed but deleted from the database: {}. "
					+ "Sometimes this happens if you do a search right after a delete operation.",
					objectsMissingFromDB.size(), appid, objectsMissingFromDB);
		}
	}

	/**
	 * Executes an ElasticSearch query. This is the core method of the class.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @param type type of object
	 * @param query the search query builder
	 * @param pager a {@link com.erudika.para.core.utils.Pager}
	 * @return a list of search results
	 */
	protected static SearchHits searchQueryRaw(String appid, String type, QueryBuilder query, Pager... pager) {
		if (StringUtils.isBlank(appid)) {
			return null;
		}
		Pager page = OSUtils.getPager(pager);
		SortOrder order = page.isDesc() ? SortOrder.DESC : SortOrder.ASC;
		int max = page.getLimit();
		int pageNum = (int) page.getPage();
		int start = (pageNum < 1 || pageNum > Para.getConfig().maxPages()) ? 0 : (pageNum - 1) * max;

		if (query == null) {
			query = matchAllQuery();
		}
		if (!StringUtils.isBlank(type)) {
			query = boolQuery().must(query).must(termQuery(Config._TYPE, type));
		}

		SearchHits hits = null;
		String debugQuery = "";
		try {
			SearchRequest search = new SearchRequest(getIndexName(appid)).
					searchType(SearchType.DFS_QUERY_THEN_FETCH).
					source(OSUtils.getSourceBuilder(query, max));

			if (pageNum <= 1 && !StringUtils.isBlank(page.getLastKey())) {
				search.source().searchAfter(new Object[]{NumberUtils.toLong(page.getLastKey())});
				search.source().from(0);
				search.source().sort(SortBuilders.fieldSort("_docid").order(order));
			} else {
				search.source().from(start);
				for (SortBuilder<?> sortField : OSUtils.getSortFieldsFromPager(page)) {
					search.source().sort(sortField);
				}
			}

			debugQuery = search.toString();
			logger.debug("Elasticsearch query: {}", debugQuery);

			hits = getRESTClient().search(search, RequestOptions.DEFAULT).getHits();
			page.setCount(hits.getTotalHits().value);
			if (hits.getHits().length > 0) {
				Object id = hits.getAt(hits.getHits().length - 1).getSourceAsMap().get("_docid");
				if (id != null) {
					page.setLastKey(id.toString());
				}
			}
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.debug("No search results for type '{}' in app '{}': {}.\nQuery: {}", type, appid, msg, debugQuery);
		}

		return hits;
	}

	/**
	 * Returns the source (a map of fields and values) for and object.
	 * The source is extracted from the index directly not the data store.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @param key the object id
	 * @return a map representation of the object
	 */
	protected static Map<String, Object> getSource(String appid, String key) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (StringUtils.isBlank(key) || StringUtils.isBlank(appid)) {
			return map;
		}
		try {
			GetRequest get = new GetRequest().index(getIndexName(appid)).id(key);
			GetResponse gres = getRESTClient().get(get, RequestOptions.DEFAULT);
			if (gres.isExists()) {
				map = gres.getSource();
			}
		} catch (IndexNotFoundException ex) {
			logger.warn("Index not created yet. Call '_setup' first.");
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.warn("Could not get any data from index '{}': {}", appid, msg);
		}
		return map;
	}

	public static Long getCountInternal(String appid, String type) {
		if (StringUtils.isBlank(appid)) {
			return 0L;
		}
		QueryBuilder query;
		if (!StringUtils.isBlank(type)) {
			query = boolQuery().must(termQuery(Config._TYPE, type));
		} else {
			query = matchAllQuery();
		}
		Long count = 0L;
		try {
			CountRequest cr = new CountRequest(getIndexName(appid)).query(query);
			count = getRESTClient().count(cr, RequestOptions.DEFAULT).getCount();
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.warn("Could not count results in index '{}': {}", appid, msg);
		}
		return count;
	}

	public static Long getCountInternal(String appid, String type, Map<String, ?> terms) {
		if (StringUtils.isBlank(appid) || terms == null || terms.isEmpty()) {
			return 0L;
		}
		Long count = 0L;
		QueryBuilder query = getTermsQuery(terms, true);
		if (query != null) {
			if (!StringUtils.isBlank(type)) {
				query = boolQuery().must(query).must(termQuery(Config._TYPE, type));
			}
			try {
				CountRequest cr = new CountRequest(getIndexName(appid)).query(query);
				count = getRESTClient().count(cr, RequestOptions.DEFAULT).getCount();
			} catch (Exception e) {
				Throwable cause = e.getCause();
				String msg = cause != null ? cause.getMessage() : e.getMessage();
				logger.warn("Could not count results in index '{}': {}", appid, msg);
			}
		}
		return count;
	}

	public boolean rebuildIndex(DAO dao, App app, Pager... pager) {
		return OSUtils.rebuildIndex(dao, app, null, pager);
	}

	public boolean rebuildIndex(DAO dao, App app, String destinationIndex, Pager... pager) {
		return OSUtils.rebuildIndex(dao, app, destinationIndex, pager);
	}



}
