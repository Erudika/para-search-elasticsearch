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

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SearchType;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.Like;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import com.erudika.para.core.Address;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Tag;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.core.utils.Utils;
import static com.erudika.para.server.search.es.ESUtils.PROPS_PREFIX;
import static com.erudika.para.server.search.es.ESUtils.convertQueryStringToNestedQuery;
import static com.erudika.para.server.search.es.ESUtils.executeRequests;
import static com.erudika.para.server.search.es.ESUtils.getIndexName;
import static com.erudika.para.server.search.es.ESUtils.getNestedKey;
import static com.erudika.para.server.search.es.ESUtils.getPager;
import static com.erudika.para.server.search.es.ESUtils.getRESTClient;
import static com.erudika.para.server.search.es.ESUtils.getTermsQuery;
import static com.erudika.para.server.search.es.ESUtils.getValueFieldName;
import static com.erudika.para.server.search.es.ESUtils.keyValueBoolQuery;
import static com.erudika.para.server.search.es.ESUtils.nestedMode;
import static com.erudika.para.server.search.es.ESUtils.nestedPropsQuery;
import static com.erudika.para.server.search.es.ESUtils.qs;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class ES {

	private static final Logger logger = LoggerFactory.getLogger(ES.class);
	private static DAO dao;

	/**
	 * No-args constructor.
	 */
	private ES() { }

	/**
	 * @param dao sets the DAO
	 */
	public static void setDao(DAO dao) {
		ES.dao = dao;
	}

	public static <P extends ParaObject> void indexAllInternal(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		try {
			executeRequests(objects.stream().
					filter(Objects::nonNull).
					map(obj -> BulkOperation.of(b -> b.update(d -> d.index(getIndexName(appid)).id(obj.getId()).
							action(a -> a.doc(ESUtils.getSourceFromParaObject(obj)).docAsUpsert(true))))).
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
					map(obj -> BulkOperation.of(b -> b.delete(d -> d.index(getIndexName(appid)).id(obj.getId())))).
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
			long unindexedCount = ESUtils.deleteByQuery(appid,
					(terms == null || terms.isEmpty()) ? QueryBuilders.matchAll().build() : getTermsQuery(terms, matchAll));
			time = System.nanoTime() - time;
			logger.info("Unindexed {} documents without failures, took {}s.",
					unindexedCount, TimeUnit.NANOSECONDS.toSeconds(time));
		} catch (Exception e) {
			logger.warn(null, e);
		}
	}

	public static <P extends ParaObject> P findByIdInternal(String appid, String id) {
		try {
			return ESUtils.getParaObjectFromSource(getSource(appid, id));
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
			List<FieldValue> values = ids.stream().filter(Objects::nonNull).
					map(v -> FieldValue.of(fv -> fv.stringValue(v))).collect(Collectors.toList());
			QueryVariant qb = QueryBuilders.terms().field(Config._ID).terms(t -> t.value(values)).build();
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
		QueryVariant qb;
		if (nestedMode() && field.startsWith(PROPS_PREFIX)) {
			QueryVariant bfb = null;
			BoolQuery.Builder fb = QueryBuilders.bool();
			for (Object term : terms) {
				bfb = keyValueBoolQuery(field, String.valueOf(term));
				fb.should(bfb._toQuery());
			}
			qb = (QueryVariant) nestedPropsQuery(terms.size() > 1 ? fb.build() : bfb).build();
		} else {
			List<FieldValue> values = terms.stream().filter(Objects::nonNull).
					map(v -> FieldValue.of(fv -> fv.stringValue(v.toString()))).collect(Collectors.toList());
			qb = QueryBuilders.terms().field(field).terms(t -> t.value(values)).build();
		}
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findPrefixInternal(String appid, String type,
			String field, String prefix, Pager... pager) {
		if (StringUtils.isBlank(field) || StringUtils.isBlank(prefix)) {
			return Collections.emptyList();
		}
		QueryVariant qb;
		if (nestedMode() && field.startsWith(PROPS_PREFIX)) {
			qb = (QueryVariant) nestedPropsQuery(keyValueBoolQuery(field, QueryBuilders.prefix().
					field(getValueFieldName(prefix)).value(prefix).build())).build();
		} else {
			qb = QueryBuilders.prefix().field(field).value(prefix).build();
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
		QueryVariant qb;
		if (nestedMode()) {
			qb = convertQueryStringToNestedQuery(query);
			if (qb == null) {
				return Collections.emptyList();
			}
		} else {
			String qs = qs(query);
			if ("*".equals(qs)) {
				qb = QueryBuilders.matchAll().build();
			} else {
				qb = QueryBuilders.queryString().query(qs).allowLeadingWildcard(false).build();
			}
		}
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findNestedQueryInternal(String appid, String type, String field,
			String query, Pager... pager) {
		if (StringUtils.isBlank(query) || StringUtils.isBlank(field)) {
			return Collections.emptyList();
		}
		String queryString = "nstd." + field + ":" + query;
		QueryVariant qb = QueryBuilders.nested().path("nstd").query(QueryBuilders.queryString().
				query(qs(queryString)).build()._toQuery()).scoreMode(ChildScoreMode.Avg).build();
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findWildcardInternal(String appid, String type,
			String field, String wildcard, Pager... pager) {
		if (StringUtils.isBlank(field) || StringUtils.isBlank(wildcard)) {
			return Collections.emptyList();
		}
		QueryVariant qb;
		if (nestedMode() && field.startsWith(PROPS_PREFIX)) {
			qb = (QueryVariant) nestedPropsQuery(keyValueBoolQuery(field, QueryBuilders.
					wildcard().field(getValueFieldName(wildcard)).value(wildcard).build())).build();
		} else {
			qb = QueryBuilders.wildcard().field(field).value(wildcard).build();
		}
		return searchQuery(appid, type, qb, pager);
	}

	public static <P extends ParaObject> List<P> findTaggedInternal(String appid, String type,
			String[] tags, Pager... pager) {
		if (tags == null || tags.length == 0 || StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}

		BoolQuery.Builder tagFilter = QueryBuilders.bool();
		//assuming clean & safe tags here
		for (String tag : tags) {
			tagFilter.must(QueryBuilders.term().field(Config._TAGS).value(v -> v.stringValue(tag)).build()._toQuery());
		}
		// The filter looks like this: ("tag1" OR "tag2" OR "tag3") AND "type"
		return searchQuery(appid, type, tagFilter.build(), pager);
	}

	@SuppressWarnings("unchecked")
	public static <P extends ParaObject> List<P> findTermsInternal(String appid, String type,
			Map<String, ?> terms, boolean mustMatchAll, Pager... pager) {
		if (terms == null || terms.isEmpty()) {
			return Collections.emptyList();
		}

		QueryVariant fb = getTermsQuery(terms, mustMatchAll);

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
		QueryVariant qb;

		String matchPercent = "70%";
		if (fields == null || fields.length == 0) {
			qb = QueryBuilders.moreLikeThis().like(l -> l.text(liketext)).
					minDocFreq(1).minTermFreq(1).minimumShouldMatch(matchPercent).build();
		} else {
			boolean containsNestedProps = Arrays.stream(fields).anyMatch((f) -> StringUtils.startsWith(f, PROPS_PREFIX));
			if (nestedMode() && containsNestedProps) {
				BoolQuery.Builder bqb = QueryBuilders.bool();
				for (String field : fields) {
					QueryVariant kQuery = QueryBuilders.match().field(PROPS_PREFIX + "k").
							query(v -> v.stringValue(getNestedKey(field))).build();
					QueryVariant vQuery = QueryBuilders.moreLikeThis().fields(PROPS_PREFIX + "v").
							like(Like.of(l -> l.text(liketext))).minDocFreq(1).minTermFreq(1).
							minimumShouldMatch(matchPercent).build();
					QueryVariant nested = (QueryVariant) nestedPropsQuery(QueryBuilders.bool().
							must(kQuery._toQuery(), vQuery._toQuery()).build()).build();
					bqb.should(nested._toQuery());
				}
				qb = bqb.build();
			} else {
				qb = QueryBuilders.moreLikeThis().fields(Arrays.asList(fields)).
						like(l -> l.text(liketext)).minDocFreq(1).minTermFreq(1).minimumShouldMatch(matchPercent).build();
			}
		}

		if (!StringUtils.isBlank(filterKey)) {
			qb = QueryBuilders.bool().mustNot(QueryBuilders.term().field(Config._ID).value(v -> v.stringValue(filterKey)).
					build()._toQuery()).filter(qb._toQuery()).build();
		}
		return searchQuery(appid, searchQueryRaw(appid, type, qb, pager));
	}

	public static <P extends ParaObject> List<P> findTagsInternal(String appid, String keyword, Pager... pager) {
		if (StringUtils.isBlank(keyword)) {
			return Collections.emptyList();
		}
		QueryVariant qb = QueryBuilders.wildcard().field("tag").value(keyword.concat("*")).build();
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
		QueryVariant qb1 = QueryBuilders.geoDistance().field("latlng").location(l -> l.latlon(ll -> ll.lat(lat).lon(lng))).
				distance(Integer.toString(radius) + " km").build();
		SearchResponse<Map> hits1 = searchQueryRaw(appid, Utils.type(Address.class), qb1, page);
		page.setLastKey(null); // will cause problems if not cleared

		if (hits1 == null) {
			return Collections.emptyList();
		}

		if (type.equals(Utils.type(Address.class))) {
			return searchQuery(appid, hits1);
		}

		// then find their parent objects
		List<String> parentIds = hits1.hits().hits().stream().filter(Objects::nonNull).
				map(h -> (String) h.source().get(Config._PARENTID)).collect(Collectors.toList());

		QueryVariant qb2 = QueryBuilders.bool().must(QueryBuilders.queryString().query(qs(query)).build()._toQuery()).
				filter(QueryBuilders.ids().values(parentIds).build()._toQuery()).build();
		SearchResponse<Map> hits2 = searchQueryRaw(appid, type, qb2, page);
		return searchQuery(appid, hits2);
	}

	private static <P extends ParaObject> List<P> searchQuery(String appid, String type,
			QueryVariant query, Pager... pager) {
		return searchQuery(appid, searchQueryRaw(appid, type, query, pager));
	}

	/**
	 * Processes the results of searchQueryRaw() and fetches the results from the data store (can be disabled).
	 * @param <P> type of object
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @param hits the search results from a query
	 * @return the list of object found
	 */
	@SuppressWarnings("unchecked")
	protected static <P extends ParaObject> List<P> searchQuery(final String appid, SearchResponse<Map> hits) {
		if (hits == null) {
			return Collections.emptyList();
		}
		List<P> results = new ArrayList<P>(hits.hits().hits().size());
		List<String> keys = new LinkedList<String>();
		boolean readFromIndex = Para.getConfig().readFromIndexEnabled();
		boolean cleanupIndex = Para.getConfig().syncIndexWithDatabaseEnabled();
		try {
			for (Hit<Map> hit : hits.hits().hits()) {
				if (readFromIndex) {
					P pobj = (P) ESUtils.getParaObjectFromSource(hit.source());
					results.add(pobj);
				} else {
					keys.add(hit.id());
				}
				logger.debug("Search result: appid={}, {}->{}", appid, hit.source().get(Config._APPID), hit.id());
			}

			if (!readFromIndex && !keys.isEmpty()) {
				List<P> objectsMissingFromDB = new ArrayList<>(results.size());
				Map<String, P> fromDB = dao.readAll(appid, keys, true);
				for (int i = 0; i < keys.size(); i++) {
					String key = keys.get(i);
					P pobj = fromDB.get(key);
					if (pobj == null) {
						pobj = (P) ESUtils.getParaObjectFromSource(hits.hits().hits().get(i).source());
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
	protected static SearchResponse<Map> searchQueryRaw(String appid, String type, QueryVariant query, Pager... pager) {
		if (StringUtils.isBlank(appid)) {
			return null;
		}
		Pager page = ESUtils.getPager(pager);
		SortOrder order = page.isDesc() ? SortOrder.Desc : SortOrder.Asc;
		int max = page.getLimit();
		int pageNum = (int) page.getPage();
		int start = (pageNum < 1 || pageNum > Para.getConfig().maxPages()) ? 0 : (pageNum - 1) * max;

		if (query == null) {
			query = QueryBuilders.matchAll().build();
		}
		if (!StringUtils.isBlank(type)) {
			query = QueryBuilders.bool().must(query._toQuery(),
					QueryBuilders.term().field(Config._TYPE).value(v -> v.stringValue(type)).build()._toQuery()).build();
		}

		SearchResponse<Map> hits = null;
		String debugQuery = "";
		try {
			SearchRequest.Builder search = new SearchRequest.Builder();
			search.index(getIndexName(appid)).
					searchType(SearchType.DfsQueryThenFetch).
					query(query._toQuery()).
					size(max).
					trackTotalHits(ESUtils.getTrackTotalHits());

			if (pageNum <= 1 && !StringUtils.isBlank(page.getLastKey())) {
				search.searchAfter(page.getLastKey());
				search.from(0);
				search.sort(SortOptions.of(b -> b.field(f -> f.field("_docid").order(order))));
			} else {
				search.from(start);
				for (SortOptions sortField : ESUtils.getSortFieldsFromPager(page)) {
					search.sort(sortField);
				}
			}

			debugQuery = search.toString();
			logger.debug("Elasticsearch query: {}", debugQuery);

			hits = getRESTClient().search(search.build(), Map.class);
			page.setCount(Optional.ofNullable(hits.hits().total()).orElse(TotalHits.
					of(t -> t.relation(TotalHitsRelation.Eq).value(page.getCount()))).value());
			if (hits.hits().hits().size() > 0) {
				Object id = hits.hits().hits().get(hits.hits().hits().size() - 1).source().get("_docid");
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
	@SuppressWarnings("unchecked")
	protected static Map<String, Object> getSource(String appid, String key) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (StringUtils.isBlank(key) || StringUtils.isBlank(appid)) {
			return map;
		}
		try {
			GetResponse<Map> gres = getRESTClient().get(b -> b.index(getIndexName(appid)).id(key), Map.class);
			if (gres.found()) {
				map = gres.source();
			}
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
		QueryVariant query;
		if (!StringUtils.isBlank(type)) {
			query = QueryBuilders.bool().must(QueryBuilders.term().field(Config._TYPE).
					value(v -> v.stringValue(type)).build()._toQuery()).build();
		} else {
			query = QueryBuilders.matchAll().build();
		}
		Long count = 0L;
		try {
			count = getRESTClient().count(b -> b.index(getIndexName(appid)).query(query._toQuery())).count();
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
		QueryVariant query = getTermsQuery(terms, true);
		if (query != null) {
			if (!StringUtils.isBlank(type)) {
				query = QueryBuilders.bool().must(query._toQuery(), QueryBuilders.term().field(Config._TYPE).
					value(v -> v.stringValue(type)).build()._toQuery()).build();
			}
			try {
				Query q = query._toQuery();
				count = getRESTClient().count(b -> b.index(getIndexName(appid)).query(q)).count();
			} catch (Exception e) {
				Throwable cause = e.getCause();
				String msg = cause != null ? cause.getMessage() : e.getMessage();
				logger.warn("Could not count results in index '{}': {}", appid, msg);
			}
		}
		return count;
	}

	public boolean rebuildIndex(DAO dao, App app, Pager... pager) {
		return ESUtils.rebuildIndex(dao, app, null, pager);
	}

	public boolean rebuildIndex(DAO dao, App app, String destinationIndex, Pager... pager) {
		return ESUtils.rebuildIndex(dao, app, destinationIndex, pager);
	}



}
