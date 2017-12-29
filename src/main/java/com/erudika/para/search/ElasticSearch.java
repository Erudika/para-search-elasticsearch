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

import com.erudika.para.AppCreatedListener;
import com.erudika.para.AppDeletedListener;
import com.erudika.para.core.Address;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Tag;
import com.erudika.para.core.utils.CoreUtils;
import com.erudika.para.persistence.DAO;
import static com.erudika.para.search.ElasticSearchUtils.getIndexName;
import static com.erudika.para.search.ElasticSearchUtils.getPager;
import static com.erudika.para.search.ElasticSearchUtils.getTermsQuery;
import static com.erudika.para.search.ElasticSearchUtils.getType;
import static com.erudika.para.search.ElasticSearchUtils.isAsyncEnabled;
import static com.erudika.para.search.ElasticSearchUtils.qs;
import com.erudika.para.utils.Config;
import com.erudika.para.utils.Pager;
import com.erudika.para.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import static org.apache.lucene.search.join.ScoreMode.Avg;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link Search} interface using ElasticSearch.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
@Singleton
public class ElasticSearch implements Search {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearch.class);
	private DAO dao;

	/**
	 * No-args constructor.
	 */
	public ElasticSearch() {
		if (Config.isSearchEnabled()) {
			ElasticSearchUtils.getClient();
		}
		// set up automatic index creation and deletion
		App.addAppCreatedListener(new AppCreatedListener() {
			public void onAppCreated(App app) {
				if (app != null) {
					String appid = app.getAppIdentifier();
					if (app.isSharingIndex()) {
						ElasticSearchUtils.addIndexAliasWithRouting(Config.getRootAppIdentifier(), appid);
					} else {
						int	shards = app.isRootApp() ? Config.getConfigInt("es.shards", 5) :
								Config.getConfigInt("es.shards_for_child_apps", 2);
						int	replicas = app.isRootApp() ? Config.getConfigInt("es.replicas", 0) :
								Config.getConfigInt("es.replicas_for_child_apps", 0);
						ElasticSearchUtils.createIndex(appid, shards, replicas);
					}
				}
			}
		});
		App.addAppDeletedListener(new AppDeletedListener() {
			public void onAppDeleted(App app) {
				if (app != null) {
					String appid = app.getAppIdentifier();
					if (app.isSharingIndex()) {
						CoreUtils.getInstance().getSearch().unindexAll(appid, null, true);
						ElasticSearchUtils.removeIndexAlias(Config.getRootAppIdentifier(), appid);
					} else {
						ElasticSearchUtils.deleteIndex(appid);
					}
				}
			}
		});
	}

	/**
	 * Default constructor.
	 * @param dao an instance of the persistence class
	 */
	@Inject
	public ElasticSearch(DAO dao) {
		this.dao = dao;
	}

	private DAO getDAO() {
		if (dao == null) {
			return CoreUtils.getInstance().getDao();
		}
		return dao;
	}

	Client client() {
		return ElasticSearchUtils.getClient();
	}

	@Override
	public void index(String appid, ParaObject po) {
		if (po == null || StringUtils.isBlank(appid)) {
			return;
		}
		try {
			IndexRequestBuilder irb = client().prepareIndex(getIndexName(appid), getType(), po.getId()).
					setSource(ElasticSearchUtils.getSourceFromParaObject(po));
			ActionListener<IndexResponse> responseHandler = ElasticSearchUtils.getIndexResponseHandler();
			if (isAsyncEnabled()) {
				irb.execute(responseHandler);
			} else {
				responseHandler.onResponse(irb.execute().actionGet());
			}
			logger.debug("Search.index() {}", po.getId());
		} catch (Exception e) {
			logger.warn(null, e);
		}
	}

	@Override
	public void unindex(String appid, ParaObject po) {
		if (po == null || StringUtils.isBlank(po.getId()) || StringUtils.isBlank(appid)) {
			return;
		}
		try {
			DeleteRequestBuilder drb = client().prepareDelete(getIndexName(appid), getType(), po.getId());
			ActionListener<DeleteResponse> responseHandler = ElasticSearchUtils.getIndexResponseHandler();
			if (isAsyncEnabled()) {
				drb.execute(responseHandler);
			} else {
				responseHandler.onResponse(drb.execute().actionGet());
			}
			logger.debug("Search.unindex() {}", po.getId());
		} catch (Exception e) {
			logger.warn(null, e);
		}
	}

	@Override
	public <P extends ParaObject> void indexAll(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		BulkRequestBuilder brb = client().prepareBulk();
		for (ParaObject po : objects) {
			brb.add(client().prepareIndex(getIndexName(appid), getType(), po.getId()).
					setSource(ElasticSearchUtils.getSourceFromParaObject(po)));
		}
		if (brb.numberOfActions() > 0) {
			ActionListener<BulkResponse> responseHandler = ElasticSearchUtils.getBulkIndexResponseHandler();
			if (isAsyncEnabled()) {
				brb.execute(responseHandler);
			} else {
				responseHandler.onResponse(brb.execute().actionGet());
			}
		}
		logger.debug("Search.indexAll() {}", objects.size());
	}

	@Override
	public <P extends ParaObject> void unindexAll(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		BulkRequestBuilder brb = client().prepareBulk();
		for (ParaObject po : objects) {
			brb.add(client().prepareDelete(getIndexName(appid), getType(), po.getId()));
		}
		if (brb.numberOfActions() > 0) {
			ActionListener<BulkResponse> responseHandler = ElasticSearchUtils.getBulkIndexResponseHandler();
			if (isAsyncEnabled()) {
				brb.execute(responseHandler);
			} else {
				responseHandler.onResponse(brb.execute().actionGet());
			}
		}
		logger.debug("Search.unindexAll() {}", objects.size());
	}

	@Override
	public void unindexAll(String appid, Map<String, ?> terms, boolean matchAll) {
		if (StringUtils.isBlank(appid)) {
			return;
		}

		QueryBuilder fb = (terms == null || terms.isEmpty()) ?
				QueryBuilders.matchAllQuery() : getTermsQuery(terms, matchAll);
		SearchResponse scrollResp = client().prepareSearch(getIndexName(appid))
				.setScroll(new TimeValue(60000))
				.setQuery(fb)
				.setSize(100).execute().actionGet();

		BulkRequestBuilder brb = client().prepareBulk();
		while (true) {
			for (SearchHit hit : scrollResp.getHits()) {
				brb.add(new DeleteRequest(getIndexName(appid), getType(), hit.getId()));
			}
			// next page
			scrollResp = client().prepareSearchScroll(scrollResp.getScrollId()).
					setScroll(new TimeValue(600000)).execute().actionGet();

			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
		if (brb.numberOfActions() > 0) {
			BulkResponse result = brb.execute().actionGet();
			if (result.hasFailures()) {
				logger.warn("Unindexed {} documents with failures ({}), took {}s.", brb.numberOfActions(),
						result.buildFailureMessage(), result.getTook().seconds());
			} else {
				logger.info("Unindexed {} documents without failures, took {}s.",
						brb.numberOfActions(), result.getTook().seconds());
			}
		}
	}

	@Override
	public <P extends ParaObject> P findById(String appid, String id) {
		try {
			return ElasticSearchUtils.getParaObjectFromSource(getSource(appid, id));
		} catch (Exception e) {
			logger.warn(null, e);
			return null;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> List<P> findByIds(String appid, List<String> ids) {
		List<P> list = new LinkedList<P>();
		if (ids == null || ids.isEmpty()) {
			return list;
		}
		try {
			QueryBuilder qb = QueryBuilders.termsQuery(Config._ID, ids);
			return searchQuery(appid, null, qb);
		} catch (Exception e) {
			logger.warn(null, e);
		}
		return list;
	}

	@Override
	public <P extends ParaObject> List<P> findTermInList(String appid, String type,
			String field, List<?> terms, Pager... pager) {
		if (StringUtils.isBlank(field) || terms == null) {
			return Collections.emptyList();
		}
		QueryBuilder qb = QueryBuilders.termsQuery(field, terms);
		return searchQuery(appid, type, qb, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findPrefix(String appid, String type,
			String field, String prefix, Pager... pager) {
		if (StringUtils.isBlank(field) || StringUtils.isBlank(prefix)) {
			return Collections.emptyList();
		}
		return searchQuery(appid, type, QueryBuilders.prefixQuery(field, prefix), pager);
	}

	@Override
	public <P extends ParaObject> List<P> findQuery(String appid, String type,
			String query, Pager... pager) {
		if (StringUtils.isBlank(query)) {
			return Collections.emptyList();
		}
		QueryBuilder qb = QueryBuilders.queryStringQuery(qs(query)).allowLeadingWildcard(false);
		return searchQuery(appid, type, qb, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findNestedQuery(String appid, String type, String field,
			String query, Pager... pager) {
		if (StringUtils.isBlank(query) || StringUtils.isBlank(field)) {
			return Collections.emptyList();
		}
		String queryString = "nstd." + field + ":" + query;
		QueryBuilder qb = QueryBuilders.nestedQuery("nstd", QueryBuilders.queryStringQuery(qs(queryString)), Avg);
		return searchQuery(appid, type, qb, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findWildcard(String appid, String type,
			String field, String wildcard, Pager... pager) {
		if (StringUtils.isBlank(field) || StringUtils.isBlank(wildcard)) {
			return Collections.emptyList();
		}
		QueryBuilder qb = QueryBuilders.wildcardQuery(field, wildcard);
		return searchQuery(appid, type, qb, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findTagged(String appid, String type,
			String[] tags, Pager... pager) {
		if (tags == null || tags.length == 0 || StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}

		BoolQueryBuilder tagFilter = QueryBuilders.boolQuery();
		//assuming clean & safe tags here
		for (String tag : tags) {
			tagFilter.must(QueryBuilders.termQuery(Config._TAGS, tag));
		}
		// The filter looks like this: ("tag1" OR "tag2" OR "tag3") AND "type"
		return searchQuery(appid, type, tagFilter, pager);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> List<P> findTerms(String appid, String type,
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

	@Override
	public <P extends ParaObject> List<P> findSimilar(String appid, String type, String filterKey,
			String[] fields, String liketext, Pager... pager) {
		if (StringUtils.isBlank(liketext)) {
			return Collections.emptyList();
		}
		QueryBuilder qb;

		if (fields == null || fields.length == 0) {
			qb = QueryBuilders.moreLikeThisQuery(new String[]{liketext}).minDocFreq(1).minTermFreq(1);
		} else {
			qb = QueryBuilders.moreLikeThisQuery(fields, new String[]{liketext}, Item.EMPTY_ARRAY).
					minDocFreq(1).minTermFreq(1);
		}

		if (!StringUtils.isBlank(filterKey)) {
			qb = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(Config._ID, filterKey)).filter(qb);
		}
		return searchQuery(appid, searchQueryRaw(appid, type, qb, pager));
	}

	@Override
	public <P extends ParaObject> List<P> findTags(String appid, String keyword, Pager... pager) {
		if (StringUtils.isBlank(keyword)) {
			return Collections.emptyList();
		}
		QueryBuilder qb = QueryBuilders.wildcardQuery("tag", keyword.concat("*"));
		return searchQuery(appid, Utils.type(Tag.class), qb, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findNearby(String appid, String type,
		String query, int radius, double lat, double lng, Pager... pager) {

		if (StringUtils.isBlank(type) || StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}
		if (StringUtils.isBlank(query)) {
			query = "*";
		}
		Pager page = getPager(pager);
		// find nearby Address objects
		QueryBuilder qb1 = QueryBuilders.geoDistanceQuery("latlng").point(lat, lng).
				distance(radius, DistanceUnit.KILOMETERS);

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

		QueryBuilder qb2 = QueryBuilders.boolQuery().
				must(QueryBuilders.queryStringQuery(qs(query))).
				filter(QueryBuilders.idsQuery().addIds(parentids));

		SearchHits hits2 = searchQueryRaw(appid, type, qb2, page);

		return searchQuery(appid, hits2);
	}

	private <P extends ParaObject> List<P> searchQuery(String appid, String type,
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
	protected <P extends ParaObject> List<P> searchQuery(final String appid, SearchHits hits) {
		if (hits == null) {
			return Collections.emptyList();
		}
		List<P> results = new ArrayList<P>(hits.getHits().length);
		List<String> keys = new LinkedList<String>();
		boolean readFromIndex = Config.getConfigBoolean("read_from_index", Config.ENVIRONMENT.equals("embedded"));
		try {
			for (SearchHit hit : hits) {
				if (readFromIndex) {
					P pobj = ElasticSearchUtils.getParaObjectFromSource(hit.getSourceAsMap());
					results.add(pobj);
				} else {
					keys.add(hit.getId());
				}
				logger.debug("Search result: appid={}, {}->{}", appid, hit.getSourceAsMap().get(Config._APPID), hit.getId());
			}

			if (!readFromIndex && !keys.isEmpty()) {
				List<String> objectsMissingFromDB = new ArrayList<String>(results.size());
				Map<String, P> fromDB = getDAO().readAll(appid, keys, true);
				for (int i = 0; i < keys.size(); i++) {
					String key = keys.get(i);
					P pobj = fromDB.get(key);
					if (pobj == null) {
						pobj = ElasticSearchUtils.getParaObjectFromSource(hits.getAt(i).getSourceAsMap());
						// show warning that object is still in index but not in DB
						if (pobj != null && appid.equals(pobj.getAppid()) && pobj.getStored()) {
							objectsMissingFromDB.add(key);
						}
					}
					if (pobj != null) {
						results.add(pobj);
					}
				}

				if (!objectsMissingFromDB.isEmpty()) {
					logger.warn("Found {} objects in app '{}' that are indexed but not in the database: {}",
							objectsMissingFromDB.size(), appid, objectsMissingFromDB);
				}
			}
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.warn("Search query failed for app '{}': {}", appid, msg);
		}
		return results;
	}

	/**
	 * Executes an ElasticSearch query. This is the core method of the class.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @param type type of object
	 * @param query the search query builder
	 * @param pager a {@link com.erudika.para.utils.Pager}
	 * @return a list of search results
	 */
	protected SearchHits searchQueryRaw(String appid, String type, QueryBuilder query, Pager... pager) {
		if (StringUtils.isBlank(appid)) {
			return null;
		}
		Pager page = ElasticSearchUtils.getPager(pager);
		SortOrder order = page.isDesc() ? SortOrder.DESC : SortOrder.ASC;
		int max = page.getLimit();
		int pageNum = (int) page.getPage();
		int start = (pageNum < 1 || pageNum > Config.MAX_PAGES) ? 0 : (pageNum - 1) * max;

		if (query == null) {
			query = QueryBuilders.matchAllQuery();
		}
		if (!StringUtils.isBlank(type)) {
			query = QueryBuilders.boolQuery().must(query).must(QueryBuilders.termQuery(Config._TYPE, type));
		}

		SearchHits hits = null;

		try {
			SearchRequestBuilder srb = client().prepareSearch(getIndexName(appid)).
					setSearchType(SearchType.DFS_QUERY_THEN_FETCH).
					setQuery(query).
					setSize(max);

			if (pageNum <= 1 && !StringUtils.isBlank(page.getLastKey())) {
				srb.searchAfter(new Object[]{NumberUtils.toLong(page.getLastKey())});
				srb.setFrom(0);
				srb.addSort(SortBuilders.fieldSort("_docid").order(order));
			} else {
				srb.setFrom(start);
				for (SortBuilder<?> sortField : ElasticSearchUtils.getSortFieldsFromPager(page)) {
					srb.addSort(sortField);
				}
			}

			logger.debug("Elasticsearch query: {}", srb.toString());

			hits = srb.execute().actionGet().getHits();
			page.setCount(hits.getTotalHits());
			if (hits.getHits().length > 0) {
				Object id = hits.getAt(hits.getHits().length - 1).getSourceAsMap().get("_docid");
				if (id != null) {
					page.setLastKey(id.toString());
				}
			}
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.warn("No search results for type '{}' in app '{}': {}.", type, appid, msg);
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
	protected Map<String, Object> getSource(String appid, String key) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (StringUtils.isBlank(key) || StringUtils.isBlank(appid)) {
			return map;
		}

		try {
			GetRequestBuilder grb = client().prepareGet().setIndex(getIndexName(appid)).setId(key);
			GetResponse gres = grb.execute().actionGet();
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

	@Override
	public Long getCount(String appid, String type) {
		if (StringUtils.isBlank(appid)) {
			return 0L;
		}
		QueryBuilder query;
		if (!StringUtils.isBlank(type)) {
			query = QueryBuilders.termQuery(Config._TYPE, type);
		} else {
			query = QueryBuilders.matchAllQuery();
		}
		Long count = 0L;
		try {
			SearchRequestBuilder crb = client().prepareSearch(getIndexName(appid)).setSize(0).setQuery(query);
			count = crb.execute().actionGet().getHits().getTotalHits();
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.warn("Could not count results in index '{}': {}", appid, msg);
		}
		return count;
	}

	@Override
	public Long getCount(String appid, String type, Map<String, ?> terms) {
		if (StringUtils.isBlank(appid) || terms == null || terms.isEmpty()) {
			return 0L;
		}
		Long count = 0L;
		QueryBuilder query = getTermsQuery(terms, true);
		if (query != null) {
			if (!StringUtils.isBlank(type)) {
				query = QueryBuilders.boolQuery().must(query).must(QueryBuilders.termQuery(Config._TYPE, type));
			}
			try {
				SearchRequestBuilder crb = client().prepareSearch(getIndexName(appid)).setSize(0).setQuery(query);
				count = crb.execute().actionGet().getHits().getTotalHits();
			} catch (Exception e) {
				Throwable cause = e.getCause();
				String msg = cause != null ? cause.getMessage() : e.getMessage();
				logger.warn("Could not count results in index '{}': {}", appid, msg);
			}
		}
		return count;
	}

	//////////////////////////////////////////////////////////////

	@Override
	public void index(ParaObject so) {
		index(Config.getRootAppIdentifier(), so);
	}

	@Override
	public void unindex(ParaObject so) {
		unindex(Config.getRootAppIdentifier(), so);
	}

	@Override
	public <P extends ParaObject> void indexAll(List<P> objects) {
		indexAll(Config.getRootAppIdentifier(), objects);
	}

	@Override
	public <P extends ParaObject> void unindexAll(List<P> objects) {
		unindexAll(Config.getRootAppIdentifier(), objects);
	}

	@Override
	public void unindexAll(Map<String, ?> terms, boolean matchAll) {
		unindexAll(Config.getRootAppIdentifier(), terms, matchAll);
	}

	@Override
	public <P extends ParaObject> P findById(String id) {
		return findById(Config.getRootAppIdentifier(), id);
	}

	@Override
	public <P extends ParaObject> List<P> findByIds(List<String> ids) {
		return findByIds(Config.getRootAppIdentifier(), ids);
	}

	@Override
	public <P extends ParaObject> List<P> findNearby(String type,
			String query, int radius, double lat, double lng, Pager... pager) {
		return findNearby(Config.getRootAppIdentifier(), type, query, radius, lat, lng, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findPrefix(String type, String field, String prefix, Pager... pager) {
		return findPrefix(Config.getRootAppIdentifier(), type, field, prefix, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findQuery(String type, String query, Pager... pager) {
		return findQuery(Config.getRootAppIdentifier(), type, query, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findNestedQuery(String type, String field, String query, Pager... pager) {
		return findNestedQuery(Config.getRootAppIdentifier(), type, field, query, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findSimilar(String type, String filterKey, String[] fields,
			String liketext, Pager... pager) {
		return findSimilar(Config.getRootAppIdentifier(), type, filterKey, fields, liketext, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findTagged(String type, String[] tags, Pager... pager) {
		return findTagged(Config.getRootAppIdentifier(), type, tags, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findTags(String keyword, Pager... pager) {
		return findTags(Config.getRootAppIdentifier(), keyword, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findTermInList(String type, String field,
			List<?> terms, Pager... pager) {
		return findTermInList(Config.getRootAppIdentifier(), type, field, terms, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findTerms(String type, Map<String, ?> terms,
			boolean mustMatchBoth, Pager... pager) {
		return findTerms(Config.getRootAppIdentifier(), type, terms, mustMatchBoth, pager);
	}

	@Override
	public <P extends ParaObject> List<P> findWildcard(String type, String field, String wildcard,
			Pager... pager) {
		return findWildcard(Config.getRootAppIdentifier(), type, field, wildcard, pager);
	}

	@Override
	public Long getCount(String type) {
		return getCount(Config.getRootAppIdentifier(), type);
	}

	@Override
	public Long getCount(String type, Map<String, ?> terms) {
		return getCount(Config.getRootAppIdentifier(), type, terms);
	}

}
