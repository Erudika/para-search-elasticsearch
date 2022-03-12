/*
 * Copyright 2013-2022 Erudika. https://erudika.com
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
package com.erudika.para.server.search;

import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.search.Search;
import com.erudika.para.core.utils.CoreUtils;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.server.search.es.ES;
import com.erudika.para.server.search.es.ESUtils;
import com.erudika.para.server.search.os.OS;
import com.erudika.para.server.search.os.OSUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.opensearch.action.ActionListener;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link Search} interface using Elasticsearch/OpenSearch.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
@Singleton
public class ElasticSearch implements Search {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearch.class);
	private DAO dao;

	/**
	 * Switch between OpenSearch and Elasticsearch flavors.
	 */
	public static final boolean OPEN_SEARCH_FLAVOR = "opensearch".equalsIgnoreCase(Para.getConfig().
			getConfigParam("es.flavor", "elasticsearch"));

	static {
		if (Para.getConfig().isSearchEnabled() && Para.getConfig().getConfigParam("search", "").
				equalsIgnoreCase(ElasticSearch.class.getSimpleName())) {
			if (OPEN_SEARCH_FLAVOR) {
				OSUtils.getRESTClient();
			} else {
				ESUtils.getRESTClient();
			}
			// set up automatic index creation and deletion
			App.addAppCreatedListener((App app) -> createIndexInternal(app));
			App.addAppDeletedListener((App app) -> deleteIndexInternal(app));
		}
	}

	/**
	 * No-args constructor.
	 */
	public ElasticSearch() {
		this(CoreUtils.getInstance().getDao());
	}

	/**
	 * Default constructor.
	 * @param dao an instance of the persistence class
	 */
	@Inject
	public ElasticSearch(DAO dao) {
		this.dao = dao;
	}

	@Override
	public boolean rebuildIndex(DAO dao, App app, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OSUtils.rebuildIndex(dao, app, null, pager);
		} else {
			return ESUtils.rebuildIndex(dao, app, null, pager);
		}
	}

	@Override
	public boolean rebuildIndex(DAO dao, App app, String destinationIndex, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OSUtils.rebuildIndex(dao, app, destinationIndex, pager);
		} else {
			return ESUtils.rebuildIndex(dao, app, destinationIndex, pager);
		}
	}

	@Override
	public boolean isValidQueryString(String queryString) {
		if (OPEN_SEARCH_FLAVOR) {
			return OSUtils.isValidQueryString(queryString);
		} else {
			return ESUtils.isValidQueryString(queryString);
		}
	}

	@Override
	public void createIndex(App app) {
		createIndexInternal(app);
	}

	private static void createIndexInternal(App app) {
		if (app != null) {
			String appid = app.getAppIdentifier();
			if (app.isSharingIndex()) {
				if (OPEN_SEARCH_FLAVOR) {
					OSUtils.addIndexAliasWithRouting(Para.getConfig().getRootAppIdentifier(), appid);
				} else {
					ESUtils.addIndexAliasWithRouting(Para.getConfig().getRootAppIdentifier(), appid);
				}
			} else {
				int shards = app.isRootApp() ? Para.getConfig().getConfigInt("es.shards", 2)
						: Para.getConfig().getConfigInt("es.shards_for_child_apps", 1);
				int replicas = app.isRootApp() ? Para.getConfig().getConfigInt("es.replicas", 0)
						: Para.getConfig().getConfigInt("es.replicas_for_child_apps", 0);
				if (OPEN_SEARCH_FLAVOR) {
					OSUtils.createIndex(appid, shards, replicas);
				} else {
					ESUtils.createIndex(appid, shards, replicas);
				}
			}
		}
	}

	@Override
	public void deleteIndex(App app) {
		deleteIndexInternal(app);
	}

	private static void deleteIndexInternal(App app) {
		if (app != null) {
			String appid = app.getAppIdentifier();
			if (app.isSharingIndex()) {
				if (OPEN_SEARCH_FLAVOR) {
					OSUtils.deleteByQuery(app.getAppIdentifier(), matchAllQuery(),
							new ActionListener<BulkByScrollResponse>() {
						public void onResponse(BulkByScrollResponse res) { }
						public void onFailure(Exception ex) {
							logger.error("Failed to delete all objects in shared index for app '" + appid + "'", ex);
						}
					});
					OSUtils.removeIndexAlias(Para.getConfig().getRootAppIdentifier(), appid);
				} else {
					ESUtils.deleteByQuery(app.getAppIdentifier(), QueryBuilders.matchAll().build(), res -> {
						if (!res.failures().isEmpty()) {
							logger.error("Failed to delete all objects in shared index for app '" + appid + "' - {}",
									res.failures().iterator().next().cause().reason());
						}
					});
					ESUtils.removeIndexAlias(Para.getConfig().getRootAppIdentifier(), appid);
				}
			} else {
				if (OPEN_SEARCH_FLAVOR) {
					OSUtils.deleteIndex(appid);
				} else {
					ESUtils.deleteIndex(appid);
				}
			}
		}
	}

	//////////////////////////////////////////////////////////////

	@Override
	public void index(ParaObject object) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.indexAllInternal(Para.getConfig().getRootAppIdentifier(), Collections.singletonList(object));
		} else {
			ES.indexAllInternal(Para.getConfig().getRootAppIdentifier(), Collections.singletonList(object));
		}
	}

	@Override
	public void index(String appid, ParaObject object) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.indexAllInternal(appid, Collections.singletonList(object));
		} else {
			ES.indexAllInternal(appid, Collections.singletonList(object));
		}
	}

	@Override
	public void unindex(ParaObject object) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.unindexAllInternal(Para.getConfig().getRootAppIdentifier(), Collections.singletonList(object));
		} else {
			ES.unindexAllInternal(Para.getConfig().getRootAppIdentifier(), Collections.singletonList(object));
		}
	}

	@Override
	public void unindex(String appid, ParaObject object) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.unindexAllInternal(appid, Collections.singletonList(object));
		} else {
			ES.unindexAllInternal(appid, Collections.singletonList(object));
		}
	}

	@Override
	public <P extends ParaObject> void indexAll(List<P> objects) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.indexAllInternal(Para.getConfig().getRootAppIdentifier(), objects);
		} else {
			ES.indexAllInternal(Para.getConfig().getRootAppIdentifier(), objects);
		}
	}

	@Override
	public <P extends ParaObject> void indexAll(String appid, List<P> objects) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.indexAllInternal(appid, objects);
		} else {
			ES.indexAllInternal(appid, objects);
		}
	}

	@Override
	public <P extends ParaObject> void unindexAll(List<P> objects) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.unindexAllInternal(Para.getConfig().getRootAppIdentifier(), objects);
		} else {
			ES.unindexAllInternal(Para.getConfig().getRootAppIdentifier(), objects);
		}
	}

	@Override
	public <P extends ParaObject> void unindexAll(String appid, List<P> objects) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.unindexAllInternal(appid, objects);
		} else {
			ES.unindexAllInternal(appid, objects);
		}
	}

	@Override
	public void unindexAll(Map<String, ?> terms, boolean matchAll) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.unindexAllInternal(Para.getConfig().getRootAppIdentifier(), terms, matchAll);
		} else {
			ES.unindexAllInternal(Para.getConfig().getRootAppIdentifier(), terms, matchAll);
		}
	}

	@Override
	public void unindexAll(String appid, Map<String, ?> terms, boolean matchAll) {
		if (OPEN_SEARCH_FLAVOR) {
			OS.unindexAllInternal(appid, terms, matchAll);
		} else {
			ES.unindexAllInternal(appid, terms, matchAll);
		}
	}

	@Override
	public <P extends ParaObject> P findById(String id) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findByIdInternal(Para.getConfig().getRootAppIdentifier(), id);
		} else {
			return ES.findByIdInternal(Para.getConfig().getRootAppIdentifier(), id);
		}
	}

	@Override
	public <P extends ParaObject> P findById(String appid, String id) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findByIdInternal(appid, id);
		} else {
			return ES.findByIdInternal(appid, id);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findByIds(List<String> ids) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findByIdsInternal(Para.getConfig().getRootAppIdentifier(), ids);
		} else {
			return ES.findByIdsInternal(Para.getConfig().getRootAppIdentifier(), ids);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findByIds(String appid, List<String> ids) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findByIdsInternal(appid, ids);
		} else {
			return ES.findByIdsInternal(appid, ids);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findNearby(String type,
			String query, int radius, double lat, double lng, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findNearbyInternal(Para.getConfig().getRootAppIdentifier(), type, query, radius, lat, lng, pager);
		} else {
			return ES.findNearbyInternal(Para.getConfig().getRootAppIdentifier(), type, query, radius, lat, lng, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findNearby(String appid, String type,
			String query, int radius, double lat, double lng, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findNearbyInternal(appid, type, query, radius, lat, lng, pager);
		} else {
			return ES.findNearbyInternal(appid, type, query, radius, lat, lng, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findPrefix(String type, String field, String prefix, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findPrefixInternal(Para.getConfig().getRootAppIdentifier(), type, field, prefix, pager);
		} else {
			return ES.findPrefixInternal(Para.getConfig().getRootAppIdentifier(), type, field, prefix, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findPrefix(String appid, String type, String field, String prefix, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findPrefixInternal(appid, type, field, prefix, pager);
		} else {
			return ES.findPrefixInternal(appid, type, field, prefix, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findQuery(String type, String query, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findQueryInternal(Para.getConfig().getRootAppIdentifier(), type, query, pager);
		} else {
			return ES.findQueryInternal(Para.getConfig().getRootAppIdentifier(), type, query, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findQuery(String appid, String type, String query, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findQueryInternal(appid, type, query, pager);
		} else {
			return ES.findQueryInternal(appid, type, query, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findNestedQuery(String type, String field, String query, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findNestedQueryInternal(Para.getConfig().getRootAppIdentifier(), type, field, query, pager);
		} else {
			return ES.findNestedQueryInternal(Para.getConfig().getRootAppIdentifier(), type, field, query, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findNestedQuery(String appid, String type, String field, String query, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findNestedQueryInternal(appid, type, field, query, pager);
		} else {
			return ES.findNestedQueryInternal(appid, type, field, query, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findSimilar(String type, String filterKey, String[] fields,
			String liketext, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findSimilarInternal(Para.getConfig().getRootAppIdentifier(), type, filterKey, fields, liketext, pager);
		} else {
			return ES.findSimilarInternal(Para.getConfig().getRootAppIdentifier(), type, filterKey, fields, liketext, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findSimilar(String appid, String type, String filterKey, String[] fields,
			String liketext, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findSimilarInternal(appid, type, filterKey, fields, liketext, pager);
		} else {
			return ES.findSimilarInternal(appid, type, filterKey, fields, liketext, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTagged(String type, String[] tags, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTaggedInternal(Para.getConfig().getRootAppIdentifier(), type, tags, pager);
		} else {
			return ES.findTaggedInternal(Para.getConfig().getRootAppIdentifier(), type, tags, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTagged(String appid, String type, String[] tags, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTaggedInternal(appid, type, tags, pager);
		} else {
			return ES.findTaggedInternal(appid, type, tags, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTags(String keyword, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTagsInternal(Para.getConfig().getRootAppIdentifier(), keyword, pager);
		} else {
			return ES.findTagsInternal(Para.getConfig().getRootAppIdentifier(), keyword, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTags(String appid, String keyword, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTagsInternal(appid, keyword, pager);
		} else {
			return ES.findTagsInternal(appid, keyword, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTermInList(String type, String field,
			List<?> terms, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTermInListInternal(Para.getConfig().getRootAppIdentifier(), type, field, terms, pager);
		} else {
			return ES.findTermInListInternal(Para.getConfig().getRootAppIdentifier(), type, field, terms, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTermInList(String appid, String type, String field,
			List<?> terms, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTermInListInternal(appid, type, field, terms, pager);
		} else {
			return ES.findTermInListInternal(appid, type, field, terms, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTerms(String type, Map<String, ?> terms,
			boolean mustMatchBoth, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTermsInternal(Para.getConfig().getRootAppIdentifier(), type, terms, mustMatchBoth, pager);
		} else {
			return ES.findTermsInternal(Para.getConfig().getRootAppIdentifier(), type, terms, mustMatchBoth, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findTerms(String appid, String type, Map<String, ?> terms,
			boolean mustMatchBoth, Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findTermsInternal(appid, type, terms, mustMatchBoth, pager);
		} else {
			return ES.findTermsInternal(appid, type, terms, mustMatchBoth, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findWildcard(String type, String field, String wildcard,
			Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findWildcardInternal(Para.getConfig().getRootAppIdentifier(), type, field, wildcard, pager);
		} else {
			return ES.findWildcardInternal(Para.getConfig().getRootAppIdentifier(), type, field, wildcard, pager);
		}
	}

	@Override
	public <P extends ParaObject> List<P> findWildcard(String appid, String type, String field, String wildcard,
			Pager... pager) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.findWildcardInternal(appid, type, field, wildcard, pager);
		} else {
			return ES.findWildcardInternal(appid, type, field, wildcard, pager);
		}
	}

	@Override
	public Long getCount(String type) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.getCountInternal(Para.getConfig().getRootAppIdentifier(), type);
		} else {
			return ES.getCountInternal(Para.getConfig().getRootAppIdentifier(), type);
		}
	}

	@Override
	public Long getCount(String appid, String type) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.getCountInternal(appid, type);
		} else {
			return ES.getCountInternal(appid, type);
		}
	}

	@Override
	public Long getCount(String type, Map<String, ?> terms) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.getCountInternal(Para.getConfig().getRootAppIdentifier(), type, terms);
		} else {
			return ES.getCountInternal(Para.getConfig().getRootAppIdentifier(), type, terms);
		}
	}

	@Override
	public Long getCount(String appid, String type, Map<String, ?> terms) {
		if (OPEN_SEARCH_FLAVOR) {
			return OS.getCountInternal(appid, type, terms);
		} else {
			return ES.getCountInternal(appid, type, terms);
		}
	}

}
