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
package com.erudika.para.server.search.rest;

import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.listeners.DestroyListener;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.rest.CustomResourceHandler;
import com.erudika.para.core.search.Search;
import com.erudika.para.core.utils.CoreUtils;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.fasterxml.jackson.databind.JsonNode;
import static jakarta.ws.rs.HttpMethod.DELETE;
import static jakarta.ws.rs.HttpMethod.GET;
import static jakarta.ws.rs.HttpMethod.PATCH;
import static jakarta.ws.rs.HttpMethod.POST;
import static jakarta.ws.rs.HttpMethod.PUT;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.net.URIBuilder;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Acts as a proxy for Elasticsearch and handles request to the custom resouce path {@code /v1/_elasticsearch}.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class ProxyResourceHandler implements CustomResourceHandler {

	private static final Logger logger = LoggerFactory.getLogger(ProxyResourceHandler.class);
	private RestClient lowLevelClient;

	/**
	 * Resource path. Defaults to '_elasticsearch'.
	 */
	public static final String PATH = Para.getConfig().elasticsearchProxyPath();

	@Override
	public String getRelativePath() {
		return PATH;
	}

	@Override
	public Response handleGet(ContainerRequestContext ctx) {
		return proxyRequest(GET, ctx);
	}

	@Override
	public Response handlePost(ContainerRequestContext ctx) {
		return proxyRequest(POST, ctx);
	}

	@Override
	public Response handlePatch(ContainerRequestContext ctx) {
		return proxyRequest(PATCH, ctx);
	}

	@Override
	public Response handlePut(ContainerRequestContext ctx) {
		return proxyRequest(PUT, ctx);
	}

	@Override
	public Response handleDelete(ContainerRequestContext ctx) {
		return proxyRequest(DELETE, ctx);
	}

	Response proxyRequest(String method, ContainerRequestContext ctx) {
		if (!Para.getConfig().elasticsearchProxyEnabled()) {
			return Response.status(Response.Status.FORBIDDEN.getStatusCode(), "This feature is disabled.").build();
		}
		String appid = ParaObjectUtils.getAppidFromAuthHeader(ctx.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
		if (StringUtils.isBlank(appid)) {
			return Response.status(Response.Status.BAD_REQUEST).build();
		}
		String path = getCleanPath(appid, getPath(ctx));
		try {
			if (path.endsWith("/reindex") && POST.equals(method)) {
				return handleReindexTask(appid, ctx.getUriInfo().getQueryParameters().getFirst("destinationIndex"));
			}

			Header[] headers = getHeaders(ctx.getHeaders());
			HttpEntity resp;
			RestClient client = getClient();
			if (client != null) {
				org.opensearch.client.Request esRequest = new Request(method, path);
				RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
				for (Header header : headers) {
					opts.addHeader(header.getName(), header.getValue());
				}
				esRequest.setOptions(opts);
				if (ctx.getEntityStream() != null && ctx.getEntityStream().available() > 0) {
					HttpEntity body = new InputStreamEntity(ctx.getEntityStream(), ContentType.APPLICATION_JSON);
					esRequest.setEntity(body);
					resp = client.performRequest(esRequest).getEntity();
				} else {
					resp = client.performRequest(esRequest).getEntity();
				}
				if (resp != null && resp.getContent() != null) {
					Optional<? extends Header> type = Optional.empty();
					if (resp.getTrailers() != null) {
						type = resp.getTrailers().get().
								stream().filter(h -> h.getName().equalsIgnoreCase(HttpHeaders.CONTENT_TYPE)).
								findFirst();
					}
					Object response = getTransformedResponse(appid, resp.getContent(), ctx);
					Response.ResponseBuilder rb = Response.ok(response);
					if (type.isPresent()) {
						rb.header(HttpHeaders.CONTENT_TYPE, type.get().getValue());
					}
					return rb.build();
				}
			}
		} catch (Exception ex) {
			logger.warn("Failed to proxy '{} {}' to Elasticsearch: {}", method, path, ex.getMessage());
		}
		return Response.status(Response.Status.BAD_REQUEST).build();
	}

	private RestClient getClient() {
		if (lowLevelClient != null) {
			return lowLevelClient;
		}
		try {
			String esScheme = Para.getConfig().elasticsearchRestClientScheme();
			String esHost = Para.getConfig().elasticsearchRestClientHost();
			int esPort = Para.getConfig().elasticsearchRestClientPort();
			lowLevelClient = RestClient.builder(new HttpHost(esScheme, esHost, esPort)).build();
			Para.addDestroyListener(new DestroyListener() {
				public void onDestroy() {
					if (lowLevelClient != null) {
						try {
							lowLevelClient.close();
						} catch (IOException ex) {
							logger.error(null, ex);
						}
					}
				}
			});
		} catch (Exception e) {
			logger.error("Failed to initialize Elasticsearch low-level client: {}", e.getMessage());
		}
		return lowLevelClient;
	}

	private Header[] getHeaders(MultivaluedMap<String, String> headers) {
		if (headers == null || headers.isEmpty()) {
			return new Header[0];
		}
		int i = 0;
		headers.remove(CONTENT_LENGTH);
		Header[] headerz = new Header[headers.size()];
		for (String key : headers.keySet()) {
			headerz[i] = new BasicHeader(key, headers.getFirst(key));
			i++;
		}
		return headerz;
	}

	private String getPath(ContainerRequestContext ctx) {
		String path = ctx.getUriInfo().getPathParameters(true).getFirst("path");
		return StringUtils.isBlank(path) ? "_search" : path;
	}

	public String getCleanPath(String appid, String path) {
		if (StringUtils.containsIgnoreCase(path, "getRawResponse")) {
			try {
				URIBuilder uri = new URIBuilder(path);
				List<NameValuePair> params = uri.getQueryParams();
				for (Iterator<NameValuePair> iterator = params.iterator(); iterator.hasNext();) {
					NameValuePair next = iterator.next();
					if (next.getName().equalsIgnoreCase("getRawResponse")) {
						iterator.remove();
						break;
					}
				}
				path = uri.setParameters(params).toString();
			} catch (URISyntaxException ex) {
				logger.warn(null, ex);
			}
		}
		if (path.startsWith("/")) {
			path = StringUtils.stripStart(path, "/");
		}
		if (StringUtils.isBlank(path) || "/".equals(path)) {
			path = "_search";
		}
		// Prefix path with appid (alias) in order to route requests to the correct index for a particular app.
		return "/".concat(appid).concat("/").concat(path);
	}

	private Response handleReindexTask(String appid, String destinationIndex) {
		if (!Para.getConfig().elasticsearchProxyReindexingEnabled() || appid == null) {
			return Response.status(Response.Status.FORBIDDEN.getStatusCode(), "This feature is disabled.").build();
		}
		Pager pager = new Pager();
		DAO dao = CoreUtils.getInstance().getDao();
		Search search = CoreUtils.getInstance().getSearch();
		App app = dao.read(App.id(appid));
		if (app != null) {
			long startTime = System.nanoTime();
			search.rebuildIndex(dao, app, destinationIndex, pager);
			long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
			Map<String, Object> response = new HashMap<String, Object>();
			response.put("reindexed", pager.getCount());
			response.put("tookMillis", tookMillis);
			return Response.ok(response, MediaType.APPLICATION_JSON).build();
		} else {
			return Response.status(404, "App not found.").build();
		}
	}

	private Object getTransformedResponse(String appid, InputStream content, ContainerRequestContext ctx) {
		if (ctx.getUriInfo().getQueryParameters().containsKey("getRawResponse") ||
				StringUtils.containsIgnoreCase(getPath(ctx), "getRawResponse=")) {
			return content;
		} else {
			try {
				JsonNode tree = ParaObjectUtils.getJsonMapper().readTree(content);
				JsonNode hits = tree.at("/hits/hits");
				if (hits.isMissingNode()) {
					return tree;
				} else {
					List<String> keys = new LinkedList<String>();
					long count = tree.at("/hits/total").asLong();
					for (JsonNode hit : hits) {
						String id = hit.get("_id").asText();
						keys.add(id);
					}
					DAO dao = CoreUtils.getInstance().getDao();
					Map<String, ParaObject> fromDB = dao.readAll(appid, keys, true);
					Map<String, Object> result = new HashMap<>();
					result.put("items", fromDB);
					result.put("totalHits", count);
					return result;
				}
			} catch (IOException ex) {
				logger.error(null, ex);
			}
			return Collections.emptyMap();
		}
	}

}
