/*
 * Copyright 2013-2026 Erudika. http://erudika.com
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
import static com.erudika.para.server.search.rest.ProxyResourceHandler.PATH;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
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
import org.apache.commons.lang3.Strings;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Acts as a proxy for Elasticsearch and handles request to the custom resouce path {@code /v1/_elasticsearch}.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
@RestController
@RequestMapping(path = "/_elasticsearch", produces = "application/json")
public class ProxyResourceHandler implements CustomResourceHandler {

	private static final Logger logger = LoggerFactory.getLogger(ProxyResourceHandler.class);
	private RestClient lowLevelClient;

	/**
	 * Resource path. Defaults to '_elasticsearch'.
	 */
	public static final String PATH = Para.getConfig().elasticsearchProxyPath();

	public String getRelativePath() {
		return PATH;
	}

	@GetMapping({"/", "/{path}"})
	public ResponseEntity<?> handleGet(@PathVariable(required = false) String path, HttpServletRequest req) {
		return proxyRequest(path, req);
	}

	@PostMapping({"/", "/{path}"})
	public ResponseEntity<?> handlePost(@PathVariable(required = false) String path, HttpServletRequest req) {
		return proxyRequest(path, req);
	}

	@PatchMapping({"/", "/{path}"})
	public ResponseEntity<?> handlePatch(@PathVariable(required = false) String path, HttpServletRequest req) {
		return proxyRequest(path, req);
	}

	@PutMapping({"/", "/{path}"})
	public ResponseEntity<?> handlePut(@PathVariable(required = false) String path, HttpServletRequest req) {
		return proxyRequest(path, req);
	}

	@DeleteMapping({"/", "/{path}"})
	public ResponseEntity<?> handleDelete(@PathVariable(required = false) String path, HttpServletRequest req) {
		return proxyRequest(path, req);
	}

	ResponseEntity<?> proxyRequest(String reqPath, HttpServletRequest req) {
		String method = req.getMethod();
		if (!Para.getConfig().elasticsearchProxyEnabled()) {
			return ResponseEntity.status(HttpStatus.FORBIDDEN).body("This feature is disabled.");
		}
		String appid = ParaObjectUtils.getAppidFromAuthHeader(req.getHeader(HttpHeaders.AUTHORIZATION));
		if (StringUtils.isBlank(appid)) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
		}
		String cleanPath = getCleanPath(appid, reqPath);
		try {
			if (cleanPath.endsWith("/reindex") && method.equalsIgnoreCase("POST")) {
				return handleReindexTask(appid, req.getParameter("destinationIndex"));
			}

//			Header[] headers = getHeaders(req.getHeaderNames());
			HttpEntity resp;
			RestClient client = getClient();
			if (client != null) {
				org.opensearch.client.Request esRequest = new Request(method, cleanPath);
				RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
				req.getHeaderNames().asIterator().forEachRemaining(header -> {
					opts.addHeader(header, req.getHeader(header));
				});
				esRequest.setOptions(opts);
				if (req.getInputStream() != null && req.getInputStream().available() > 0) {
					HttpEntity body = new InputStreamEntity(req.getInputStream(), ContentType.APPLICATION_JSON);
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
					Object response = getTransformedResponse(appid, resp.getContent(), req);
					BodyBuilder rb = ResponseEntity.ok();
					if (type.isPresent()) {
						rb.header(HttpHeaders.CONTENT_TYPE, type.get().getValue());
					}
					rb.body(response);
					return rb.build();
				}
			}
		} catch (Exception ex) {
			logger.warn("Failed to proxy '{} {}' to Elasticsearch: {}", method, cleanPath, ex.getMessage());
		}
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
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

//	private Header[] getHeaders(Enumeration<String> headers) {
//		if (headers == null || headers.isEmpty()) {
//			return new Header[0];
//		}
//		int i = 0;
//		headers.remove(HttpHeaders.CONTENT_LENGTH);
//		Header[] headerz = new Header[headers.size()];
//		for (String key : headers.keySet()) {
//			headerz[i] = new BasicHeader(key, headers.getFirst(key));
//			i++;
//		}
//		return headerz;
//	}
//
//	private String getPath(HttpServletRequest ctx) {
//		String path = ctx.getUriInfo().getPathParameters(true).getFirst("path");
//		return StringUtils.isBlank(path) ? "_search" : path;
//	}

	public String getCleanPath(String appid, String path) {
		if (Strings.CI.contains(path, "getRawResponse")) {
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

	private ResponseEntity<?> handleReindexTask(String appid, String destinationIndex) {
		if (!Para.getConfig().elasticsearchProxyReindexingEnabled() || appid == null) {
			return ResponseEntity.status(HttpStatus.FORBIDDEN).body("This feature is disabled.");
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
			return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response);
		} else {
			return ResponseEntity.status(404).body("App not found.");
		}
	}

	private Object getTransformedResponse(String appid, InputStream content, HttpServletRequest req) {
		if (req.getParameter("getRawResponse") != null) {
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
