/*
 * Copyright 2013-2017 Erudika. http://erudika.com
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
package com.erudika.para.rest;

import com.erudika.para.core.App;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.utils.Config;
import com.erudika.para.utils.Utils;
import java.io.IOException;
import java.util.Map;
import static javax.ws.rs.HttpMethod.DELETE;
import static javax.ws.rs.HttpMethod.GET;
import static javax.ws.rs.HttpMethod.PATCH;
import static javax.ws.rs.HttpMethod.POST;
import static javax.ws.rs.HttpMethod.PUT;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Acts as a proxy for Elasticsearch and handles request to the custom resouce path {@code /v1/elasticsearch}.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class ProxyResourceHandler implements CustomResourceHandler {

	private static final Logger logger = LoggerFactory.getLogger(ProxyResourceHandler.class);
	private static final boolean ENABLED = Config.getConfigBoolean("es.proxy_enabled", false);
	private final String esScheme = Config.getConfigParam("es.restclient_scheme", Config.IN_PRODUCTION ? "https" : "http");
	private final String esHost = Config.getConfigParam("es.restclient_host",
		Config.getConfigParam("es.transportclient_host", "localhost"));
	private final int esPort = Config.getConfigInt("es.restclient_port", 9200);


	@Override
	public String getRelativePath() {
		return "elasticsearch";
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
		if (!ENABLED) {
			return Response.status(Response.Status.FORBIDDEN.getStatusCode(), "This feature is disabled.").build();
		}
		String appid = getAppidFromAuthHeader(ctx.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
		String path = ctx.getUriInfo().getQueryParameters().getFirst("path");
		try {
			if (StringUtils.isBlank(path)) {
				path = "_search";
			}
			Header[] headers = getHeaders(ctx.getHeaders());
			HttpEntity resp;
			if (ctx.getEntityStream() != null && ctx.getEntityStream().available() > 0) {
				HttpEntity body = new InputStreamEntity(ctx.getEntityStream());
				resp = getClient(appid).performRequest(method, path, null, body, headers).getEntity();
			} else {
				resp = getClient(appid).performRequest(method, path, headers).getEntity();
			}
			if (resp != null && resp.getContent() != null) {
				Header type = resp.getContentType();
				return Response.ok(resp.getContent()).header(type.getName(), type.getValue()).build();
			}
		} catch (IOException ex) {
			logger.warn("Failed to proxy GET {} to Elasticsearch: {}", path, ex.getMessage());
		}
		return Response.status(Response.Status.BAD_REQUEST).build();
	}

	String getAppidFromAuthHeader(String authorization) {
		if (authorization == null) {
			return "";
		}
		String appid = "";
		if (StringUtils.startsWith(authorization, "Bearer")) {
			try {
				String[] parts = StringUtils.split(authorization, '.');
				if (parts.length > 1) {
					Map<String, Object> jwt = ParaObjectUtils.getJsonReader(Map.class).
							readValue(Utils.base64dec(parts[1]));
					if (jwt != null && jwt.containsKey(Config._APPID)) {
						appid = (String) jwt.get(Config._APPID);
					}
				}
			} catch (Exception e) { }
		} else {
			appid = StringUtils.substringBetween(authorization, "=", "/");
		}
		if (StringUtils.isBlank(appid)) {
			return "";
		}
		return App.id(appid).substring(4);
	}

	private RestClient getClient(String appid) {
		try {
			return RestClient.builder(new HttpHost(esHost, esPort, esScheme)).
					// We prefix path with appid in order to route request to the correct index
					// for a particular app. Also, append '/' to prevent other mishap.
					setPathPrefix(appid.concat("/")).build();
		} catch (Exception e) {
			logger.error("Failed to build Elasticsearch client for app '{}': {}", appid, e.getMessage());
			return null;
		}
	}

	private Header[] getHeaders(MultivaluedMap<String, String> headers) {
		if (headers == null || headers.isEmpty()) {
			return new Header[0];
		}
		int i = 0;
		Header[] headerz = new Header[headers.size()];
		for (String key : headers.keySet()) {
			headerz[i] = new BasicHeader(key, headers.getFirst(key));
			i++;
		}
		return headerz;
	}

}
