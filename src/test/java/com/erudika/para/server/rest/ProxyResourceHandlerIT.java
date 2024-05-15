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
package com.erudika.para.server.rest;

import com.erudika.para.core.rest.GenericExceptionMapper;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.core.utils.Utils;
import com.erudika.para.server.search.es.ESUtils;
import com.erudika.para.server.search.rest.ProxyResourceHandler;
import static com.erudika.para.server.search.rest.ProxyResourceHandler.PATH;
import com.erudika.para.server.search.rest.ProxySubResourceHandler;
import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import static jakarta.ws.rs.HttpMethod.DELETE;
import static jakarta.ws.rs.HttpMethod.GET;
import static jakarta.ws.rs.HttpMethod.PATCH;
import static jakarta.ws.rs.HttpMethod.POST;
import static jakarta.ws.rs.HttpMethod.PUT;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.HttpHeaders;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.OK;
import static jakarta.ws.rs.core.Response.Status.UNSUPPORTED_MEDIA_TYPE;
import java.util.Collections;
import java.util.Map;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.process.Inflector;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class ProxyResourceHandlerIT extends JerseyTest {

	private static final String JSON = APPLICATION_JSON;
	private static final String JWT = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBpZCI6ImFwcDpteWFwcCJ9."
				+ "M4uitKDuclLuZzadxNzL_3fjeShKBxPdncsNKkA-rfY";

	@BeforeAll
	public static void setUpClass() {
		System.setProperty("para.env", "embedded");
		System.setProperty("para.app_name", "para-test");
		System.setProperty("para.cluster_name", "test");
		System.setProperty("para.es.proxy_enabled", "true");
		System.setProperty("para.es.shards", "2");
		ESUtils.createIndex("myapp");
	}

	@AfterAll
	public static void tearDownClass() {
		ESUtils.deleteIndex("myapp");
	}

	@Override
	protected void configureClient(ClientConfig config) {
		config.register(GenericExceptionMapper.class);
		config.register(new JacksonJsonProvider(ParaObjectUtils.getJsonMapper()));
//		config.connectorProvider(new HttpUrlConnectorProvider().useSetMethodWorkaround());
	}

	@Override
	protected Application configure() {
		ResourceConfig resource = new ResourceConfig();
		ProxyResourceHandler proxy = new ProxyResourceHandler();
		ProxySubResourceHandler proxySub = new ProxySubResourceHandler();
		register(resource, proxy);
		register(resource, proxySub);
		return resource;
	}

	@Test
	public void testGetCleanPath() {
		ProxyResourceHandler prh = new ProxyResourceHandler();
		String appid = "app";
		String prefix = "/" + appid;
		assertEquals(prh.getCleanPath(appid, ""), prefix + "/_search");
		assertEquals(prh.getCleanPath(appid, "_search?param=123"), prefix + "/_search?param=123");
		assertEquals(prh.getCleanPath(appid, "_search?param=123&param2=345"), prefix + "/_search?param=123&param2=345");
		assertEquals(prh.getCleanPath(appid, "_search?getRawResponse=true&param2=345"), prefix + "/_search?param2=345");
		assertEquals(prh.getCleanPath(appid, "_search?getRawResponse=1&param2=345"), prefix + "/_search?param2=345");
		assertEquals(prh.getCleanPath(appid, "_search?getrawresponse=1&param2=345"), prefix + "/_search?param2=345");
	}

	@Test
	public void testProxyDisabledByDefault() {
		System.setProperty("para.es.proxy_enabled", "false");
		Response response1 = target(PATH).request(JSON).get();
		assertEquals(FORBIDDEN.getStatusCode(), response1.getStatus());
		System.setProperty("para.es.proxy_enabled", "true");
		Response response2 = target(PATH).request(JSON).get();
		assertNotEquals(FORBIDDEN.getStatusCode(), response2.getStatus());
	}

	@Test
	public void testHandleGet() {
		Response badReqNoAppid = target(PATH).request(JSON).get();
		assertEquals(BAD_REQUEST.getStatusCode(), badReqNoAppid.getStatus());

		MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
		headers.putSingle(HttpHeaders.AUTHORIZATION, JWT);

		Response ok1 = target(PATH + "/_search").request(JSON).headers(headers).get();
		assertEquals(OK.getStatusCode(), ok1.getStatus());
		// path is URL-encoded
		Response ok3 = target(PATH + "/" + Utils.urlEncode("/_count?q=*")).request(JSON).headers(headers).get();
		assertEquals(OK.getStatusCode(), ok3.getStatus());
		assertTrue(ok3.readEntity(Map.class).containsKey("count"));
	}

	@Test
	public void testHandlePost() {
		Response badReqNoAppid = target(PATH).request(JSON).post(Entity.json(""));
		assertEquals(BAD_REQUEST.getStatusCode(), badReqNoAppid.getStatus());

		MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
		headers.putSingle(HttpHeaders.AUTHORIZATION, JWT);

		Response badReqEntityNotJSON = target(PATH).request(JSON).headers(headers).post(Entity.text("bad"));
		assertEquals(UNSUPPORTED_MEDIA_TYPE.getStatusCode(), badReqEntityNotJSON.getStatus());

		Entity<?> entity1 = Entity.json(Collections.singletonMap("query", Collections.singletonMap("term",
				Collections.singletonMap("type", "cat"))));

		// this will return the transformed ES JSON response to Para response
		Response ok1 = target(PATH + "/_search").request(JSON).headers(headers).post(entity1);
		assertEquals(OK.getStatusCode(), ok1.getStatus());
		Map<?, ?> transformed = ok1.readEntity(Map.class);
		assertTrue(transformed.containsKey("items"));
		assertTrue(transformed.containsKey("totalHits"));

		Response ok2 = target(PATH + "/" + Utils.urlEncode("_count?pretty=true")).
				request(JSON).headers(headers).post(entity1);
		assertEquals(OK.getStatusCode(), ok2.getStatus());
		assertTrue(ok2.readEntity(Map.class).containsKey("count"));

		// Return the raw ES JSON
		Response ok3 = target(PATH + "/_search").queryParam("getRawResponse", 1).request(JSON).headers(headers).post(entity1);
		assertEquals(OK.getStatusCode(), ok3.getStatus());
		assertTrue(ok3.readEntity(Map.class).containsKey("hits"));
	}

	private void register(ResourceConfig resource, ProxyResourceHandler proxy) {
		resource.register(GenericExceptionMapper.class);
		resource.register(new JacksonJsonProvider(ParaObjectUtils.getJsonMapper()));
		Resource.Builder custom = Resource.builder(proxy.getRelativePath());
		custom.addMethod(GET).produces(JSON).
				handledBy(new Inflector<ContainerRequestContext, Response>() {
					public Response apply(ContainerRequestContext ctx) {
						return proxy.handleGet(ctx);
					}
				});
		custom.addMethod(POST).produces(JSON).consumes(JSON).
				handledBy(new Inflector<ContainerRequestContext, Response>() {
					public Response apply(ContainerRequestContext ctx) {
						return proxy.handlePost(ctx);
					}
				});
		custom.addMethod(PATCH).produces(JSON).consumes(JSON).
				handledBy(new Inflector<ContainerRequestContext, Response>() {
					public Response apply(ContainerRequestContext ctx) {
						return proxy.handlePatch(ctx);
					}
				});
		custom.addMethod(PUT).produces(JSON).consumes(JSON).
				handledBy(new Inflector<ContainerRequestContext, Response>() {
					public Response apply(ContainerRequestContext ctx) {
						return proxy.handlePut(ctx);
					}
				});
		custom.addMethod(DELETE).produces(JSON).
				handledBy(new Inflector<ContainerRequestContext, Response>() {
					public Response apply(ContainerRequestContext ctx) {
						return proxy.handleDelete(ctx);
					}
				});
		resource.registerResources(custom.build());
	}

}
