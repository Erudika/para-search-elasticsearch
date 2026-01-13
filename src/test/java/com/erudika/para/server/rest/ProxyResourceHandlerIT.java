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
package com.erudika.para.server.rest;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
//@SpringBootTest(classes = ProxyResourceHandler.class)
//@Import(ProxyResourceHandler.class)
//@ComponentScan(basePackages = "com.erudika.para.server.search.rest")
public class ProxyResourceHandlerIT {

//	@Autowired
//	private ProxyResourceHandler proxy;
//
//	private static MockMvc mockMvc;
//
//	private static final MediaType JSON = MediaType.APPLICATION_JSON;
//	private static final String JWT = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBpZCI6ImFwcDpteWFwcCJ9."
//				+ "M4uitKDuclLuZzadxNzL_3fjeShKBxPdncsNKkA-rfY";
////
//	@BeforeAll
//	public static void setUpClass() {
//		System.setProperty("para.env", "embedded");
//		System.setProperty("para.app_name", "para-test");
//		System.setProperty("para.cluster_name", "test");
//		System.setProperty("para.es.proxy_enabled", "true");
//		System.setProperty("para.es.shards", "2");
//		ESUtils.createIndex("myapp");
//
//		mockMvc = standaloneSetup(new ProxyResourceHandler())
//				.defaultRequest(get("/").accept(MediaType.APPLICATION_JSON))
//				.alwaysExpect(status().isOk())
//				.alwaysExpect(content().contentType("application/json;charset=UTF-8"))
//				.build();
//	}
//
//	@AfterAll
//	public static void tearDownClass() {
//		ESUtils.deleteIndex("myapp");
//	}
//
//	@Test
//	public void testGetCleanPath() {
//		ProxyResourceHandler prh = new ProxyResourceHandler();
//		String appid = "app";
//		String prefix = "/" + appid;
//		assertEquals(prh.getCleanPath(appid, ""), prefix + "/_search");
//		assertEquals(prh.getCleanPath(appid, "_search?param=123"), prefix + "/_search?param=123");
//		assertEquals(prh.getCleanPath(appid, "_search?param=123&param2=345"), prefix + "/_search?param=123&param2=345");
//		assertEquals(prh.getCleanPath(appid, "_search?getRawResponse=true&param2=345"), prefix + "/_search?param2=345");
//		assertEquals(prh.getCleanPath(appid, "_search?getRawResponse=1&param2=345"), prefix + "/_search?param2=345");
//		assertEquals(prh.getCleanPath(appid, "_search?getrawresponse=1&param2=345"), prefix + "/_search?param2=345");
//	}
//
//	@Test
//	public void testProxyDisabledByDefault() throws Exception {
//		HttpServletRequest mockreq = Mockito.mock(HttpServletRequest.class);
//		System.setProperty("para.es.proxy_enabled", "false");
//		mockMvc.perform(get("/_elasticsearch/{path}", "", mockreq).contentType(JSON)).andExpect(status().isForbidden());
//		System.setProperty("para.es.proxy_enabled", "true");
//		mockMvc.perform(get("/_elasticsearch/{path}", "", mockreq).contentType(JSON)).andExpect(status().is2xxSuccessful());
//	}


//	@Test
//	public void testHandleGet() {
//		Response badReqNoAppid = target(PATH).request(JSON).get();
//		assertEquals(BAD_REQUEST.getStatusCode(), badReqNoAppid.getStatus());
//
//		MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
//		headers.putSingle(HttpHeaders.AUTHORIZATION, JWT);
//
//		Response ok1 = target(PATH + "/_search").request(JSON).headers(headers).get();
//		assertEquals(OK.getStatusCode(), ok1.getStatus());
//		// path is URL-encoded
//		Response ok3 = target(PATH + "/" + Utils.urlEncode("/_count?q=*")).request(JSON).headers(headers).get();
//		assertEquals(OK.getStatusCode(), ok3.getStatus());
//		assertTrue(ok3.readEntity(Map.class).containsKey("count"));
//	}
//
//	@Test
//	public void testHandlePost() {
//		Response badReqNoAppid = target(PATH).request(JSON).post(Entity.json(""));
//		assertEquals(BAD_REQUEST.getStatusCode(), badReqNoAppid.getStatus());
//
//		MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
//		headers.putSingle(HttpHeaders.AUTHORIZATION, JWT);
//
//		Response badReqEntityNotJSON = target(PATH).request(JSON).headers(headers).post(Entity.text("bad"));
//		assertEquals(UNSUPPORTED_MEDIA_TYPE.getStatusCode(), badReqEntityNotJSON.getStatus());
//
//		Entity<?> entity1 = Entity.json(Collections.singletonMap("query", Collections.singletonMap("term",
//				Collections.singletonMap("type", "cat"))));
//
//		// this will return the transformed ES JSON response to Para response
//		Response ok1 = target(PATH + "/_search").request(JSON).headers(headers).post(entity1);
//		assertEquals(OK.getStatusCode(), ok1.getStatus());
//		Map<?, ?> transformed = ok1.readEntity(Map.class);
//		assertTrue(transformed.containsKey("items"));
//		assertTrue(transformed.containsKey("totalHits"));
//
//		Response ok2 = target(PATH + "/" + Utils.urlEncode("_count?pretty=true")).
//				request(JSON).headers(headers).post(entity1);
//		assertEquals(OK.getStatusCode(), ok2.getStatus());
//		assertTrue(ok2.readEntity(Map.class).containsKey("count"));
//
//		// Return the raw ES JSON
//		Response ok3 = target(PATH + "/_search").queryParam("getRawResponse", 1).request(JSON).headers(headers).post(entity1);
//		assertEquals(OK.getStatusCode(), ok3.getStatus());
//		assertTrue(ok3.readEntity(Map.class).containsKey("hits"));
//	}
//
//	private void register(ResourceConfig resource, ProxyResourceHandler proxy) {
////		resource.register(GenericExceptionMapper.class);
////		resource.register(new JacksonJsonProvider(ParaObjectUtils.getJsonMapper()));
//		Resource.Builder custom = Resource.builder(proxy.getRelativePath());
//		custom.addMethod(GET).produces(JSON).
//				handledBy(new Inflector<ContainerRequestContext, Response>() {
//					public Response apply(ContainerRequestContext ctx) {
//						return proxy.handleGet(ctx);
//					}
//				});
//		custom.addMethod(POST).produces(JSON).consumes(JSON).
//				handledBy(new Inflector<ContainerRequestContext, Response>() {
//					public Response apply(ContainerRequestContext ctx) {
//						return proxy.handlePost(ctx);
//					}
//				});
//		custom.addMethod(PATCH).produces(JSON).consumes(JSON).
//				handledBy(new Inflector<ContainerRequestContext, Response>() {
//					public Response apply(ContainerRequestContext ctx) {
//						return proxy.handlePatch(ctx);
//					}
//				});
//		custom.addMethod(PUT).produces(JSON).consumes(JSON).
//				handledBy(new Inflector<ContainerRequestContext, Response>() {
//					public Response apply(ContainerRequestContext ctx) {
//						return proxy.handlePut(ctx);
//					}
//				});
//		custom.addMethod(DELETE).produces(JSON).
//				handledBy(new Inflector<ContainerRequestContext, Response>() {
//					public Response apply(ContainerRequestContext ctx) {
//						return proxy.handleDelete(ctx);
//					}
//				});
//		resource.registerResources(custom.build());
//	}

}
