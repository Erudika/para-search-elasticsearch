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
package com.erudika.para.server.search.es;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Sysprop;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.server.search.ElasticSearch;
import static com.erudika.para.server.search.es.SearchTest.appid1;
import static com.erudika.para.server.search.es.SearchTest.u;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class ElasticSearchIT extends SearchTest {

	@BeforeAll
	public static void setUpClass() {
		System.setProperty("para.env", "embedded");
		System.setProperty("para.read_from_index", "true");
		System.setProperty("para.app_name", "para-test");
		System.setProperty("para.cluster_name", "test");
		System.setProperty("para.es.shards", "2");
		System.setProperty("para.es.root_index_sharing_enabled", "true");
		System.setProperty("para.es.bulk.flush_immediately", "true");
		//System.setProperty("para.es.async_enabled", "true");
		//System.setProperty("para.es.bulk.concurrent_requests", "0");
		s = new ElasticSearch();
		ESUtils.createIndex(Para.getConfig().getRootAppIdentifier());
		ESUtils.createIndex(appid1);
		ESUtils.createIndex(appid2);
		ESUtils.createIndex(appid3);
		SearchTest.init();
	}

	@AfterAll
	public static void tearDownClass() {
		ESUtils.deleteIndex(Para.getConfig().getRootAppIdentifier());
		ESUtils.deleteIndex(appid1);
		ESUtils.deleteIndex(appid2);
		ESUtils.deleteIndex(appid3);
		SearchTest.cleanup();
		ESUtils.shutdownClient();
	}

	@Test
	public void testCreateDeleteExistsIndex() {
		String appid = "test-index";
		String badAppid = "test index 123";

		ESUtils.createIndex("");
		assertFalse(ESUtils.existsIndex(""));

		ESUtils.createIndex(appid);
		assertTrue(ESUtils.existsIndex(appid));

		ESUtils.deleteIndex(appid);
		assertFalse(ESUtils.existsIndex(appid));

		assertFalse(ESUtils.createIndex(badAppid));
		assertFalse(ESUtils.existsIndex(badAppid));
		assertFalse(ESUtils.deleteIndex(appid));
		assertFalse(ESUtils.deleteIndex(badAppid));
	}

	@Test
	public void testRebuildIndex() {
		// TODO
	}

	@Test
	public void testGetIndexNameForAlias() {
		String indexWithAlias = "test-index-with-alias";
		ESUtils.createIndex(indexWithAlias);
		assertEquals("", ESUtils.getIndexNameForAlias(""));
		assertEquals(indexWithAlias + "_1", ESUtils.getIndexNameForAlias(indexWithAlias));
		ESUtils.deleteIndex(indexWithAlias);
	}

	@Test
	public void testRangeQuery() {
		// many terms
		Map<String, Object> terms1 = new HashMap<String, Object>();
		terms1.put(Config._TIMESTAMP + " <", 1111111111L);

		Map<String, Object> terms2 = new HashMap<String, Object>();
		terms2.put(Config._TIMESTAMP + "<=", u.getTimestamp());

		List<ParaObject> res1 = s.findTerms(u.getType(), terms1, true);
		List<ParaObject> res2 = s.findTerms(u.getType(), terms2, true);

		assertEquals(1, res1.size());
		assertEquals(1, res2.size());

		assertEquals(u.getId(), res1.get(0).getId());
		assertEquals(u.getId(), res2.get(0).getId());
	}

	@Test
	public void testSharedIndex() {
		String app1 = "myapp1";
		String app2 = " myapp2"; // IMPORTANT! See "para.prepend_shared_appids_with_space"
		String root = Para.getConfig().getRootAppIdentifier();
		String type = "cat";

		App rootApp = new App("rootapp");
		rootApp.setAppid(root);
		s.index(root, rootApp);

		assertTrue(ESUtils.addIndexAliasWithRouting(root, app1));
		assertTrue(ESUtils.addIndexAliasWithRouting(root, app2));

		Sysprop t1 = new Sysprop("t1");
		Sysprop t2 = new Sysprop("t2");
		Sysprop t3 = new Sysprop("t3");

		t1.setType(type);
		t2.setType(type);
		t3.setType(type);
		t1.setAppid(app1);
		t2.setAppid(app2);
		t3.setAppid(app1);
		t1.setTimestamp(System.currentTimeMillis());
		t2.setTimestamp(System.currentTimeMillis());
		t3.setTimestamp(System.currentTimeMillis());

		s.index(t1.getAppid(), t1);
		s.index(t2.getAppid(), t2);
		s.index(t3.getAppid(), t3);

		try {
			Thread.sleep(1200);
		} catch (InterruptedException ex) { }

		// top view of all docs in shared index
		assertEquals(1, s.getCount(root, "app").intValue());
//		assertEquals(3, s.getCount(root, type).intValue()); // fails on Travis CI
		// local view for each app space
		assertEquals(2, s.getCount(app1, type).intValue());
		assertEquals(1, s.getCount(app2, type).intValue());

		List<Sysprop> ls1 = s.findQuery(app1, type, "*");
		assertEquals(2, ls1.size());
		List<Sysprop> ls2 = s.findQuery(app2, type, "*");
		assertEquals(ls2.get(0), t2);

		ESUtils.deleteByQuery(app2, QueryBuilders.matchAll().build());
		assertNull(s.findById(app2, t2.getId()));
		assertNotNull(s.findById(app1, t1.getId()));
		assertNotNull(s.findById(app1, t3.getId()));

		ESUtils.deleteByQuery(app1, QueryBuilders.matchAll().build());
		assertNull(s.findById(app1, t1.getId()));
		assertNull(s.findById(app1, t3.getId()));

//		s.unindexAll(Arrays.asList(t1, t2, t3));
		ESUtils.removeIndexAlias(root, app1);
		ESUtils.removeIndexAlias(root, app2);
	}

	@Test
	public void testNestedIndexing() {
		System.setProperty("para.es.use_nested_custom_fields", "true");
		String indexInNestedMode = "app-nested-mode";
		ESUtils.createIndex(indexInNestedMode);
		String type = "cat";
		Sysprop c1 = new Sysprop("c1");
		Sysprop c2 = new Sysprop("c2");
		Sysprop c3 = new Sysprop("c3");

		c1.setType(type);
		c2.setType(type);
		c3.setType(type);
		c1.setName("Kitty 1");
		c2.setName("Kitty 2");
		c3.setName("Kitty 3");
		c1.setAppid(indexInNestedMode);
		c2.setAppid(indexInNestedMode);
		c3.setAppid(indexInNestedMode);
		c1.setTimestamp(12345678L);
		c2.setTimestamp(123456789L);
		c3.setTimestamp(1234567890L);
		c3.setTags(Arrays.asList("kitty", "pet"));

		Map<String, Object> owner1 = new HashMap<>();
		Map<String, Object> owner2 = new HashMap<>();
		Map<String, Object> owner3 = new HashMap<>();
		owner1.put("name", "Alice");
		owner2.put("name", "Bob");
		owner3.put("name", "Chris");
		owner1.put("age", 33);
		owner2.put("age", 34);
		owner3.put("age", 35);
		owner1.put("nestedArray", Arrays.asList(Collections.singletonMap("sk", "one1"), Collections.singletonMap("sk", "one2")));
		owner2.put("nestedArray", Arrays.asList(Collections.singletonMap("sk", "two1"), Collections.singletonMap("sk", "two2")));
		owner3.put("nestedArray", Arrays.asList(Collections.singletonMap("sk", "tri1"), Collections.singletonMap("sk", "tri2")));
		owner1.put("nestedTags", Arrays.asList("one1", "one2"));
		owner2.put("nestedTags", Arrays.asList("two1", "two2"));
		owner3.put("nestedTags", Arrays.asList("tri1", "tri2"));

		c1.addProperty("owner", owner1);
		c2.addProperty("owner", owner2);
		c3.addProperty("owner", owner3);

		c1.addProperty("text", "This is a little test sentence. Testing, one, two, three.");
		c2.addProperty("text", "We are testing this thing. This sentence is a test. One, two.");
		c3.addProperty("text", "totally different text - kitty 3.");

		c1.addProperty("year", 2018);
		c2.addProperty("year", 2019);
		c3.addProperty("year", 2020);

		s.index(indexInNestedMode, c1);
		s.index(indexInNestedMode, c2);
		s.index(indexInNestedMode, c3);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) { }

		// findTermInList
		ArrayList<String> terms1 = new ArrayList<String>();
		terms1.add("alice");
		terms1.add("bob");
		List<ParaObject> r1 = s.findTermInList(indexInNestedMode, "cat", "properties.owner.name", terms1);
		assertEquals(2, r1.size());
		assertTrue(r1.stream().noneMatch((c) -> c.getId().equals("c3")));
		assertTrue(s.findTermInList(indexInNestedMode, "cat", "properties.owner.name", Collections.singletonList("Bo")).isEmpty());

		// findPrefix
		List<ParaObject> r2 = s.findPrefix(indexInNestedMode, "cat", "properties.owner.name", "ali");
		assertEquals(1, r2.size());
		assertTrue(r2.get(0).getName().equals("Kitty 1"));
		assertTrue(s.findPrefix(indexInNestedMode, "cat", "properties.owner.name", "Deb").isEmpty());

		// findQuery
		List<ParaObject> r31 = s.findQuery(indexInNestedMode, "cat", "\"Kitty 2\" AND properties.owner.age:34");
		List<ParaObject> r32 = s.findQuery(indexInNestedMode, "cat", "timestamp:{12345678 TO *} AND properties.owner.age:{* TO 34]");
		List<ParaObject> r33 = s.findQuery(indexInNestedMode, "cat", "-properties.owner.age:[* TO 33]");
		List<ParaObject> r34 = s.findQuery(indexInNestedMode, "cat", "properties.owner.name:Chris");
		List<ParaObject> r35 = s.findQuery(indexInNestedMode, "cat", "properties.owner.age:[* TO *]");
		List<ParaObject> r36 = s.findQuery(indexInNestedMode, "cat", "properties.owner.nestedArray[1].sk:two2");
		assertTrue(s.findQuery(indexInNestedMode, "cat", "dog AND properties.owner.age:34").isEmpty());
		assertEquals(1, s.findQuery(indexInNestedMode, "cat", "pet AND properties.owner.age:35").size());
		assertEquals(1, s.findQuery(indexInNestedMode, "cat", "pet").size());
		assertEquals(2, s.findQuery(indexInNestedMode, "cat", "pet OR Bob").size());
		assertEquals(3, s.findQuery(indexInNestedMode, "cat", "*").size());
		assertEquals(1, s.findQuery(indexInNestedMode, "cat", "dog OR properties.owner.age:34").size());
		assertEquals(3, s.findQuery(indexInNestedMode, "cat", "properties.owner.name:[alice TO chris]").size());
		assertEquals(2, s.findQuery(indexInNestedMode, "cat", "properties.owner.name:[alice TO chris}").size());
		assertEquals(1, s.findQuery(indexInNestedMode, "cat", "properties.owner.name:{alice TO chris}").size());
		assertEquals(0, s.findQuery(indexInNestedMode, "cat", "properties.owner.nestedTags").size());
		assertEquals(1, r31.size());
		assertEquals("c2", r31.get(0).getId());
		assertEquals(1, r32.size());
		assertEquals("c2", r32.get(0).getId());
		assertEquals(2, r33.size());
		assertTrue(r33.stream().allMatch((c) -> c.getId().equals("c2") || c.getId().equals("c3")));
		assertEquals(1, r34.size());
		assertEquals("c3", r34.get(0).getId());
		assertEquals(3, r35.size());
		assertEquals(1, r36.size());
		assertEquals("c2", r36.get(0).getId());

		// max query depth: 10
		assertFalse(s.findQuery(indexInNestedMode, "cat", "properties.owner.age:[* TO *] AND (c1 OR (c2 AND "
				+ "(c3 OR (c4 AND (c5 OR (c6 AND (c7 OR (c8 AND (c9 OR c10)))))))))").isEmpty());
		assertTrue(s.findQuery(indexInNestedMode, "cat", "properties.owner.age:[* TO *] AND (c1 OR (c2 AND "
				+ "(c3 OR (c4 AND (c5 OR (c6 AND (c7 OR (c8 AND (c9 OR (c10 AND (c11 OR c12)))))))))))").isEmpty());

		// findWildcard
		assertEquals(1, s.findWildcard(indexInNestedMode, "cat", "properties.owner.name", "ali*").size());
		assertEquals(1, s.findWildcard(indexInNestedMode, "", "properties.owner.name", "chr*").size());
		assertEquals(0, s.findWildcard(indexInNestedMode, "cat", "", "ali*").size());
		assertEquals(2, s.findWildcard(indexInNestedMode, null, "properties.text", "test*").size());

		// findTerms
		Map<String, Object> terms = new HashMap<String, Object>();
		terms.put("timestamp>", 12345678);
		terms.put("properties.owner.age>=", 33);
		assertEquals(2, s.findTerms(indexInNestedMode, "cat", terms, true).size());
		assertEquals(3, s.findTerms(indexInNestedMode, "cat", terms, false).size());
		// pinpoint the exact term in a nested array - special syntax "properties.arr[0]:term"
		Map<String, Object> terms2 = new HashMap<String, Object>();
		terms2.put("properties.owner.nestedArray[0].sk", "tri1");
		List<ParaObject> nestedArrayPinpoint = s.findTerms(indexInNestedMode, "cat", terms2, true);
		assertEquals(1, nestedArrayPinpoint.size());
		assertEquals("c3", nestedArrayPinpoint.get(0).getId());

		Map<String, Object> tags = new HashMap<String, Object>();
		tags.put("properties.owner.nestedTags", "tri2");
		List<ParaObject> tagsRes = s.findTerms(indexInNestedMode, "cat", tags, true);
		assertEquals(1, tagsRes.size());
		assertEquals("c3", tagsRes.get(0).getId());

		// findSimilar
		assertTrue(s.findSimilar(indexInNestedMode, "cat", "", null, null).isEmpty());
		assertTrue(s.findSimilar(indexInNestedMode, "cat", "c3", new String[]{"properties.text"},
				(String) c3.getProperty("text")).isEmpty());
		assertTrue(s.findSimilar(indexInNestedMode, "cat", "", new String[0], "").isEmpty());
		List<Sysprop> res = s.findSimilar(indexInNestedMode, "cat", "c1",
				new String[]{"properties.text"}, (String) c2.getProperty("text"));
		assertFalse(res.isEmpty());
		assertEquals(c2, res.get(0));

		// findQuery - without properties prefix, should search across all fields, nested or otherwise
		assertEquals(1, s.findQuery(indexInNestedMode, "cat", "different").size());
		assertEquals(1, s.findQuery(indexInNestedMode, "cat", "totally").size());
		assertEquals(0, s.findQuery(indexInNestedMode, "cat", "totally AND properties.text:(testing*)").size());
		assertEquals(3, s.findQuery(indexInNestedMode, "cat", "pet OR sentence").size());

		// test nested sorting
		Pager p = new Pager(1, "properties.year", true, 5);
		List<ParaObject> rs1 = s.findQuery(indexInNestedMode, c1.getType(), "*", p);
		assertEquals(3, rs1.size());
		assertEquals("c3", rs1.get(0).getId());
		assertEquals("c2", rs1.get(1).getId());
		assertEquals("c1", rs1.get(2).getId());

		p = new Pager(1, "properties.year", false, 5);
		List<ParaObject> rs2 = s.findQuery(indexInNestedMode, c1.getType(), "*", p);
		assertEquals(3, rs2.size());
		assertEquals("c1", rs2.get(0).getId());
		assertEquals("c2", rs2.get(1).getId());
		assertEquals("c3", rs2.get(2).getId());

		s.unindexAll(indexInNestedMode, Arrays.asList(c1, c2, c3));
		ESUtils.deleteIndex(indexInNestedMode);
		System.setProperty("para.es.use_nested_custom_fields", "false");
	}

	@Test
	public void testNestedMapping() {
		System.setProperty("para.es.use_nested_custom_fields", "true");
		Property p = ESUtils.getDefaultMapping().get("properties");
		assertTrue(p != null && p.nested().enabled());

		System.setProperty("para.es.use_nested_custom_fields", "false");
		p = ESUtils.getDefaultMapping().get("properties");
		assertTrue(p != null && p.object().enabled());
	}

	@Test
	public void testVersionConflict() {
		Sysprop t1 = new Sysprop("vc1");
		t1.setType("test");
		t1.setAppid(appid1);
		t1.setTimestamp(System.currentTimeMillis());
		t1.setName("v1");
		s.index(t1.getAppid(), t1);
		assertEquals("v1", s.findById(appid1, t1.getId()).getName());
		t1.setName("v2");
		s.index(t1.getAppid(), t1);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) { }
		assertEquals("v2", s.findById(appid1, t1.getId()).getName());
		s.unindex(t1);
	}
}