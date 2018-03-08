/*
 * Copyright 2013-2018 Erudika. https://erudika.com
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

import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Sysprop;
import static com.erudika.para.search.SearchTest.appid1;
import static com.erudika.para.search.SearchTest.u;
import com.erudika.para.utils.Config;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class ElasticSearchIT extends SearchTest {

	@BeforeClass
	public static void setUpClass() {
		System.setProperty("para.env", "embedded");
		System.setProperty("para.app_name", "para-test");
		System.setProperty("para.cluster_name", "test");
		System.setProperty("para.es.shards", "2");
		System.setProperty("para.es.root_index_sharing_enabled", "true");
		s = new ElasticSearch();
		ElasticSearchUtils.createIndex(Config.getRootAppIdentifier());
		ElasticSearchUtils.createIndex(appid1);
		ElasticSearchUtils.createIndex(appid2);
		ElasticSearchUtils.createIndex(appid3);
		SearchTest.init();
	}

	@AfterClass
	public static void tearDownClass() {
		ElasticSearchUtils.deleteIndex(Config.getRootAppIdentifier());
		ElasticSearchUtils.deleteIndex(appid1);
		ElasticSearchUtils.deleteIndex(appid2);
		ElasticSearchUtils.deleteIndex(appid3);
		ElasticSearchUtils.shutdownClient();
		SearchTest.cleanup();
	}

	@Test
	public void testCreateDeleteExistsIndex() {
		String appid = "test-index";
		String badAppid = "test index 123";

		ElasticSearchUtils.createIndex("");
		assertFalse(ElasticSearchUtils.existsIndex(""));

		ElasticSearchUtils.createIndex(appid);
		assertTrue(ElasticSearchUtils.existsIndex(appid));

		ElasticSearchUtils.deleteIndex(appid);
		assertFalse(ElasticSearchUtils.existsIndex(appid));

		assertFalse(ElasticSearchUtils.createIndex(badAppid));
		assertFalse(ElasticSearchUtils.existsIndex(badAppid));
		assertFalse(ElasticSearchUtils.deleteIndex(appid));
		assertFalse(ElasticSearchUtils.deleteIndex(badAppid));
	}

	@Test
	public void testRebuildIndex() {
		// TODO
	}

	@Test
	public void testGetIndexNameForAlias() throws InterruptedException {
		String indexWithAlias = "test-index-with-alias";
		ElasticSearchUtils.createIndex(indexWithAlias);
		assertEquals("", ElasticSearchUtils.getIndexNameForAlias(""));
		assertEquals(indexWithAlias + "_1", ElasticSearchUtils.getIndexNameForAlias(indexWithAlias));
		ElasticSearchUtils.deleteIndex(indexWithAlias);
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
	public void testSharedIndex() throws InterruptedException {
		String app1 = "myapp1";
		String app2 = " myapp2"; // IMPORTANT! See "para.prepend_shared_appids_with_space"
		String root = Config.getRootAppIdentifier();
		String type = "cat";

		App rootApp = new App("rootapp");
		rootApp.setAppid(root);
		s.index(root, rootApp);

		assertTrue(ElasticSearchUtils.addIndexAliasWithRouting(root, app1));
		assertTrue(ElasticSearchUtils.addIndexAliasWithRouting(root, app2));

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

		Thread.sleep(1000);

		// top view of all docs in shared index
		assertEquals(1, s.getCount(root, "app").intValue());
		assertEquals(3, s.getCount(root, type).intValue());
		// local view for each app space
		assertEquals(2, s.getCount(app1, type).intValue());
		assertEquals(1, s.getCount(app2, type).intValue());

		List<Sysprop> l1 = s.findQuery(app1, type, "*");
		assertEquals(2, l1.size());
		List<Sysprop> l2 = s.findQuery(app2, type, "*");
		assertEquals(l2.get(0), t2);

		s.unindexAll(Arrays.asList(t1, t2, t3));
		ElasticSearchUtils.removeIndexAlias(root, app1);
		ElasticSearchUtils.removeIndexAlias(root, app2);
	}

	@Test
	public void testNestedIndexing() throws InterruptedException {
		System.setProperty("para.es.use_nested_custom_fields", "true");
		String indexInNestedMode = "app-nested-mode";
		ElasticSearchUtils.createIndex(indexInNestedMode);
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

		s.index(indexInNestedMode, c1);
		s.index(indexInNestedMode, c2);
		s.index(indexInNestedMode, c3);

		Thread.sleep(1000);

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
		List<ParaObject> r34 = s.findQuery(indexInNestedMode, "cat", "chris");
		List<ParaObject> r35 = s.findQuery(indexInNestedMode, "cat", "properties.owner.age:[* TO *]");
		List<ParaObject> r36 = s.findQuery(indexInNestedMode, "cat", "properties.owner.nestedArray[1].sk:two2");
		assertTrue(s.findQuery(indexInNestedMode, "cat", "dog AND properties.owner.age:34").isEmpty());
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


		s.unindexAll(indexInNestedMode, Arrays.asList(c1, c2, c3));
		ElasticSearchUtils.deleteIndex(indexInNestedMode);
		System.setProperty("para.es.use_nested_custom_fields", "false");
	}
}