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

import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class ProxyResourceHandlerTest {

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Test
	public void testGetAppidFromAuthHeader() {
		ProxyResourceHandler instance = new ProxyResourceHandler();
		String jwtGood1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBpZCI6ImFwcDpteWFwcCJ9."
				+ "M4uitKDuclLuZzadxNzL_3fjeShKBxPdncsNKkA-rfY";
		String jwtGood2 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBpZCI6Im15YXBwIn0."
				+ "rChFKBeaKvlV9p_dkMveh1v85YT144IHilaeMpuVhx8";
		String jwtBad1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
		String jwtBad2 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzIjoibXlhcHAifQ."
				+ "0nqax4hqUIRGhtmPIhLbUgCrnKJFC1q3eIeRkgQX8F0";
		String signature = "AWS4-HMAC-SHA256 Credential=app:myapp/20171103/us-east-1/para/aws4_request, "
				+ "SignedHeaders=content-type;host;x-amz-date, Signature=d60fd1be560d3ed14ff383061772055";

		assertEquals("", instance.getAppidFromAuthHeader(null));
		assertEquals("", instance.getAppidFromAuthHeader(" "));
		assertEquals("", instance.getAppidFromAuthHeader("Bearer " + jwtBad1));
		assertEquals("", instance.getAppidFromAuthHeader("Bearer " + jwtBad2));
		assertEquals("myapp", instance.getAppidFromAuthHeader("Bearer " + jwtGood1));
		assertEquals("myapp", instance.getAppidFromAuthHeader("Bearer " + jwtGood2));
		assertEquals("myapp", instance.getAppidFromAuthHeader(signature));
	}

}
