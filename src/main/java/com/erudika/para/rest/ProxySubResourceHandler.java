/*
 * Copyright 2013-2021 Erudika. http://erudika.com
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

import static com.erudika.para.rest.ProxyResourceHandler.PATH;

/**
 * Acts as a proxy for Elasticsearch and handles request to the custom resouce path {@code /v1/_elasticsearch/{path} }.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class ProxySubResourceHandler extends ProxyResourceHandler implements CustomResourceHandler {

	private static final String SUB_PATH = PATH + "/{path}";

	@Override
	public String getRelativePath() {
		return SUB_PATH;
	}

}
