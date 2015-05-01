/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.vootoo.logging.logback;

import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.apache.solr.logging.LogWatcher;

public class LogbackEventAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
	private final LogWatcher<ILoggingEvent> watcher;
	private final ThresholdFilter thresholdFilter = new ThresholdFilter();
	private String threshold;

	public LogbackEventAppender(LogWatcher<ILoggingEvent> framework) {
		this.watcher = framework;
		addFilter(thresholdFilter);
	}

	@Override
	protected void append(ILoggingEvent eventObject) {
		watcher.add(eventObject, eventObject.getTimeStamp());
	}

	@Override
	public void start() {
		thresholdFilter.start();
		super.start();
	}

	@Override
	public void stop() {
		watcher.reset();
		super.stop();
		thresholdFilter.stop();
	}

	public String getThreshold() {
		return threshold;
	}

	public void setThreshold(String level) {
		thresholdFilter.setLevel(level);
		threshold = level;
	}
}
