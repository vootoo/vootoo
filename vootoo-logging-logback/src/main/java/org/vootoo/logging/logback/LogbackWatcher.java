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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.google.common.base.Throwables;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.logging.CircularList;
import org.apache.solr.logging.ListenerConfig;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LoggerInfo;
import org.slf4j.impl.StaticLoggerBinder;

import java.util.*;

/**
 * logback LogWatcher <p/>
 *
 * solr.xml
 * <pre>
 *   &lt;solr&gt;
 *     &lt;-- ... --&gt;
 *     &lt;logging&gt;
 *       &lt;str name="class"&gt;org.vootoo.logging.logback.LogbackLogWatcher&lt;/str&gt;
 *       &lt;bool name="enabled"&gt;true&lt;/bool&gt;
 *       &lt;watcher&gt;
 *         &lt;int name="size"&gt;50&lt;/int&gt;
 *         &lt;str name="threshold"&gt;WARN&lt;/str&gt;
 *       &lt;/watcher&gt;
 *     &lt;/logging&gt;
 *     &lt;-- ... --&gt;
 *   &lt;/solr&gt;
 * </pre>
 */
public class LogbackWatcher extends LogWatcher<ILoggingEvent> {

	private final String name = StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr();

	private LogbackEventAppender appenderBase = null;

	private ch.qos.logback.classic.LoggerContext getLoggerContext() {
		return (ch.qos.logback.classic.LoggerContext) StaticLoggerBinder.getSingleton().getLoggerFactory();
	}

	private ch.qos.logback.classic.Logger getRootLogger(ch.qos.logback.classic.LoggerContext loggerContext) {
		if(loggerContext == null) {
			loggerContext = getLoggerContext();
		}
		return loggerContext.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
	}

	@Override
	public String getName() {
		return "Logback ("+name+")";
	}

	@Override
	public List<String> getAllLevels() {
		return Arrays.asList(
				ch.qos.logback.classic.Level.ALL.toString(),
				ch.qos.logback.classic.Level.TRACE.toString(),
				ch.qos.logback.classic.Level.DEBUG.toString(),
				ch.qos.logback.classic.Level.INFO.toString(),
				ch.qos.logback.classic.Level.WARN.toString(),
				ch.qos.logback.classic.Level.ERROR.toString(),
				ch.qos.logback.classic.Level.OFF.toString()
		);
	}

	@Override
	public void setLogLevel(String category, String level) {
		ch.qos.logback.classic.Logger log;
		ch.qos.logback.classic.LoggerContext loggerContext = getLoggerContext();
		if(LoggerInfo.ROOT_NAME.equals(category)) {
			log = getRootLogger(loggerContext);
		} else {
			log = loggerContext.getLogger(category);
		}
		if(level==null||"unset".equals(level)||"null".equals(level)) {
			log.setLevel(null);
		} else {
			log.setLevel(ch.qos.logback.classic.Level.toLevel(level));
		}
	}

	@Override
	public Collection<LoggerInfo> getAllLoggers() {
		ch.qos.logback.classic.LoggerContext loggerContext = getLoggerContext();
		Map<String,LoggerInfo> map = new HashMap<String,LoggerInfo>();
		ch.qos.logback.classic.Logger root = getRootLogger(loggerContext);
		List<Logger> loggers = loggerContext.getLoggerList();
		for(Logger logger : loggers) {
			if(root == logger) {
				continue;
			}
			String name = logger.getName();
			map.put(name, new LogbackInfo(name, logger));

			while (true) {
				int dot = name.lastIndexOf(".");
				if (dot < 0)
					break;
				name = name.substring(0, dot);
				if(!map.containsKey(name)) {
					map.put(name, new LogbackInfo(name, null));
				}
			}
		}
		map.put(LoggerInfo.ROOT_NAME, new LogbackInfo(LoggerInfo.ROOT_NAME, root));
		return map.values();
	}

	@Override
	public void setThreshold(String level) {
		if(appenderBase==null) {
			throw new IllegalStateException("Must have an appender");
		}
		appenderBase.setThreshold(level);
	}

	@Override
	public String getThreshold() {
		if(appenderBase==null) {
			throw new IllegalStateException("Must have an appender");
		}
		return appenderBase.getThreshold();
	}

	@Override
	public void registerListener(ListenerConfig cfg) {
		if(history!=null) {
			throw new IllegalStateException("History already registered");
		}
		history = new CircularList<ILoggingEvent>(cfg.size);
		appenderBase = new LogbackEventAppender(this);
		if(cfg.threshold != null) {
			appenderBase.setThreshold(cfg.threshold);
		} else {
			appenderBase.setThreshold(ch.qos.logback.classic.Level.WARN.toString());
		}
		appenderBase.start();	//start for logging valid
		Logger log = getRootLogger(null);
		log.addAppender(appenderBase);
	}

	@Override
	public long getTimestamp(ILoggingEvent event) {
		return event.getTimeStamp();
	}

	@Override
	public SolrDocument toSolrDocument(ILoggingEvent event) {
		SolrDocument doc = new SolrDocument();
		doc.setField("time", new Date(event.getTimeStamp()));
		doc.setField("level", event.getLevel().toString());
		doc.setField("logger", event.getLoggerName());
		doc.setField("message", event.getFormattedMessage());
		ThrowableProxy t = (ThrowableProxy)event.getThrowableProxy();
		if(t!=null) {
			doc.setField("trace", Throwables.getStackTraceAsString(t.getThrowable()));
		}
		return doc;
	}
}
