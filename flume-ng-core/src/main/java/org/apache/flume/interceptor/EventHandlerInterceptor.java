/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.eventhandler.AbstractEventHandler;
import org.apache.flume.eventhandler.DefaultEventHandler;
import org.apache.flume.timehandler.AbstractTimeHandler;
import org.apache.flume.timehandler.TimeHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.flume.interceptor.EventHandlerInterceptor.Constants.*;
import static org.apache.flume.interceptor.EventHandlerInterceptor.Constants.INFO_KEYS_DEFAULT;
import static org.apache.flume.interceptor.EventHandlerInterceptor.Constants.TIME_TYPE_DEFAULT;


/**
 * flume-parent org.apache.flume.interceptor
 * 描述：
 * 时间：2017-3-9 13:41.
 */

/**
 * EventHandler Interceptor class that handle the time and level on all events
 * that are intercepted.<p>
 *
 * Properties:<p>
 *
 *   timeType: Timehandler type<p>
 *
 *   fatalKeys: The fatal level keys <p>
 *
 *   errorKeys: The fatal level keys <p>
 *
 *   warnKeys: The warn level keys <p>
 *
 *   infoKeys: The info level keys <p>
 *
 *   debugKeys: The debug level keys <p>
 *
 *   defaultLevel: The default level <p>
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.s1.interceptors = i1<p>
 *   agent.sources.s1.interceptors.i1.type = event_handler<p>
 *   agent.sources.s1.interceptors.i1.timeType = standard<p>
 *   agent.sources.s1.interceptors.i1.fatalKeys = [FATAL]<p>
 *   agent.sources.s1.interceptors.i1.errorKeys = [ERROR]<p>
 *   agent.sources.s1.interceptors.i1.warnKeys = [WARN ]<p>
 *   agent.sources.s1.interceptors.i1.infoKeys = [INFO ]<p>
 *   agent.sources.s1.interceptors.i1.debugKeys = [DEBUG]<p>
 *   agent.sources.s1.interceptors.i1.defaultLevel = INFO<p>
 * </code>
 *
 */
public class EventHandlerInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(HostInterceptor.class);

    private final String fatalKeys;
    private final String errorKeys;
    private final String warnKeys;
    private final String infoKeys;
    private final String debugKeys;
    private final String timeType;
    private final String defaultLevel;
    private final AbstractEventHandler eventHandler;

    /**
     * Only {@link EventHandlerInterceptor.Builder} can build me
     */
    private EventHandlerInterceptor(String fatalKeys,
                                    String errorKeys,
                                    String warnKeys,
                                    String infoKeys,
                                    String debugKeys,
                                    String defaultLevel,
                                    String timeType) {
        this.fatalKeys = fatalKeys;
        this.errorKeys = errorKeys;
        this.warnKeys = warnKeys;
        this.infoKeys = infoKeys;
        this.debugKeys = debugKeys;
        this.defaultLevel = defaultLevel;
        this.timeType = timeType;

        AbstractTimeHandler timeHandler = TimeHandlerFactory.create(timeType);
        this.eventHandler = new DefaultEventHandler(fatalKeys, errorKeys, warnKeys, infoKeys, debugKeys, defaultLevel, timeHandler);
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        eventHandler.handle(event);
        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    /**
     * Builder which builds new instances of the EventHandlerInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private String fatalKeys = FATAL_KEYS_DEFAULT;
        private String errorKeys = ERROR_KEYS_DEFAULT;
        private String warnKeys = WARN_KEYS_DEFAULT;
        private String infoKeys = INFO_KEYS_DEFAULT;
        private String debugKeys = DEBUG_KEYS_DEFAULT;
        private String defaultLevel = DEFAULT_LEVEL_DEFAULT;
        private String timeType = TIME_TYPE_DEFAULT;

        @Override
        public Interceptor build() {
            return new EventHandlerInterceptor(fatalKeys, errorKeys, warnKeys, infoKeys, debugKeys, defaultLevel, timeType);
        }

        @Override
        public void configure(Context context) {
            fatalKeys = context.getString(FATAL_KEYS, FATAL_KEYS_DEFAULT);
            errorKeys = context.getString(ERROR_KEYS, ERROR_KEYS_DEFAULT);
            warnKeys = context.getString(WARN_KEYS, WARN_KEYS_DEFAULT);
            infoKeys = context.getString(INFO_KEYS, INFO_KEYS_DEFAULT);
            debugKeys = context.getString(DEBUG_KEYS, DEBUG_KEYS_DEFAULT);
            defaultLevel = context.getString(DEFAULT_LEVEL, DEFAULT_LEVEL_DEFAULT);
            timeType = context.getString(TIME_TYPE, TIME_TYPE_DEFAULT);
        }

    }

    public static class Constants {

        public static String FATAL_KEYS = "fatalKeys";
        public static String ERROR_KEYS = "errorKeys";
        public static String WARN_KEYS = "warnKeys";
        public static String INFO_KEYS = "infoKeys";
        public static String DEBUG_KEYS = "debugKeys";
        public static String DEFAULT_LEVEL = "defaultLevel";
        public static String TIME_TYPE = "timeType";

        public static String FATAL_KEYS_DEFAULT = "FATAL";
        public static String ERROR_KEYS_DEFAULT = "ERROR";
        public static String WARN_KEYS_DEFAULT = "WARN";
        public static String INFO_KEYS_DEFAULT = "INFO";
        public static String DEBUG_KEYS_DEFAULT = "DEBUG";
        public static String DEFAULT_LEVEL_DEFAULT = "INFO";
        public static String TIME_TYPE_DEFAULT = "";

    }
}
