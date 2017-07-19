/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.timehandler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * flume-parent org.apache.flume.eventhandler
 * 描述：
 * 时间：2017-3-30 11:12.
 */

public abstract class AbstractTimeHandler {

    protected SimpleDateFormat original;
    protected Pattern pattern;
    protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sssZ");

    public AbstractTimeHandler() {
    }

    public AbstractTimeHandler(SimpleDateFormat original, Pattern pattern) {
        this.original = original;
        this.pattern = pattern;
    }

    public String handle(String body) {
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            String time = matcher.group();
            Date date = null;
            try {
                date = original.parse(time);
            } catch (ParseException e) {
                date = new Date();
            }
            time = sdf.format(date);
            return time;
        } else {
            return null;
        }
    };

}
