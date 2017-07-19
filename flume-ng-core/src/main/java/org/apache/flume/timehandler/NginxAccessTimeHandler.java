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

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * flume-parent org.apache.flume.eventhandler
 * 描述：
 * 时间：2017-3-30 12:42.
 */

public class NginxAccessTimeHandler extends AbstractTimeHandler {

    public NginxAccessTimeHandler() {
        this.original = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH);
        this.pattern = Pattern.compile("\\d{2}\\/\\w{3}\\/\\d{4}:\\d{2}:\\d{2}:\\d{2} \\+\\d{4}");
    }

}
