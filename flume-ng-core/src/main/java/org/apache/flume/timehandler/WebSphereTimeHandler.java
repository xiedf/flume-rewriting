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
import java.util.regex.Pattern;

/**
 * Created by 27802 on 2017/7/3.
 */
public class WebSphereTimeHandler extends AbstractTimeHandler{
    public WebSphereTimeHandler() {
        this.original = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
        this.pattern = Pattern.compile("\\d{2}-\\d{1}-\\d{2} \\d{2}:\\d{2}:\\d{2}:\\d{3} \\w{3}");
    }

}
