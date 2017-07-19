/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.eventhandler;

import org.apache.flume.Event;
import org.apache.flume.timehandler.AbstractTimeHandler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * flume-parent org.apache.flume.eventhandler
 * 描述：
 * 时间：2017-3-9 13:38.
 */

public class DefaultEventHandler extends AbstractEventHandler {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sssZ");

    public DefaultEventHandler(String fatalKeys, String errorKeys, String warnKeys,
                               String infoKeys, String debugKeys, String defaultLevel, AbstractTimeHandler timeHandler) {
        super(fatalKeys, errorKeys, warnKeys, infoKeys, debugKeys, defaultLevel, timeHandler);
    }

    @Override
    public void handle(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        // handle log level
        handleLevel(headers, body);

        // handle time
        String time = timeHandler.handle(body);

        // 如果日志中没有提取到时间,就设置为当前时间,并用insertTime为true加以标识
        if (time == null) {
            time = sdf.format(new Date());
            headers.put("insertTime", "true");
        }
        headers.put("time", time);

    }


    private void handleLevel(Map<String, String> headers, String body) {
        String level = null;

        if (include(body, fatalKeysArray)) {
            level = "FATAL";
        } else if (include(body, errorKeysArray)) {
            level = "ERROR";
        } else if (include(body, warnKeysArray)) {
            level = "WARN";
        } else if (include(body, infoKeysArray)) {
            level = "INFO";
        } else if (include(body, debugKeysArray)) {
            level = "DEBUG";
        }

        if(level == null) {
            level = this.defaultLevel;
            headers.put("defaultLevel", "true");
        }

        headers.put("level", level);

    }

    private boolean include(String body, String[] keys) {
        for (String key : keys) {
            if (body.indexOf(key) >= 0) {
                return true;
            }
        }
        return false;
    }


}
