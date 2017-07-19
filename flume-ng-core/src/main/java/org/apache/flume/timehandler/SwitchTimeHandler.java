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
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * flume-parent org.apache.flume.eventhandler
 * 描述：
 * 时间：2017-3-31 09:54.
 */


public class SwitchTimeHandler extends AbstractTimeHandler {

    public SwitchTimeHandler() {
        this.original = new SimpleDateFormat("MMM dd HH:mm:ss", Locale.ENGLISH);
        this.pattern = Pattern.compile("\\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2}");
    }


    /**
     * 交换机日志默认不输出年份，要单独处理
     * @param body 日志体
     * @return 时间
     */
    @Override
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

            // 获取当前时间
            Calendar calendar = Calendar.getInstance();
            // 获取当前年
            int currentYear = calendar.get(Calendar.YEAR);
            // 获取当前月
            int currentMonth = calendar.get(Calendar.MONTH) + 1;
            // 将calendar 设置为日志提取到的不包含年份的时间(默认是1970年)
            calendar.setTime(date);
            // 获取日志中的时间的月份
            int logMonth = calendar.get(Calendar.MONTH) + 1;

            // 如果当前月份比日志中的月份小,说明该日志产生于去年,否则该日志产生于今年
            if (currentMonth < logMonth) {
                calendar.set(Calendar.YEAR, currentYear-1);
            } else {
                calendar.set(Calendar.YEAR, currentYear);
            }
            // 重新获取当前date时间对象
            date = calendar.getTime();

            // 格式化后返回
            time = sdf.format(date);
            return time;
        } else {
            return null;
        }

    }
}
