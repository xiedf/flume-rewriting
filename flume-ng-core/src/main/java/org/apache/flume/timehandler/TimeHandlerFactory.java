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

/**
 * flume-parent org.apache.flume.eventhandler
 * 描述：
 * 时间：2017-3-30 11:25.
 */

public class TimeHandlerFactory {

    public static AbstractTimeHandler create(String timeTpye) {
        switch (timeTpye) {
            case "standard" :
                return new StandardTimeHandler();
            case "nginxAccess" :
                return new NginxAccessTimeHandler();
            case "nginxError" :
            case "semiStandardBySlash" :
                return new SemiStandardTimeHandler("slash");
            case "mysqlError" :
            case "semiStandardByMinus" :
                return new SemiStandardTimeHandler("minus");
            case "mysqlQuery" :
            case "simplifiedStandard" :
                return new SimplifiedStandardTimeHandler();
            case "switch" :
                return new SwitchTimeHandler();
            case "webSphere":
                return new WebSphereTimeHandler();
            case "webLogic":
                return new WebLogicTimeHandler();
            default :
                return new DefaultTimeHandler();
        }
    }

}
