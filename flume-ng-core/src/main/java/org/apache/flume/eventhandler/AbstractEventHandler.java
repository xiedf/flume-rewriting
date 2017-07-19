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

package org.apache.flume.eventhandler;

import org.apache.flume.Event;
import org.apache.flume.timehandler.AbstractTimeHandler;

/**
 * flume-parent org.apache.flume.eventhandler
 * 描述：
 * 时间：2017-3-9 13:31.
 */

public abstract class AbstractEventHandler {

    protected String[] fatalKeysArray;

    protected String[] errorKeysArray;

    protected String[] warnKeysArray;

    protected String[] infoKeysArray;

    protected String[] debugKeysArray;

    protected String defaultLevel;

    protected AbstractTimeHandler timeHandler;

    public AbstractEventHandler(String fatalKeys, String errorKeys, String warnKeys,
                                String infoKeys, String debugKeys, String defaultLevel, AbstractTimeHandler timeHandler) {
        this.fatalKeysArray = fatalKeys.split(",");
        this.errorKeysArray = errorKeys.split(",");
        this.warnKeysArray = warnKeys.split(",");
        this.infoKeysArray = infoKeys.split(",");
        this.debugKeysArray = debugKeys.split(",");
        this.defaultLevel = defaultLevel;
        this.timeHandler = timeHandler;
    }

    public abstract void handle(Event event);

}
