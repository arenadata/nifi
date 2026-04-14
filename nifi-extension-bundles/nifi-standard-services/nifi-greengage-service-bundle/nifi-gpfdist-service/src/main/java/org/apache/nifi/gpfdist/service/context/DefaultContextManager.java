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
package org.apache.nifi.gpfdist.service.context;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.logging.ComponentLog;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultContextManager implements ContextManager<Context> {
    private final ComponentLog logger;
    private final Map<ContextId, Context> contextMap;

    public DefaultContextManager(ComponentLog logger) {
        this.logger = logger;
        this.contextMap = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<Context> get(ContextId contextId) {
        return Optional.ofNullable(contextMap.get(contextId));
    }

    @Override
    public void add(Context context) {
        contextMap.put(context.getContextId(), context);
        logger.debug("Added context {}", context.getContextId());
    }

    @Override
    public void remove(ContextId contextId) {
        Optional.ofNullable(contextMap.remove(contextId))
                .ifPresent(ctx -> logger.info("Removed context {}", ctx.getContextId()));
    }
}
