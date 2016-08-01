/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCheckedException;

/**
 * Description and access method for query entity field.
 */
public abstract class GridQueryProperty {
    /** Property ownership flag. */
    private final boolean key;

    /** */
    public GridQueryProperty(boolean key) {
        this.key = key;
    }

    /**
     * Gets this property value from the given object.
     *
     * @param key Key.
     * @param val Value.
     * @return Property value.
     * @throws IgniteCheckedException If failed.
     */
    public abstract Object value(Object key, Object val) throws IgniteCheckedException;

    /**
     * Sets this property value for the given object.
     *
     * @param key Key.
     * @param val Value.
     * @param propVal Property value.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException;

    /**
     * @return Property name.
     */
    public abstract String name();

    /**
     * @return Class member type.
     */
    public abstract Class<?> type();

    /**
     * Property ownership flag.
     * @return {@code true} if this property belongs to key, {@code false} if it belongs to value.
     */
    public boolean key() {
        return key;
    }
}
