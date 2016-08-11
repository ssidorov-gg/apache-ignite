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

import _ from 'lodash';

export class EmptyBean {
    /**
     * @param {String} clsName
     */
    constructor(clsName) {
        this.properties = [];

        this.clsName = clsName;
    }

    isEmpty() {
        return _.isEmpty(this.properties);
    }

    nonEmpty() {
        return !this.isEmpty();
    }
}

export class Map {
    /**
     *
     * @param {String} keyClsName
     * @param {String} valClsName
     * @param {String} id
     * @param {Array.<{String, String}>} items
     */
    constructor(keyClsName, valClsName, id, items) {
        this.keyClsName = keyClsName;
        this.valClsName = valClsName;

        this.id = id;

        this.items = items;
    }
}

export class Bean extends EmptyBean {
    /**
     * @param {String} clsName
     * @param {String} id
     * @param {Object} src
     * @param {Object} dflts
     */
    constructor(clsName, id, src, dflts = {}) {
        super(clsName);

        this.id = id;

        this.src = src;
        this.dflts = dflts;
    }

    valueOf(path) {
        return (this.src && this.src[path]) || this.dflts[path];
    }

    property(model, name = model) {
        if (!this.src)
            return this;

        const value = this.src[model];

        if (!_.isNil(value) && value !== this.dflts[model])
            this.properties.push({type: 'PROPERTY', name, value});

        return this;
    }

    enumProperty(model, name = model) {
        if (!this.src)
            return this;

        const value = this.src[model];
        const dflt = this.dflts[model];

        if (!_.isNil(value) && value !== dflt.value)
            this.properties.push({type: 'ENUM', clsName: dflt.clsName, name, value, mapper: dflt.mapper });

        return this;
    }

    emptyBeanProperty(model, name = model) {
        if (!this.src)
            return this;

        const cls = this.src[model];

        if (!_.isEmpty(cls) && cls !== this.dflts[model])
            this.properties.push({type: 'BEAN', name, value: new EmptyBean(cls)});

        return this;
    }

    /**
     * @param {String} name
     * @param {Bean|MethodBean} value
     * @returns {Bean}
     */
    beanProperty(name, value) {
        this.properties.push({type: 'BEAN', name, value});

        return this;
    }

    /**
     * @param {String} name
     * @param {Array} items
     * @param {String} clsName
     * @param {String} clsName
     * @param {String} implClsName
     * @returns {Bean}
     */
    collectionProperty(name, items, clsName = 'java.util.Collection', typeClsName = 'java.util.String', implClsName = 'java.util.ArrayList') {
        if (items.length)
            this.properties.push({type: 'COLLECTION', name, items, clsName, typeClsName, implClsName});

        return this;
    }

    /**
     * @param {String} name
     * @param {String} id
     * @returns {Bean}
     */
    mapProperty(name, id) {
        if (!this.src)
            return this;

        const entries = this.src[name];
        const dflt = this.dflts[name];

        if (!_.isEmpty(entries) && entries !== dflt.items) {
            this.properties.push({
                type: 'MAP',
                name,
                value: new Map(dflt.keyClsName, dflt.valClsName, id, entries)
            });
        }

        return this;
    }
}

export class MethodBean extends Bean {
    constructor(clsName, id, src, dflts) {
        super(clsName, id, src, dflts);
    }
}