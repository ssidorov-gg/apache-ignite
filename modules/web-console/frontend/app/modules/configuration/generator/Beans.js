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
        this.arguments = [];

        this.clsName = clsName;
    }

    isEmpty() {
        return _.isEmpty(this.arguments) && _.isEmpty(this.properties);
    }

    nonEmpty() {
        return !this.isEmpty();
    }

    isComplex() {
        return _.nonEmpty(this.properties) || !!_.find(this.arguments, (arg) => arg.clsName === 'Bean');
    }

    nonComplex() {
        return !this.isComplex();
    }

    findProperty(name) {
        return _.find(this.properties, {name});
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

    /**
     * @param acc
     * @param clsName
     * @param model
     * @param name
     * @param {Function} nonEmpty Non empty function.
     * @returns {Bean}
     * @private
     */
    _property(acc, clsName, model, name, nonEmpty = () => true) {
        if (!this.src)
            return this;

        const value = _.get(this.src, model);

        if (nonEmpty(value) && value !== _.get(this.dflts, model))
            acc.push({clsName, name, value});

        return this;
    }

    constructorArgument(clsName, value) {
        this.arguments.push({clsName, value});

        return this;
    }

    stringConstructorArgument(model) {
        return this._property(this.arguments, 'String', model, null, _.nonEmpty);
    }

    intConstructorArgument(model) {
        return this._property(this.arguments, 'int', model, null, _.nonEmpty);
    }

    classConstructorArgument(model) {
        return this._property(this.arguments, 'java.lang.Class', model, null, _.nonEmpty);
    }

    pathConstructorArgument(model) {
        return this._property(this.arguments, 'Path', model, null, _.nonEmpty);
    }

    constantConstructorArgument(model) {
        if (!this.src)
            return this;

        const value = _.get(this.src, model);
        const dflt = _.get(this.dflts, model);

        if (_.nonNil(value) && _.nonNil(dflt) && value !== dflt.value)
            this.arguments.push({clsName: dflt.clsName, constant: true, value});

        return this;
    }

    /**
     * @param {String} id
     * @param {EmptyBean|Bean|MethodBean} value
     * @returns {Bean}
     */
    beanConstructorArgument(id, value) {
        this.arguments.push({clsName: 'Bean', id, value});

        return this;
    }

    valueOf(path) {
        return _.get(this.src, path) || _.get(this.dflts, path + '.value') || _.get(this.dflts, path);
    }

    includes(...paths) {
        return this.src && _.every(paths, (path) => {
            const value = _.get(this.src, path);
            const dflt = _.get(this.dflts, path);

            return _.nonNil(value) && value !== dflt;
        });
    }

    boolProperty(model, name = model) {
        return this._property(this.properties, 'boolean', model, name, _.nonNil);
    }

    byteProperty(model, name = model) {
        return this._property(this.properties, 'byte', model, name, _.nonNil);
    }

    intProperty(model, name = model) {
        return this._property(this.properties, 'int', model, name, _.nonNil);
    }

    property(clsName, name, value) {
        this.properties.push({clsName, name, value});

        return this;
    }

    propertyChar(name, value) {
        this.properties.push({clsName: 'PropertyChar', name, value});

        return this;
    }

    stringProperty(model, name = model) {
        return this._property(this.properties, 'String', model, name, _.nonEmpty);
    }

    pathProperty(model, name = model) {
        return this._property(this.properties, 'Path', model, name, _.nonEmpty);
    }

    classProperty(model, name = model) {
        return this._property(this.properties, 'java.lang.Class', model, name, _.nonEmpty);
    }

    enumProperty(model, name = model) {
        if (!this.src)
            return this;

        const value = _.get(this.src, model);
        const dflt = _.get(this.dflts, model);

        if (_.nonNil(value) && _.nonNil(dflt) && value !== dflt.value)
            this.properties.push({clsName: dflt.clsName, name, value: dflt.mapper ? dflt.mapper(value) : value});

        return this;
    }

    emptyBeanProperty(model, name = model) {
        if (!this.src)
            return this;

        const cls = _.get(this.src, model);
        const dflt = _.get(this.dflts, model);

        if (_.nonEmpty(cls) && cls !== dflt)
            this.properties.push({clsName: 'Bean', name, value: new EmptyBean(cls)});

        return this;
    }

    /**
     * @param {String} name
     * @param {EmptyBean|Bean|MethodBean} value
     * @returns {Bean}
     */
    beanProperty(name, value) {
        this.properties.push({clsName: 'Bean', name, value});

        return this;
    }

    /**
     * @param {String} id
     * @param {String} name
     * @param {Array} items
     * @param {String} typeClsName
     * @returns {Bean}
     */
    arrayProperty(id, name, items, typeClsName = 'String') {
        if (items.length)
            this.properties.push({clsName: 'Array', id, name, items, typeClsName});

        return this;
    }

    /**
     * @param {String} id
     * @param {String} name
     * @param {Array} items
     * @param {String} typeClsName
     * @returns {Bean}
     */
    varArgProperty(id, name, items, typeClsName = 'String') {
        if (items.length)
            this.properties.push({clsName: 'VarArg', id, name, items, typeClsName});

        return this;
    }

    /**
     * @param {String} id
     * @param {String} name
     * @param {Array} items
     * @param {String} clsName
     * @param {String} typeClsName
     * @param {String} implClsName
     * @returns {Bean}
     */
    collectionProperty(id, name, items, clsName = 'java.util.Collection', typeClsName = 'String', implClsName = 'java.util.ArrayList') {
        if (items.length)
            this.properties.push({id, name, items, clsName, typeClsName, implClsName});

        return this;
    }

    /**
     * @param {String} id
     * @param {String} model
     * @param {String} [name]
     * @returns {Bean}
     */
    mapProperty(id, model, name = model) {
        if (!this.src)
            return this;

        const entries = _.isString(model) ? _.get(this.src, model) : model;
        const dflt = _.isString(model) ? _.get(this.dflts, model) : _.get(this.dflts, name);

        if (_.nonEmpty(entries) && _.nonNil(dflt) && entries !== dflt.entries) {
            this.properties.push({
                clsName: dflt.clsName || 'java.util.HashMap',
                id,
                name,
                keyClsName: dflt.keyClsName,
                keyField: dflt.keyField || 'name',
                valClsName: dflt.valClsName,
                valField: dflt.valField || 'value',
                entries
            });
        }

        return this;
    }

    propsProperty(id, model, name = model) {
        if (!this.src)
            return this;

        const entries = _.get(this.src, model);

        if (_.nonEmpty(entries))
            this.properties.push({clsName: 'java.util.Properties', id, name, entries});

        return this;
    }

    /**
     * @param {String} id
     * @param {String} name
     * @param {Bean} value
     */
    dataSource(id, name, value) {
        if (value)
            this.properties.push({clsName: 'DataSource', id, name, value});

        return this;
    }

    /**
     * @param {String} id
     * @param {String} name
     * @param {Array<String>} eventTypes
     */
    eventTypes(id, name, eventTypes) {
        this.properties.push({type: 'EVENT_TYPES', id, name, eventTypes});
    }
}

export class MethodBean extends Bean {
    constructor(clsName, id, src, dflts) {
        super(clsName, id, src, dflts);
    }
}
