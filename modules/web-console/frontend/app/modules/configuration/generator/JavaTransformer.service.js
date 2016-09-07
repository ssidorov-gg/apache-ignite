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
import AbstractTransformer from './AbstractTransformer';
import StringBuilder from './StringBuilder';
import { MethodBean } from './Beans';

import $generatorJava from './generator-java';

export default ['JavaTransformer', ['JavaTypes', 'igniteEventGroups', 'ConfigurationGenerator', (JavaTypes, eventGroups, generator) => {
    class JavaTransformer extends AbstractTransformer {
        static generator = generator;

        static comment(sb, ...lines) {
            _.forEach(lines, (line) => sb.append(`// ${line}`));
        }

        static commentBlock(sb, ...lines) {
            sb.append('/**');

            _.forEach(lines, (line) => sb.append(` * ${line}`));

            sb.append(' **/');
        }

        /**
         * @param {Bean} bean
         */
        static _newBean(bean) {
            const shortClsName = JavaTypes.shortClassName(bean.clsName);

            if (bean.isEmptyConstructor())
                return `new ${shortClsName}()`;

            const args = _.map(bean.arguments, (arg) => {
                if (_.isNil(arg.value))
                    return 'null';

                switch (arg.type) {
                    case 'PROPERTY':
                        return arg.value;
                    case 'STRING':
                        return `"${arg.value}"`;
                    case 'PATH':
                        return `"${arg.value.replace(/\\/g, '\\\\')}"`;
                    case 'CLASS':
                        return `${JavaTypes.shortClassName(arg.value)}.class`;
                    default:
                }
            });

            return `new ${shortClsName}(${args.join(', ')})`;
        }

        /**
         * @param {StringBuilder} sb
         * @param {Bean} bean
         */
        static _defineBean(sb, bean) {
            const shortClsName = JavaTypes.shortClassName(bean.clsName);

            sb.append(`${shortClsName} ${bean.id} = ${this._newBean(bean)};`);
        }

        /**
         * @param {StringBuilder} sb
         * @param {String} id
         * @param {Bean} propertyName
         * @param {String|Bean} value
         * @private
         */
        static _setProperty(sb, id, propertyName, value) {
            sb.append(`${id}.set${_.upperFirst(propertyName)}(${value});`);
        }

        /**
         * @param {StringBuilder} sb
         * @param {Bean} bean
         * @param {Boolean} limitLines
         * @private
         */
        static constructBean(sb, bean, limitLines = false) {
            this._defineBean(sb, bean);

            sb.emptyLine();

            this._setProperties(sb, bean, limitLines);
        }

        static _toObject(clsName, ...items) {
            switch (clsName) {
                case 'Serializable':
                case 'String':
                    return _.map(items, (item) => `"${item}"`);
                case 'Path':
                    return _.map(items, (item) => `"${item.replace(/\\/g, '\\\\')}"`);
                case 'Class':
                    return _.map(items, (item) => `${JavaTypes.shortClassName(item)}.class`);
                case 'CharProperty':
                    return _.map(items, (item) => `props.getProperty("${item}").toCharArray()`);
                case 'Short':
                case 'Integer':
                case 'Long':
                    return items;

                default:
                    return _.map(items, (item) => `${clsName}.${item}`);
            }
        }

        /**
         *
         * @param {StringBuilder} sb
         * @param {Bean} bean
         * @param {Boolean} limitLines
         * @returns {StringBuilder}
         */
        static _setProperties(sb = new StringBuilder(), bean, limitLines = false) {
            _.forEach(bean.properties, (prop) => {
                switch (prop.type) {
                    case 'STRING':
                        this._setProperty(sb, bean.id, prop.name, `"${prop.value}"`);

                        break;
                    case 'PATH':
                        this._setProperty(sb, bean.id, prop.name, `"${prop.value.replace(/\\/g, '\\\\')}"`);

                        break;
                    case 'PASSWORD':
                        this._setProperty(sb, bean.id, prop.name, `props.getProperty("${prop.value}").toCharArray()`);

                        break;
                    case 'CLASS':
                        this._setProperty(sb, bean.id, prop.name, `${JavaTypes.shortClassName(prop.value)}.class`);

                        break;
                    case 'PROPERTY':
                        this._setProperty(sb, bean.id, prop.name, prop.value);

                        break;
                    case 'DATASOURCE':
                        this._setProperty(sb, bean.id, prop.name, `DataSources.INSTANCE_${prop.id}`);

                        break;
                    case 'EVENT_TYPES':
                        if (prop.eventTypes.length === 1) {
                            const evtGrp = _.find(eventGroups, {value: _.head(prop.eventTypes)});

                            if (evtGrp)
                                this._setProperty(sb, bean.id, prop.name, `${evtGrp.class}.${evtGrp.value}`);
                        }
                        else {
                            sb.append(`int[] events = new int[${_.head(prop.eventTypes).value}.length`);

                            _.forEach(_.tail(prop.eventTypes), (evtGrp) => {
                                sb.append(`    + ${evtGrp.value}.length`);
                            });

                            sb.append('];');

                            sb.emptyLine();

                            sb.append('int k = 0;');

                            let evtGrp;

                            _.forEach(prop.eventTypes, (value, idx) => {
                                sb.emptyLine();

                                evtGrp = _.find(eventGroups, {value});

                                if (evtGrp) {
                                    sb.append(`System.arraycopy(${evtGrp.value}, 0, events, k, ${evtGrp.value}.length);`);

                                    if (idx < prop.eventTypes.length - 1)
                                        sb.append(`k += ${evtGrp.value}.length;`);
                                }
                            });

                            sb.emptyLine();

                            sb.append('cfg.setIncludeEventTypes(events);');
                        }

                        break;
                    case 'ENUM':
                        const value = `${JavaTypes.shortClassName(prop.clsName)}.${prop.value}`;

                        this._setProperty(sb, bean.id, prop.name, value);

                        break;
                    case 'ARRAY':
                        const arrType = JavaTypes.shortClassName(prop.typeClsName);

                        switch (arrType) {
                            case 'String':
                            case 'Class':
                            case 'int':
                            case 'Integer':
                                const arrItems = this._toObject(arrType, ...prop.items);

                                if (arrItems.length > 1) {
                                    sb.startBlock(`${bean.id}.set${_.upperFirst(prop.name)}(new ${arrType}[] {`);

                                    const lastIdx = arrItems.length - 1;

                                    _.forEach(arrItems, (item, idx) => sb.append(item + (lastIdx !== idx ? ',' : '')));

                                    sb.endBlock('});');
                                }
                                else
                                    this._setProperty(sb, bean.id, prop.name, `new ${arrType}[] {${_.head(arrItems)}}`);

                                break;
                            default:
                                sb.append(`${arrType}[] ${prop.id} = new ${arrType}[${prop.items.length}];`);

                                sb.emptyLine();

                                _.forEach(prop.items, (nested, idx) => {
                                    const nestedId = `${prop.id}[${idx}]`;

                                    const clsName = JavaTypes.shortClassName(nested.clsName);

                                    sb.append(`${nestedId} = ${this._newBean(nested)};`);

                                    _.forEach(nested.properties, (p) => {
                                        this._setProperty(sb, `((${clsName})${nestedId})`, prop.name, this._toObject(p.type, p.value));
                                    });
                                });

                                sb.emptyLine();

                                this._setProperty(sb, bean.id, prop.name, prop.id);
                        }

                        break;
                    case 'COLLECTION':
                        const clsName = JavaTypes.shortClassName(prop.clsName);
                        const colTypeClsName = JavaTypes.shortClassName(prop.typeClsName);
                        const implClsName = JavaTypes.shortClassName(prop.implClsName);

                        if (JavaTypes.nonBuiltInClass(colTypeClsName) || implClsName !== 'ArrayList') {
                            sb.append(`${clsName}<${colTypeClsName}> ${prop.id} = new ${implClsName}<>();`);

                            sb.emptyLine();

                            _.forEach(prop.items, (item) => {
                                if (_.isString(item))
                                    sb.append(`${prop.id}.add("${item}");`);
                                else if (_.isNumber(item))
                                    sb.append(`${prop.id}.add(${item});`);
                                else if (item instanceof MethodBean && limitLines)
                                    sb.append(`${prop.id}.add(${item.id}());`);
                                else {
                                    this._defineBean(sb, item);

                                    sb.emptyLine();

                                    this._setProperties(sb, item, limitLines);

                                    if (item.properties.length)
                                        sb.emptyLine();

                                    sb.append(`${prop.id}.add(${item.id});`);
                                }

                                sb.emptyLine();
                            });

                            sb.append(`${bean.id}.set${_.upperFirst(prop.name)}(${prop.id});`);
                        }
                        else {
                            const items = _.map(prop.items, (item) => _.isString(item) ? `"${item}"` : item);

                            sb.append(`${bean.id}.set${_.upperFirst(prop.name)}(Arrays.asList(${items.join(', ')}));`);
                        }

                        break;
                    case 'MAP':
                        const keyClsName = JavaTypes.shortClassName(prop.keyClsName);
                        const valClsName = JavaTypes.shortClassName(prop.valClsName);

                        sb.append(`Map<${keyClsName}, ${valClsName}> ${prop.id} = new HashMap<>();`);

                        sb.emptyLine();

                        const keyMapper = this._toObject.bind(this, keyClsName);
                        const valMapper = this._toObject.bind(this, valClsName);

                        _.forEach(prop.entries, (entry) => {
                            sb.append(`${bean.id}.put(${keyMapper(entry[prop.keyField])}, ${valMapper(entry[prop.valField])});`);
                        });

                        if (_.nonEmpty(prop.entries))
                            sb.emptyLine();

                        this._setProperty(sb, bean.id, prop.name, prop.id);

                        break;
                    default:
                        const embedded = prop.value;

                        if (embedded.nonEmpty()) {
                            this.constructBean(sb, embedded);

                            sb.emptyLine();

                            this._setProperty(sb, bean.id, prop.name, embedded.id);
                        }
                        else {
                            const shortClsName = JavaTypes.shortClassName(embedded.clsName);

                            this._setProperty(sb, bean.id, prop.name, `new ${shortClsName}()`);
                        }
                }
            });

            return sb;
        }

        static generateSection(bean) {
            const sb = new StringBuilder();

            this._setProperties(sb, bean);

            return sb.asString();
        }

        /**
         * @param {Bean} bean
         * @returns {Array.<String>}
         */
        collectClasses(bean) {
            const classes = [bean.clsName];

            _.forEach(bean.properties, (prop) => {
                switch (prop.type) {
                    case 'ENUM':
                        classes.push(prop.clsName);

                        break;

                    case 'BEAN':
                        classes.push(...this.collectClasses(prop.value));

                        break;

                    case 'MAP':
                        classes.push('java.util.Map', 'java.util.HashMap', prop.keyClsName, prop.valClsName);

                        break;

                    default:
                    // No-op.
                }
            });

            return _.uniq(classes);
        }

        /**
         * Build Java startup class with configuration.
         *
         * @param {Bean} cfg
         * @param pkg Package name.
         * @param clsName Class name for generate factory class otherwise generate code snippet.
         * @param clientNearCfg Optional near cache configuration for client node.
         * @returns {String}
         */
        static toClassFile(cfg, pkg, clsName) {
            const sb = new StringBuilder();

            sb.append(`package ${pkg};`);
            sb.emptyLine();

            _.forEach(_.sortBy(_.filter(this.collectClasses(cfg), JavaTypes.nonBuiltInClass)), (cls) => sb.append(`import ${cls};`));
            sb.emptyLine();

            this.mainComment(sb);
            sb.startBlock('public class ' + clsName + ' {');

            this.commentBlock(sb, 'Configure grid.',
                '',
                '@return Ignite configuration.',
                '@throws Exception If failed to construct Ignite configuration instance.'
            );
            sb.startBlock('public static IgniteConfiguration createConfiguration() throws Exception {');

            this.constructBean(sb, cfg, true);

            sb.emptyLine();

            sb.append(`return ${cfg.id};`);

            sb.endBlock('}');

            sb.endBlock('}');

            return sb.asString();
        }

        static clusterCaches(caches, igfss, isSrvCfg, res) {
            return $generatorJava.clusterCaches(caches, igfss, isSrvCfg, res);
        }
    }

    return JavaTransformer;
}]];