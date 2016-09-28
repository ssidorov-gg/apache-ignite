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

const STORE_FACTORY = ['org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory', 'org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory'];

export default ['JavaTypes', 'igniteEventGroups', 'IgniteConfigurationGenerator', (JavaTypes, eventGroups, generator) => {
    class JavaTransformer extends AbstractTransformer {
        static generator = generator;

        static comment(sb, ...lines) {
            _.forEach(lines, (line) => sb.append(`// ${line}`));
        }

        static commentBlock(sb, ...lines) {
            if (lines.length === 1)
                sb.append(`/** ${_.head(lines)} **/`);
            else {
                sb.append('/**');

                _.forEach(lines, (line) => sb.append(` * ${line}`));

                sb.append(' **/');
            }
        }

        /**
         * @param {Bean} bean
         */
        static _newBean(bean) {
            const shortClsName = JavaTypes.shortClassName(bean.clsName);

            if (_.isEmpty(bean.arguments))
                return `new ${shortClsName}()`;

            const args = _.map(bean.arguments, (arg) => {
                const shortArgClsName = JavaTypes.shortClassName(arg.clsName);

                return this._toObject(shortArgClsName, arg.value);
            });

            return `new ${shortClsName}(${args.join(', ')})`;
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
         * @param {Array.<String>} vars
         * @param {Boolean} limitLines
         * @private
         */
        static constructBean(sb, bean, vars, limitLines = false) {
            _.forEach(bean.arguments, (arg) => {
                if (arg.clsName === 'Bean' && arg.value.nonEmpty()) {
                    this.constructBean(sb, arg.value, vars, limitLines);

                    sb.emptyLine();
                }
            });

            if (_.includes(vars, bean.id))
                sb.append(`${bean.id} = ${this._newBean(bean)};`);
            else {
                vars.push(bean.id);

                const shortClsName = JavaTypes.shortClassName(bean.clsName);

                sb.append(`${shortClsName} ${bean.id} = ${this._newBean(bean)};`);
            }

            if (_.nonEmpty(bean.properties)) {
                sb.emptyLine();

                this._setProperties(sb, bean, vars, limitLines);
            }
        }

        /**
         * @param {StringBuilder} sb
         * @param {Bean} bean
         * @param {Array.<String>} vars
         * @param {Boolean} limitLines
         * @private
         */
        static constructStoreFactory(sb, bean, vars, limitLines = false) {
            const shortClsName = JavaTypes.shortClassName(bean.clsName);

            if (_.includes(vars, bean.id))
                sb.startBlock(`${bean.id} = ${this._newBean(bean)} {`);
            else {
                vars.push(bean.id);

                sb.startBlock(`${shortClsName} ${bean.id} = ${this._newBean(bean)} {`);
            }

            this.commentBlock(sb, '{@inheritDoc}');
            sb.startBlock(`@Override public ${shortClsName} create() {`);

            sb.append(`setDataSource(DataSources.INSTANCE_${bean.findProperty('dataSourceBean').id});`);

            sb.emptyLine();

            sb.append('return super.create();');

            sb.endBlock('};');

            sb.endBlock('};');

            sb.emptyLine();

            const storeFactory = _.cloneDeep(bean);

            _.remove(storeFactory.properties, (p) => _.includes(['dataSourceBean', 'dialect'], p.name));

            this._setProperties(sb, storeFactory, vars, limitLines);
        }

        static _isBean(clsName) {
            return JavaTypes.nonBuiltInClass(clsName) && JavaTypes.nonEnum(clsName) && !JavaTypes.isJavaPrimitive(clsName);
        }

        static _toObject(shortClsName, val) {
            const items = _.isArray(val) ? val : [val];

            return _.map(items, (item, idx) => {
                if (_.isNil(item))
                    return 'null';

                switch (shortClsName) {
                    case 'byte':
                        return `(byte) ${item}`;
                    case 'Serializable':
                    case 'String':
                        if (items.length > 1)
                            return `"${item}"${idx !== items.length - 1 ? ' +' : ''}`;

                        return `"${item}"`;
                    case 'Path':
                        return `"${item.replace(/\\/g, '\\\\')}"`;
                    case 'Class':
                        return `${JavaTypes.shortClassName(item)}.class`;
                    case 'UUID':
                        return `UUID.fromString("${item}")`;
                    case 'PropertyChar':
                        return `props.getProperty("${item}").toCharArray()`;
                    case 'Property':
                        return `props.getProperty("${item}")`;
                    case 'Bean':
                        if (item.isComplex())
                            return item.id;

                        return this._newBean(item);
                    default:
                        if (JavaTypes.nonEnum(shortClsName))
                            return item;

                        return `${shortClsName}.${item}`;
                }
            });
        }

        static _setArray(sb, bean, prop, vars, limitLines, arrWrapper) {
            const arrType = JavaTypes.shortClassName(prop.typeClsName);

            if (this._isBean(arrType)) {
                if (arrWrapper) {
                    sb.append(`${arrType}[] ${prop.id} = new ${arrType}[${prop.items.length}];`);

                    sb.emptyLine();

                    _.forEach(prop.items, (nested, idx) => {
                        nested = _.cloneDeep(nested);
                        nested.id = `${prop.id}[${idx}]`;

                        sb.append(`${nested.id} = ${this._newBean(nested)};`);

                        this._setProperties(sb, nested, vars, limitLines);

                        sb.emptyLine();
                    });

                    this._setProperty(sb, bean.id, prop.name, prop.id);
                } else {
                    sb.startBlock(`${bean.id}.set${_.upperFirst(prop.name)}(`);

                    const lastIdx = prop.items.length - 1;

                    _.forEach(prop.items, (item, idx) => {
                        sb.append(this._newBean(item) + (lastIdx !== idx ? ',' : ''));
                    });

                    sb.endBlock(');');
                }
            }
            else {
                const arrItems = this._toObject(arrType, prop.items);

                if (arrItems.length > 1) {
                    sb.startBlock(`${bean.id}.set${_.upperFirst(prop.name)}(${arrWrapper ? `new ${arrType}[] {` : ''}`);

                    const lastIdx = arrItems.length - 1;

                    _.forEach(arrItems, (item, idx) => sb.append(item + (lastIdx !== idx ? ',' : '')));

                    sb.endBlock(arrWrapper ? '});' : ');');
                }
                else
                    this._setProperty(sb, bean.id, prop.name, `new ${arrType}[] {${_.head(arrItems)}}`);
            }
        }

        /**
         *
         * @param {StringBuilder} sb
         * @param {Bean} bean
         * @param {Array.<String>} vars
         * @param {Boolean} limitLines
         * @returns {StringBuilder}
         */
        static _setProperties(sb = new StringBuilder(), bean, vars = [], limitLines = false) {
            _.forEach(bean.properties, (prop) => {
                const clsName = JavaTypes.shortClassName(prop.clsName);

                switch (clsName.toUpperCase()) {
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
                    case 'VARARG':
                        this._setArray(sb, bean, prop, vars, limitLines, false);

                        break;
                    case 'ARRAY':
                        this._setArray(sb, bean, prop, vars, limitLines, true);

                        break;
                    case 'COLLECTION':
                    case 'ARRAYLIST':
                        const implClsName = JavaTypes.shortClassName(prop.implClsName);

                        const colTypeClsName = JavaTypes.shortClassName(prop.typeClsName);

                        const nonBean = !this._isBean(colTypeClsName);

                        if (nonBean && implClsName === 'ArrayList') {
                            const items = this._toObject(colTypeClsName, prop.items);

                            sb.append(`${bean.id}.set${_.upperFirst(prop.name)}(Arrays.asList(${items.join(', ')}));`);
                        }
                        else {
                            if (_.includes(vars, prop.id))
                                sb.append(`${prop.id} = new ${implClsName}<>();`);
                            else {
                                vars.push(prop.id);

                                sb.append(`${clsName}<${colTypeClsName}> ${prop.id} = new ${implClsName}<>();`);
                            }

                            sb.emptyLine();

                            if (nonBean) {
                                const items = this._toObject(colTypeClsName, prop.items);

                                _.forEach(items, (item) => {
                                    sb.append(`${prop.id}.add("${item}");`);

                                    sb.emptyLine();
                                });
                            }
                            else {
                                _.forEach(prop.items, (item) => {
                                    this.constructBean(sb, item, vars, limitLines);

                                    sb.append(`${prop.id}.add(${item.id});`);

                                    sb.emptyLine();
                                });

                                this._setProperty(sb, bean.id, prop.name, prop.id);
                            }
                        }

                        break;
                    case 'HASHMAP':
                    case 'LINKEDHASHMAP':
                        const keyClsName = JavaTypes.shortClassName(prop.keyClsName);
                        const valClsName = JavaTypes.shortClassName(prop.valClsName);

                        if (_.includes(vars, prop.id))
                            sb.append(`${prop.id} = new ${clsName}<>();`);
                        else {
                            vars.push(prop.id);

                            sb.append(`${clsName}<${keyClsName}, ${valClsName}> ${prop.id} = new ${clsName}<>();`);
                        }

                        sb.emptyLine();

                        _.forEach(prop.entries, (entry) => {
                            const key = this._toObject(keyClsName, entry[prop.keyField]);
                            const val = entry[prop.valField];

                            if (_.isArray(val)) {
                                sb.startBlock(`${prop.id}.put(${key},`);

                                sb.append(this._toObject(valClsName, val));

                                sb.endBlock(');');
                            }
                            else
                                sb.append(`${prop.id}.put(${key}, ${this._toObject(valClsName, val)});`);
                        });

                        if (_.nonEmpty(prop.entries))
                            sb.emptyLine();

                        this._setProperty(sb, bean.id, prop.name, prop.id);

                        break;
                    case 'PROPERTIES':
                        sb.append(`Properties ${prop.id} = new Properties();`);

                        sb.emptyLine();

                        _.forEach(prop.entries, (entry) => {
                            sb.append(`${prop.id}.setProperty(${this._toObject('String', entry.name)}, ${this._toObject('String', entry.value)});`);
                        });

                        sb.emptyLine();

                        this._setProperty(sb, bean.id, prop.name, prop.id);

                        break;
                    case 'BEAN':
                        const embedded = prop.value;

                        if (_.includes(STORE_FACTORY, embedded.clsName)) {
                            this.constructStoreFactory(sb, embedded, vars, limitLines);

                            sb.emptyLine();

                            this._setProperty(sb, bean.id, prop.name, embedded.id);
                        }
                        else if (_.nonEmpty(embedded.properties) || embedded.isComplex()) {
                            this.constructBean(sb, embedded, vars, limitLines);

                            sb.emptyLine();

                            this._setProperty(sb, bean.id, prop.name, embedded.id);
                        }
                        else
                            this._setProperty(sb, bean.id, prop.name, this._newBean(embedded));

                        break;
                    default:
                        this._setProperty(sb, bean.id, prop.name, this._toObject(clsName, prop.value));
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
                switch (prop.clsName) {
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
         * @param sb
         * @returns {String}
         */
        static cluster(cfg, pkg, clsName, clientNearCfg, sb = new StringBuilder()) {
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
    }

    return JavaTransformer;
}];
