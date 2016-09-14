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
            if (_.includes(vars, bean.id))
                sb.append(`${bean.id} = ${this._newBean(bean)};`);
            else {
                vars.push(bean.id);

                const shortClsName = JavaTypes.shortClassName(bean.clsName);

                sb.append(`${shortClsName} ${bean.id} = ${this._newBean(bean)};`);
            }

            sb.emptyLine();

            this._setProperties(sb, bean, vars, limitLines);
        }

        static _isBean(clsName) {
            return JavaTypes.nonBuiltInClass(clsName) && JavaTypes.nonEnum(clsName);
        }

        static _toObject(shortClsName, ...items) {
            if (!JavaTypes.nonEnum(shortClsName))
                return _.map(items, (item) => `${shortClsName}.${item}`);

            return _.map(items, (item) => {
                if (_.isNil(item))
                    return 'null';

                switch (shortClsName) {
                    case 'Serializable':
                    case 'String':
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

                    default:
                        return item;
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
                const arrItems = this._toObject(arrType, ...prop.items);

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

                                    if (item.nonEmpty())
                                        sb.emptyLine();

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

                        const keyMapper = this._toObject.bind(this, keyClsName);
                        const valMapper = this._toObject.bind(this, valClsName);

                        _.forEach(prop.entries, (entry) => {
                            sb.append(`${prop.id}.put(${keyMapper(entry[prop.keyField])}, ${valMapper(entry[prop.valField])});`);
                        });

                        if (_.nonEmpty(prop.entries))
                            sb.emptyLine();

                        this._setProperty(sb, bean.id, prop.name, prop.id);

                        break;
                    case 'BEAN':
                        const embedded = prop.value;

                        if (embedded.nonEmpty()) {
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