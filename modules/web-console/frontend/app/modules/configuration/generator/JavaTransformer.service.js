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
                switch (arg.clsName) {
                    case 'MAP':
                        return arg.id;
                    default:
                        return this._toObject(arg.clsName, arg.value);
                }
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
         * @param {Array.<String>} [vars]
         * @param {Boolean} [limitLines]
         * @private
         */
        static constructBean(sb, bean, vars = [], limitLines = false) {
            _.forEach(bean.arguments, (arg) => {
                switch (arg.clsName) {
                    case 'MAP':
                        this._constructMap(sb, arg, vars);

                        sb.emptyLine();

                        break;
                    default:
                        if (this._isBean(arg.clsName) && arg.value.isComplex()) {
                            this.constructBean(sb, arg.value, vars, limitLines);

                            sb.emptyLine();
                        }
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
                sb.startBlock(`${bean.id} = ${this._newBean(bean)};`);
            else {
                vars.push(bean.id);

                sb.startBlock(`${shortClsName} ${bean.id} = ${this._newBean(bean)};`);
            }

            sb.emptyLine();

            sb.append(`${bean.id}.setDataSourceFactory(new Factory<DataSource>() {`);
            this.commentBlock(sb, '{@inheritDoc}');
            sb.startBlock('@Override public DataSource create() {');

            sb.append(`return DataSources.INSTANCE_${bean.findProperty('dataSourceBean').id};`);

            sb.endBlock('};');
            sb.endBlock('});');

            sb.emptyLine();

            const storeFactory = _.cloneDeep(bean);

            _.remove(storeFactory.properties, (p) => _.includes(['dataSourceBean', 'dialect'], p.name));

            this._setProperties(sb, storeFactory, vars, limitLines);
        }

        static _isBean(clsName) {
            return JavaTypes.nonBuiltInClass(clsName) && JavaTypes.nonEnum(clsName) && _.includes(clsName, '.');
        }

        static _toObject(clsName, val) {
            const items = _.isArray(val) ? val : [val];

            return _.map(items, (item, idx) => {
                if (_.isNil(item))
                    return 'null';

                switch (clsName) {
                    case 'byte':
                        return `(byte) ${item}`;
                    case 'java.io.Serializable':
                    case 'java.lang.String':
                        if (items.length > 1)
                            return `"${item}"${idx !== items.length - 1 ? ' +' : ''}`;

                        return `"${item}"`;
                    case 'PATH':
                        return `"${item.replace(/\\/g, '\\\\')}"`;
                    case 'java.lang.Class':
                        return `${JavaTypes.shortClassName(item)}.class`;
                    case 'java.util.UUID':
                        return `UUID.fromString("${item}")`;
                    case 'PROPERTY_CHAR':
                        return `props.getProperty("${item}").toCharArray()`;
                    case 'PROPERTY':
                        return `props.getProperty("${item}")`;
                    default:
                        if (this._isBean(clsName)) {
                            if (item.isComplex())
                                return item.id;

                            return this._newBean(item);
                        }

                        if (JavaTypes.nonEnum(clsName))
                            return item;

                        return `${JavaTypes.shortClassName(clsName)}.${item}`;
                }
            });
        }

        static _methodName(bean) {
            switch (bean.clsName) {
                case 'org.apache.ignite.configuration.CacheConfiguration':
                    return JavaTypes.toJavaName('cache', bean.findProperty('name').value);
                default:
            }
        }

        static _setArray(sb, bean, prop, vars, limitLines, varArg) {
            const arrType = JavaTypes.shortClassName(prop.typeClsName);

            if (this._isBean(prop.typeClsName)) {
                if (varArg) {
                    if (prop.items.length === 1) {
                        const head = _.head(prop.items);

                        if (head.isComplex()) {
                            this.constructBean(sb, head, vars, limitLines);

                            sb.emptyLine();

                            sb.append(`${bean.id}.set${_.upperFirst(prop.name)}(${head.id});`);
                        }
                        else
                            sb.append(`${bean.id}.set${_.upperFirst(prop.name)}(${this._newBean(head)});`);
                    }
                    else {
                        sb.startBlock(`${bean.id}.set${_.upperFirst(prop.name)}(`);

                        const lastIdx = prop.items.length - 1;

                        _.forEach(prop.items, (item, idx) => {
                            if (item.isComplex())
                                sb.append(this._methodName(item) + '()' + (lastIdx !== idx ? ',' : ''));
                            else
                                sb.append(this._newBean(item) + (lastIdx !== idx ? ',' : ''));
                        });

                        sb.endBlock(');');
                    }
                } else {
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
                }
            }
            else {
                const arrItems = this._toObject(prop.typeClsName, prop.items);

                if (arrItems.length > 1) {
                    sb.startBlock(`${bean.id}.set${_.upperFirst(prop.name)}(${varArg ? '' : `new ${arrType}[] {`}`);

                    const lastIdx = arrItems.length - 1;

                    _.forEach(arrItems, (item, idx) => sb.append(item + (lastIdx !== idx ? ',' : '')));

                    sb.endBlock(varArg ? ');' : '});');
                }
                else
                    this._setProperty(sb, bean.id, prop.name, `new ${arrType}[] {${_.head(arrItems)}}`);
            }
        }

        static _constructMap(sb, map, vars = []) {
            const keyClsName = JavaTypes.shortClassName(map.keyClsName);
            const valClsName = JavaTypes.shortClassName(map.valClsName);

            const mapClsName = map.ordered ? 'LinkedHashMap' : 'HashMap';

            if (_.includes(vars, map.id))
                sb.append(`${map.id} = new ${mapClsName}<>();`);
            else {
                vars.push(map.id);

                sb.append(`${mapClsName}<${keyClsName}, ${valClsName}> ${map.id} = new ${mapClsName}<>();`);
            }

            sb.emptyLine();

            _.forEach(map.entries, (entry) => {
                const key = this._toObject(map.keyClsName, entry[map.keyField]);
                const val = entry[map.valField];

                if (_.isArray(val)) {
                    sb.startBlock(`${map.id}.put(${key},`);

                    sb.append(this._toObject(map.valClsName, val));

                    sb.endBlock(');');
                }
                else
                    sb.append(`${map.id}.put(${key}, ${this._toObject(map.valClsName, val)});`);
            });
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
                switch (prop.clsName) {
                    case 'DATA_SOURCE':
                        this._setProperty(sb, bean.id, prop.name, `DataSources.INSTANCE_${prop.id}`);

                        break;
                    case 'EVENT_TYPES':
                        if (prop.eventTypes.length === 1)
                            this._setProperty(sb, bean.id, prop.name, _.head(prop.eventTypes));
                        else {
                            sb.append(`int[] ${prop.id} = new int[${_.head(prop.eventTypes)}.length`);

                            _.forEach(_.tail(prop.eventTypes), (evtGrp) => {
                                sb.append(`    + ${evtGrp}.length`);
                            });

                            sb.append('];');

                            sb.emptyLine();

                            sb.append('int k = 0;');

                            _.forEach(prop.eventTypes, (evtGrp, idx) => {
                                sb.emptyLine();

                                sb.append(`System.arraycopy(${evtGrp}, 0, ${prop.id}, k, ${evtGrp}.length);`);

                                if (idx < prop.eventTypes.length - 1)
                                    sb.append(`k += ${evtGrp}.length;`);
                            });

                            sb.emptyLine();

                            sb.append(`cfg.setIncludeEventTypes(${prop.id});`);
                        }

                        break;
                    case 'ARRAY':
                        this._setArray(sb, bean, prop, vars, limitLines, prop.varArg);

                        break;
                    case 'COLLECTION':
                        const implClsName = JavaTypes.shortClassName(prop.implClsName);
                        const colTypeClsName = JavaTypes.shortClassName(prop.typeClsName);

                        const nonBean = !this._isBean(prop.typeClsName);

                        if (nonBean && implClsName === 'ArrayList') {
                            const items = this._toObject(prop.typeClsName, prop.items);

                            sb.append(`${bean.id}.set${_.upperFirst(prop.name)}(Arrays.asList(${items.join(', ')}));`);
                        }
                        else {
                            if (_.includes(vars, prop.id))
                                sb.append(`${prop.id} = new ${implClsName}<>();`);
                            else {
                                vars.push(prop.id);

                                sb.append(`${implClsName}<${colTypeClsName}> ${prop.id} = new ${implClsName}<>();`);
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
                    case 'MAP':
                        this._constructMap(sb, prop, vars);

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
                        else if (embedded.isComplex()) {
                            this.constructBean(sb, embedded, vars, limitLines);

                            sb.emptyLine();

                            this._setProperty(sb, bean.id, prop.name, embedded.id);
                        }
                        else
                            this._setProperty(sb, bean.id, prop.name, this._newBean(embedded));

                        break;
                    default:
                        this._setProperty(sb, bean.id, prop.name, this._toObject(prop.clsName, prop.value));
                }
            });

            return sb;
        }

        static generateSection(bean) {
            const sb = new StringBuilder();

            this._setProperties(sb, bean);

            return sb.asString();
        }

        static collectBeanImports(bean) {
            const imports = [bean.clsName];

            _.forEach(bean.arguments, (arg) => {
                switch (arg.clsName) {
                    case 'BEAN':
                        imports.push(...this.collectPropertiesImports(arg.value.properties));

                        break;
                    case 'java.lang.Class':
                        imports.push(JavaTypes.fullClassName(arg.value));

                        break;
                    default:
                        imports.push(arg.clsName);
                }
            });

            imports.push(...this.collectPropertiesImports(bean.properties));

            if (_.includes(STORE_FACTORY, bean.clsName))
                imports.push('javax.sql.DataSource', 'javax.cache.configuration.Factory');

            return imports;
        }

        /**
         * @param {Array.<Object>} props
         * @returns {Array.<String>}
         */
        static collectPropertiesImports(props) {
            const imports = [];

            _.forEach(props, (prop) => {
                switch (prop.clsName) {
                    case 'DATA_SOURCE':
                        imports.push(prop.value.clsName);

                        break;
                    case 'PROPERTY':
                    case 'PROPERTY_CHAR':
                        imports.push('java.io.InputStream', 'java.util.Properties');

                        break;
                    case 'BEAN':
                        imports.push(...this.collectBeanImports(prop.value));

                        break;
                    case 'ARRAY':
                        imports.push(prop.typeClsName);

                        if (this._isBean(prop.typeClsName))
                            _.forEach(prop.items, (item) => imports.push(...this.collectBeanImports(item)));

                        break;
                    case 'COLLECTION':
                        if (this._isBean(prop.typeClsName) || prop.implClsName !== 'java.util.ArrayList')
                            imports.push(prop.typeClsName, prop.implClsName);
                        else
                            imports.push('java.util.Arrays', prop.typeClsName);

                        break;
                    case 'MAP':
                        imports.push(prop.ordered ? 'java.util.LinkedHashMap' : 'java.util.HashMap');
                        imports.push(prop.keyClsName);
                        imports.push(prop.valClsName);

                        break;
                    default:
                        if (!JavaTypes.nonEnum(prop.clsName))
                            imports.push(prop.clsName);
                }
            });

            return imports;
        }

        static _prepareImports(imports) {
            return _.sortedUniq(_.sortBy(_.filter(imports, (cls) => !cls.startsWith('java.lang.') && _.includes(cls, '.'))));
        }

        /**
         * @param {Bean} bean
         * @returns {Array.<String>}
         */
        static collectStaticImports(bean) {
            const imports = [];

            _.forEach(bean.properties, (prop) => {
                switch (prop.clsName) {
                    case 'EVENT_TYPES':
                        _.forEach(prop.eventTypes, (value) => {
                            const evtGrp = _.find(eventGroups, {value});

                            imports.push(`${evtGrp.class}.${evtGrp.value}`);
                        });

                        break;
                    default:
                        // No-op.
                }
            });

            return imports;
        }

        /**
         * @param {Bean} bean
         * @returns {Array.<String>}
         */
        static collectComplexBeans(bean) {
            const beans = [];

            _.forEach(bean.properties, (prop) => {
                switch (prop.clsName) {
                    case 'BEAN':
                        beans.push(...this.collectComplexBeans(prop.value));

                        break;
                    case 'ARRAY':
                        if (this._isBean(prop.typeClsName)) {
                            const collectedBeans = _.reduce(prop.items, (acc, nestedBean) => {
                                if (nestedBean.isComplex())
                                    acc.push(nestedBean);

                                return acc;
                            }, []);

                            beans.push(...collectedBeans);
                        }

                        break;
                    default:
                    // No-op.
                }
            });

            return beans;
        }

        static cacheConfiguration(sb, ccfg) {
            const cacheName = ccfg.findProperty('name').value;
            const dataSources = this.collectDataSources(ccfg);

            const javadoc = [
                `Create configuration for cache "${cacheName}".`,
                '',
                '@return Configured cache.'
            ];

            if (dataSources.length)
                javadoc.push('@throws Exception if failed to create cache configuration.');

            this.commentBlock(sb, ...javadoc);
            sb.startBlock(`public static CacheConfiguration ${this._methodName(ccfg)}()${dataSources.length ? ' throws Exception' : ''} {`);

            this.constructBean(sb, ccfg);

            sb.emptyLine();
            sb.append(`return ${ccfg.id};`);

            sb.endBlock('}');

            return sb;
        }

        /**
         * Build Java startup class with configuration.
         *
         * @param {Bean} cfg
         * @param pkg Package name.
         * @param {String} clsName Class name for generate factory class otherwise generate code snippet.
         * @param {Boolean} clientNearCaches Is client node.
         * @returns {StringBuilder}
         */
        static igniteConfiguration(cfg, pkg, clsName, clientNearCaches) {
            const sb = new StringBuilder();

            sb.append(`package ${pkg};`);
            sb.emptyLine();

            const imports = this.collectBeanImports(cfg);

            if (_.nonEmpty(clientNearCaches))
                imports.push('org.apache.ignite.configuration.NearCacheConfiguration');

            if (_.includes(imports, 'oracle.jdbc.pool.OracleDataSource'))
                imports.push('java.sql.SQLException');

            _.forEach(this._prepareImports(imports), (cls) => sb.append(`import ${cls};`));

            sb.emptyLine();

            const staticImports = this._prepareImports(this.collectStaticImports(cfg));

            if (staticImports.length) {
                _.forEach(this._prepareImports(staticImports), (cls) => sb.append(`import static ${cls};`));

                sb.emptyLine();
            }

            this.mainComment(sb);
            sb.startBlock(`public class ${clsName} {`);

            // 2. Add external property file
            if (this.hasProperties(cfg)) {
                this.commentBlock(sb, 'Secret properties loading.');
                sb.append('private static final Properties props = new Properties();');
                sb.emptyLine();
                sb.startBlock('static {');
                sb.startBlock('try (InputStream in = IgniteConfiguration.class.getClassLoader().getResourceAsStream("secret.properties")) {');
                sb.append('props.load(in);');
                sb.endBlock('}');
                sb.startBlock('catch (Exception ignored) {');
                sb.append('// No-op.');
                sb.endBlock('}');
                sb.endBlock('}');
                sb.emptyLine();
            }

            // 3. Add data sources.
            const dataSources = this.collectDataSources(cfg);

            if (dataSources.length) {
                this.commentBlock(sb, 'Helper class for datasource creation.');
                sb.startBlock('public static class DataSources {');

                _.forEach(dataSources, (ds) => {
                    const dsClsName = JavaTypes.shortClassName(ds.clsName);

                    sb.append(`public static final ${dsClsName} INSTANCE_${ds.id} = create${ds.id}();`);
                    sb.emptyLine();

                    sb.startBlock(`private static ${dsClsName} create${ds.id}() {`);

                    if (dsClsName === 'OracleDataSource')
                        sb.startBlock('try {');

                    this.constructBean(sb, ds);

                    if (dsClsName === 'OracleDataSource') {
                        sb.endBlock('}');
                        sb.startBlock('catch (SQLException ex) {');
                        sb.append('throw new Error(ex);');
                        sb.endBlock('}');
                    }

                    sb.emptyLine();
                    sb.append(`return ${ds.id};`);

                    sb.endBlock('}');

                    sb.emptyLine();
                });

                sb.endBlock('}');

                sb.emptyLine();
            }

            _.forEach(clientNearCaches, (cache) => {
                this.commentBlock(sb, `Configuration of near cache for cache: ${cache.name}.`,
                    '',
                    '@return Near cache configuration.',
                    '@throws Exception If failed to construct near cache configuration instance.'
                );

                const nearCacheBean = generator.cacheNearClient(cache);

                sb.startBlock(`public static NearCacheConfiguration ${nearCacheBean.id}() throws Exception {`);

                this.constructBean(sb, nearCacheBean);
                sb.emptyLine();

                sb.append(`return ${nearCacheBean.id};`);
                sb.endBlock('}');

                sb.emptyLine();
            });

            this.commentBlock(sb, 'Configure grid.',
                '',
                '@return Ignite configuration.',
                '@throws Exception If failed to construct Ignite configuration instance.'
            );
            sb.startBlock('public static IgniteConfiguration createConfiguration() throws Exception {');

            this.constructBean(sb, cfg, [], true);

            sb.emptyLine();

            sb.append(`return ${cfg.id};`);

            sb.endBlock('}');

            const complexBeans = this.collectComplexBeans(cfg);

            if (complexBeans.length) {
                _.forEach(complexBeans, (bean, idx) => {
                    switch (bean.clsName) {
                        case 'org.apache.ignite.configuration.CacheConfiguration':
                            this.cacheConfiguration(sb, bean);

                            break;
                        default:
                            return;
                    }

                    if (idx !== complexBeans.length - 1)
                        sb.emptyLine();
                });
            }

            sb.endBlock('}');

            return sb;
        }

        static cluster(cluster, pkg, clsName, client) {
            const cfg = this.generator.igniteConfiguration(cluster, client);

            const clientNearCaches = client ? _.filter(cluster.caches, (cache) => _.get(cache, 'clientNearConfiguration.enabled')) : [];

            return this.igniteConfiguration(cfg, pkg, clsName, clientNearCaches);
        }

        /**
         * Generate java class code.
         *
         * @param fullClsName Full class name.
         * @param fields Fields.
         * @param addConstructor If 'true' then empty and full constructors should be generated.
         * @returns {StringBuilder}
         */
        static domainClass(fullClsName, fields, addConstructor) {
            const dotIdx = fullClsName.lastIndexOf('.');

            const pkg = fullClsName.substring(0, dotIdx);
            const clsName = fullClsName.substring(dotIdx + 1);

            const sb = new StringBuilder();

            sb.append(`package ${pkg};`);
            sb.emptyLine();

            const imports = ['java.io.Serializable'];

            _.forEach(fields, (field) => imports.push(JavaTypes.fullClassName(field.javaFieldType)));

            _.forEach(this._prepareImports(imports), (cls) => sb.append(`import ${cls};`));

            sb.emptyLine();

            this.mainComment(sb,
                `${clsName} definition.`,
                ''
            );
            sb.startBlock(`public class ${clsName} implements Serializable {`);
            sb.append('/** */');
            sb.append('private static final long serialVersionUID = 0L;');
            sb.emptyLine();

            // Generate fields declaration.
            _.forEach(fields, (field) => {
                const fldName = field.javaFieldName;
                const fldType = JavaTypes.shortClassName(field.javaFieldType);

                sb.append(`/** Value for ${fldName}. */`);
                sb.append(`private ${fldType} ${fldName};`);

                sb.emptyLine();
            });

            // Generate constructors.
            if (addConstructor) {
                this.commentBlock(sb, 'Empty constructor.');
                sb.startBlock(`public ${clsName}() {`);
                this.comment(sb, 'No-op.');
                sb.endBlock('}');

                sb.emptyLine();

                this.commentBlock(sb, 'Full constructor.');

                const arg = (field) => {
                    const fldType = JavaTypes.shortClassName(field.javaFieldType);

                    return `${fldType} ${field.javaFieldName}`;
                };

                sb.startBlock(`public ${clsName}(${arg(_.head(fields))}${fields.length === 1 ? ') {' : ','}`);

                _.forEach(_.tail(fields), (field, idx) => {
                    sb.append(`${arg(field)}${idx !== fields.length - 1 ? ',' : ') {'}`);
                });

                _.forEach(fields, (field) => sb.append(`this.${field.javaFieldName} = ${field.javaFieldName};`));

                sb.endBlock('}');

                sb.emptyLine();
            }

            // Generate getters and setters methods.
            _.forEach(fields, (field) => {
                const fldType = JavaTypes.shortClassName(field.javaFieldType);
                const fldName = field.javaFieldName;

                this.commentBlock(sb,
                    `Gets ${fldName}`,
                    '',
                    `@return Value for ${fldName}.`
                );
                sb.startBlock(`public ${fldType} ${JavaTypes.toJavaName('get', fldName)}() {`);
                sb.append('return ' + fldName + ';');
                sb.endBlock('}');

                sb.emptyLine();

                this.commentBlock(sb,
                    `Sets ${fldName}`,
                    '',
                    `@param ${fldName} New value for ${fldName}.`
                );
                sb.startBlock(`public void ${JavaTypes.toJavaName('set', fldName)}(${fldType} ${fldName}) {`);
                sb.append(`this.${fldName} = ${fldName};`);
                sb.endBlock('}');

                sb.emptyLine();
            });

            // Generate equals() method.
            this.commentBlock(sb, '{@inheritDoc}');
            sb.startBlock('@Override public boolean equals(Object o) {');
            sb.startBlock('if (this == o)');
            sb.append('return true;');

            sb.endBlock('');

            sb.startBlock(`if (!(o instanceof ${clsName}))`);
            sb.append('return false;');

            sb.endBlock('');

            sb.append(`${clsName} that = (${clsName})o;`);

            _.forEach(fields, (field) => {
                sb.emptyLine();

                const javaName = field.javaFieldName;
                const javaType = field.javaFieldType;

                switch (javaType) {
                    case 'float':
                        sb.startBlock(`if (Float.compare(${javaName}, that.${javaName}) != 0)`);

                        break;
                    case 'double':
                        sb.startBlock(`if (Double.compare(${javaName}, that.${javaName}) != 0)`);

                        break;
                    default:
                        if (JavaTypes.isJavaPrimitive(javaType))
                            sb.startBlock('if (' + javaName + ' != that.' + javaName + ')');
                        else
                            sb.startBlock('if (' + javaName + ' != null ? !' + javaName + '.equals(that.' + javaName + ') : that.' + javaName + ' != null)');
                }

                sb.append('return false;');

                sb.endBlock('');
            });

            sb.append('return true;');
            sb.endBlock('}');

            sb.emptyLine();

            // Generate hashCode() method.
            this.commentBlock(sb, '{@inheritDoc}');
            sb.startBlock('@Override public int hashCode() {');

            let first = true;
            let tempVar = false;

            _.forEach(fields, (field) => {
                const javaName = field.javaFieldName;
                const javaType = field.javaFieldType;

                let fldHashCode;

                switch (javaType) {
                    case 'boolean':
                        fldHashCode = `${javaName} ? 1 : 0`;

                        break;
                    case 'byte':
                    case 'short':
                        fldHashCode = `(int)${javaName}`;

                        break;
                    case 'int':
                        fldHashCode = `${javaName}`;

                        break;
                    case 'long':
                        fldHashCode = `(int)(${javaName} ^ (${javaName} >>> 32))`;

                        break;
                    case 'float':
                        fldHashCode = `${javaName} != +0.0f ? Float.floatToIntBits(${javaName}) : 0`;

                        break;
                    case 'double':
                        sb.append(`${tempVar ? 'ig_hash_temp' : 'long ig_hash_temp'} = Double.doubleToLongBits(${javaName});`);

                        tempVar = true;

                        fldHashCode = `${javaName} != +0.0f ? Float.floatToIntBits(${javaName}) : 0`;

                        break;
                    default:
                        fldHashCode = `${javaName} != null ? ${javaName}.hashCode() : 0`;
                }

                sb.append(first ? `int res = ${fldHashCode};` : `res = 31 * res + ${fldHashCode.startsWith('(') ? fldHashCode : `(${fldHashCode})`};`);

                first = false;

                sb.emptyLine();
            });

            sb.append('return res;');
            sb.endBlock('}');

            sb.emptyLine();

            this.commentBlock(sb, '{@inheritDoc}');
            sb.startBlock('@Override public String toString() {');
            sb.startBlock(`return "${clsName} [" + `);

            _.forEach(fields, (field, idx) => {
                sb.append(`"${field.javaFieldName}=" + ${field.javaFieldName}${idx < fields.length - 1 ? ' + ", " + ' : ' +'}`);
            });

            sb.endBlock('"]";');
            sb.endBlock('}');

            sb.endBlock('}');

            return sb.asString();
        }

        /**
         * Generate source code for type by its domain models.
         *
         * @param caches List of caches to generate POJOs for.
         * @param addConstructor If 'true' then generate constructors.
         * @param includeKeyFields If 'true' then include key fields into value POJO.
         */
        static pojos(caches, addConstructor, includeKeyFields) {
            const pojos = [];

            _.forEach(caches, (cache) => {
                _.forEach(cache.domains, (domain) => {
                    // Process only  domains with 'generatePojo' flag and skip already generated classes.
                    if (domain.generatePojo && !_.find(pojos, {valueType: domain.valueType}) &&
                        // Skip domain models without value fields.
                        _.nonEmpty(domain.valueFields)) {
                        const pojo = {};

                        // Key class generation only if key is not build in java class.
                        if (_.nonNil(domain.keyFields) && domain.keyFields.length > 0) {
                            pojo.keyType = domain.keyType;
                            pojo.keyClass = this.domainClass(domain.keyType, domain.keyFields, addConstructor);
                        }

                        const valueFields = _.clone(domain.valueFields);

                        if (includeKeyFields) {
                            _.forEach(domain.keyFields, ({fld}) => {
                                if (!_.find(valueFields, {name: fld.name}))
                                    valueFields.push(fld);
                            });
                        }

                        pojo.valueType = domain.valueType;
                        pojo.valueClass = this.domainClass(domain.valueType, valueFields, addConstructor);

                        pojos.push(pojo);
                    }
                });
            });

            return pojos;
        }
    }

    return JavaTransformer;
}];
