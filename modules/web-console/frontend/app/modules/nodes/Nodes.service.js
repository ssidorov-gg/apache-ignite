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

import nodesDialogTemplate from './nodes-dialog.jade';

class Nodes {
    /**
     * @param $modal
     */
    constructor($q, $modal) {
        this.$q = $q;
        this.$modal = $modal;
    }

    selectNodes(settings) {
        const { $q, $modal } = this;
        const defer = $q.defer();

        const modalInstance = $modal(_.assign({
            templateUrl: nodesDialogTemplate,
            show: true,
            resolve: {
                nodes: () => []
            },
            placement: 'center',
            controller: 'nodesDialogController',
            controllerAs: '$ctrl'
        }, settings));

        modalInstance.$scope._hide = modalInstance.$scope.$hide;
        modalInstance.$scope.$hide = (data) => {
            defer.resolve(data);
            modalInstance.$scope._hide();
        };

        console.log(modalInstance);

        return defer.promise;
    }
}

Nodes.$inject = ['$q', '$modal'];

export default Nodes;
