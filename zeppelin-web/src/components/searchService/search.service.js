/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

angular.module('zeppelinWebApp').service('searchService', function($resource, baseUrlSrv) {

  this.search = function(term) {
    this.searchTerm = term.q;
     console.log('Searching for: %o', term.q);
    if (!term.q) { //TODO(bzz): empty string check
      return;
    }
    var encQuery = window.encodeURIComponent(term.q);
    return $resource(baseUrlSrv.getRestApiBase()+'/notebook/search?q='+encQuery, {}, {
      query: {method:'GET'}
    });
  };

  this.searchTerm = '';

});
