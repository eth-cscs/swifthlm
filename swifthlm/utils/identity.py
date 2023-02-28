#!/usr/bin/python

# (C) Copyright 2017-2022 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
# Giuseppe Lo Re (giuseppe.lore@cscs.ch)  
# Harald Seipp (seipp@de.ibm.com)  
# Juraj Miscik (juraj.miscik@ibm.com)  
# Marek Tomko (mtomko@sk.ibm.com)  
# Radovan Stas (radovas.stas@sk.ibm.com)  
# Robert J. Zima (robert.juraj.zima@ibm.com)  
# Slavisa Sarafijanovic (sla@zurich.ibm.com)   
# Stanislav Kocan (stanislavkocan@ibm.com) 

from swift.common.utils import list_from_csv


def get_identity(environ):
    """Extract the identity from the Keystone auth component."""
    if (environ.get('HTTP_X_IDENTITY_STATUS') != 'Confirmed'
        or environ.get(
            'HTTP_X_SERVICE_IDENTITY_STATUS') not in (None, 'Confirmed')):
        return
    roles = list_from_csv(environ.get('HTTP_X_ROLES', ''))
    service_roles = list_from_csv(environ.get('HTTP_X_SERVICE_ROLES', ''))
    identity = {'user': (environ.get('HTTP_X_USER_ID'),
                         environ.get('HTTP_X_USER_NAME')),
                'tenant': (environ.get('HTTP_X_PROJECT_ID',
                                       environ.get('HTTP_X_TENANT_ID')),
                           environ.get('HTTP_X_PROJECT_NAME',
                                       environ.get('HTTP_X_TENANT_NAME'))),
                'roles': roles,
                'service_roles': service_roles}
    token_info = environ.get('keystone.token_info', {})
    auth_version = 0
    user_domain = project_domain = (None, None)
    if 'access' in token_info:
        # ignore any domain id headers that authtoken may have set
        auth_version = 2
    elif 'token' in token_info:
        auth_version = 3
        user_domain = (environ.get('HTTP_X_USER_DOMAIN_ID'),
                       environ.get('HTTP_X_USER_DOMAIN_NAME'))
        project_domain = (environ.get('HTTP_X_PROJECT_DOMAIN_ID'),
                          environ.get('HTTP_X_PROJECT_DOMAIN_NAME'))
    identity['user_domain'] = user_domain
    identity['project_domain'] = project_domain
    identity['auth_version'] = auth_version
    return identity
