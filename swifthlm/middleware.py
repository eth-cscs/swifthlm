#!/usr/bin/python

# (C) Copyright 2016-2022 IBM Corp.
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

# Authors:
# Giuseppe Lo Re (giuseppe.lore@cscs.ch)  
# Harald Seipp (seipp@de.ibm.com)  
# Juraj Miscik (juraj.miscik@ibm.com)  
# Marek Tomko (mtomko@sk.ibm.com)  
# Radovan Stas (radovas.stas@sk.ibm.com)  
# Robert J. Zima (robert.juraj.zima@ibm.com)  
# Slavisa Sarafijanovic (sla@zurich.ibm.com)   
# Stanislav Kocan (stanislavkocan@ibm.com)  

# SwiftHLM is useful for running OpenStack Swift on top of high latency media
# (HLM) storage, such as tape or optical disk archive based backends, allowing
# to store cheaply and access efficiently large amounts of infrequently used
# object data.
#
# This file implements SwiftHLM Middleware component of SwiftHLM, which is the
# middleware added to Swift's proxy server.
#
# SwiftHLM middleware extends Swift's interface and thus allows to explicitly
# control and query the state (on disk or on HLM) of Swift object data,
# including efficient prefetch of bulk of objects from HLM to disk when those
# objects need to be accessed.
#
# SwiftHLM provides the following basic HLM functions on the external Swift
# interface:
# - MIGRATE (container or an object from disk to HLM)
# - RECALL (i.e. prefetch a container or an object from HLM to disk)
# - STATUS (get status for a container or an object)
# - REQUESTS (get status of migration and recall requests previously submitted
#   for a contaner or an object).
#
# MIGRATE and RECALL are asynchronous operations, meaning that the request from
# user is queued and user's call is responded immediately, then the request is
# processed as a background task. Requests are currently processed in a FIFO
# manner (scheduling optimizations are future work).
# REQUESTS and STATUS are synchronous operations that block the user's call
# until the queried information is collected and returned.
#
# For each of these functions, SwiftHLM Middleware invokes additional SwiftHLM
# components to perform the task, which includes calls to HLM storage backend.
#
# -------
# MIGRATE
# -------
#
# Trigger a migration from disk to HLM of a single object or all objects within
# a container.
# MIGRATE request is an HTTP POST request, with the following syntax:
#
#     POST http://<host>:<port>/hlm/v1/MIGRATE/<account>/<container>/<object>
#     POST http://<host>:<port>/hlm/v1/MIGRATE/<account>/<container>
#     POST http://<host>:<port>/hlm/v1/PREMIGRATE/<account>/<container>/<object>
#
#     Note: SwiftHLM request keywords are case-insensitive, MIGRATE or migrate
#     can be used, RECALL or recall, STATUS or status, REQUESTS or requests.
#
# ------
# RECALL
# ------
#
# Trigger a recall from HLM to disk for a single object or all objects within a
# container.
# RECALL request is an HTTP POST request, with the following syntax:
#
#     POST http://<host>:<port>/hlm/v1/RECALL/<account>/<container>/<object>
#     POST http://<host>:<port>/hlm/v1/RECALL/<account>/<container>
#
# ------
# STATUS
# ------
#
# Return, as the response body, a JSON encoded dictionary of objects and their
# status (on HLM or on disk) for a given object or all objects within a
# container.
# STATUS query request is an HTTP GET request, with the following syntax:
#
#     GET http://<host>:<port>/hlm/v1/STATUS/<account>/<container>/<object>
#     GET http://<host>:<port>/hlm/v1/STATUS/<account>/<container>
#
# ------
# REQUESTS
# ------
#
# Return, as the response body, a JSON encoded list of pending or failed
# migration and recall requests submitted for a contaner or an object.
# REQUESTS query request is an HTTP GET request, with the following syntax:
#
#     GET http://<host>:<port>/hlm/v1/REQUESTS/<account>/<container>/<object>
#     GET http://<host>:<port>/hlm/v1/REQUESTS/<account>/<container>

import datetime
import getpass
import os
import re
import socket
import threading
import time
import uuid
from collections import defaultdict
from errno import ENOENT
from socket import gethostbyname, gethostname

from eventlet import GreenPile, Timeout
from paramiko import AutoAddPolicy, SSHClient
from simpleeval import simple_eval
from swift.common.direct_client import (ClientException,
                                        direct_put_container_object)
from swift.common.exceptions import ClientException
from swift.common.http import (HTTP_BAD_REQUEST, HTTP_INTERNAL_SERVER_ERROR,
                               HTTP_NOT_FOUND, HTTP_OK,
                               HTTP_PRECONDITION_FAILED)
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.memcached import MemcacheConnectionError
from swift.common.ring import Ring
from swift.common.storage_policy import POLICIES
from swift.common.swob import Request, Response, str_to_wsgi, wsgi_to_str
from swift.common.utils import (FileLikeIter, Timestamp, cache_from_env,
                                config_true_value, json,
                                last_modified_date_to_timestamp, quorum_size,
                                readconf, register_swift_info)
from swift.common.wsgi import ConfigString
from swift.proxy.controllers.base import get_container_info

from swifthlm.utils.common_logger import get_common_logger
from swifthlm.utils.identity import get_identity

# SwiftHLM Queues: account and container names
SWIFTHLM_ACCOUNT = '.swifthlm'
SWIFTHLM_PENDING_REQUESTS_CONTAINER = 'pending-hlm-requests'
SWIFTHLM_FAILED_REQUESTS_CONTAINER = 'failed-hlm-requests'
SWIFTHLM_MIGRATION_POLICIES = 'migration-policies'

# The default internal client config body is to support upgrades without
# requiring deployment of the new /etc/swift/internal-client.conf
ic_conf_body = """
[DEFAULT]
# swift_dir = /etc/swift
# user = swift
# You can specify default log routing here if you want:
# log_name = swift
# log_facility = LOG_LOCAL0
# log_level = INFO
# log_address = /dev/log
#
# comma separated list of functions to call to setup custom log handlers.
# functions get passed: conf, name, log_to_console, log_route, fmt, logger,
# adapted_logger
# log_custom_handlers =
#
# If set, log_udp_host will override log_address
# log_udp_host =
# log_udp_port = 514
#
# You can enable StatsD logging here:
# log_statsd_host = localhost
# log_statsd_port = 8125
# log_statsd_default_sample_rate = 1.0
# log_statsd_sample_rate_factor = 1.0
# log_statsd_metric_prefix =
[pipeline:main]
pipeline = catch_errors proxy-logging cache proxy-server
[app:proxy-server]
use = egg:swift#proxy
# See proxy-server.conf-sample for options
allow_account_management = false
[filter:cache]
use = egg:swift#memcache
# See proxy-server.conf-sample for options
[filter:proxy-logging]
use = egg:swift#proxy_logging
[filter:catch_errors]
use = egg:swift#catch_errors
# See proxy-server.conf-sample for options
""".lstrip()


class HlmMiddleware(object):

    def __init__(self, app, conf):
        """Magical init method.

        Args:
            app (any): Application.
            conf (dict): Configuration content.
        """
        # App is the final application
        self.app = app

        # Config
        self.conf = conf

        # This host ip address
        self.ip = gethostbyname(gethostname())

        # Swift directory
        self.swift_dir = conf.get('swift_dir', '/etc/swift')

        # Logging
        self.conf['log_level'] = 'DEBUG'
        self.logger = get_common_logger(
            self.conf, logger_name='hlm-middleware')
        self.logger.info('Common logger imported')

        # Request
        self.req = ''
        # Per storage node request list
        self.per_node_request = defaultdict(list)
        # Responses received from storage nodes
        self.response_in = defaultdict(list)
        # Locks
        self.stdin_lock = threading.Lock()
        self.stout_lock = threading.Lock()
        # Internal swift client
        self.create_internal_swift_client()
        # Container ring
        self.container_ring = Ring(self.swift_dir, ring_name='container')
        self.logger.debug('Initialized container ring "container"')

        # Memcache client
        self.mcache = None

        self.logger.info('info: Initialized SwiftHLM Middleware')
        self.logger.debug('dbg: Initialized SwiftHLM Middleware')

    # Get ring info needed for determining storage nodes
    def get_object_ring(self, storage_policy_index):
        """Returns object ring assigned to storage policy index.

        Args:
            storage_policy_index (int): Storage policy index.

        Returns:
            any: Appropriate ring object.
        """
        try:
            return POLICIES.get_object_ring(
                storage_policy_index, self.swift_dir)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    # Determine storage policy index uses self.app inside proxy
    def get_storage_policy_index(self, account, container):
        """Returns storage policy index of container.

        Args:
            account (str): ID of account.
            container (str): Name of container.

        Returns:
            int: Storage policy index.
        """
        try:
            container_info = get_container_info(
                {
                    'PATH_INFO': f'/v1/{str_to_wsgi(account)}/{str_to_wsgi(container)}'},
                self.app, swift_source='LE')
            return container_info['storage_policy']
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    # Determine storage nodes and other info for locating data file
    def get_obj_storage_nodes(self, account, container, obj, spi):
        """Gets information about corresponding storage nodes.

        Args:
            account (str): ID of account.
            container (str): Name of container.
            obj (str): Name of object.
            spi (int): Storage policy index.

        Returns:
            list: List of IP addresses.
            list: List of devices.
            int: Storage policy index.
            str: SWIFT directory.
        """
        try:
            obj_ring = self.get_object_ring(storage_policy_index=spi)
            swift_dir = self.swift_dir
            partition, nodes = obj_ring.get_nodes(account, container, obj)
            self.logger.debug(f'Storage nodes: {nodes}')
            ips = []
            devices = []
            for node in nodes:
                ips.append(node['ip'])
                devices.append(node['device'])
            return ips, devices, spi, swift_dir
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def __call__(self, env, start_response):
        """Magical call method handles all requests.

        Args:
            env (dict): Environment.
            start_response (str): Previous response.

        Returns:
            Response: HTTP response.
        """
        try:
            self.logger.debug('env[PATH_INFO]: %s', str(env['PATH_INFO']))

            req = Request(env)
            self.req = req

            if 'allow_hlm_for' in self.conf:
                self.logger.debug(
                    'allow hlm for: %s', str(
                        self.conf['allow_hlm_for']))
                if len(self.conf['allow_hlm_for']) == 0:
                    self.set_allow_for_hlm_fallback()
            else:
                self.set_allow_for_hlm_fallback()

            if self.mcache is None:
                self.mcache = cache_from_env(req.environ)
            if not self.mcache:
                self.logger.error('Memcache not found')
            else:
                self.logger.debug('Memcache registered')

            # Split request path to determine version, account, container,
            # object
            try:
                (namespace, ver_ifhlm, cmd_ifhlm, acc_ifhlm, con_ifhlm, obj_ifhlm)\
                    = req.split_path(1, 6, True)
            except ValueError:
                self.logger.debug('split_path exception')
                return self.app(env, start_response)
            self.logger.debug(':%s:%s:%s:%s:%s:%s:', namespace, ver_ifhlm,
                              cmd_ifhlm, acc_ifhlm, con_ifhlm, obj_ifhlm)

            if namespace == 'hlm':
                try:
                    hlm_req = cmd_ifhlm.lower()
                except AttributeError:
                    self.logger.debug('unexpected hlm_req attribute')
                    return self.app(env, start_response)
                if hlm_req == "migrate_policy" and req.method in [
                    "GET",
                    "DELETE",
                    "PUT",
                ]:
                    policy_id = acc_ifhlm
                    account = None
                    container = None
                    obj = None
                else:
                    account = wsgi_to_str(acc_ifhlm)
                    container = wsgi_to_str(con_ifhlm)
                    obj = wsgi_to_str(obj_ifhlm)
            else:
                try:
                    (version, account, container, obj) = req.split_path(1, 4, True)
                except ValueError:
                    self.logger.debug('split_path exception')
                    return self.app(env, start_response)
                self.logger.debug(':%s:%s:%s:%s:', version, account, container,
                                  obj)

            # More debug info
            self.logger.debug('req.headers: %s', str(self.req.headers))

            # If request is not HLM request or not a GET, it is not processed
            # by this middleware
            method = req.method
            if namespace != 'hlm':
                hlm_req = None

            self.logger.debug(f"method: {method}")
            self.logger.debug(f"hlm_req: {hlm_req}")
            self.logger.debug(f"account: {account}")
            self.logger.debug(f"container: {container}")
            self.logger.debug(f"obj: {obj}")

            # Authorization when it is HLM request
            if hlm_req:
                if hlm_req in ['migrate', 'recall',
                               'premigrate', 'status', 'requests']:
                    is_authorized, response = self.is_authorized_user(
                        env, account, container, hlm_req)
                elif hlm_req in ['migrate_policy', 'migrate_policies']:
                    is_authorized, response = self.is_authorized_user_policies(
                        env, account, container, hlm_req, method)
                if not is_authorized:
                    return Response(status=HTTP_BAD_REQUEST,
                                    body=response,
                                    content_type="text/plain")(env, start_response)

            if method == 'GET':
                if not hlm_req:
                    # covers:
                    # curl -X GET -H "X-Storage-Token: $TOKEN"
                    # "http://spectrumscale:8080/v1/$ACCT/TestContainer/test_object_1"
                    if obj:
                        return self.process_request_nonspec_get(
                            env, start_response, hlm_req, account, container, obj)
                    else:
                        # covers:
                        # curl -X GET -H "X-Storage-Token: $TOKEN"
                        # "http://spectrumscale:8080/v1/$ACCT/TestContainer"
                        return self.app(env, start_response)
                elif hlm_req == 'status':
                    # covers:
                    # curl -X GET -H "X-Storage-Token: $TOKEN"
                    # "http://spectrumscale:8080/hlm/v1/status/$ACCT/TestContainer"
                    return self.process_request_status_get(
                        env, start_response, hlm_req, account, container, obj)

                elif hlm_req == 'requests':
                    # covers:
                    # curl -X GET -H "X-Storage-Token: $TOKEN"
                    # "http://spectrumscale:8080/hlm/v1/requests/$ACCT/TestContainer"
                    return self.process_request_requests_get(
                        env, start_response, hlm_req, account, container, obj)

                elif hlm_req == 'migrate_policies':
                    return self.process_request_migrate_policies_get(
                        env, start_response, hlm_req, account, container, obj)

                elif hlm_req == 'migrate_policy':
                    return self.process_request_migrate_policy_get(
                        env, start_response, hlm_req, account, container, policy_id)

                else:
                    return self.app(env, start_response)

            elif method == 'PUT':

                if hlm_req:
                    return self.process_request_migrate_policy_put(
                        env, start_response, hlm_req, account, container, obj, policy_id)

            elif method == 'POST':

                if hlm_req:
                    if hlm_req in ['migrate', 'recall', 'premigrate']:
                        # Get identity of request sender
                        sender_identity = get_identity(env)
                        if (sender_identity is not None):
                            sender_user_id, sender_user_name = sender_identity['user']
                        else:
                            sender_user_name = "*"
                        # covers:
                        # curl -X POST -H "X-Storage-Token: $TOKEN" "http://spectrumscale:8080/hlm/v1/migrate/$ACCT/TestContainer/test_object_1"
                        # curl -X POST -H "X-Storage-Token: $TOKEN" "http://spectrumscale:8080/hlm/v1/recall/$ACCT/TestContainer/test_object_1"
                        # curl -X POST -H "X-Storage-Token: $TOKEN"
                        # "http://spectrumscale:8080/hlm/v1/premigrate/$ACCT/TestContainer/testDocument”
                        return self.process_request_hlm_post(
                            env, start_response, hlm_req, account, container, obj, sender_user_name)

                    elif hlm_req in ['smigrate', 'srecall']:
                        # covers:
                        # curl -X POST -H "X-Storage-Token: $TOKEN" "http://spectrumscale:8080/hlm/v1/smigrate/$ACCT/TestContainer/test_object_1"
                        # curl -X POST -H "X-Storage-Token: $TOKEN"
                        # "http://spectrumscale:8080/hlm/v1/srecall/$ACCT/TestContainer/test_object_1"
                        return self.process_request_sync_hlm_post(
                            env, start_response, hlm_req, account, container, obj)

                    elif hlm_req == 'migrate_policy':
                        return self.process_request_migrate_policy_post(
                            env, start_response, hlm_req, account, container, obj)

                    else:
                        return self.app(env, start_response)

            elif method == 'DELETE':

                if hlm_req:
                    if hlm_req in ['migrate_policy']:
                        return self.process_request_migrate_policy_delete(
                            env, start_response, hlm_req, account, container, policy_id)

                    else:
                        return self.app(env, start_response)

            else:
                return self.app(env, start_response)

            return self.app(env, start_response)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def is_authorized_user(self, env, account, container, hlm_req):
        """Checks if user is authorized to execute HLM requests.

        Args:
            env (dict): Environment.
            account (str): Account ID.
            container (str): Name of container.
            hlm_req (str): HLM request.

        Returns:
            bool: True if authorized, False otherwise.
            str: Text response.
        """
        try:
            sender_identity = get_identity(env)
            enable_hlm_operations_on_public_containers = self.conf.get(
                'enable_hlm_operations_on_public_containers', "False").lower()
            self.logger.debug(
                f"'enable_hlm_operations_on_public_containers' is set to {enable_hlm_operations_on_public_containers}")

            # If sender_identity is None, user identity is NOT confimed (not valid token / no token)
            # If "enable_recall_of_public_containers" is set to True in config,
            # "recall" and "status" HLM operations are allowed on public containers
            # If container is NOT public OR request is NOT ("recall" or "status") OR
            # "enable_recall_of_public_containers" is set to False, reject a request
            if sender_identity is None:
                if self._has_ACL_rights(env, sender_identity, account, container) and \
                        hlm_req in ['recall', 'status', 'requests'] and \
                        config_true_value(enable_hlm_operations_on_public_containers):
                    return True, None
                response = "Error: not authorized to execute HLM operations."
                return False, response

            _, sender_user_name = sender_identity['user']
            sender_tenant_id, _ = sender_identity['tenant']
            sender_roles = sender_identity['roles']

            # Reject request if user is not whitelisted
            if not (self._is_whitelisted(sender_user_name, sender_roles)):
                response = "Error: user is not authorized to execute HLM operations."
                return False, response

            # Reject request if user does NOT have EITHER
            # valid token for requested account AND assigned a role that is in the operator_roles
            # OR reseller_admin_role (can operate on all accounts)
            # OR was given the access right to the container via ACL
            if not ((self._has_valid_token(sender_tenant_id, account) and self._has_operator_role(sender_roles)) or
                    (self._has_reseller_role(sender_roles)) or
                    (self._has_ACL_rights(env, sender_identity, account, container))):
                response = "Error: user is not authorized to execute HLM operations."
                return False, response

            # Accept the request
            return True, None
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def is_authorized_user_policies(
            self, env, account, container, hlm_req, method):
        """Checks if user is authorized to use methods for migration policies.

        Args:
            env (dict): Environment.
            account (str): Account ID.
            container (str): Name of container
            hlm_req (str): HLM request.
            method (str): HTTP method.

        Returns:
            bool: True if authorized, False otherwise.
            str: Text response.
        """
        try:
            sender_identity = get_identity(env)
            # If sender_identity is None, user identity is NOT confimed (not valid token / no token)
            # reject request in this case
            if sender_identity is None:
                response = "Error: not authorized to execute HLM operations."
                return False, response

            _, sender_user_name = sender_identity['user']
            sender_tenant_id, _ = sender_identity['tenant']
            sender_roles = sender_identity['roles']

            # Reject request if user is not whitelisted
            if not (self._is_whitelisted(sender_user_name, sender_roles)):
                response = "Error: user is not authorized to execute HLM operations."
                return False, response

            if method == "POST":
                # Reject request if user does NOT have EITHER
                # valid token for requested account AND assigned a role that is in the operator_roles
                # OR reseller_admin_role (can operate on all accounts)
                # OR was given the access right to the container via ACL
                if not ((self._has_valid_token(sender_tenant_id, account) and self._has_operator_role(sender_roles)) or
                        (self._has_reseller_role(sender_roles)) or
                        (self._has_ACL_rights(env, sender_identity, account, container))):
                    response = "Error: user is not authorized to execute HLM operations."
                    return False, response
            elif method == "GET" and hlm_req == "migrate_policies":
                if account is None:
                    # case curl -H "X-Auth-Token: $TOKEN" -X GET "http://m1.example.com:8080/hlm/v1/migrate_policies"
                    # listing of all policies
                    # reject request if user does NOT have a role which is in
                    # reseller_admin_role
                    if not self._has_reseller_role(sender_roles):
                        response = "Error: user is not authorized to execute HLM operations."
                        return False, response
                if account is not None and container is None:
                    # case curl -H "X-Auth-Token: $TOKEN" -X GET "http://m1.example.com:8080/hlm/v1/migrate_policies/$ACCT“
                    # listing of all policies within account
                    # Reject request if user does NOT have EITHER
                    # valid token for requested account AND assigned a role that is in the operator_roles
                    # OR reseller_admin_role (can operate on all accounts)
                    if not ((self._has_valid_token(sender_tenant_id, account) and self._has_operator_role(sender_roles)) or
                            (self._has_reseller_role(sender_roles))):
                        response = "Error: user is not authorized to execute HLM operations."
                        return False, response
                if account is not None and container is not None:
                    # case curl -H "X-Auth-Token: $TOKEN" -X GET "http://m1.example.com:8080/hlm/v1/migrate_policies/$ACCT/<container>“
                    # listing of all policies within account and container
                    # Reject request if user does NOT have EITHER
                    # valid token for requested account AND assigned a role that is in the operator_roles
                    # OR reseller_admin_role (can operate on all accounts)
                    # OR was given the access right to the container via AC
                    if not ((self._has_valid_token(sender_tenant_id, account) and self._has_operator_role(sender_roles)) or
                            (self._has_reseller_role(sender_roles)) or
                            (self._has_ACL_rights(env, sender_identity, account, container))):
                        response = "Error: user is not authorized to execute HLM operations."
                        return False, response

            # Accept the request
            return True, None
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _has_valid_token(self, tenant_id, account, prefixes=["AUTH"]):
        """Checks token validity.

        Args:
            tenant_id (str): ID of tenant.
            account (str): ID of account.
            prefixes (list, optional): Possible prefixes. Defaults to ["AUTH"].

        Returns:
            bool: True if valid token, False otherwise.
        """
        return any(f"{prefix}_{tenant_id}" == account for prefix in prefixes)

    def _is_whitelisted(self, user, sender_roles):
        """Checks if user is whitelisted.

        Args:
            user (str): Name of user.
            sender_roles (list): List of sender's roles.

        Returns:
            bool: True if whitelisted, False otherwise.
        """
        try:
            allow_hlm_for = self.conf.get('allow_hlm_for', "").split(' ')
            enable_all_operator_role_users = self.conf.get(
                'enable_all_operator_role_users_hlm_operations', "False").lower()
            enable_all_ResellerAdmin_users = self.conf.get(
                'enable_all_ResellerAdmin_users_hlm_operations', "False").lower()
            self.logger.debug(
                f"'enable_all_operator_role_users_hlm_operations' is set to {enable_all_operator_role_users}")
            self.logger.debug(
                f"'enable_all_ResellerAdmin_users_hlm_operations' is set to {enable_all_ResellerAdmin_users}")

            # When enable_all_operator_role_users_hlm_operations in config is enabled
            # and user has an operator role, he is whitelisted automatically
            if config_true_value(
                    enable_all_operator_role_users) and self._has_operator_role(sender_roles):
                self.logger.debug(
                    f"User {user} has operator role and is whitelisted automatically.")
                return True

            # When enable_all_ResellerAdmin_users_hlm_operations in config is enabled
            # and user has a ResellerAdmin role, he is whitelisted
            # automatically
            if config_true_value(
                    enable_all_ResellerAdmin_users) and self._has_reseller_role(sender_roles):
                self.logger.debug(
                    f"User {user} has ResellerAdmin role and is whitelisted automatically.")
                return True

            # When these 2 options are disabled or user has not an operator role
            # or user has not an ResellerAdmin role, we check allow_hlm_for
            # list of users
            self.logger.debug(
                f"Is whitelisted: {'*' in allow_hlm_for or user in allow_hlm_for}")
            return "*" in allow_hlm_for or user in allow_hlm_for
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _has_ACL_rights(self, env, sender_identity, account, container):
        """Checks if sender has ACL rights.

        Args:
            env (dict): Environment.
            sender_identity (dict): Metadata of sender.
            account (str): Account ID.
            container (str): Name of container.

        Returns:
            bool: True if sender has ACL rights, False otherwise.
        """
        try:
            enable_hlm_operations_on_public_containers = self.conf.get(
                'enable_hlm_operations_on_public_containers', "False").lower()
            container_metadata = get_container_info(
                {'PATH_INFO': f'/v1/{account}/{container}'},
                self.app, swift_source='LE')

            # parsing referer
            referer = env.get("HTTP_REFERER", None)
            self.logger.debug(f"Referer: {referer}")
            # parsing read_acl elements
            read_acl = (container_metadata.get('read_acl') or "").split(',')
            read_acl = [acl.strip() for acl in read_acl]
            self.logger.debug(f"Read ACL: {read_acl}")
            # parsing write_acl elements
            write_acl = (container_metadata.get('write_acl') or "").split(',')
            write_acl = [acl.strip() for acl in write_acl]
            self.logger.debug(f"Write ACL: {write_acl}")

            # case: .r:*, r:<referrer>, .r:-<referrer>
            # containers are public
            if config_true_value(enable_hlm_operations_on_public_containers):
                if ".r:*" in read_acl:
                    return True
                if referer is not None:
                    if f".r:{referer}" in read_acl:
                        return True
                    if f".r:-{referer}" in read_acl:
                        return False

            if sender_identity is not None:
                sender_user_id, sender_user_name = sender_identity['user']
                sender_tenant_id, sender_tenant_name = sender_identity['tenant']
                sender_roles = sender_identity['roles']

                # case: <role_name>
                if self._has_valid_token(sender_tenant_id, account):
                    for role in sender_roles:
                        if (role in read_acl or role in write_acl):
                            self.logger.debug("User role found in ACL")
                            return True

                # cases: <project-id>:<user-id>, <project-id>:*, *:<user-id>,
                # *:*
                tenant_match = [sender_tenant_id, sender_tenant_name, '*']
                user_match = [sender_user_id, sender_user_name, '*']
                for tenant in tenant_match:
                    for user in user_match:
                        entry = f"{tenant}:{user}"
                        if (entry in read_acl) or (entry in write_acl):
                            self.logger.debug("User found in ACL")
                            return True
            return False
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _has_reseller_role(self, sender_roles):
        """Checks if user has reseller role.

        Args:
            sender_roles (list): List of roles from sender.

        Returns:
            bool: True if user has reseller role, False otherwise.
        """
        try:
            # search for "reseller_admin_role = ..." in config
            with open(self.conf["__file__"], "r") as f:
                reseller_admin_role_line = [
                    line.rstrip() for line in f if line.startswith("reseller_admin_role")]
            reseller_admin_role = reseller_admin_role_line[0].split("=")
            reseller_admin_role = reseller_admin_role[1].strip()
            reseller_roles = [reseller_admin_role]

            self.logger.debug(f"reseller_admin_role: {reseller_roles}")
            self.logger.debug(
                f"Has reseller_admin_role: {bool(set(reseller_roles) & set(sender_roles))}")
            return bool(set(reseller_roles) & set(sender_roles))
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _has_operator_role(self, sender_roles):
        """Checks if user has operator role.

        Args:
            sender_roles (list): List of roles from sender.

        Returns:
            bool: True if user has operator, False otherwise.
        """
        try:
            # search for "operator_roles = ..." in config
            with open(self.conf["__file__"], "r") as f:
                operator_roles_line = [
                    line.rstrip() for line in f if line.startswith("operator_roles")]
            operator_roles = operator_roles_line[0].split("=")
            operator_roles = operator_roles[1].split(",")
            operator_roles = [role.strip() for role in operator_roles]

            self.logger.debug(f"operator_roles: {operator_roles}")
            self.logger.debug(
                f"Has operator role: {bool(set(operator_roles) & set(sender_roles))}")
            return bool(set(operator_roles) & set(sender_roles))
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def process_request_migrate_policy_delete(
            self, env, start_response, hlm_req, account, container, policy_id):
        """Handler for DELETE method of migration policy requests.

        Args:
            env (dict): Environment.
            start_response (str): Previous response.
            hlm_req (str): HLM request.
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            self.logger.debug('Migration policy DELETE query')
            if not policy_id:
                self.logger.warning('Policy id was not specified.')
                rbody_json = {'error': 'Policy id was not specified.'}
                rbody_json_str = json.dumps(rbody_json)
                return Response(status=HTTP_PRECONDITION_FAILED,
                                body=rbody_json_str,
                                content_type="application/json")(env, start_response)
            if self.delete_migration_policy(policy_id):
                self.logger.debug(
                    f"Deleted migration policy with id {policy_id}")
                return Response(status=HTTP_OK,
                                body=f'Deleted migration policy with id {policy_id}\n',
                                content_type="application/json")(env, start_response)
            else:
                self.logger.debug(
                    f"Failed to delete migration policy with id {policy_id}")
                rbody = f"Policy with id {policy_id} does not exist."
                rbody_json = {'error': rbody}
                rbody_json_str = json.dumps(rbody_json)
                return Response(status=HTTP_NOT_FOUND,
                                body=rbody_json_str,
                                content_type="application/json")(
                    env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in DELETE/migrate_policy: %s',
                exception)

    def process_request_migrate_policy_post(
            self, env, start_response, hlm_req, account, container, obj):
        """Handler for POST method of migration policy requests.

        Args:
            env (dict): Environment.
            start_response (str): Previous response.
            hlm_req (str): HLM request.
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object to remove.

        Returns:
            Response: HTTP response.
        """
        try:
            if account is None:
                return Response(status=HTTP_BAD_REQUEST,
                                body=f"Invalid {hlm_req} request. Missing account.",
                                content_type="text/plain")(env, start_response)

            self.logger.debug('Migrate policy POST query')
            try:
                request_body_size = int(env.get('CONTENT_LENGTH', 0))
            except (ValueError):
                request_body_size = 0

            request_body = env['wsgi.input'].read(request_body_size)
            self.logger.debug(f"Request body : {request_body}")

            policy_req_succes, policy_req_output = self.list_migration_policy_request(
                account, container, request_body)
            if policy_req_succes:
                self.logger.debug(
                    "Listed migration policy requests; policy_id: %s",
                    policy_req_output)
                rbody_json = {
                    'success': True,
                    'policy_id': policy_req_output,
                    'body': request_body.decode(),
                }
                rbody_json_str = json.dumps(rbody_json)
                return Response(status=HTTP_OK,
                                body=rbody_json_str,
                                content_type="application/json")(env, start_response)
            else:
                self.logger.warning("Failed to list migration policy request")
                rbody_json = {
                    'success': False,
                    'body': policy_req_output,
                }
                rbody_json_str = json.dumps(rbody_json)
                return Response(status=HTTP_INTERNAL_SERVER_ERROR,
                                body=rbody_json_str,
                                content_type="application/json")(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in POST/migrate_policy: %s',
                exception)

    def process_request_sync_hlm_post(
            self, env, start_response, hlm_req, account, container, obj):
        """Handler for POST method of synchronous HLM requests.

        Args:
            env (dict): Environment.
            start_response (str): Previous response.
            hlm_req (str): HLM request.
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            # returns response (HTTP_NOT_FOUND, HTTP_INTERNAL_SERVER_ERROR) or
            # None (by default), if both exists
            container_object_check_response = self.check_container_object_exist(
                env, start_response, account, container, obj)
            # if response is not None, return it
            if container_object_check_response:
                return container_object_check_response

            hlm_req = hlm_req[1:]
            # Distribute request to storage nodes get responses
            self.distribute_request_to_storage_nodes_get_responses(
                hlm_req, account, container, obj)
            # Merge responses from storage nodes
            # i.e. merge self.response_in into self.response_out
            self.merge_responses_from_storage_nodes(hlm_req)
            # Report result
            # jout = json.dumps(out) + str(len(json.dumps(out)))
            jout = json.dumps(self.response_out)  # testing w/ response_in
            return Response(status=HTTP_OK,
                            body=jout,
                            content_type="text/plain")(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in POST/shlm: %s',
                exception)

    def process_request_hlm_post(
            self, env, start_response, hlm_req, account, container, obj, sender_user_name):
        """Handler for POST method of HLM requests.

        Args:
            env (dict): Environment.
            start_response (str): Previous response
            hlm_req (str): HLM request
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            # returns response (HTTP_NOT_FOUND, HTTP_INTERNAL_SERVER_ERROR) or
            # None (by default), if both exists
            container_object_check_response = self.check_container_object_exist(
                env, start_response, account, container, obj)
            # if response is not None, return it
            if container_object_check_response:
                return container_object_check_response

            # Get limit for recall requests and check it with current request
            if hlm_req == 'recall':
                self.logger.info(
                    'Recall request is being processed. Special conditions will apply.')
                current_limit = -1
                current_limit = self.get_configured_limit_recall_requests(
                    account, container, sender_user_name)

                # Evaluate if limit is not reached
                self.logger.debug(
                    'Limit for recall requests: %s',
                    str(current_limit))
                currently_pending_recalls = self.get_str_pending_recall_requests(
                    account, container, obj)
                self.logger.debug(
                    'Pending recalls: %s',
                    str(currently_pending_recalls))
                self.logger.debug(
                    'Pending recalls length: %s', str(
                        len(currently_pending_recalls)))
                if int(current_limit) > -1:
                    if int(len(currently_pending_recalls)
                           ) >= int(current_limit):
                        self.logger.warning(
                            'Limit for concurrent recall requests was reached: %s', current_limit)
                        rbody_json = {
                            'error': 'Limit for concurrent recall requests was reached. Request was aborted. Try again later.'}
                        rbody_json_str = json.dumps(rbody_json)
                        return Response(status=HTTP_PRECONDITION_FAILED,
                                        body=rbody_json_str,
                                        content_type="application/json")(env, start_response)
                    else:
                        self.logger.info(
                            'Limit for recall requests was not reached. Request allowed.')
                else:
                    self.logger.info('Limit for recall requests is -1.')

            # if not self.swift:
            #    self.create_internal_swift_client()
            self.logger.debug(':%s:%s:%s:%s:', account, container, obj,
                              hlm_req)

            if hlm_req in ["migrate", "premigrate"] and obj is not None:
                object_metadata = self.swift.get_object_metadata(
                    account, container, obj)
                object_size = object_metadata.get('content-length')
                try:
                    min_object_size = self.conf.get(
                        'minimum_object_size_for_migration')
                    self.logger.debug(
                        f"minimum_object_size was found in config and is set to {min_object_size}")
                except Exception:
                    self.logger.info(
                        "minimum_object_size_for_migration was not found in config")
                    self.logger.info(
                        "default value of 131072 B will be applied")
                    min_object_size = 131072  # default conf. value

                if (object_size is not None and int(
                        object_size) < int(min_object_size)):
                    self.logger.info(
                        f"Object size is too small for migration. Min. object size: {min_object_size}")
                    rbody_json = {
                        'error': f"Object size is too small for migration. Minimum object size: {min_object_size} B."
                    }
                    rbody_json_str = json.dumps(rbody_json)
                    return Response(status=HTTP_PRECONDITION_FAILED,
                                    body=rbody_json_str,
                                    content_type="application/json")(env, start_response)

            # Pass to Dispatcher also storage policy index spi, because
            # self.app is not available in Dispatcher
            spi = self.get_storage_policy_index(account, container)
            self.logger.debug('spi: %s', str(spi))
            self.queue_migration_or_recall_request(hlm_req, account, container,
                                                   spi, obj)
            self.logger.debug('Queued %s request.', hlm_req)
            # If migrating/pre-migrating, set cached target status to 'migrated'/'premigrated'
            # at dispatch time, only correct in case migration failed
            if hlm_req == 'migrate':
                self._put_migrated_state_to_cache(
                    account, container, obj, cache="ts")
            elif hlm_req == 'premigrate':
                self._put_premigrated_state_to_cache(
                    account, container, obj, cache="ts")
            return Response(status=HTTP_OK,
                            body=f'Accepted {hlm_req} request.\n',
                            content_type="text/plain")(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in POST/hlm: %s',
                exception)

    def process_request_migrate_policy_put(
            self, env, start_response, hlm_req, account, container, obj, policy_id):
        """PUT migration policy.

        Args:
            env (dict): Environment
            start_response (str): Previous response
            hlm_req (str): HLM request
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            if obj:
                objpath = f'/{account}/{container}/{obj}'
                self._put_state_to_cache(objpath, 'resident', cache="os")

            if hlm_req == 'migrate_policy':
                try:
                    self.logger.debug('Migrate policy PUT query')
                    if not policy_id:
                        self.logger.warning('Policy id was not specified.')
                        rbody_json = {'error': 'Policy id was not specified.'}
                        rbody_json_str = json.dumps(rbody_json)
                        return Response(status=HTTP_PRECONDITION_FAILED,
                                        body=rbody_json_str,
                                        content_type="application/json")(env, start_response)
                    try:
                        request_body_size = int(env.get('CONTENT_LENGTH', 0))
                    except (ValueError):
                        request_body_size = 0

                    request_body = env['wsgi.input'].read(request_body_size)
                    self.logger.debug(f"Request body : {request_body}")

                    if self.put_migration_policy(policy_id, request_body):
                        self.logger.debug(
                            f"Updated migration policy with id {policy_id}")
                        return Response(status=HTTP_OK,
                                        body=f'Updated migration policy with id {policy_id}\n',
                                        content_type="text/plain")(env, start_response)
                    else:
                        self.logger.debug(
                            f"Failed to update migration policy with id {policy_id}")
                        rbody = f"Policy with id {policy_id} does not exist."
                        rbody_json = {'error': rbody}
                        rbody_json_str = json.dumps(rbody_json)
                        return Response(status=HTTP_NOT_FOUND,
                                        body=rbody_json_str,
                                        content_type="application/json")(
                            env, start_response)
                except Exception as exception:
                    self.logger.error(
                        'An exception occured in PUT/migrate_policy: %s', exception)

            else:
                self.app(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in PUT/migrate_policy: %s',
                exception)

    def process_request_nonspec_get(
            self, env, start_response, hlm_req, account, container, obj):
        """Nonspecified GET request.

        Args:
            env (dict): Environment.
            start_response (str): Previous response.
            hlm_req (str): HLM request.
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            self.logger.debug(
                f"Running process_request_nonspec_get with {hlm_req} {account}/{container}/{obj}")

            # returns response (HTTP_NOT_FOUND, HTTP_INTERNAL_SERVER_ERROR) or
            # None (by default), if both exists
            container_object_check_response = self.check_container_object_exist(
                env, start_response, account, container, obj)
            # if response is not None, return it
            self.logger.debug(
                f"container_object_check_response: {container_object_check_response}")
            if container_object_check_response:
                return container_object_check_response

            if obj:
                # check status and either let GET proceed or return error code
                hlm_req = 'status'
                status = 'unknown'
                # check if status is cached in memcached
                self.logger.debug(
                    f"check if status is cached in memcached, {hlm_req}, {status}")
                if self.mcache:
                    (valid, cached) = self._get_state_from_cache(account,
                                                                 container,
                                                                 obj, cache="os")
                    if valid:
                        status = cached

                if status == 'unknown':
                    # Distribute request to storage nodes get responses
                    self.distribute_request_to_storage_nodes_get_responses(
                        hlm_req,
                        account,
                        container,
                        obj)

                    # Merge responses from storage nodes
                    # i.e. merge self.response_in into self.response_out
                    self.merge_responses_from_storage_nodes(hlm_req)

                    # Resident or premigrated state is condition to pass request,
                    # else return error code
                    self.logger.debug('self.response_out: %s',
                                      str(self.response_out))
                    obj = "/" + "/".join([account, container, obj])
                    status = self.response_out[obj]

                if status not in ['resident', 'premigrated']:
                    return Response(status=HTTP_PRECONDITION_FAILED,
                                    body="Object %s needs to be RECALL-ed before "
                                    "it can be accessed.\n" % obj,
                                    content_type="text/plain")(env, start_response)

                return self.app(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in GET/nonspec: %s',
                exception)

    def process_request_migrate_policies_get(
            self, env, start_response, hlm_req, account, container, obj):
        """Get migration policies.

        Args:
            env (dict): Environment
            start_response (str): Previous response.
            hlm_req (str): HLM request.
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            self.logger.debug('Migration policies GET query')
            self.get_migration_policies_list(
                account, container, policy_id=None)

            if len(self.response_out) == 0:
                txt_msg = "There are no migration policies."
                self.response_out.append(txt_msg)
            jout = json.dumps(self.response_out)
            return Response(status=HTTP_OK,
                            body=jout,
                            content_type="text/plain")(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in GET/migrate_policies: %s',
                exception)

    def process_request_migrate_policy_get(
            self, env, start_response, hlm_req, account, container, policy_id):
        """Get migration policy.

        Args:
            env (dict): Environment
            start_response (str): Previous response
            hlm_req (str): HLM request
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            self.logger.debug('Migration policy GET query')
            if not policy_id:
                self.logger.warning('Policy id was not specified.')
                rbody_json = {'error': 'Policy id was not specified.'}
                rbody_json_str = json.dumps(rbody_json)
                return Response(status=HTTP_PRECONDITION_FAILED,
                                body=rbody_json_str,
                                content_type="application/json")(env, start_response)

            self.get_migration_policies_list(account, container, policy_id)

            if len(self.response_out) == 0:
                txt_msg = f"There is no policy with id {policy_id}"
                self.response_out.append(txt_msg)
            jout = json.dumps(self.response_out)
            return Response(status=HTTP_OK,
                            body=jout,
                            content_type="text/plain")(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in GET/migrate_policy: %s',
                exception)

    def process_request_status_get(
            self, env, start_response, hlm_req, account, container, obj):
        """Get status of object/container.

        Args:
            env (dict): Environment.
            start_response (str): Previous response.
            hlm_req (str): HLM request.
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            cached = False
            if obj:
                (valid, cache_data) = self._get_state_from_cache(
                    account, container, obj, cache="os")
                if valid:
                    cached = True
                    self.response_out = {}
                    objpath = f'/{account}/{container}/{obj}'
                    self.response_out[objpath] = cache_data
            elif self._objects_cached(account, container, cache="os"):
                cached = True

            if not cached:
                self.logger.debug(
                    'Responses from storage nodes are not cached yet.')
                # Distribute request to storage nodes get responses
                self.distribute_request_to_storage_nodes_get_responses(
                    hlm_req, account, container, obj)

                # Merge responses from storage nodes
                # i.e. merge self.response_in into self.response_out
                self.merge_responses_from_storage_nodes(hlm_req)
            else:
                self.logger.debug(
                    'Responses from storage nodes are loaded from cache.')

            # Report result
            # jout = json.dumps(out) + str(len(json.dumps(out)))
            jout = json.dumps(self.response_out)  # testing w/ response_in
            return Response(status=HTTP_OK,
                            body=jout,
                            content_type="text/plain")(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in GET/status: %s',
                exception)

    def process_request_requests_get(
            self, env, start_response, hlm_req, account, container, obj):
        """Get requests of object/container.

        Args:
            env (dict): Environment
            start_response (str): Previous response
            hlm_req (str): HLM request
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response.
        """
        try:
            self.logger.debug('Requests query')
            self.get_pending_and_failed_requests(account, container, obj)
            if len(self.response_out) == 0:
                txt_msg = "There are no pending or failed SwiftHLM requests."
                self.response_out.append(txt_msg)
            jout = json.dumps(self.response_out)
            return Response(status=HTTP_OK,
                            body=jout,
                            content_type="text/plain")(env, start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in GET/requests: %s',
                exception)

    def check_container_object_exist(
            self, env, start_response, account, container, obj):
        """Checks if container or object exists.

        Args:
            env (dict): Environment.
            start_response (str): Previous response.
            account (str): Account ID.
            container (str): Name of container.
            obj (str): Name of object.

        Returns:
            Response: HTTP response with status.
        """
        try:
            # Process by this middleware. First check if the container and/or
            # object exist
            if not self.swift.container_exists(account, container):
                rbody = f"/account/container /{account}/{container} does not exist."
                rbody_json = {'error': rbody}
                rbody_json_str = json.dumps(rbody_json)
                return Response(status=HTTP_NOT_FOUND,
                                body=rbody_json_str,
                                content_type="application/json")(
                                    env, start_response)
            elif obj:
                obj_exists = False
                try:
                    path = self.swift.make_path(account, container, obj)
                    resp = self.swift.make_request('HEAD', path, {},
                                                   (2, HTTP_NOT_FOUND))
                    if resp.status_int != HTTP_NOT_FOUND:
                        obj_exists = True
                except Exception as e:
                    self.logger.error(
                        'List container objects error: %s', str(e))
                    rbody = 'Unable to check does object %s belong to /%s/%s.' % (
                        obj,
                        account,
                        container,
                    )

                    rbody_json = {'error': rbody}
                    rbody_json_str = json.dumps(rbody_json)
                    return Response(
                        status=HTTP_INTERNAL_SERVER_ERROR,
                        body=rbody_json_str,
                        content_type='application/json',
                    )(env, start_response)

                if not obj_exists:
                    rbody = f"Object /{account}/{container}/{obj} does not exist."
                    rbody_json = {'error': rbody}
                    rbody_json_str = json.dumps(rbody_json)
                    return Response(status=HTTP_NOT_FOUND,
                                    body=rbody_json_str,
                                    content_type="application/json")(
                                        env,
                                        start_response)
        except Exception as exception:
            self.logger.error(
                'An exception occured in check_container_object_exist: %s',
                exception,
            )

    def get_list_of_objects(self, account, container):
        """Gets the list of objects in container.

        Args:
            account (str): Account ID.
            container (str): Name of container.

        Returns:
            list: List of objects.
        """
        try:
            objects_iter = self.swift.iter_objects(account, container)
        except UnexpectedResponse as err:
            self.logger.error('List container objects error: %s', err)
            return False
        except Exception as e:  # noqa
            self.logger.error('List container objects error: %s', str(e))
            return False

        try:
            objects = []
            if objects_iter:
                # pick and return a request
                for obj in objects_iter:
                    objects.append(obj['name'])
            return objects
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def get_sorted_list_of_policies(self):
        """Gets sorted list of migration policies.

        Returns:
            list: Sorted list of migration policies.
        """
        try:
            objects_iter = self.swift.iter_objects(
                SWIFTHLM_ACCOUNT, SWIFTHLM_MIGRATION_POLICIES)
        except UnexpectedResponse as err:
            self.logger.error('List container objects error: %s', err)
            return False
        except Exception as e:  # noqa
            self.logger.error('List container objects error: %s', str(e))
            return False

        try:
            if objects_iter:
                objects = [json.loads(obj['name']) for obj in objects_iter]
                return sorted(objects, key=lambda k: int(
                    k['priority']), reverse=True)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _get_state_from_cache(self, account, container, object, cache="os"):
        """Gets state of object from cache.

        Args:
            account (str): Account id.
            container (str): Name of container.
            objname (str): Name of object.
            cache (str, optional): Type of cache. Defaults to "os".

        Returns:
            bool: Action result.
            str: Cached data.
        """
        try:
            memcache_key = f"{cache}:hlm/{account}/{container}/{object}"
            # sladel
            self.logger.debug('memcache_key: %s', memcache_key)
            # sladel
            cache_data = None
            cache_data = self.mcache.get(memcache_key)
            if cache_data is None:
                return False, None
            # sladel
            self.logger.debug('cache_data: %s', cache_data)
            # sladel
            # self.logger.debug('Got cache entry %s: %s', memcache_key,
            #                   cache_data)
            return True, cache_data

        except MemcacheConnectionError:
            self.logger.error('Memcache connection error')
            return False, None
        except Exception as exception:
            self.logger.error(
                'An exception occured in _get_state_from_cache: %s',
                exception)
            return False, None

    def set_object_state_based_on_request(
            self, hlm_req, account, container, object, objects=None):
        """Sets state of object based on a request.

        Args:
            hlm_req (str): HLM request.
            account (str): Account id.
            container (str): Name of container.
            object (str): Name of object.
            objects (list, optional): List of objects. Defaults to "None".

        Returns:
            bool: Action result.
        """
        try:
            # value to which the object state should be changed
            object_state = "migrated" if hlm_req == "migrate" else "premigrated"

            # object request
            if object:
                objpath = f"/{account}/{container}/{object}"
                if self._put_state_to_cache(objpath, object_state, cache="os"):
                    self.logger.debug(
                        f"Object state of {object} was updated to '{object_state}'.")
                    return True

            # container request (without filtered objects)
            # in that case, get list of all objects in container
            if not objects:
                objects = self.get_list_of_objects(account, container)

            if objects:
                for obj in objects:
                    objpath = f"/{account}/{container}/{obj}"
                    if self._put_state_to_cache(
                            objpath, object_state, cache="os"):
                        self.logger.debug(
                            f"Object state of {obj} was updated to '{object_state}'.")
                return True

            return False
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def delete_target_state_from_cache(
            self, account, container, object, objects=None):
        """Deletes objects from cache.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            object (str): Name of object.
            objects (list, optional): List of objects. Defaults to "None".

        Returns:
            bool: Action result.
        """
        try:
            if objects:
                for obj in objects:
                    self._remove_from_cache(
                        account, container, obj, cache="ts")
            else:
                self._remove_from_cache(account, container, object, cache="ts")
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _put_state_to_cache(self, objpath, state, cache="os"):
        """Puts a state to object in cache.

        Args:
            account (str]: Account ID.
            container (str): Name of container.
            objname (str): Name of object.
            cache (str, optional): Type of cache. Defaults to "os".

        Returns:
            bool: Action result.
        """
        try:
            if self.mcache:
                memcache_key = f"{cache}:hlm{objpath}"
                try:
                    self.mcache.set(memcache_key, state)
                    self.logger.debug('Cached: %s: %s', memcache_key, state)
                except MemcacheConnectionError:
                    self.logger.error('Memcache connection error')
                    return False
            else:
                self.logger.debug('Cache not found')
                return False
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _put_migrated_state_to_cache(
            self, account, container, objname, cache="os"):
        """Puts state migrated to object.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            objname (str): Name of object.
            cache (str, optional): Type of cache. Defaults to "os".

        Returns:
            bool: Action result.
        """
        try:
            if objname:
                objpath = f'/{account}/{container}/{objname}'
                return self._put_state_to_cache(objpath, 'migrated', cache)
            else:
                try:
                    objects_iter = self.swift.iter_objects(account, container)
                except UnexpectedResponse as err:
                    self.logger.error('List container objects error: %s', err)
                    return False
                except Exception as e:  # noqa
                    self.logger.error(
                        'List container objects error: %s', str(e))
                    return False
                if objects_iter:
                    for obj in objects_iter:
                        objpath = f"/{account}/{container}/{obj['name']}"
                        if not self._put_state_to_cache(
                                objpath, 'migrated', cache):
                            return False
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _put_premigrated_state_to_cache(
            self, account, container, objname, cache="os"):
        """Puts state premigrated to object in cache.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            objname (str): Name of object.
            cache (str, optional): Type of cache. Defaults to "os".

        Returns:
            bool: Action result.
        """

        if objname:
            objpath = f'/{account}/{container}/{objname}'
            return self._put_state_to_cache(objpath, 'premigrated', cache)
        else:
            try:
                objects_iter = self.swift.iter_objects(account, container)
            except UnexpectedResponse as err:
                self.logger.error('List container objects error: %s', err)
                return False
            except Exception as e:  # noqa
                self.logger.error('List container objects error: %s', str(e))
                return False
            if objects_iter:
                for obj in objects_iter:
                    objpath = f"/{account}/{container}/{obj['name']}"
                    if not self._put_state_to_cache(
                            objpath, 'premigrated', cache):
                        return False
        return True

    def _remove_obj_from_cache(self, objpath, cache="os"):
        """Removes object from cache.

        Args:
            objpath (str): Path to the object.
            cache (str, optional): Type of cache. Defaults to "os".

        Returns:
            bool: Action result.
        """

        try:
            if self.mcache:
                memcache_key = f"{cache}:hlm{objpath}"
                try:
                    self.mcache.delete(memcache_key)
                    self.logger.debug('Deleted %s from cache', memcache_key)
                except MemcacheConnectionError:
                    self.logger.error('Memcache connection error')
                    return False
            else:
                return False
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _remove_from_cache(self, account, container, objname, cache="os"):
        """Helper to remove object from cache.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            objname (str): Name of object.
            cache (str, optional): Type of cache. Defaults to "os".

        Returns:
            bool: Action result.
        """
        try:
            if objname:
                objpath = f'/{account}/{container}/{objname}'
                return self._remove_obj_from_cache(objpath, cache)
            else:
                try:
                    objects_iter = self.swift.iter_objects(account, container)
                except UnexpectedResponse as err:
                    self.logger.error('List container objects error: %s', err)
                    return False
                except Exception as e:  # noqa
                    self.logger.error(
                        'List container objects error: %s', str(e))
                    return False
                if objects_iter:
                    for obj in objects_iter:
                        objpath = f"/{account}/{container}/{obj['name']}"
                        if not self._remove_obj_from_cache(objpath, cache):
                            return False
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _objects_cached(self, account, container, cache="os"):
        """Returns cached objects of container to response output.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            cache (str, optional): Type of cache. Defaults to "os".

        Returns:
            bool: Action result.
        """
        try:
            self.response_out = {}
            try:
                objects_iter = self.swift.iter_objects(account, container)
            except UnexpectedResponse as err:
                self.logger.error('List container objects error: %s', err)
                return False
            except Exception as e:  # noqa
                self.logger.error('List container objects error: %s', str(e))
                return False
            if objects_iter:
                for obj in objects_iter:
                    (valid, cache_data) = self._get_state_from_cache(
                        account, container, (obj['name']), cache)
                    if not valid:
                        return False
                    objpath = f"/{account}/{container}/{obj['name']}"
                    self.response_out[objpath] = cache_data
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def filter_objects_based_on_size(self, request, hlm_req, account, container,
                                     spi, obj, objects=None):
        """Filters objects on defined path based on their size.

        Args:
            request (dict): Request.
            hlm_req (str): HLM operation.
            account (str): Account ID.
            container (str): Name of container.
            spi (str): Storage policy index.
            obj (str): Name of object.
            objects (list, optional): List of related objects. Defaults to None.

        Returns:
            dict: Request.
        """
        try:
            # check a "apply_minimum_object_size_in_container_request" value in
            # config
            try:
                apply_minimum_object_size_in_container_request = \
                    self.conf.get('filter:hlm').get(
                        'apply_minimum_object_size_in_container_request').lower()
                self.logger.info(
                    f"apply_minimum_object_size_in_container_request set to {apply_minimum_object_size_in_container_request}")
            except Exception:
                self.logger.info(
                    "apply_minimum_object_size_in_container_request not found in config")
                self.logger.info(
                    "apply_minimum_object_size_in_container_request was set to False by default")
                apply_minimum_object_size_in_container_request = False

            # if request is NOT migrate/premigrate
            # OR it is NOT container request (obj is not None)
            # OR apply_minimum_object_size_in_container_request in config is set to False
            # return unmodified request
            if (hlm_req not in ["migrate", "premigrate"]) \
                    or (obj is not None) \
                    or (not config_true_value(apply_minimum_object_size_in_container_request)):
                self.logger.info(
                    "Skipping filtering of objects based on size.")
                return request

            # if it is NOT merged requests (which already have "objects" list)
            # get a list of all objects in container
            if objects is None:
                objects = self.get_list_of_objects(account, container)

            # check a "min_object_size" value in config
            try:
                min_object_size = self.conf.get('filter:hlm').get(
                    'minimum_object_size_for_migration')
            except Exception:
                self.logger.info(
                    "minimum_object_size_for_migration was not found in config")
                self.logger.info("default valie of 131072 B will be applied")
                min_object_size = 131072  # default conf. value

            if objects:
                filtered_objects = []
                for object in objects:
                    object_metadata = self.swift.get_object_metadata(
                        account, container, object)
                    object_size = object_metadata.get('content-length')
                    if (object_size is not None and int(
                            object_size) < int(min_object_size)):
                        self.logger.info(
                            f"Object {object} is too small for migration and will be skipped.")
                        self.delete_target_state_from_cache(
                            account, container, object)
                        continue
                    filtered_objects.append(object)
                    objects = filtered_objects

                # Creating a modified request with updated list of objects and
                # deleting the old one
                self.delete_request_from_queue(
                    json.dumps(request), SWIFTHLM_PENDING_REQUESTS_CONTAINER)
                request['objects'] = objects

                self.logger.debug(
                    f"New request with updated list of objects: {request}")
                headers = {'X-Size': 0,
                           'X-Etag': 'swifthlm_task_etag',
                           'X-Timestamp': Timestamp(time.time()).internal,
                           'X-Content-Type': 'application/swifthlm-task'}
                self.direct_put_to_swifthlm_account(
                    SWIFTHLM_PENDING_REQUESTS_CONTAINER, json.dumps(request), headers)

                return request
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def create_per_storage_node_objects_list_and_request(self, hlm_req,
                                                         account, container,
                                                         obj, spi, related_objects=None):
        """Creates per node list of object(s) replicas.

        Args:
            hlm_req (str): HLM operation.
            account (str): Account ID.
            container (str): Name of container.
            spi (str): Storage policy index.
            obj (str): Name of object.
            related_objects (list, optional): List of related objects. Defaults to None.
        """
        # Create per node list of object(s) replicas
        # Syntax: per_node_list={'node1':[obj1,obj3], 'node2':[obj3,obj4]}
        # First get list of objects
        try:
            self.logger.info('Create per node list of object(s) replicas')
            objects = []
            if obj:
                self.logger.debug('Object request: %s', obj)
                objects.append(obj)
            else:
                self.logger.debug('Container request')
                # Get list of objects
                if related_objects:
                    objects = related_objects
                else:
                    objects = self.get_list_of_objects(account, container)
                    if objects:
                        self.logger.debug(
                            'objects(first 1024 bytes): %s',
                            str(objects)[
                                :1023])
            # Add each object to its nodes' lists
            per_node_list = defaultdict(list)
            # Set container storage policy (if not passed to and set by
            # Dispatcher)
            if not spi:
                spi = self.get_storage_policy_index(account, container)
            for obj in objects:
                obj_path = '/' + account + '/' + container + '/' + obj
                ips, devices, storage_policy_index, swift_dir \
                    = self.get_obj_storage_nodes(account, container, obj, spi)
                for i, ip_addr in enumerate(ips):
                    obj_path_and_dev = {
                        'object': obj_path, 'device': devices[i]}
                    # per_node_list[ip_addr].append(obj_path)
                    per_node_list[ip_addr].append(obj_path_and_dev)

            # Create json-formatted requests
            self.per_node_request = defaultdict(list)
            for ip_addr in per_node_list:
                request = {
                    'request': hlm_req,
                    'objects': per_node_list[ip_addr],
                    'storage_policy_index': storage_policy_index,
                    'swift_dir': swift_dir,
                }

                self.per_node_request[ip_addr] = request
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def submit_request_to_storage_node_and_get_response(self, ip_addr):
        """Submits the request to a storage node and wait for response.

        Args:
            ip_addr (str): IP address of storage node.
        """
        try:
            self.logger.info('Submitting request to storage node')
            self.stdin_lock.acquire()
            self.logger.debug('Dispatching request to %s', str(ip_addr))
            ssh_client = SSHClient()
            ssh_client.set_missing_host_key_policy(AutoAddPolicy())
            ssh_client.load_system_host_keys()
            ssh_client.connect(ip_addr, username=getpass.getuser())
            # Prepare remote Handler execution ssh pipe
            stdin, stdout, stderr = ssh_client.exec_command(
                'python3 -m ' + 'swifthlm.handler')
            ich = stdin.channel
            och = stdout.channel
            # Remote process stderr is not (yet) used here
            ech = stderr.channel
            # Send request data
            ich.sendall(json.dumps(self.per_node_request[ip_addr]))
            stdin.flush()
            ich.shutdown_write()
            stdin.close()
            response = ''
            self.stdin_lock.release()

            self.stout_lock.acquire()
            # Set initial max amount of data to receive, after first receive we
            # adapt to the receive buffer size
            max_receive_bytes = 100
            # Ensure that reads are blocking (non-busy wait)
            och.setblocking(1)
            while True:
                chunk = och.recv(max_receive_bytes)
                self.logger.debug("Bytes received: <%d>", len(chunk))
                if (len(chunk) == 0) and och.eof_received:
                    # Stop receiving as channel stream has closed.
                    # Inspired by Paramiko channel.py source comments.
                    # Other methods to determine command completion are
                    # unreliable.
                    break
                # Adjust max retrieve size to receive buffer length.
                # Note that it is assumed that data is delivered constantly once
                # available ie. max_receive_bytes is non-zero after first data
                # receiption.
                max_receive_bytes = len(och.in_buffer)
                response += chunk.decode()

            self.logger.debug(
                (
                    "Response (first+last 512 bytes): <%s ... %s>"
                    % (response[:512], response[-512:])
                )
            )

            self.stout_lock.release()
            # Closing the client will close the transport and channels
            ssh_client.close()
            self.response_in[ip_addr] = response
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def distribute_request_to_storage_nodes_get_responses(self, hlm_req,
                                                          account,
                                                          container,
                                                          obj, spi=None, related_objects=None):
        """Distributes request to storage nodes.

        Args:
            hlm_req (str): HLM operation.
            account (str): Account ID.
            container (str): Name of container.
            spi (str): Storage policy index.
            obj (str): Name of object.
            related_objects (list, optional): List of related objects. Defaults to None.
        """
        try:
            self.logger.info(
                'Distributing requests to storage nodes to collect responses')
            # Create per storage node list of object(s) replicas
            # Syntax: per_node_list={'node1':[obj1,obj3], 'node2':[obj3,obj4]}
            # ... and the request for submitting to Handler
            self.create_per_storage_node_objects_list_and_request(hlm_req,
                                                                  account,
                                                                  container,
                                                                  obj, spi, related_objects)

            self.logger.debug(
                'After'
                ' self.create_per_storage_node_objects_list_and_request()')

            # For each storage node/list dispatch request to the storage node
            # and get response
            self.response_in = defaultdict(list)
            threads = []
            for ip_addr in self.per_node_request:
                # logs inside loop outside of threads nok...
                th = threading.Thread(
                    target=self.submit_request_to_storage_node_and_get_response,
                    args=(ip_addr,))
                th.start()
                threads.append(th)
            for th in threads:
                th.join()
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def merge_responses_from_storage_nodes(self, hlm_req):
        """Merges response from storage nodes to response output.

        Args:
            hlm_req (str): HLM request.
        """
        try:
            self.logger.info('Merging responses from storage nodes')
            if hlm_req == 'status':
                # STATUS
                self.response_out = {}
                for ip_addr in self.response_in:
                    self.logger.debug(
                        'response_in[ip_addr](first 1024 bytes): %s',
                        str(self.response_in[ip_addr])[:1023],
                    )

                    resp_in = {}
                    try:
                        resp_in = (json.loads(
                            self.response_in[ip_addr]))['objects']
                    except ValueError as err:
                        self.logger.error('Could not decode JSON response: %s',
                                          err)
                    except KeyError as err:
                        msg = "Internal error. It was not possible to retrieve the list of files to provide status on."
                        self.response_out = msg
                        return
                    for dct in resp_in:
                        self.logger.debug('dct: %s', str(dct))
                        obj = dct['object']
                        if obj not in self.response_out:
                            self.response_out[obj] = dct['status']
                        elif self.response_out[obj] != dct['status']:
                            self.response_out[obj] = 'unknown'
                        # Cache status response
                        self._put_state_to_cache(obj, self.response_out[obj])
            else:
                # MIGRATE or RECALL
                self.response_out = "1"
                self.logger.debug(
                    f"Response in: {json.dumps(self.response_in)}")
                for ip_addr in self.response_in:
                    if self.response_in[ip_addr] == "0":
                        self.response_out = "0"
                if self.response_out == "0":
                    self.response_out = "SwiftHLM " + hlm_req + \
                        " request completed successfully."
                elif self.response_out == "1":
                    self.response_out = "SwiftHLM " + hlm_req + \
                        " request failed."
                else:
                    self.response_out = "Unable to invoke SwiftHLM " + \
                        hlm_req + " request."
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    # Create internal swift client self.swift
    def create_internal_swift_client(self):
        """Creates and prepares instance of SWIFT client for internal usage.

        Raises:
            SystemExit: In case of error.
        """
        try:
            self.logger.info('Creating internal SWIFT client')
            conf = self.conf
            request_tries = int(conf.get('request_tries') or 3)
            internal_client_conf_path = conf.get('internal_client_conf_path')
            object_config_file = r'/etc/swift/object-server.conf'
            object_config = readconf(object_config_file)
            self.object_conf = object_config
            proxy_config_file = r'/etc/swift/proxy-server.conf'
            proxy_config = readconf(proxy_config_file)
            proxy_app_config = proxy_config.get('app:proxy-server', None)
            allow_acc_mgmt = proxy_app_config.get(
                'allow_account_management', None)
            self.logger.debug('allow_account_management: %s\n',
                              allow_acc_mgmt)
            if allow_acc_mgmt == 'true':
                ic_conf_body_use = re.sub(r'allow_account_management = .+\n',
                                          r'allow_account_management = true\n',
                                          ic_conf_body)
            else:
                ic_conf_body_use = ic_conf_body
            if not internal_client_conf_path:
                # self.logger.warning(
                # ('Configuration option internal_client_conf_path not '
                # 'defined. Using default configuration, See '
                # 'internal-client.conf-sample for options'))
                internal_client_conf = ConfigString(ic_conf_body_use)
            else:
                internal_client_conf = internal_client_conf_path
            self.logger.debug('internal_client_conf: %s\n',
                              str(internal_client_conf))
            try:
                self.swift = InternalClient(
                    internal_client_conf, 'SwiftHLM Middleware', request_tries)
            except IOError as err:
                if err.errno != ENOENT:
                    raise
                raise SystemExit(
                    ('Unable to load internal client from config: %r (%s)') %
                    (internal_client_conf_path, err))
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def direct_put_to_swifthlm_account(self, container, obj, headers):
        """Put to SwiftHLM account.

        Args:
            container (str): Name of container.
            obj (str): Name of object.
            headers (dict): A dict of headers.

        Returns:
            bool: True if succeed, False otherwise.
        """
        def _check_success(*args, **kwargs):
            try:
                direct_put_container_object(*args, **kwargs)
                return 1
            except (ClientException, Timeout, socket.error):
                return 0

        try:
            pile = GreenPile()
            part, nodes = self.container_ring.get_nodes(
                SWIFTHLM_ACCOUNT, container)
            for node in nodes:
                pile.spawn(_check_success, node, part,
                           SWIFTHLM_ACCOUNT, container, obj, headers=headers,
                           conn_timeout=5, response_timeout=15)

            successes = sum(pile)
            if successes >= quorum_size(len(nodes)):
                return True
            else:
                return False
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def queue_migration_or_recall_request(self, hlm_req,
                                          account, container, spi, obj, priority=50):
        """Put request to queue in
        /SWIFTHLM_ACCOUNT/SWIFTHLM_PENDING_REQUESTS_CONTAINER.

        Args:
            hlm_req (str): HLM operation.
            account (str): Account ID.
            container (str): Name of container.
            spi (int): Storage policy index.
            obj (str): Name of object.
            priority (int, optional): Priority of request. Defaults to 50.

        Returns:
            bool: True if succeed, False otherwise.
        """
        try:
            # Debug info
            self.logger.info('Queue HLM %s request\n', hlm_req)
            self.logger.debug(
                '/acc/con/obj: %s/%s/%s',
                str(account),
                str(container),
                str(obj))
            # Queue request as empty object in special /account/container
            # /SWIFTHLM_ACCOUNT/SWIFTHLM_PENDING_REQUESTS_CONTAINER
            # Name object using next syntax
            # /yyyymmddhhmmss.msc/migrate|recall/account/container/spi
            # /yyyymmddhhmmss.msc/migrate|recall/account/container/spi/object
            curtime = datetime.datetime.now().strftime("%Y%m%d%H%M%S.%f")[:-3]
            req_name = "/".join(['', curtime, hlm_req, account, container,
                                 str(spi)])
            if obj:
                req_name += "/" + obj

            req = {"req_name": req_name, "req_priority": priority}
            # Queue SwiftHLM task by storing empty object to special container
            headers = {'X-Size': 0,
                       'X-Etag': 'swifthlm_task_etag',
                       'X-Timestamp': Timestamp(time.time()).internal,
                       'X-Content-Type': 'application/swifthlm-task'}
            try:
                self.direct_put_to_swifthlm_account(
                    SWIFTHLM_PENDING_REQUESTS_CONTAINER, json.dumps(req), headers)
            except Exception:
                self.logger.exception(
                    'Unhandled Exception trying to create queue')
                return False
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def list_migration_policy_request(
            self, account, container, request_body, policy_id=None):
        """Adds a migration policy into
        /SWIFTHLM_ACCOUNT/SWIFTHLM_MIGRATION_POLICIES.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            request_body (dict): Request body.
            policy_id (str, optional): Migration policy id. Defaults to None.

        Returns:
            (bool, any): True with policy id if succeed, False with reason otherwise.
        """
        try:
            # Debug info
            self.logger.info('List HLM migrate_policy request\n')
            # List request as empty object in special /account/container
            # /SWIFTHLM_ACCOUNT/SWIFTHLM_MIGRATION_POLICIES

            policy = self.parse_migration_policy_request_body(
                account, container, request_body, policy_id)
            if policy is None:
                return False, 'Could not parse policy request'
            headers = {'X-Size': 0,
                       'X-Etag': 'swifthlm_task_etag',
                       'X-Timestamp': Timestamp(time.time()).internal,
                       'X-Content-Type': 'application/swifthlm-task'}
            self.logger.debug(f"Policy : {json.dumps(policy)}")
            try:
                self.direct_put_to_swifthlm_account(
                    SWIFTHLM_MIGRATION_POLICIES, json.dumps(policy), headers)
            except Exception:
                self.logger.exception(
                    'Unhandled Exception trying to create list')
                return False, 'Unhandled Exception trying to create list'
            return True, policy['policy_id']
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def get_migration_policies_list(self, account, container, policy_id):
        """Fetches filtered list of migration policies.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            policy_id (int): ID of policy.

        Returns:
            list: List of migration policies.
        """
        try:
            self.logger.debug("Getting migration policies")
            self.logger.debug(
                "account %s container %s policy id %s",
                account,
                container,
                policy_id)
            migration_policies = self.get_sorted_list_of_policies()
            self.logger.debug(
                'migration policies: %s',
                str(migration_policies))
            self.response_out = []

            # Listing migration policy with specified id
            if policy_id:
                self.logger.debug(
                    "Listing migration policies by policy id %s", policy_id)
                for policy in migration_policies:
                    if policy['policy_id'] == policy_id:
                        self.response_out.append(policy)
                        break
                else:
                    self.logger.debug(
                        "Not listing migration policies by policy id")

            # Listing all migration policies within specified account
            if account is not None and container is None and policy_id is None:
                self.logger.debug(
                    "Listing migration policies by account %s", account)
                for policy in migration_policies:
                    if policy['path'].startswith(account):
                        self.response_out.append(policy)
            else:
                self.logger.debug("Not listing migration policies by account")

            # Listing all migration policies within specified account and
            # container
            if account is not None and container is not None and policy_id is None:
                self.logger.debug(
                    "Listing migration policies by account %s and container %s",
                    account,
                    container)
                path = f"{account}/{container}"
                self.logger.debug(path)
                for policy in migration_policies:
                    if policy['path'] == path:
                        self.response_out.append(policy)
            else:
                self.logger.debug(
                    "Not listing migration policies by account and container")

            # Listing all migration policies
            if account is None and container is None and policy_id is None:
                self.logger.debug("Listing all migration policies")
                for policy in migration_policies:
                    self.response_out.append(policy)

            # Returns list of policies for external processes
            return self.response_out
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def parse_migration_policy_request_body(
            self, account, container, request_body, policy_id):
        """Parses and sanitizes migration policy's request body.

        Args:
            account (str): Account ID.
            container (str): Container name.
            request_body (dict): Request body.
            policy_id (int): Migration policy ID.

        Returns:
            dict: Parsed migration policy.
        """
        try:
            self.logger.debug("Parsing request body %s", request_body)
            req_body = request_body.decode()
            try:
                req_body = json.loads(req_body)
            except Exception as exception:
                self.logger.debug("Empty request body")
                req_body = {}

            pol_prio = req_body.get("priority", 50)
            pol_disk_pool_util = req_body.get("disk_pool_utilization")
            pol_storage_policy = req_body.get("storage_policy")
            pol_filter = req_body.get("filter", "")
            pol_req = req_body.get("operation", "migrate")
            pol_scheduled_time = req_body.get("scheduled_time")

            if pol_prio not in range(101):
                self.logger.error("Priority can be integer between 0 and 100")
                return None
            if pol_req not in ['migrate', 'premigrate']:
                self.logger.error("Operation can be migrate or premigrate.")
                return None
            pol_path = f"{account}/{container}" if container else f"{account}"

            if not policy_id:
                policy_id = str(uuid.uuid4())
            policy = {
                'policy_id': policy_id,
                'priority': pol_prio,
                'operation': pol_req,
                'scheduled_time': pol_scheduled_time,
                'path': pol_path,
                'storage_policy': pol_storage_policy,
                'disk_pool_utilization': pol_disk_pool_util,
                'filter': pol_filter,
            }
            self.logger.debug("Policy: %s", json.dumps(policy))
            return policy
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def delete_migration_policy(self, policy_id):
        """Deletes a migration policy.

        Args:
            policy_id (int): ID of migration policy.

        Returns:
            bool: True if succeed, False otherwise.
        """
        try:
            self.logger.info('Delete migration policy with id %s', policy_id)
            migration_policies = self.get_list_of_objects(
                SWIFTHLM_ACCOUNT, SWIFTHLM_MIGRATION_POLICIES)
            for policy in migration_policies:
                if policy_id == json.loads(policy)['policy_id']:
                    if self.delete_request_from_queue(
                            policy, SWIFTHLM_MIGRATION_POLICIES):
                        return True
            return False
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def put_migration_policy(self, policy_id, request_body):
        """Handles PUT operation on migration policy.

        Args:
            policy_id (int): ID of migration policy.
            request_body (dict): Body of migration policy.

        Returns:
            bool: True if succeed, False otherwise.
        """
        try:
            old_policy = None
            migration_policies = self.get_list_of_objects(
                SWIFTHLM_ACCOUNT, SWIFTHLM_MIGRATION_POLICIES)
            for policy in migration_policies:
                if policy_id == json.loads(policy)['policy_id']:
                    old_policy = json.loads(policy)
                    break

            if old_policy:
                path = old_policy['path'].split("/")
                account = path[0]
                container = path[1]
                if self.delete_migration_policy(policy_id):
                    self.list_migration_policy_request(
                        account, container, request_body, policy_id)
                    return True
            return False
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def convert_migration_policy(self, policy):
        """Handles converting of values in migration policy.

        Args:
            policy (dict): Migration policy.

        Returns:
            (dict): Converted migration policy.
        """
        try:
            self.logger.info('Converting migration policy: %s', policy)
            policy['disk_pool_utilization'] = self._convert_disk_pool_util(
                policy)
            policy['storage_policy'] = self._convert_storage_policy(policy)
            policy['scheduled_time'] = self._convert_scheduled_time(policy)
            policy['filter'] = self._convert_policy_filter(policy)
            self.logger.debug('Converted migration policy: %s', policy)
            return policy
        except Exception as exception:
            self.logger.error(
                'An exception occured in convert_migration_policy: %s',
                exception)

    def _convert_scheduled_time(self, policy):
        """Converts scheduled time into unix timestamp.

        Args:
            policy (dict): Migration policy.

        Raises:
            ConvertError: Raised if any error occured during conversion.

        Returns:
            int: Scheduled time as unix timestamp.
        """
        try:
            scheduled_time = policy.get('scheduled_time')
            if scheduled_time is None:
                return

            TIME_PATTERN = """(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])
                            -(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])
                            |(?:[2468][048]|[13579][26])00)-02-29)\\s(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:
                            |[+-][01]\\d:[0-5]\\d)*"""
            matches = re.findall(TIME_PATTERN, scheduled_time)
            if not matches:
                raise ConvertError("Error when converting scheduled_time")

            scheduled_time = datetime.datetime.strptime(
                scheduled_time, "%Y-%m-%d %H:%M:%S")
            return scheduled_time.timestamp()
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _convert_disk_pool_util(self, policy):
        """Converts disk pool utilization to readable form.

        Args:
            policy (dict): Migration policy

        Raises:
            ConvertError: Raised if any error occured during conversion.

        Returns:
            int: Disk pool utilization.
        """
        try:
            if policy.get('disk_pool_utilization') is None:
                return

            disk_pool_util = policy['disk_pool_utilization']
            if isinstance(disk_pool_util, str) and "%" in disk_pool_util:
                disk_pool_util = re.sub('%$', '', disk_pool_util)
                return float(disk_pool_util)
            elif isinstance(disk_pool_util, float):
                disk_pool_util = float(disk_pool_util) * 100
                return disk_pool_util
            else:
                raise ConvertError(
                    "Error when converting disk_pool_utilization")
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)
            return None

    def _convert_storage_policy(self, policy):
        """Converts storage policy of migration policy into integer.

        Args:
            policy (dict): Migration policy.

        Raises:
            ConvertError: Raised if storage policy does not exist.

        Returns:
            int: Storage policy index.
        """
        try:
            if policy.get('storage_policy') is None:
                return

            storage_policy = str(policy['storage_policy'])
            # treating as storage policy name, converting to index
            if not storage_policy.isdigit():
                for pol in POLICIES:
                    if pol.name == storage_policy:
                        policy['storage_policy'] = pol.idx
                        break
                else:
                    raise ConvertError(
                        f"Storage policy with name {storage_policy} does not exists")
            # treating as storage policy index, verify that exists
            else:
                pol = POLICIES.get_by_index(int(storage_policy))
                if pol is None:
                    raise ConvertError(
                        f"Storage policy with index {storage_policy} does not exists")

            return int(policy['storage_policy'])
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)
            return None

    def _convert_policy_filter(self, policy):
        """Converts units in policy filter to appropriate form.

        Args:
            policy (dict): Migration policy.

        Returns:
            dict: Converted migration policy filter.
        """
        try:
            policy_filter = policy['filter']
            # Converting object_size into bytes
            SIZE_UNITS = {"B": 1, "KB": 2 ** 10, "MB": 2 ** 20, "GB": 2 ** 30}
            SIZE_PATTERN = r'([0-9]+)\s*(GB|MB|KB|B)'

            matches = re.findall(SIZE_PATTERN, policy_filter)
            for match in matches:
                value = int(float(match[0]) * SIZE_UNITS[match[1]])
                policy_filter = re.sub(
                    SIZE_PATTERN, str(value), policy_filter, count=1)

            # Converting modification_time into timestamp
            TIME_PATTERN = """(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])
                            -(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])
                            |(?:[2468][048]|[13579][26])00)-02-29)\\s(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:
                            |[+-][01]\\d:[0-5]\\d)*"""

            matches = re.findall(TIME_PATTERN, policy_filter)
            for match in matches:
                date = datetime.datetime.strptime(match, "%Y-%m-%d %H:%M:%S")
                policy_filter = policy_filter.replace(
                    match, str(date.timestamp()))

            # Converting object_age into seconds
            AGE_UNITS = {"seconds": 1, "second": 1, "minutes": 60, "minute": 60,
                         "hours": 3600, "hour": 3600, "days": 86400, "day": 86400,
                         "months": 2629743, "month": 2629743, "years": 31556926, "year": 31556926}

            AGE_PATTERN = r'([0-9]+)\s*(?i)(years|year|months|month|days|day|hours|hour|minutes|minute|seconds|seconds)'

            matches = re.findall(AGE_PATTERN, policy_filter)
            for match in matches:
                value = int(float(match[0]) * AGE_UNITS[match[1].lower()])
                policy_filter = re.sub(
                    AGE_PATTERN, str(value), policy_filter, count=1)

            return policy_filter
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)
            return None

    def pull_a_mig_pol_from_list(self, index):
        """Fetches a migration policy from list of migration policies.

        Args:
            index (int): Index of the migration policy.

        Returns:
            dict: Migration policy.
        """
        try:
            self.logger.info('Pulling a migration policy from list')
            # Pull a policy from SWIFTHLM_MIGRATION_POLICIES
            # First list the objects (requests) from the queue
            try:
                policies = self.get_sorted_list_of_policies()
            except Exception as exception:  # noqa
                self.logger.error('An exception occured: %s', exception)
                return None
            policy = None
            if policies:
                try:
                    policy = policies[index]
                    self.logger.debug('Pulled a policy from list: %s', policy)
                except IndexError:
                    # policy is None
                    self.logger.info('No policy found with index %s', index)
            return policy
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def filter_objects_to_migrate(self, policy):
        """Filters objects according to migration policy's filter, creates
        request with filtered objects and adds appropriate state into cache.

        In case of scheduled policy, policy is deleted after objects are filtered.

        Args:
            policy (dict): Migration policy.
        """
        try:
            self.logger.info(f"Filtering objects to {policy['operation']}")
            path = policy['path'].split('/')
            account = path[0]
            container = path[1] if len(path) == 2 else None
            priority = policy['priority']
            containers_list = []
            objects_to_migrate = []

            if policy['scheduled_time'] and not self._scheduled_time_reached(
                    policy):
                return []

            if container:
                # /account/container migration policy
                if self.swift.container_exists(account, container):
                    containers_list.append(container)
                else:
                    self.logger.debug(
                        f"Container {container} does not exists.")
            else:
                # /account/ migration policy
                containers_list = list(
                    map(lambda item: item['name'], self.swift.iter_containers(account)))

            self.logger.debug(
                f"Following containers match migration policy {policy['policy_id']}: {containers_list}")
            for container_item in containers_list:
                self.logger.debug(
                    f"Fetching storage policy index for container {container_item}")
                container_metadata = self.swift.get_container_metadata(
                    account, container_item)
                container_spi = int(
                    container_metadata['x-backend-storage-policy-index'])
                self.logger.debug(
                    f"Storage policy index for container {container_item} is {container_spi}")
                if policy['storage_policy'] is not None:
                    self.logger.debug(
                        "Storage policy index is set. Checking container's storage policy index.")
                    if container_spi != policy['storage_policy']:
                        self.logger.debug(
                            "Storage policy of migration policy does not match container policy.")
                        continue

                objects_iter = self.swift.iter_objects(account, container_item)
                for obj in objects_iter:
                    if self.mcache:
                        (valid, cached) = self._get_state_from_cache(
                            account, container_item, obj['name'], cache="ts")
                        if valid:
                            if (policy['operation'] ==
                                    "migrate" and cached == "migrated"):
                                self.logger.debug(
                                    f"Object {obj['name']} was already chosen for migration.")
                                continue
                            elif (policy['operation'] == "premigrate" and cached == "premigrated"):
                                self.logger.debug(
                                    f"Object {obj['name']} was already chosen for premigration.")
                                continue

                    if policy['filter'] and not self._filter_evaluation(
                        policy, obj
                    ):
                        self.logger.debug(
                            f"Object {account}/{container_item}/{obj['name']} failed to match filter of policy {policy['policy_id']}")
                        continue

                    if policy['disk_pool_utilization']:
                        if int(policy['disk_pool_utilization']) >= int(self.get_device_current_max_utilization(
                                account, container_item, obj['name'], container_spi)):
                            self.logger.debug(
                                f"Object {account}/{container_item}/{obj['name']} failed to match disk pool utilization condition of policy {policy['policy_id']} ({int(policy['disk_pool_utilization'])}%)")
                            continue
                        else:
                            self.logger.debug(
                                f"Object {account}/{container_item}/{obj['name']} matched disk pool utilization condition of policy {policy['policy_id']}")
                    objects_to_migrate.append(obj['name'])
                    self.logger.debug(
                        f"Object {obj['name']} meets the criteria of the migration policy. Adding it to list of objects to migrate.")

            self.logger.debug(
                f"List of objects to {policy['operation']}: {objects_to_migrate}")
            if objects_to_migrate:
                curtime = datetime.datetime.now().strftime(
                    "%Y%m%d%H%M%S.%f")[:-3]
                req_name = "/".join(['', curtime, policy['operation'], account, container,
                                    str(container_spi)])
                request = {
                    'req_name': req_name,
                    'objects': objects_to_migrate,
                    'req_priority': priority,
                    'policy_request': True,
                }

                self.logger.debug(f"Request: {request}")
                headers = {'X-Size': 0,
                           'X-Etag': 'swifthlm_task_etag',
                           'X-Timestamp': Timestamp(time.time()).internal,
                           'X-Content-Type': 'application/swifthlm-task'}
                self.direct_put_to_swifthlm_account(
                    SWIFTHLM_PENDING_REQUESTS_CONTAINER, json.dumps(request), headers)

                for obj in objects_to_migrate:
                    if policy['operation'] == 'migrate':
                        self._put_migrated_state_to_cache(
                            account, container, obj, cache="ts")
                    elif policy['operation'] == 'premigrate':
                        self._put_premigrated_state_to_cache(
                            account, container, obj, cache="ts")

            # Delete SCHEDULED migration policy after it was processed
            if policy['scheduled_time'] and self.delete_migration_policy(
                    policy['policy_id']):
                self.logger.info(
                    f"Migration policy with id {policy['policy_id']} was deleted.")
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _filter_evaluation(self, policy, obj):
        """Evaluates if object matches policy's filter criteria.

        Args:
            policy (dict): Migration policy
            obj (dict): Object metadata.

        Returns:
            bool: Evaluation result.
        """
        try:
            obj_size = obj['bytes']
            obj_modification_time = last_modified_date_to_timestamp(
                obj['last_modified'])
            obj_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(obj_modification_time)

            self.logger.debug("obj: %s, obj_size: %s bytes, obj_modification_time: %s, obj_age: %s seconds" %
                              (obj['name'], obj_size, float(obj_modification_time), obj_age.total_seconds()))

            return simple_eval(
                policy['filter'],
                names={
                    "object_size": int(obj_size),
                    "modification_time": float(obj_modification_time),
                    "object_age": obj_age.total_seconds(),
                },
            )
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def _scheduled_time_reached(self, policy):
        """Checks if scheduled time of scheduled migration policy was reached.

        Args:
            policy (dict): Migration policy.

        Returns:
            bool: True if time was reached, False otherwise.
        """
        try:
            self.logger.info(
                "Scheduled migration policy. Checking scheduled time.")
            current_timestamp = datetime.datetime.now().timestamp()
            policy_timestamp = policy['scheduled_time']
            if current_timestamp >= policy_timestamp:
                self.logger.info(
                    "Scheduled time has been reached. Continue to process migration policy.")
                return True
            else:
                self.logger.info("Scheduled time has not been reached yet.")
                self.logger.info(
                    f"Skipping migration policy {policy['policy_id']}")
            return False
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def get_device_current_max_utilization(
            self, account, container, object_name, spi):
        """Calculates current maximum disk space utilization of node.

        Args:
            account (str): Account ID.
            container (str): Name of container.
            object_name (str): Name of object.
            spi (int): Storage policy index.

        Returns:
            int: Maximum disk space utilization.
        """
        try:
            obj_storage_devices = self.get_obj_storage_nodes(
                account, container, object_name, spi)
            obj_devices = obj_storage_devices[1]
            self.logger.debug(
                f"Object {account}/{container}/{object_name} found on devices {obj_devices}.")

            free_percent_list = []
            for obj_device in obj_devices:
                st = os.statvfs(
                    f"{self.object_conf['pipeline:main']['devices']}/{obj_device}")
                free_bytes = st.f_frsize * st.f_bavail
                size_bytes = st.f_frsize * st.f_blocks
                free_percent = float(free_bytes) / float(size_bytes) * 100
                free_percent_list.append(free_percent)
                self.logger.debug(
                    f"Free disk space {free_percent} on device {obj_device}")

            max_utilization = 100 - min(free_percent_list)
            self.logger.debug(f"The maximal utilization is {max_utilization}")
            return max_utilization
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def pull_a_mig_or_rec_request_from_queue(self):
        """Pulls a migrate/recall request from
        SWIFTHLM_PENDING_REQUESTS_CONTAINER with highest priority.

        Returns:
            str: String representing request.
        """
        try:
            self.logger.info('Pulling a migrate/recall request from queue')
            # Pull a request from SWIFTHLM_PENDING_REQUESTS_CONTAINER
            try:
                objects_iter = self.swift.iter_objects(
                    account=SWIFTHLM_ACCOUNT,
                    container=SWIFTHLM_PENDING_REQUESTS_CONTAINER)
                # headers=headers_out)
            except UnexpectedResponse as err:
                self.logger.error('Pull request error: %s', err)
                return False
            except Exception as e:  # noqa
                self.logger.error('Pull request error: %s', str(e))
                return False
            request = None
            if objects_iter:
                # Pick a request with highest priority
                objects = [json.loads(obj['name']) for obj in objects_iter]
                if objects:
                    request = max(
                        objects, key=lambda k: int(
                            k['req_priority']))
                    self.logger.debug(
                        'Pulled a request from queue with the highest priority: %s', request)
                    # If there are objects from migration policy already, we do
                    # not want to find related requests
                    if request.get('objects') is None:
                        request = self.find_and_merge_related_requests(request)
                else:
                    self.logger.info('No objects and requests found')
            return request
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def find_and_merge_related_requests(self, request):
        """Finds related requests in queue and creates merged request in case
        original request is container request.

        Args:
            request (str): Original request.

        Returns:
            dict: Merged request or original request.
        """
        try:
            related_objects = []
            self.logger.debug(f"Current request: {request}")
            _, current_req, current_acc, current_con, current_spi, current_obj = self.decode_request(
                request)
            if current_obj is None:
                return request
            current_priority = request['req_priority']

            other_pending_requests = self.get_list_of_objects(SWIFTHLM_ACCOUNT,
                                                              SWIFTHLM_PENDING_REQUESTS_CONTAINER)
            for other_request in other_pending_requests:
                other_request = json.loads(other_request)
                if other_request == request:
                    continue
                self.logger.debug(f"Other request: {other_request}")
                _, other_hlm_req, other_acc, other_con, other_spi, other_obj = self.decode_request(
                    other_request)
                other_priority = other_request['req_priority']
                if other_obj is None:
                    continue
                if current_acc == other_acc and current_con == other_con and \
                        current_priority == other_priority and current_req == other_hlm_req and \
                        current_spi == other_spi and current_obj != other_obj:
                    self.logger.debug(
                        f"Found related request in queue: {json.dumps(other_request)}")
                    related_objects.append(other_obj)
                    self.delete_request_from_queue(
                        json.dumps(other_request), SWIFTHLM_PENDING_REQUESTS_CONTAINER)

            self.logger.debug(f"Related objects: {related_objects}")
            if not related_objects:
                return request

            self.delete_request_from_queue(
                json.dumps(request), SWIFTHLM_PENDING_REQUESTS_CONTAINER)
            related_objects.append(current_obj)

            # Creating a new request
            curtime = datetime.datetime.now().strftime("%Y%m%d%H%M%S.%f")[:-3]
            new_req_name = "/".join(['', curtime, current_req, current_acc, current_con,
                                     str(current_spi)])
            request['req_name'] = new_req_name
            request['objects'] = related_objects

            self.logger.debug(f"New request: {request}")
            headers = {'X-Size': 0,
                       'X-Etag': 'swifthlm_task_etag',
                       'X-Timestamp': Timestamp(time.time()).internal,
                       'X-Content-Type': 'application/swifthlm-task'}
            self.direct_put_to_swifthlm_account(
                SWIFTHLM_PENDING_REQUESTS_CONTAINER, json.dumps(request), headers)

            return request
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def queue_failed_migration_or_recall_request(self, request):
        """Adds request into queue of failed requests in case migrate/recall
        request failed.

        Args:
            request (dict): Request.

        Returns:
            bool: True if succeed, False otherwise.
        """
        try:
            self.logger.info('Queue failed request: %s', request)
            # Create container/queue for failed requests, if not existing
            if not self.swift.container_exists(
                    account=SWIFTHLM_ACCOUNT,
                    container=SWIFTHLM_FAILED_REQUESTS_CONTAINER):
                try:
                    self.swift.create_container(
                        account=SWIFTHLM_ACCOUNT,
                        container=SWIFTHLM_FAILED_REQUESTS_CONTAINER)
                except Exception as e:  # noqa
                    self.logger.error('Queue request error: %s', str(e))
                    return False
            # queue failed request
            body = ''
            try:
                self.swift.upload_object(
                    FileLikeIter(body),
                    account=SWIFTHLM_ACCOUNT,
                    container=SWIFTHLM_FAILED_REQUESTS_CONTAINER,
                    obj=request)
            except UnexpectedResponse as err:
                self.logger.error('Queue failed request error: %s', err)
                return False
            except Exception as e:  # noqa
                self.logger.error('Queue failed request error: %s', str(e))
                return False
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def success_remove_related_requests_from_failed_queue(self, request):
        """Removes request from queue of failed requests in case migrate/recall
        request succeed.

        Args:
            request (str): Request.
        """
        try:
            self.logger.info(
                'Cleaning up failed requests from queue related to successful request %s',
                request)
            ts, hlm_req, acc, con, spi, obj = \
                self.decode_request(request)
            failed_requests = \
                self.get_list_of_objects(SWIFTHLM_ACCOUNT,
                                         SWIFTHLM_FAILED_REQUESTS_CONTAINER)
            for freq in failed_requests:
                fts, fhlm_req, facc, fcon, fspi, fobj = \
                    self.decode_request(json.loads(freq))
                if fobj == obj and fcon == con and facc == acc:
                    if not self.delete_request_from_queue(
                            freq, SWIFTHLM_FAILED_REQUESTS_CONTAINER):
                        self.logger.warning('Stale failed request %s', freq)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def delete_request_from_queue(self, request, queue):
        """Helper to remove request from the queue.

        Args:
            request (str): String representing request
            queue (str): Name of special container (queue).

        Returns:
            bool: True if succeed, False otherwise.
        """
        try:
            # Debug info
            self.logger.info('Delete %s from %s', request, queue)
            # delete request
            try:
                self.swift.delete_object(
                    account=SWIFTHLM_ACCOUNT,
                    container=queue,
                    obj=request)
            except UnexpectedResponse as err:
                self.logger.error('Delete request error: %s', err)
                return False
            except Exception as e:  # noqa
                self.logger.error('Delete request error: %s', str(e))
                return False
            return True
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def decode_request(self, request):
        """Decodes information, e.g. timestamp, HLM operation, account,
        container, storage policy index, object name, from specific request.

        Args:
            request (str): Request string.

        Returns:
            list: List with information.
        """
        try:
            self.logger.info('Decoding request: %s', request)
            req_parts = request['req_name'].split('/')[1:]
            timestamp = req_parts[0]
            hlm_req = req_parts[1]
            account = req_parts[2]
            container = req_parts[3]
            spi = req_parts[4]
            obj = '/'.join(req_parts[5:]) if len(req_parts) > 5 else None
            return timestamp, hlm_req, account, container, spi, obj
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def get_pending_and_failed_requests(self, acc, con, obj):
        """Populates response output with list of failed requests.

        Args:
            acc (str): Account ID.
            con (str): Container name.
            obj (str): Object name.
        """
        try:
            self.logger.info('Get pending hlm requests')
            pending_requests = \
                self.get_list_of_objects(SWIFTHLM_ACCOUNT,
                                         SWIFTHLM_PENDING_REQUESTS_CONTAINER)
            self.logger.debug('pending: %s', str(pending_requests))
            self.logger.info('Get failed hlm requests')
            failed_requests = \
                self.get_list_of_objects(SWIFTHLM_ACCOUNT,
                                         SWIFTHLM_FAILED_REQUESTS_CONTAINER)
            self.logger.debug('failed: %s', str(failed_requests))
            self.response_out = []
            for preq in pending_requests:
                self.logger.debug('pending: %s', str(preq))
                ts, hlm_req, a, c, sp, o = \
                    self.decode_request(json.loads(preq))
                if not obj and a == acc and c == con or \
                    obj and a == acc and c == con and o == obj or \
                        obj and not o and a == acc and c == con:
                    self.response_out.append(
                        json.loads(preq)['req_name'] + '--pending')
            for freq in failed_requests:
                ts, hlm_req, a, c, sp, o = \
                    self.decode_request(json.loads(freq))
                if not obj and a == acc and c == con or \
                    obj and a == acc and c == con and o == obj or \
                        obj and not o and a == acc and c == con:
                    self.response_out.append(
                        json.loads(freq)['req_name'] + '--failed')
            self.logger.debug('reqs: %s', str(self.response_out))
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def get_str_pending_recall_requests(self, acc, con, obj):
        """Returns stringified list of pending recall requests.

        Args:
            acc (str): Account ID.
            con (str): Container name.
            obj (str): Object name.

        Returns:
            str: List of pending recall requests.
        """
        try:
            self.logger.info('Get pending hlm requests')
            pending_requests = \
                self.get_list_of_objects(SWIFTHLM_ACCOUNT,
                                         SWIFTHLM_PENDING_REQUESTS_CONTAINER)
            self.logger.debug('pending: %s', str(pending_requests))
            str_list_out = []
            for preq in pending_requests:
                self.logger.debug('pending: %s', str(preq))
                ts, hlm_req, a, c, sp, o = \
                    self.decode_request(json.loads(preq))
                if hlm_req != 'recall':
                    continue
                if (not obj and a == acc and c == con) or \
                    (obj and not o and a == acc and c == con) or \
                    (obj and a == acc and c == con and o == obj) or \
                        (obj and a == acc and c == con and o != obj):
                    str_list_out.append(f"/{hlm_req}/{a}/{c}/{sp}/{o}")
            self.logger.debug('reqs: %s', str(str_list_out))
            return str_list_out
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)
            return []

    def assign_new_recall_limit(self, current_limit, new_limit, rule_key):
        """Comparator helping to fetch current limit for concurrent recall
        requests.

        Args:
            current_limit (int): Current value of limit from previous iterations.
            new_limit (int): New value of limit.
            rule_key (str): Key of proxy-config's rule setting new limit.

        Returns:
            int: Current limit.
        """
        try:
            self.logger.debug(
                'assign_new_recall_limit: %s, %s, %s',
                current_limit,
                new_limit,
                rule_key)
            if current_limit == -1:
                self.logger.debug(
                    'Current limit is default value, -1. Will be changed.')
                current_limit = int(new_limit)
                self.logger.info(
                    f'Applied limit for submitted recall requests from rule "{rule_key}"')
            else:
                self.logger.debug(
                    'Current limit is different than default value -1. Currently %d.' %
                    current_limit)
                if int(new_limit) < current_limit:
                    current_limit = int(new_limit)
                    self.logger.info(
                        'Applied limit for submitted recall requests from rule "%s"', rule_key)
                else:
                    self.logger.info(
                        'Applied previous value limit for submitted recall requests: "%d"' %
                        current_limit)
            return current_limit
        except Exception as exception:
            self.logger.error(
                'An exception occured in assign_new_recall_limit: %s',
                exception)
            return current_limit

    def get_configured_limit_recall_requests(
            self, account, container, sender_user_name):
        """Calculates limit for recall requests according to configuration and
        context variables.

        Args:
            account (str): ID of account.
            container (str): Container name.
            sender_user_name (str): User name of request sender.

        Returns:
            int: Limit for concurrent recall requests.
        """
        try:
            current_limit = -1
            conf_keys = list(self.conf.keys())
            key_re = re.compile('limit_recall_requests_[^.]+_[0-9]+')
            filtered_conf_keys = [s for s in conf_keys if key_re.match(s)]

            for rule_key in filtered_conf_keys:
                self.logger.debug(f"rule_key: {rule_key}")
                rule_groups_list = self.conf[rule_key].split(' ')
                for rule_group in rule_groups_list:
                    group_rules_list = rule_group.split(' ')
                    for group_rule in group_rules_list:
                        group_rule_params = group_rule.split(':')
                        group_rule_param_accounts_list = group_rule_params[0].split(
                            ',')
                        group_rule_param_containers_list = group_rule_params[1].split(
                            ',')
                        group_rule_param_users_list = group_rule_params[2].split(
                            ',')
                        group_rule_param_limit = int(group_rule_params[3])

                        if len(group_rule_param_users_list[0]) == 0:
                            # account:container::limit
                            self.logger.debug(
                                'Recognized pattern "account:container::limit"')
                            if container in group_rule_param_containers_list and account in group_rule_param_accounts_list:
                                current_limit = self.assign_new_recall_limit(
                                    current_limit, group_rule_param_limit, rule_key)
                                self.logger.debug(
                                    f"Set limit from rule {rule_key}")
                        else:
                            if len(group_rule_param_containers_list[0]) == 0:
                                # ::user:limit
                                # account::user:limit
                                self.logger.debug(
                                    'Recognized pattern "::user:limit"')
                                if sender_user_name in group_rule_param_users_list:
                                    current_limit = self.assign_new_recall_limit(
                                        current_limit, group_rule_param_limit, rule_key)
                                    self.logger.debug(
                                        f"Set limit from rule {rule_key}")
                            else:
                                # account:container:user:limit
                                self.logger.debug(
                                    'Recognized pattern "account:container:user:limit"')
                                if sender_user_name in group_rule_param_users_list:
                                    if container in group_rule_param_containers_list and account in group_rule_param_accounts_list:
                                        current_limit = self.assign_new_recall_limit(
                                            current_limit, group_rule_param_limit, rule_key)
                                        self.logger.debug(
                                            f"Set limit from rule {rule_key}")

            if 'total_recall_limit' in self.conf and (
                    int(self.conf['total_recall_limit']) < current_limit or current_limit == -1):
                current_limit = self.assign_new_recall_limit(
                    current_limit, self.conf['total_recall_limit'], 'total_recall_limit')
                self.logger.debug('Total recall limit was used.')

            self.logger.debug('Current limit set to %s', current_limit)
            return current_limit

        except Exception as exception:
            self.logger.error(
                'An exception occured in get_configured_limit_recall_requests: %s',
                exception)
            return current_limit

    def set_allow_for_hlm_fallback(self):
        """Sets "allow_hlm_for" property in configuration dictionary.

        Returns:
            dict: Enhanced configuration dictionary.
        """
        try:
            self.conf['allow_hlm_for'] = '*'
            self.logger.info(
                'allow_hlm_for was not defined in proxy-server.conf. No restrictions applied.')
            self.logger.debug(
                'allow hlm for: %s', str(
                    self.conf['allow_hlm_for']))
            return self.conf
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    register_swift_info('hlm')

    def hlm_filter(app):
        return HlmMiddleware(app, conf)
    return hlm_filter


class ConvertError(Exception):
    """Raised when failed to convert migration policy element."""
