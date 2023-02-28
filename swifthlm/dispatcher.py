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
#
# This file implements Disaptcher component of SwiftHLM.

import json
import sys
from multiprocessing import Process
from socket import gethostbyname, gethostname

from eventlet import sleep
from swift.common.middleware.memcache import MemcacheMiddleware as memcache
from swift.common.utils import readconf

from swifthlm import middleware
from swifthlm.utils.common_logger import get_common_logger


class SwiftHlmDispatcher(object):
    """Daemon to dispatch asynchronous SwiftHLM requests, i.e. migration and
    recall requests."""

    def __init__(self):
        try:
            # Config
            configFile = r'/etc/swift/proxy-server.conf'
            self.conf = readconf(configFile)

            # This host ip address
            self.ip = gethostbyname(gethostname())

            # Logging
            hlm_stor_node_config = self.conf.get('filter:hlm', None)

            if hlm_stor_node_config:
                hlm_stor_node_log_level = hlm_stor_node_config.get('set log_level',
                                                                   None)

            if hlm_stor_node_log_level:
                self.conf['log_level'] = hlm_stor_node_log_level

            # Import common logger that will log messages
            self.logger = get_common_logger(
                self.conf, logger_name='hlm-dispatcher')
            self.logger.info('Common logger imported')

            # Show log level
            self.logger.info('Log level is set to %s', self.conf['log_level'])

            # Import SwiftHLM middleware function that can be reused by
            # Dispatcher
            self.swifthlm_mw = middleware.HlmMiddleware(
                'proxy-server', self.conf)
            self.logger.info('Hlm middleware "proxy-server" imported')

            # Instantiate memcache instance to be injected into swifthlm middleware
            # for status cache refresh
            self.memcache_mw = memcache(
                'proxy-server', self.conf['filter:cache'])
            # Use our own memcache middleware instance and "patch" its MemcacheRing
            # object into our hlm middleware instance to allow cache updates as the
            # directly called middleware code does not have access to the WSGI
            # environment that keeps the pointer to the memcache class.
            setattr(self.swifthlm_mw, 'mcache',
                    getattr(self.memcache_mw, 'memcache'))
            self.logger.info(
                'Memcache instance injected into swifthlm middleware')

            # Memory of previous queue pulling result, used to adapt pulling
            # period
            self.found_empty_queue = False
            self.policy_index = 0
            self.found_empty_list = False
            self.logger.info('Initialized Dispatcher')
        except Exception as e:
            self.logger.error('Error occured: %s', e)
            print(e)

    def update_queue(self, mw, request, success):
        """Removes request from the pending-hlm-requests queue.

        If success is False, request is queued to failed-hlm-requests container/queue.

        Args:
            mw (HlmMiddleware): SwiftHLM middleware.
            request (dict): Processed request.
            success (bool): State of request.
        """
        try:
            self.logger.info('Entering queue update')
            # Queue failed request to failed-hlm-requests container/queue
            if not success:
                if mw.queue_failed_migration_or_recall_request(
                        json.dumps(request)):
                    self.logger.info('Queued failed request: %s', request)
                else:
                    self.logger.error('Failed to queue failed req.: %s',
                                      request)

            # If a request is resubmitted upon a failure(s) and succeeds,
            # clean up the related failed requests
            if success:
                mw.success_remove_related_requests_from_failed_queue(request)

            # Delete the processed request from the pending-hlm-requests queue
            if mw.delete_request_from_queue(
                    json.dumps(request), 'pending-hlm-requests'):
                self.logger.info('Deleted request from queue: %s', request)
            else:
                self.logger.error('Failed to delete request: %s', request)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def update_status(self, mw, request):
        """Updates the cached status at the end of the request processing.

        Args:
            mw (HlmMiddleware): SwiftHLM middleware.
            request (dict): Processed request.
        """
        try:
            self.logger.info('Entering status update')
            timestamp, hlm_req, account, container, spi, obj = \
                mw.decode_request(request)
            self.logger.debug(
                'Decoded request: %s; %s, %s, %s',
                request,
                hlm_req,
                account,
                container)
            # Invoke the status request directly - keep in mind that this
            # is only possible as we patched a directly instantiated memcache
            # middleware into the swifthlm middleware instance we are using
            new_hlm_req = 'status'
            self.logger.info(
                'Distributing the request %s to storage nodes: %s, %s',
                new_hlm_req,
                account,
                container)
            mw.distribute_request_to_storage_nodes_get_responses(
                new_hlm_req,
                account,
                container,
                obj, spi)
            mw.merge_responses_from_storage_nodes(new_hlm_req)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def process_next_request(self):
        """Pulls a request from the queue, dispatches it to storage nodes, gets
        and merges the responses."""
        try:
            self.logger.info('Process next request in queue')
            self.logger.info(
                'Middleware used for next request: %s.%s',
                self.swifthlm_mw.__class__.__module__,
                self.swifthlm_mw.__class__.__name__)
            mw = self.swifthlm_mw
            # Pull request
            request = mw.pull_a_mig_or_rec_request_from_queue()
            if not request:
                self.found_empty_queue = True
            else:
                # Found a request to process
                self.found_empty_queue = False
                self.logger.info('Processing request %s', request)
                if len(sys.argv) >= 2:
                    print('Processing request ' + request)
                timestamp, hlm_req, account, container, spi, obj = \
                    mw.decode_request(request)
                self.logger.debug('ts:%s, hlmreq:%s, ac:%s, c:%s, spi:%s, o:%s',
                                  timestamp, hlm_req, account, container, spi, obj)
                # if not (pre)migration policy request, continue with filtering
                # container objects based on their size
                if request.get('policy_request') is None:
                    request = mw.filter_objects_based_on_size(request, hlm_req, account,
                                                              container, spi, obj, request.get('objects'))
                # Distribute request to storage nodes get responses
                if request.get('objects') is not None:
                    mw.distribute_request_to_storage_nodes_get_responses(
                        hlm_req, account, container, obj, spi, request['objects'])
                else:
                    mw.distribute_request_to_storage_nodes_get_responses(
                        hlm_req, account, container, obj, spi)
                # Merge responses from storage nodes
                mw.merge_responses_from_storage_nodes(hlm_req)

                self.logger.debug(f"Storage nodes response: {mw.response_out}")
                success = 'successful' in mw.response_out
                if success:
                    self.logger.info("Request succeeded: %s", request)
                    if hlm_req == "recall":
                        self.logger.debug(
                            "Recall reqest - status update needed.")
                        self.update_status(mw, request)
                    if (hlm_req in ['migrate', 'premigrate']) and (not mw.set_object_state_based_on_request(
                            hlm_req, account, container, obj, request.get('objects'))):
                        self.logger.debug(
                            "Updating object state based on target state failed - status update needed.")
                        self.update_status(mw, request)
                else:
                    self.logger.info("Request failed: %s", request)
                    if hlm_req == "recall":
                        self.logger.debug(
                            "Recall reqest - status update needed.")
                        self.update_status(mw, request)
                    if hlm_req in ['migrate', 'premigrate']:
                        # delete object target state from cache
                        mw.delete_target_state_from_cache(
                            account, container, obj, request.get('objects'))
                        self.update_status(mw, request)
                self.update_queue(mw, request, success)
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def process_next_policy(self, index):
        """Pulls and process (pre)migration policy from the list.

        Args:
            index (int): Index of (pre)migration policy.
        """
        try:
            self.logger.info('Process next policy in list')
            self.logger.info(
                'Middleware used for next request: %s.%s',
                self.swifthlm_mw.__class__.__module__,
                self.swifthlm_mw.__class__.__name__)
            mw = self.swifthlm_mw
            # Pull policy
            policy = mw.pull_a_mig_pol_from_list(index)
            if not policy:
                self.policy_index = 0  # reset index
                self.found_empty_list = True
            else:
                # Found a policy to process
                self.found_empty_list = False
                self.logger.info('Processing policy %s', policy)
                policy = mw.convert_migration_policy(policy)
                if policy is not None:
                    mw.filter_objects_to_migrate(policy)
                else:
                    self.logger.error(
                        "Could not convert migration policy. Continue with next migration policy")
                self.policy_index += 1
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def run_requests(self, *args, **kwargs):
        """Runs a dispatcher for requests.

        Dispatcher is running until stopped, or unless it is invoked
        with sys.argv[1] == 1 (testing mode).
        """
        try:
            self.logger.info("Dispatcher for requests is running")
            if len(sys.argv) == 2 and str(sys.argv[1]) == "1":
                self.logger.debug('The first sys.argv is "1"')
                self.logger.debug('Polling the requests queue')
                print('Polling the requests queue')
                self.process_next_request()
                return
            else:
                while True:
                    if len(sys.argv) >= 2:
                        print('Polling the requests queue')
                    self.logger.debug('Polling the requests queue in loop')
                    self.process_next_request()
                    if self.found_empty_queue:
                        sleep(5)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def run_policies(self, *args, **kwargs):
        """Runs a dispatcher for policies.

        Dispatcher is running until stopped, or unless it is invoked
        with sys.argv[1] == 1 (testing mode).
        """
        try:
            if 'migration_policy_sleep_time' in self.conf['filter:hlm']:
                migration_policy_sleep_time = int(
                    self.conf['filter:hlm']['migration_policy_sleep_time'])
            else:
                self.logger.warning(
                    "Migration policy sleep time was not set in proxy-server.conf.")
                self.logger.warning("Will use default value 600 seconds.")
                migration_policy_sleep_time = 600

            self.logger.info(
                "Dispatcher for policies is running. Sleep time is set to %s seconds.",
                migration_policy_sleep_time)
            if len(sys.argv) == 2 and str(sys.argv[1]) == "1":
                self.logger.debug('Polling the policies list')
                print('Polling the policies list')
                self.process_next_policy(self.policy_index)
                return
            else:
                while True:
                    if len(sys.argv) >= 2:
                        print('Polling the policies list')
                    self.logger.debug('Polling the policies list')
                    self.process_next_policy(self.policy_index)
                    if self.found_empty_list:
                        sleep(migration_policy_sleep_time)
        except Exception as exception:
            self.logger.error(
                'An exception occured: %s',
                exception,
                exc_info=True)


if __name__ == '__main__':
    dispatcher = SwiftHlmDispatcher()
    process_requests = Process(
        target=dispatcher.run_requests,
        args=sys.argv).start()
    process_policies = Process(
        target=dispatcher.run_policies,
        args=sys.argv).start()
