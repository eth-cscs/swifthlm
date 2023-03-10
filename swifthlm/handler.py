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
# This file contains implementation of the SwiftHLM Handler component of the
# SwiftHLM - a Swift extension for high latency media support. SwiftHLM Handler
# is installed on a Swift storage node, and invoked by a SwiftHLM proxy
# middleware (SwiftHLM Middleware) when processing STATUS request or by
# SwiftHLM Dispatcher process when processing MIGRATE or RECALL request. For an
# object or a list of objects from a container, SwiftHLM Handler creates a list
# that maps objects to file path(s) and submits the list to the HLM backend for
# migration, recall or status processing.

import imp
import importlib
import os
from configparser import RawConfigParser
from sys import stdin, stdout

from swift.common.exceptions import DiskFileDeviceUnavailable
from swift.common.storage_policy import POLICIES
from swift.common.utils import hash_path, json, readconf, split_path
from swift.obj.server import ObjectController

import swifthlm.dummy_connector
from swifthlm.utils.common_logger import get_common_logger


class Handler(object):
    """SwiftHLM Handler maps objects to files and submits requests to Storage
    Backend via Backend Connector."""

    def __init__(self):
        self.request_in = ''
        self.request_out = {}
        self.response_out = {}

        # Config
        configFile = r'/etc/swift/object-server.conf'
        self.conf = readconf(configFile)
        # readconf does not load the [DEFAULT] section, adding that manually
        rcp = RawConfigParser()
        with open(configFile, 'r') as cf:
            rcp.readfp(cf)
            full_conf = self.conf.copy()
            full_conf.update(rcp.defaults())
        self.conf = full_conf

        # Logging
        hlm_stor_node_config = self.conf.get('hlm', None)
        if hlm_stor_node_config:
            hlm_stor_node_log_level = hlm_stor_node_config.get('set log_level',
                                                               None)
        if hlm_stor_node_log_level:
            self.conf['log_level'] = hlm_stor_node_log_level

        # Import common logger that will log messages
        self.logger = get_common_logger(self.conf, logger_name='hlm-handler')
        self.logger.info('Common logger imported')

        self.logger.info('Initialized Handler')
        # self.logger.info('conf: %s', self.conf)

        # Generic backend interface (GBI) configuration options
        self.gbi_provide_dirpaths_instead_of_filepaths = False
        conf_gbi_provide_dirpaths_instead_of_filepaths = \
            hlm_stor_node_config.get(
                'gbi_provide_dirpaths_instead_of_filepaths',
                'False')
        if conf_gbi_provide_dirpaths_instead_of_filepaths == 'True':
            self.gbi_provide_dirpaths_instead_of_filepaths = True
        self.logger.debug(
            'GBI will provide dirpaths instead of filepaths? %s', str(
                self.gbi_provide_dirpaths_instead_of_filepaths))

        # Backend connector (directory and .py filename) can be configured in
        # /etc/swift/object-server.conf
        # If nothing is configured a dummy backend connector, that is provided
        # and installed with SwiftHLM is used by default
        swifthlm_connector_module = hlm_stor_node_config.get(
            'swifthlm_connector_module',
            '')
        swifthlm_connector_dir = hlm_stor_node_config.get(
            'swifthlm_connector_dir',
            '')
        swifthlm_connector_filename = hlm_stor_node_config.get(
            'swifthlm_connector_filename',
            '')
        swifthlm_connector_path = swifthlm_connector_dir + '/' + \
            swifthlm_connector_filename
        self.logger.info(
            'SwiftHLM connector: %s, %s, %s',
            swifthlm_connector_module,
            swifthlm_connector_dir,
            swifthlm_connector_filename)

        if swifthlm_connector_module:
            self.logger.debug('swifthlm_connector_module: %s',
                              swifthlm_connector_module)
            self.swifthlm_connector_mod = \
                importlib.import_module(swifthlm_connector_module,
                                        package=None)
        elif swifthlm_connector_filename:
            swifthlm_connector_module = swifthlm_connector_filename[:-3]
            self.logger.debug('swifthlm_connector_path: %s',
                              swifthlm_connector_path)
            self.swifthlm_connector_mod = imp.load_source(
                swifthlm_connector_module,
                swifthlm_connector_path)
        else:
            self.logger.debug('Using default swifthlm_connector_module: %s',
                              'swifthlm.dummy_connector')
            self.swifthlm_connector_mod = swifthlm.dummy_connector

    def receive_request(self):
        """Receives a request from dispatcher."""
        try:
            self.logger.info('Receiving request from Dispatcher')
            self.request_in = str(stdin.read())
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def map_objects_to_targets(self):
        """Maps objects to their local storage server data replicas and creates
        a request for the generic backend interface."""
        try:
            self.logger.info('Mapping objects to files')
            self.logger.debug('request_in(first 1024 bytes): %s',
                              str(self.request_in)[:1023])

            request_in_dict = json.loads(self.request_in)
            self.request_out = {'request': request_in_dict['request']}
            objects_and_files = []
            oc = ObjectController(self.conf, self.logger)
            self.logger.debug('oc.node_timeout: %s', oc.node_timeout)
            for obj_and_dev in request_in_dict['objects']:
                obj_and_file = {'object': obj_and_dev['object']}
                self.logger.debug('obj: %s', obj_and_dev)
                try:
                    (account, container, obj) = split_path(
                        obj_and_dev['object'], 3, 3, True)
                except ValueError:
                    self.logger.debug('split_path exception')
                    raise
                device = obj_and_dev['device']
                swift_dir = request_in_dict['swift_dir']
                storage_policy_index = request_in_dict['storage_policy_index']
                self.logger.debug(f'Storage policy index: {storage_policy_index}')
                obj_ring = POLICIES.get_object_ring(storage_policy_index,
                                                    swift_dir)
                # need partition, same comment as for storage_policy_index
                partition, nodes = obj_ring.get_nodes(account, container, obj)
                self.logger.debug(f'Storage nodes: {nodes}')
                self.logger.debug('partition: %s', partition)
                # scor (aux)
                # key = hash_path(account, container, obj, raw_digest=True)
                key = hash_path(account, container, obj)
                self.logger.debug('hash_path or key: %s', key)

                # Create/use Object Controller to map objects to files
                policy = POLICIES.get_by_index(storage_policy_index)
                self.logger.debug(
                    'policy: %s index: %s', policy, str(
                        int(policy)))
                try:
                    oc.disk_file = oc.get_diskfile(
                        device, partition, account, container, obj,
                        policy=policy)
                except DiskFileDeviceUnavailable:  # scor
                    self.logger.error("Unavailable device: %s, for object: %s,"
                                      "storage policy: %s", device,
                                      obj_and_dev['object'], policy)
                data_dir = oc.disk_file._datadir
                self.logger.debug('data_dir: %s', data_dir)
                # Swift-on-File detection
                sof_detected = False
                # Get the device path from the object server config file
                devpath = self.conf.get('devices', None)
                # The Swift-on-File device directory is a symlink
                # in the devpath directory constructed like shown below
                sofpath = devpath + '/' + obj_and_dev['device']
                if data_dir.find(sofpath) == 0 and os.path.islink(sofpath):
                    # data_dir starts with sofpath and sofpath is a symlink ->
                    # SoF
                    sof_detected = True
                    self.logger.debug('SOF detected, sofpath: %s, realpath: %s',
                                      sofpath, os.path.realpath(sofpath))
                    # Follow the symlink and append a/c/o to get the data file
                    # path
                    oc._data_file = os.path.realpath(sofpath) + \
                        obj_and_file['object']
                elif not self.gbi_provide_dirpaths_instead_of_filepaths:
                    files = os.listdir(oc.disk_file._datadir)
                    file_info = {}
                    # DiskFile method got renamed between Liberty and Mitaka
                    try:
                        file_info = oc.disk_file._get_ondisk_file(files)
                    except AttributeError:
                        file_info = oc.disk_file._get_ondisk_files(files)
                    oc._data_file = file_info.get('data_file')
                    self.logger.debug('data_file: %s', oc._data_file)
                # Add file path to the request
                self.logger.debug('obj_and_dev: %s', obj_and_dev)
                if (not self.gbi_provide_dirpaths_instead_of_filepaths) or \
                   sof_detected:
                    obj_and_file['file'] = oc._data_file
                else:
                    obj_and_file['file'] = data_dir
                self.logger.debug('obj_and_file: %s', obj_and_file)
                objects_and_files.append(obj_and_file)

            self.logger.debug(
                'objects_and_files(first 1024 bytes): %s',
                str(objects_and_files[:1023]),
            )

            self.request_out['objects'] = objects_and_files

            self.logger.debug(
                'request_in(first 1024 bytes): %s', str(self.request_in)[:1023]
            )

            self.logger.debug(
                'request_out(first 1024 bytes): %s', str(
                    self.request_out)[:1023]
            )

        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def submit_request_get_response(self):
        """Submit request to Backend via Backend Connector and get Response
        from Backend."""
        try:
            self.logger.info('Submitting request to backend')
            # self.response_out = self.request_in
            connector = self.swifthlm_connector_mod.SwiftHlmBackendConnector()
            self.response_out = \
                connector.submit_request_get_response(self.request_out)
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def return_response(self):
        """Return request result back to SwiftHLM Proxy Middleware (STATUS) or
        to Asynchronous Distributor (MIGRATION or RECAL)."""
        try:
            self.logger.info('Return response to Dispatcher')
            self.logger.debug("Response: %s", json.dumps(self.response_out))
            stdout.write(json.dumps(self.response_out))
            stdout.flush()
            stdout.close()
            self.logger.debug('Exiting Handler')
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)


if __name__ == '__main__':
    handler = Handler()
    handler.receive_request()
    handler.map_objects_to_targets()
    handler.submit_request_get_response()
    handler.return_response()
