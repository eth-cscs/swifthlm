#!/usr/bin/python

# (C) Copyright 2018-2022 IBM Corp.
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
# This file implements SwiftHLM Connector for LTFS Data Management (LTFS DM)
# backend, i.e. the connector between SwiftHLM and the tape-enabled file storage
# backend LTFS DM. LTFS DM is a software that adds tape storage to a standard
# disk based Linux filesystem - it keeps the original namespace of the disk file
# system exposed to users and applications (via standard POSIX interface) but
# allows migrating file data to and from attached tape storage. LTFS DM is open
# sourced at https://github.com/ibm-research/LTFS-Data-Management.
# Toward SwiftHLM the connector implements SwiftHLM Generic Backend API as
# declared in dummy_connector.py of SwiftHLM. On the backend side the connector
# maps SwiftHLM requests to the backend's migrate, recall and query status
# operations.

import errno
import os
import subprocess
import uuid

from swift.common.utils import json, readconf

from swifthlm.utils.common_logger import get_common_logger


class SwiftHlmBackendConnector(object):
    """SwiftHLM Backend Connector."""

    def __init__(self):
        self.__request_in = {}
        self.__request_out = {}
        self.__request_out_request = ''
        self.__request_out_filelist = ''
        self.__response_in = {}
        self.__response_out = {}

        # Config
        configFile = r'/etc/swift/object-server.conf'
        self.conf = readconf(configFile)

        # Logging
        self.hlm_stor_node_config = self.conf.get('hlm', None)
        if self.hlm_stor_node_config:
            hlm_stor_node_log_level = self.hlm_stor_node_config.get(
                'set log_level', None)
        if hlm_stor_node_log_level:
            self.conf['log_level'] = hlm_stor_node_log_level
        self.logger = get_common_logger(self.conf, logger_name='hlm-connector')
        self.logger.info('Common logger imported')

        self.logger.info('info: Initialized Connector')
        self.logger.debug('dbg: Initialized Connector')
        self.logger.info('conf: %s', self.conf['log_level'])
        self.logger.debug('conf: %s', json.dumps(self.conf.get('hlm', None)))
        self.logger.debug('conf: %s', json.dumps(
            self.conf.get('ltfsdm', None)))

        # Connector settings
        self.ltfsdm_cfg = self.conf.get('ltfsdm', None)
        if not self.ltfsdm_cfg:
            self.logger.error('LTFS DM connector not configured in \
                    /etc/swift/object-server.conf')
            raise Exception('LTFS DM connector not configured in /etc/swift/object-server.conf')
        # Check connector settings, make temporary directory if it does not
        # exist
        self.ltfsdm_path = self.ltfsdm_cfg.get('ltfsdm_path',
                                               '/usr/local/bin/ltfsdm')
        # if not os.path.isfile(self.ltfsdm_path):
        if os.system('sudo -i ' + self.ltfsdm_path +
                     ' help > /dev/null 2>&1') != 0:
            self.logger.error("ERROR: ltfsdm binary not present at"
                              " configured (or default) path %s", self.ltfsdm_path)
            raise Exception('LTFSDM binary not present at configured (or default) path.')
        self.connector_tmp_dir = self.ltfsdm_cfg.get('connector_tmp_dir', None)
        if self.connector_tmp_dir:
            self.mkdir_minus_p(self.connector_tmp_dir)
        else:
            self.logger.error('Swifthlm temporary directory not configured')
            raise Exception('Swifthlm temporary directory not configured')
        self.tape_storage_pool = self.ltfsdm_cfg.get('tape_storage_pool', None)
        if not self.tape_storage_pool:
            self.logger.error('Tape storage pool not configured.')
            raise Exception('Tape storage pool not configured.')

    def submit_request_get_response(self, request):
        """Adapts SwiftHLM request for LTFS DM backend, invokes the backend
        operations, reformats the backend response to GBI format, and returns
        the response to SwitHLM Handler.

        Invoked by SwiftHLM Handler using SwiftHLM Generic Backend Interface (GBI).

        Args:
            request (dict): Processed request.

        Returns:
            dict: Response to SwiftHLM Handler.
        """
        try:
            self.logger.info('Prepare request to submit to backend')
            self.logger.debug('Request: %s', request)
            self.__receive_request(request)
            self.__reformat_swifthlm_request_to_specific_backend_api()
            self.__submit_request_to_backend_get_response()
            self.__reformat_backend_response_to_generic_backend_api()
            return self.__response_out
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def __receive_request(self, request):
        """Receives the request from SwiftHLM Handler.

        Args:
            request (dict): Processed request.
        """
        try:
            self.logger.info('Receiving request from Handler')
            self.__request_in = request

            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def __reformat_swifthlm_request_to_specific_backend_api(self):
        """Reformats request to backend API."""
        try:
            self.logger.info(
                'Reformatting request to the specific Backend API')
            self.logger.debug(
                'request_in(first 1024 bytes): %s', str(
                    self.__request_in)[:1023]
            )

            # Backend specific part
            self.__request_out_request = self.__request_in['request']
            if str.lower((self.__request_in['request']).encode(
                    'utf-8')) == 'status':
                # status: reuse input request as is
                self.__request_out = self.__request_in
            else:
                # migration or recall: prepare list for bulk migration/recall
                # in a temporary file
                tmp_filename = str(uuid.uuid1())
                self.__request_out_list = self.connector_tmp_dir + '/' + \
                    tmp_filename
                with open(self.__request_out_list, 'w') as f:
                    for obj_and_file in self.__request_in['objects']:
                        f.write(str(obj_and_file['file']) + '\n')
                with open(self.__request_out_list, 'r') as fr:
                    file_list_content = fr.read()
                    self.logger.debug('file_list: %s', file_list_content)
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def __submit_request_to_backend_get_response(self):
        """Submits request to Backend and gets Response from Backend."""
        try:
            self.logger.info('Submitting request to backend')
            if self.__request_out_request == 'status':
                # query status
                self.query_status_receive_response()
            elif self.__request_out_request == 'migrate':
                # self.__response_in = 0
                self.migrate_receive_response()
            elif self.__request_out_request == 'recall':
                self.recall_receive_response()
            else:  # wrong request
                raise Exception("Wrong request.")
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def __reformat_backend_response_to_generic_backend_api(self):
        """Reformats response to Generic Backend API."""
        try:
            self.logger.info('Reformatting response to Generic Backend API')
            self.logger.debug(
                'response_in(first 1024 bytes): %s', str(
                    self.__response_in)[:1023]
            )

            # In this connector implementaiton, the mapping of the response from
            # the backend to the GBI is done in the functions
            # migrate_receive_response(), recall_receive_response() and
            # query_status_receive_response() when setting response_in varible, it
            # only remains to copy it to response_out.
            self.__response_out = self.__response_in

            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def mkdir_minus_p(self, dir_path):
        """Creates a directory recursively.

        Args:
            dir_path (str): A path-like object representing a file system path.
        """
        try:
            os.makedirs(dir_path)
        except OSError as err:
            if err.errno != errno.EEXIST or not os.path.isdir(dir_path):
                raise
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)
        return

    def migrate_receive_response(self):
        """Receives response and migrates object files."""
        try:
            self.logger.info('Receive response - migrate request')
            listfile = self.__request_out_list
            request = self.__request_out_request
            # Migrate object files - unfortunately ltfsdm migrate must be run as
            # root
            self.logger.debug('self.ltfsdm_path: %s', self.ltfsdm_path)
            cmd = [
                "sudo",
                "-i",
                self.ltfsdm_path,
                "migrate",
                "-f",
                listfile,
                '-P']
            for pool in self.tape_storage_pool.split():
                cmd.append(pool)
            self.logger.debug('cmd: %s', cmd)
            p = subprocess.Popen(cmd,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 stdin=subprocess.PIPE)
            out, error = p.communicate()
            rc = p.returncode
            self.logger.debug(
                'migrate.out(first 1024 bytes): %s',
                str(out)[
                    :1023])
            self.logger.debug('rc: %s', rc)
            if rc == 6:
                rc = 0
            self.__response_in = rc
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def recall_receive_response(self):
        """Receives response and recalls object files."""
        try:
            self.logger.info('Receive response - recall request')
            listfile = self.__request_out_list
            request = self.__request_out_request
            # Recall object files - unfortunately ltfsdm migrate must be run as
            # root
            cmd = ["sudo", "-i", self.ltfsdm_path, "recall", "-f", listfile]
            self.logger.debug('cmd: %s', cmd)
            p = subprocess.Popen(cmd,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 stdin=subprocess.PIPE)
            out, error = p.communicate()
            rc = p.returncode
            self.logger.debug(
                'recall.out(first 1024 bytes): %s',
                str(out)[
                    :1023])
            self.logger.debug('rc: %s', rc)
            self.__response_in = rc
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def query_status_receive_response(self):
        """Receives response and queries object files."""
        try:
            self.logger.info('Receive response - status request')
            # prepare temporary lists unique file name prefix
            lists_prefix = str(uuid.uuid1())
            input_list = self.connector_tmp_dir + '/' + lists_prefix + \
                '.list.status.input'
            self.logger.debug('input_list: %s', input_list)
            with open(input_list, 'w') as f:
                for obj_and_file in self.__request_in['objects']:
                    f.write(str(obj_and_file['file']) + '\n')
            # mmapplypolicy output is by default owned by root, 0600 file mode
            # so we create it as swift user to be able to process it later
            output_list = self.connector_tmp_dir + '/' + lists_prefix + \
                '.list.status.output'
            self.logger.debug('output_list: %s', output_list)
            open(output_list, 'w').close()
            output_list_prefix = self.connector_tmp_dir + '/' + lists_prefix

            # Prepare status scan command
            cmd = ["sudo" +
                   " -i " +
                   self.ltfsdm_path +
                   " info" +
                   " files" +
                   " -f " + input_list +
                   " | awk 'NR > 1 { print }'" +
                   " >" + output_list]
            self.logger.debug('cmd: %s', cmd)
            # Invoke the command
            p = subprocess.Popen(cmd,
                                 shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            out, err = p.communicate()
            # Check result
            if p.returncode:
                self.logger.error('Status query errors: %s', err)
                return

            with open(output_list, 'r') as fr:
                file_list_content = fr.read()
                self.logger.debug(
                    'output_list(first 1024 bytes): %s',
                    str(file_list_content)[:1023],
                )

            # get file-status pairs
            names_statuses = {}
            fr = open(output_list, 'r')
            for line in fr.readlines():
                self.logger.debug('line: %s', str(line))
                file_name = line.split()[-1]
                file_status = line.split()[0]
                if file_status == 'r':
                    file_status = 'resident'
                elif file_status == 'p':
                    file_status = 'premigrated'
                elif file_status == 'm':
                    file_status = 'migrated'
                self.logger.debug('file_name: %s', file_name)
                self.logger.debug('file_status: %s', file_status)
                names_statuses[file_name] = file_status

            # create object to file to status mapping
            objects = []
            for obj_and_file in self.__request_out['objects']:
                obj_file_status = {
                    'object': obj_and_file['object'],
                    'file': obj_and_file['file'],
                }

                filenamekey = obj_and_file['file']
                self.logger.debug('filenamekey: %s', filenamekey)
                filenamekey = os.path.realpath(filenamekey)
                self.logger.debug('filenamekey: %s', filenamekey)
                obj_file_status['status'] = names_statuses[filenamekey]
                objects.append(obj_file_status)
            self.__response_in['objects'] = objects
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)

    def set_statuses_to_unknown(self):
        """Sets status of request to unknown."""
        try:
            self.logger.info('Set status of request to unknown')
            objects = []
            for obj_and_file in self.__request_out['objects']:
                obj_file_status = obj_and_file
                obj_file_status['status'] = 'unknown'
                objects.append(obj_file_status)
            self.__response_in['objects'] = objects
            return
        except Exception as exception:
            self.logger.error('An exception occured: %s', exception)


if __name__ == '__main__':
    # SwiftHlmConnector class is not assumed to be used standalone, instead it
    # is imported for a configured backend by SwiftHLM Handler and invoked from
    # the Handler.
    raise Exception("SwiftHLMConnector class is not assumed to be used standalone.")
