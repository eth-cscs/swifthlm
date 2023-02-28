[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# SwiftHLM (Swift Hight-Latency Media) middleware

    (C) Copyright 2016-2022 IBM Corp.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
    http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
    implied. See the License for the specific language governing permissions and
    limitations under the License.

## Authors:
* Giuseppe Lo Re (giuseppe.lore@cscs.ch)  
* Harald Seipp (seipp@de.ibm.com)  
* Juraj Miscik (juraj.miscik@ibm.com)  
* Marek Tomko (mtomko@sk.ibm.com)  
* Radovan Stas (radovas.stas@sk.ibm.com)  
* Robert J. Zima (robert.juraj.zima@ibm.com)  
* Slavisa Sarafijanovic (sla@zurich.ibm.com)   
* Stanislav Kocan (stanislavkocan@ibm.com)    

## Content:
1. Description (Function Overview)
2. Requirements
3. HLM Backend
3. Install
4. Desing/internals Overview
5. References

# 1. Description (Function Overview)
***

SwiftHLM is useful for running OpenStack Swift on top of high latency media (HLM) storage, such as tape or optical disk archive based backends, allowing to store cheaply and access efficiently large amounts of infrequently used object data.

SwiftHLM can be added to OpenStack Swift (without modifying Swift itself) to extend Swift's interface and thus allow to explicitly control and query the state (on disk or on HLM) of Swift objects data, including efficient prefetch of bulk of objects from HLM to disk when those objects need to be accessed. This function previously missing in Swift can be seen similar to Amazon Glacier [1], either through the Glacier API or the Amazon S3 Lifecycle Management API [2].

BDT Tape Library Connector (open source) [3] and IBM Spectrum Archive [4] are examples of HLM backends that provide important and complex functions to manage HLM resources (tape mounts/unmounts to drives, serialization of requests for tape media and tape drives resources) and can use SwiftHLM functions for a proper integration with Swift.

Access to data stored on HLM could be done transparently, without using SwiftHLM, but that does not work well in practice for many important use cases and for various reasons as discussed in [5]. In [5] it is also explained how SwiftHLM function can be orthogonal and complementary to Swift (ring to ring) tiering [6].

SwiftHLM version 1.0.1 provides the following basic HLM functions on the external Swift interface:

* `MIGRATE` (container or an object from disk to HLM),
* `PREMIGRATE` (creates a copy of the object data on secondary storage tier),
* `RECALL` (i.e. prefetch a container or an object from HLM to disk),
* `STATUS` (get status for a container or an object),
* `REQUESTS` (get status of migration and recall requests previously submitted
  for a contaner or an object).

MIGRATE, PREMIGRATE and RECALL are asynchronous operations, meaning that the request from user is queued and user's call is responded immediately, then the request is processed as a background task. Requests are currently processed in a FIFO manner (scheduling optimizations are future work).  REQUESTS and STATUS are synchronous operations that block the user's call until the queried information is collected and returned.

For each of these functions, SwiftHLM Middleware invokes additional SwiftHLM components to perform the task, which includes calls to HLM storage backend, for which a generic backend interface is defined below in section "3. HLM Backend". Description of other components is provided in the header of the implementation file for each component. 

# 2. Requirements
***

* OpenStack Swift Juno, Kilo, Liberty, Train (tested) or a later release (not
  tested)
* HLM backend that supports SwiftHLM functions, see HLM Backend section below
  for details
* Python 3.6+

# 3. HLM Backend
***

An HLM backend that supports SwiftHLM functions (MIGRATE, PREMIGRATE, RECALL, STATUS) is exposed to Swift in the same way as if SwiftHLM is not used (via a file system interface and a Swift ring definition), plus it needs to additionally support processing and responding requests from SwiftHLM middleware for performing SwiftHLM functions.

SwiftHLM Handler is the component of SwiftHLM  that invokes backend HLM operations via SwiftHLM generic backend interface (GBI). For each backend a Connector needs to be implemented that maps GBI requests to the backend HLM operations. 

A backend specific connector can be installed as a standard python module, or simply stored as a .py file at arbirary location to which the swift user has access. Then SwiftHLM should be configured to use that specific connector/backend, by appending the content of `swifthlm/object-server.conf.merge` file to `/etc/swift/object-server.conf`, and edditing the corresponding configuration values to match the specific connector/backend. 

If SwiftHLM is not configured to use a specific connector/backend, a dummy connector/backend provided and installed as part of SwiftHLM will be used as the default one. Connector for LTFS DM backend is installed as part of SwiftHLM installation, but is not enabled by default. In order to enable and use LTFS DM connector, LTFS DM backend needs to be installed from https://github.com/ibm-research/LTFS-Data-Management LTFS DM is an open source software that adds tape storage to a standard disk based Linux filesystem - it keeps the original namespace of the disk file system exposed to users and applications (via standard POSIX interface) but allows migrating file data to and from attached tape storage. 

To use SwiftHLM with IBM Spectrum Archive or IBM Spectrum Protect connector (proprietary), please see http://www.redbooks.ibm.com/abstracts/redp5430.html?Open.

# 3. Install
***

Please, see [installation instructions](https://github.ibm.com/tape-research/swifthlm/wiki/SwiftHLM---SwiftHLM-connector---installation-reinstallation) if you already have running environment with IBM Spectrum Scale installed. If not, please follow:

* [Environment setup with IBM Spectrum Protect as backend (Vagrant environment)](https://github.ibm.com/tape-research/swifthlm/wiki/Environment-setup-with-IBM-Spectrum-Protect-as-backend-(Vagrant-environment))  
**or**  
* [Environment setup with IBM Spectrum Archive as backend (Vagrant environment)](https://github.ibm.com/tape-research/swifthlm/wiki/Environment-setup-with-IBM-Spectrum-Archive-as-backend-(Vagrant-environment))

# 4. Design/Internals Overview
***

This section provides overview of SwiftHLM components and their join operation
for providing the function described in section 1.

SwiftHLM workflow for processing MIGRATION requests (and it is same for PREMIGRATE and RECALL requests) is as follows.  SwiftHLM middleware on proxy server intrcepts SwiftHLM migration requests and queues them inside Swift, by storing them into a special HLM-dedicated container as zero size objects.  After SwiftHLM request is queued, 202 code is returned to the application.

Another SwiftHLM process, called SwiftHLM Dispatcher, is processing the queued requests asynchronously with respect to user/application that submitted them. It picks a request from the queue, in FIFO or a more advance manner, and groups the requests into one list per involved storage node. 

For each storage node/list Dispatcher invokes remotely a SwiftHLM program on that storage node (the name of that program is SwiftHLM Handler), and provides it with the list. Handler could also be a long running process listening for and processing submissions from Dispatcher. Either way, the function performed by Handler is to map the objects to files (or to HLM backend objects) and submits the file list and the migration requests to HLM backend, if the backend already provide the function to move data between LLM (low latency media) and HLM (hight latency media). Examples of backends with such function are IBM Spectrum Archive and BDT Tape Library Connector. 

In order to support different backends, a Generic Backend Interface is defined and used by Handler to submit the request to HLM backend, via the backend specific Connector that maps the request to the backend specific API. If HLM backend does not support moving data and managing object state, the backend Connector needs to implement that function as well.

Once the backend completes the operation the result (succes or failure) is propagated back to the dispatcher. In case of success, the request is removed from the queue, otherwise it is marked as failed and kept in the queue for some period (to be able to answer the request status queries). One could also consider implementing request retries. 

Querying object status (STATUS) is processed by SwiftHLM middleware synchrounously, by groupping the queries per storage nodes and invoking the Handler (same as Dispatcher does for migration and recall), but for status the SwiftHLM middleware also merges the statuses reportd by the backend and provides the merged result to the Swift application.

Querying requests status (REQUESTS) Query for requests status for an object or a container are processed by SwiftHLM middleware, by reading listing of the special HLM-dedicated container(s). If there are not pending (incompleted or failed) requests for a container, the previously submitted operations for that container may be considered completed. This is more efficient than to query state for each object of a container.

# 5. References
***

[1] Amazon Glacier API, http://docs.aws.amazon.com/amazonglacier/latest/dev/amazon-glacier-api.html  
[2] Amazon S3 integration with Glacier, https://aws.amazon.com/blogs/aws/archive-s3-to-glacier  
[3] Tape Library Connector, https://github.com/BDT-GER/SWIFT-TLC  
[4] IBM Spectrum Archive, http://www-03.ibm.com/systems/storage/tape/ltfs/  
[5] SwiftHLM design discussion,  https://wiki.openstack.org/wiki/Swift/HighLatencyMedia  
[6] Swift ring to ring tiering, https://review.openstack.org/#/c/151335/3/specs/in_progress/tiering.rst  
