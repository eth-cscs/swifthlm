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

from swift.common.utils import get_logger


def get_common_logger(
        conf,
        logger_name="unknown",
        log_route="swifthlm",
        log_format="%(server)s: %(msecs)03d [%(filename)s:%(funcName)20s():%(lineno)s] %(message)s"):
    return get_logger(
        conf,
        name=logger_name,
        log_route=log_route,
        fmt=log_format
    )
