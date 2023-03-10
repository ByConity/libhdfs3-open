/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_COMMON_UNORDEREDMAP_H_
#define _HDFS_LIBHDFS3_COMMON_UNORDEREDMAP_H_

#include "platform.h"

#ifdef NEED_BOOST

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

namespace Hdfs {
namespace Internal {

using boost::unordered_map;
using boost::unordered_set;

}
}

#else

#include <unordered_map>
#include <unordered_set>

namespace Hdfs {
namespace Internal {

using std::unordered_map;
using std::unordered_set;

}
}
#endif

#endif /* _HDFS_LIBHDFS3_COMMON_UNORDEREDMAP_H_ */
