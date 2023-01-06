/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
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

#ifndef _HDFS_LIBHDFS3_RPC_PROTOUTIL_H_
#define _HDFS_LIBHDFS3_RPC_PROTOUTIL_H_

#include <string>

namespace Hdfs {
namespace Internal {

namespace ProtoUtil {

const std::string VERSION_KEY = "version";
const std::string FS_NAME_KEY = "fileSystemName";
const std::string STRING_TO_SIGN_KEY = "stringToSign";
const std::string SIGNATURE_KEY = "signature";
const std::string ACCESS_KEY = "accessKey";
const std::string SECURITY_TOKEN_KEY = "securityToken";

} // namespace ProtoUtil

} // namespace Internal
} // namespace Hdfs

#endif /* _HDFS_LIBHDFS3_RPC_PROTOUTIL_H_ */
