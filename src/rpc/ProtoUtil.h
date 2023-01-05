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