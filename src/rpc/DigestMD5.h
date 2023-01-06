
/*
 * This file may have been modified by ByteDance Ltd. (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) ByteDance Ltd..
 */

#ifndef _HDFS_LIBHDFS3_RPC_DIGESTMD5_H_
#define _HDFS_LIBHDFS3_RPC_DIGESTMD5_H_

namespace Hdfs {
namespace Internal {

struct digest_md5_challenge {
    size_t nrealms;
    char **realms;
    char *nonce;
    int qops;
    int stale;
    unsigned long servermaxbuf;
    int utf8;
    int ciphers;
};
typedef struct digest_md5_challenge digest_md5_challenge;

void hsasl_put_int_to_bytes(std::string& buf, int paramInt);
std::string hsasl_base64_encode(const std::string& source);
int hsasl_digestmd5_step(const std::string& saslToken, Gsasl_session *sctx,
                         const char *input, size_t input_len,
                         char **output, size_t *output_len);

}
}

#endif //_HDFS_LIBHDFS3_RPC_DIGESTMD5_H_
