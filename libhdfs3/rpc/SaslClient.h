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
#ifndef _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_
#define _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_

#include <gsasl.h>

#include "client/Token.h"
#include "network/Socket.h"
#include "RpcAuth.h"
#include "RpcHeader.pb.h"
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

namespace Hdfs {
namespace Internal {

#define SWITCH_TO_SIMPLE_AUTH -88

class AESClient {
public:
    AESClient(std::string enckey, std::string enciv,
              std::string deckey, std::string deciv, int bufsize);
    ~AESClient();

    std::string encode(const char *input, size_t input_len);
    std::string decode(const char *input, size_t input_len);

private:
    EVP_CIPHER_CTX *encrypt;
    EVP_CIPHER_CTX *decrypt;

    int packetsSent;
    long decoffset;
    int bufsize;

    std::string enckey;
    std::string enciv;
    std::string deckey;
    std::string deciv;
    std::string initdeciv;
    static bool initialized;
};

class SaslClient {
public:
    SaslClient(const RpcSaslProto_SaslAuth & auth, const Token & token,
               const std::string & principal, bool encryptedData=false,
               int protection=0);

    ~SaslClient();

    bool needsLength();
    void setAes(AESClient *client);
   
    std::string evaluateChallenge(const std::string & chanllege);

    bool isComplete();

    bool isPrivate();
    bool isIntegrity();

    std::string encode(const char *input, size_t input_len);
    std::string decode(const char *input, size_t input_len, bool force=false);

private:
    int findPreferred(int possible);
    void initKerberos(const RpcSaslProto_SaslAuth & auth,
                      const std::string & principal);
    void initDigestMd5(const RpcSaslProto_SaslAuth & auth, const Token & token);

private:
    int protection;
    AESClient *aes;
    Gsasl * ctx;
    Gsasl_session * session;
    bool changeLength;
    bool complete;
    bool privacy;
    bool integrity;
    const RpcSaslProto_SaslAuth theAuth;
    const Token theToken;
    const std::string thePrincipal;
    bool encryptedData;
    std::string saslToken;
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_ */
