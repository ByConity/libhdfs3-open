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
#include <algorithm>
#include <cctype>

#include "Exception.h"
#include "ExceptionInternal.h"
#include "SaslClient.h"
#include "DigestMD5.h"
#include "StringUtil.h"

#define SASL_SUCCESS 0

namespace Hdfs {
namespace Internal {

// Parse digest-md5's qops into a int
static int parseDigestMd5Qops(const std::string& qops) {
    std::vector<std::string> components = StringSplit(qops, ",");
    int parsed_qops = 0;
    for (const std::string& qop : components) {
        std::string trimmed_qop = StringTrim(qop);
        if (trimmed_qop == "qop-auth") {
            parsed_qops |= GSASL_QOP_AUTH;
        } else if (trimmed_qop == "qop-int") {
            parsed_qops |= GSASL_QOP_AUTH_INT;
        } else if (trimmed_qop == "qop-conf") {
            parsed_qops |= GSASL_QOP_AUTH_CONF;
        } else {
            THROW(HdfsIOException, "Unknown qop string, component: %s, all: %s",
                trimmed_qop.c_str(), qops.c_str());
        }
    }
    return parsed_qops;
}

static int digestMd5PropertyCallback (Gsasl* ctx, Gsasl_session* sctx, Gsasl_property prop) {
    int rc = GSASL_NO_CALLBACK;

    if (prop == GSASL_QOP) {
        const char* qops = gsasl_property_get(sctx, GSASL_QOPS);

        int viable_qops = parseDigestMd5Qops(std::string(qops));

        const char* selected_qop = nullptr;
        if (viable_qops & GSASL_QOP_AUTH) {
            selected_qop = "qop-auth";
        } else if (viable_qops & GSASL_QOP_AUTH_INT) {
            selected_qop = "qop-int";
        } else if (viable_qops & GSASL_QOP_AUTH_CONF) {
            selected_qop = "qop-conf";
        } else {
            selected_qop = "qop-auth";
        }
        gsasl_property_set(sctx, prop, selected_qop);

        rc = GSASL_OK;
    }

    return rc;
}

std::string calculateIV(std::string initIV, long counter) {
std::string IV;
IV.resize(initIV.length());
int i = initIV.length();
int j = 0;
int sum = 0;
unsigned c;
while (i-- > 0) {
  // (sum >>> Byte.SIZE) is the carry for addition
  sum = (((unsigned char)initIV.c_str()[i]) & 0xff) + ((unsigned int)sum >> 8);
  if (j++ < 8) { // Big-endian, and long is 8 bytes length
    sum += (unsigned char) counter & 0xff;
    c = (unsigned long) counter;
    c >>= (unsigned)8;
    counter = c;
  }
  IV[i] = (unsigned char) sum;
}
return IV;
}

void printArray(std::string str, const char* text) {
    int i=0;
    printf("length %d: %s\n", (int)str.length(), text);
    for (i=0; i < (int)str.length(); i++) {
        printf("%02d ", (int)str[i]);
    }
    printf("\n");

}
bool AESClient::initialized = false;

AESClient::AESClient(std::string enckey, std::string enciv,
    std::string deckey, std::string deciv, int bufsize) :
              encrypt(NULL), decrypt(NULL), packetsSent(0), decoffset(0), bufsize(bufsize),
              enckey(enckey), enciv(enciv), deckey(deckey), deciv(deciv), initdeciv(deciv)
{
if (!initialized) {
  ERR_load_crypto_strings();
  OpenSSL_add_all_algorithms();
  OPENSSL_config(NULL);
  initialized = true;
}
encrypt = NULL;
decrypt = NULL;
encrypt = EVP_CIPHER_CTX_new();
if (!encrypt) {
    std::string err = ERR_lib_error_string(ERR_get_error());
    THROW(HdfsIOException, "Cannot initialize aes encrypt context %s",
          err.c_str());
}
decrypt = EVP_CIPHER_CTX_new();
if (!decrypt) {
    std::string err = ERR_lib_error_string(ERR_get_error());
    THROW(HdfsIOException, "Cannot initialize aes decrypt context %s",
          err.c_str());
}
std::string iv = enciv;
const EVP_CIPHER *cipher = NULL;
if (enckey.length() == 32)
    cipher = EVP_aes_256_ctr();
else if (enckey.length() == 16)
    cipher = EVP_aes_128_ctr();
else
    cipher = EVP_aes_192_ctr();
if (!EVP_CipherInit_ex(encrypt, cipher, NULL,
    (const unsigned char*)enckey.c_str(), (const unsigned char*)iv.c_str(), 1)) {
    std::string err = ERR_lib_error_string(ERR_get_error());
    THROW(HdfsIOException, "Cannot initialize aes encrypt cipher %s",
          err.c_str());
}
iv = deciv;
if (!EVP_CipherInit_ex(decrypt, cipher, NULL, (const unsigned char*)deckey.c_str(),
    (const unsigned char*)iv.c_str(), 0)) {
    std::string err = ERR_lib_error_string(ERR_get_error());
    THROW(HdfsIOException, "Cannot initialize aes decrypt cipher %s",
          err.c_str());
}
EVP_CIPHER_CTX_set_padding(encrypt, 0);
EVP_CIPHER_CTX_set_padding(decrypt, 0);

}

AESClient::~AESClient() {
if (encrypt)
    EVP_CIPHER_CTX_free(encrypt);
if (decrypt)
    EVP_CIPHER_CTX_free(decrypt);
}

std::string AESClient::encode(const char *input, size_t input_len) {
int len;
std::string result;
result.resize(input_len);
int offset = 0;
int remaining = input_len;

while (remaining > bufsize) {
    if (!EVP_CipherUpdate (encrypt, (unsigned char*)&result[offset], &len, (const unsigned char*)input+offset, bufsize)) {
        std::string err = ERR_lib_error_string(ERR_get_error());
        THROW(HdfsIOException, "Cannot encrypt AES data %s",
              err.c_str());
    }
    offset += len;
    remaining -= len;
}
if (remaining) {

    if (!EVP_CipherUpdate (encrypt, (unsigned char*)&result[offset], &len, (const unsigned char*)input+offset, remaining)) {
        std::string err = ERR_lib_error_string(ERR_get_error());
        THROW(HdfsIOException, "Cannot encrypt AES data %s",
              err.c_str());
    }
}
return result;
}


std::string AESClient::decode(const char *input, size_t input_len) {
int len;
std::string result;
result.resize(input_len);
int offset = 0;
int remaining = input_len;

while (remaining > bufsize) {
    if (!EVP_CipherUpdate (decrypt, (unsigned char*)&result[offset], &len, (const unsigned char*)input+offset, bufsize)) {
        std::string err = ERR_lib_error_string(ERR_get_error());
        THROW(HdfsIOException, "Cannot decrypt AES data %s",
              err.c_str());
    }
    offset += len;
    remaining -= len;
}
if (remaining) {

    if (!EVP_CipherUpdate (decrypt, (unsigned char*)&result[offset], &len, (const unsigned char*)input+offset, remaining)) {
        std::string err = ERR_lib_error_string(ERR_get_error());
        THROW(HdfsIOException, "Cannot decrypt AES data %s",
              err.c_str());
    }
}
decoffset += input_len;
return result;

}




SaslClient::SaslClient(const RpcSaslProto_SaslAuth & auth, const Token & token,
                       const std::string & principal, bool encryptedData, int protection,
                       bool use_internal_cred) :
    protection(protection), aes(NULL), ctx(NULL), session(NULL), changeLength(false), complete(false),
    privacy(false), integrity(false),
    theAuth(auth), theToken(token), thePrincipal(principal), encryptedData(encryptedData),
    use_internal_credential(use_internal_cred) {
    int rc;
    ctx = NULL;
    RpcAuth method = RpcAuth(RpcAuth::ParseMethod(auth.method()));
    rc = gsasl_init(&ctx);

    if (rc != GSASL_OK) {
        THROW(HdfsIOException, "cannot initialize libgsasl");
    }

    switch (method.getMethod()) {
    case AuthMethod::KERBEROS:
        initKerberos(auth, principal);
        break;

    case AuthMethod::TOKEN:
        initDigestMd5(auth, token);
        break;

    default:
        THROW(HdfsIOException, "unknown auth method.");
        break;
    }
}

SaslClient::~SaslClient() {
    if (aes)
        delete aes;

    if (session != NULL) {
        gsasl_finish(session);
        session = NULL;
    }

    if (ctx != NULL) {
        gsasl_done(ctx);
        ctx = NULL;
    }
}

bool SaslClient::needsLength() {
    if (aes != NULL)
        return false;
    if ((!privacy && !integrity) || (!complete))
        return false;
    return true;
}

void SaslClient::setAes(AESClient *client) {
    aes = client;
}
   

void SaslClient::initKerberos(const RpcSaslProto_SaslAuth & auth,
                              const std::string & principal) {
    int rc;

    /* Create new authentication session. */
    if ((rc = gsasl_client_start(ctx, auth.mechanism().c_str(), &session)) != GSASL_OK) {
        THROW(HdfsIOException, "Cannot initialize client (%d): %s", rc,
              gsasl_strerror(rc));
    }

    gsasl_property_set(session, GSASL_SERVICE, auth.protocol().c_str());
    gsasl_property_set(session, GSASL_AUTHID, principal.c_str());
    gsasl_property_set(session, GSASL_HOSTNAME, auth.serverid().c_str());

}

std::string Base64Encode(const std::string & in) {
    char * temp;
    size_t len;
    std::string retval;

    int rc = gsasl_base64_to(in.c_str(), in.size(), &temp, &len);

    if (rc != GSASL_OK) {
        throw std::bad_alloc();
    }

    if (temp) {
        retval = temp;
        free(temp);
    }

    if (!temp || retval.length() != len) {
        THROW(HdfsIOException, "SaslClient: Failed to encode string to base64");
    }

    return retval;
}

void SaslClient::initDigestMd5(const RpcSaslProto_SaslAuth & auth,
                               const Token & token) {
    // Setup callback for digest-md5 which only contains qop selector
    gsasl_callback_set(ctx, digestMd5PropertyCallback);

    int rc;

    if ((rc = gsasl_client_start(ctx, auth.mechanism().c_str(), &session)) != GSASL_OK) {
        THROW(HdfsIOException, "Cannot initialize client (%d): %s", rc, gsasl_strerror(rc));
    }

    std::string user_name;
    std::string password;

    if (use_internal_credential) {
        password = Base64Encode(token.getPassword());
        std::string identifier;

        if (!encryptedData)
            identifier = Base64Encode(token.getIdentifier());
        else
            identifier = token.getIdentifier();

        // TODO: need set right host and service
        this->saslToken = token.getIdentifier();

        hsasl_put_int_to_bytes(user_name, int(this->saslToken.size()));
        user_name.append(this->saslToken);
        user_name = hsasl_base64_encode(user_name);
        password = hsasl_base64_encode(this->saslToken);
    } else {
        user_name = Base64Encode(token.getIdentifier());
        password = Base64Encode(token.getPassword());
    }

    gsasl_property_set(session, GSASL_AUTHID, user_name.c_str());
    gsasl_property_set(session, GSASL_PASSWORD, password.c_str());
    gsasl_property_set(session, GSASL_HOSTNAME, auth.serverid().c_str());
    gsasl_property_set(session, GSASL_SERVICE, auth.protocol().c_str());
    changeLength = true;

}

int SaslClient::findPreferred(int possible) {
    if (possible & GSASL_QOP_AUTH)
        return GSASL_QOP_AUTH;
    if (possible & GSASL_QOP_AUTH_INT)
        return GSASL_QOP_AUTH_INT;
    if (possible & GSASL_QOP_AUTH_CONF)
        return GSASL_QOP_AUTH_CONF;
    return GSASL_QOP_AUTH;
}


std::string SaslClient::evaluateChallenge(const std::string & challenge) {
    int rc;
    char * output = NULL;
    size_t outputSize;
    std::string retval;
    std::string copied_challenge = challenge;

    if (!use_internal_credential) {
        rc = gsasl_step(session, &challenge[0], challenge.size(), &output,
            &outputSize);
    } else {
        rc = hsasl_digestmd5_step(this->saslToken, session, &challenge[0], challenge.size(), &output,
            &outputSize);
    }

    RpcAuth method = RpcAuth(RpcAuth::ParseMethod(theAuth.method()));
    if (rc == GSASL_GSSAPI_INIT_SEC_CONTEXT_ERROR && method.getMethod() == AuthMethod::KERBEROS) {
        // Try again using principal instead
        gsasl_finish(session);
        initKerberos(theAuth, thePrincipal);
        gsasl_property_set(session, GSASL_GSSAPI_DISPLAY_NAME, thePrincipal.c_str());
        rc = gsasl_step(session, &challenge[0], challenge.size(), &output,
                    &outputSize);
    }

    if (rc == GSASL_NEEDS_MORE || rc == GSASL_OK) {
        retval.resize(outputSize);
        memcpy(&retval[0], output, outputSize);

        if (output) {
            free(output);
        }
    } else {
        if (output) {
            free(output);
        }

        THROW(AccessControlException, "Failed to evaluate challenge: %s", gsasl_strerror(rc));
    }

    if (rc == GSASL_OK) {
        complete = true;
        int preferred = 0;
        if (method.getMethod() == AuthMethod::TOKEN) {
            const char *qop = gsasl_property_get (session, GSASL_QOP);
            preferred = parseDigestMd5Qops(std::string(qop));
        }
        else if (challenge.length()) {
            if (protection != 0)
                preferred = protection;
            else {
                std::string decoded = decode(copied_challenge.c_str(), copied_challenge.length(), true);
                int qop = (int)decoded.c_str()[0];
                preferred = findPreferred(qop);
            }
        }
        if (preferred & GSASL_QOP_AUTH_CONF) {
            privacy = true;
            integrity = true;
        } else if (preferred & GSASL_QOP_AUTH_INT) {
            integrity = true;
        }
    }

    return retval;
}

std::string SaslClient::encode(const char *input, size_t input_len) {
    std::string result;
    if ((!privacy && !integrity) || (!complete)) {
        result.resize(input_len);
        memcpy(&result[0], input, input_len);
        return result;
    }
    if (aes)
        return aes->encode(input, input_len);
   
    char *output=NULL;
    size_t output_len;
    int rc = gsasl_encode(session, input, input_len, &output, &output_len);
    if (rc != GSASL_OK) {
        THROW(AccessControlException, "Failed to encode wrapped data: %s", gsasl_strerror(rc));
    }
    if (output_len) {

        if (output_len > 4 && changeLength) {
            result.resize(output_len-4);
            memcpy(&result[0], output+4, output_len-4);
        } else {
            result.resize(output_len);
            memcpy(&result[0], output, output_len);
        }
        free(output);
    }
    return result;
}

std::string  SaslClient::decode(const char *input, size_t input_len, bool force) {
    std::string result;
    if ((!privacy && !integrity && !force) || (!complete)) {
        result.resize(input_len);
        memcpy(&result[0], input, input_len);
        return result;
    }
    if (aes)
        return aes->decode(input, input_len);

    char *output=NULL;
    size_t output_len;
    std::string actualInput;
    if (changeLength) {
        actualInput.resize(input_len+4);
        actualInput[0] = (input_len>> 24) & 0xFF;
        actualInput[1] = (input_len >> 16) & 0xFF;
        actualInput[2] = (input_len >> 8) & 0xFF;
        actualInput[3] = input_len & 0xFF;
        memcpy(&actualInput[4], input, input_len);

    } else {
        actualInput.resize(input_len);
        memcpy(&actualInput[0], input, input_len);
    }
    int rc = gsasl_decode(session, actualInput.c_str(), actualInput.length(), &output, &output_len);
    if (rc != GSASL_OK) {
        THROW(AccessControlException, "Failed to decode wrapped data: %s", gsasl_strerror(rc));
    }
    if (output_len) {
        result.resize(output_len);
        memcpy(&result[0], output, output_len);
        free(output);
    }

    return result;
}


bool SaslClient::isComplete() {
    return complete;
}

bool SaslClient::isPrivate() {
    return privacy;
}

bool SaslClient::isIntegrity() {
    return integrity;
}

}
}

