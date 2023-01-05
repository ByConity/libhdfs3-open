#include <iostream>
#include <string>
#include <bitset>
#include <vector>
#include <openssl/md5.h>

#include "SaslClient.h"
#include "StringUtil.h"
#include "DigestMD5.h"

namespace Hdfs {
namespace Internal {

struct Challenge {
    std::string realm;
    std::string nonce;
    std::string qop;
    std::string charset;
    std::string algorithm;
};

void update_gsasl_ctx_qop(Gsasl_session* sctx, const std::string& qop) {
    if (qop == "auth-conf") {
        gsasl_property_set(sctx, GSASL_QOP, "qop-conf");
    } else if (qop == "auth-int") {
        gsasl_property_set(sctx, GSASL_QOP, "qop-int");
    } else {
        gsasl_property_set(sctx, GSASL_QOP, "qop-auth");
    }
}

void hsasl_put_int_to_bytes(std::string& buf, int paramInt) {
    char byteArray[2];
    byteArray[0] = (int)((paramInt & 0x0000FF00) >> 8 );
    byteArray[1] = (int)((paramInt & 0X000000FF));
    for (int i = 0; i < 2; i++) {
        buf.push_back(byteArray[i]);
    }
}


static const std::string base64_chars = 
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz"
"0123456789+/";


static inline bool is_base64(char c) {
    return (isalnum(c) || (c == '+') || (c == '/'));
}

std::string hsasl_base64_encode(const std::string& source) {
    char const* bytes_to_encode = source.c_str();
    unsigned int in_len = source.size();
    std::string ret;
    int i = 0;
    int j = 0;
    char char_array_3[3];
    char char_array_4[4];

    while (in_len--) {
        char_array_3[i++] = *(bytes_to_encode++);
        if (i == 3) {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for(i = 0; (i <4) ; i++)
                ret += base64_chars[char_array_4[i]];
            i = 0;
        }
    }

    if (i) {
        for(j = i; j < 3; j++)
            char_array_3[j] = '\0';

        char_array_4[0] = ( char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

        for (j = 0; (j < i + 1); j++)
            ret += base64_chars[char_array_4[j]];

        while((i++ < 3))
            ret += '=';
    }

    return ret;

}

static std::string base64_decode(std::string const& encoded_string) {
    int in_len = encoded_string.size();
    int i = 0;
    int j = 0;
    int in_ = 0;
    char char_array_4[4], char_array_3[3];
    std::string ret;

    while (in_len-- && ( encoded_string[in_] != '=') && is_base64(encoded_string[in_])) {
        char_array_4[i++] = encoded_string[in_]; in_++;
        if (i ==4) {
            for (i = 0; i <4; i++)
                char_array_4[i] = base64_chars.find(char_array_4[i]);

            char_array_3[0] = ( char_array_4[0] << 2       ) + ((char_array_4[1] & 0x30) >> 4);
            char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
            char_array_3[2] = ((char_array_4[2] & 0x3) << 6) +   char_array_4[3];

            for (i = 0; (i < 3); i++)
                ret += char_array_3[i];
            i = 0;
        }
    }

    if (i) {
        for (j = 0; j < i; j++)
            char_array_4[j] = base64_chars.find(char_array_4[j]);

        char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
        char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);

        for (j = 0; (j < i - 1); j++) ret += char_array_3[j];
    }

    return ret;
}

static std::string binary_to_hex(const std::string& binary_str) {
    std::string res;
    const unsigned char* source = (const unsigned char*)binary_str.c_str();
    char digit[3]; //sprintf will add a null terminate
    for(size_t i = 0; i < binary_str.size(); i++) {
        sprintf(digit, "%02x", source[i]);
        res.append(digit, 2);
    }
    return res;
}

static std::string md5(std::string& input) {
    char digest[16];
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, (const unsigned char*)input.c_str(), input.size());
    MD5_Final((unsigned char*)digest, &ctx);
    return std::string(digest, 16);
}

int hsasl_digestmd5_step(const std::string& saslToken, Gsasl_session *sctx,
                         const char *input, size_t input_len,
                         char **output, size_t *output_len) {
    // parse challenge
    Challenge challenge;
    std::string inputStr(input, input_len);
    std::vector <std::string> items = StringSplit(inputStr, ",");
    for (size_t i = 0; i < items.size(); i++) {
        std::vector <std::string> item = StringSplit(items[i], "=");
        if (item.size() != 2) {
            continue;
        }
        StringReplaceAll(item[0], " ", "");
        StringReplaceAll(item[0], "\"", "");
        StringReplaceAll(item[1], " ", "");
        StringReplaceAll(item[1], "\"", "");
        if (item[0].compare("realm") == 0) {
            challenge.realm = item[1];
        } else if (item[0].compare("nonce") == 0) {
            challenge.nonce = item[1];
        } else if (item[0].compare("qop") == 0) {
            challenge.qop = item[1];
        } else if (item[0].compare("charset") == 0) {
            challenge.charset = item[1];
        } else if (item[0].compare("algorithm") == 0) {
            challenge.algorithm = item[1];
        } 
    }

    std::string token;

    std::string identify;
    hsasl_put_int_to_bytes(identify, int(saslToken.size()));
    identify.append(saslToken);
    identify = hsasl_base64_encode(identify);

    std::string password = hsasl_base64_encode(saslToken);

    std::string var14("AUTHENTICATE:/");
    var14.append(challenge.realm);
    std::string var18 = md5(var14);
    std::string var13 = binary_to_hex(var18);

    std::string var4 = identify;
    std::string var5 = challenge.realm;
    std::string var6 = password;
    std::string var15;
    var15.append(var4);
    var15.push_back(':');
    var15.append(var5);
    var15.push_back(':');
    var15.append(var6);
    var18 = md5(var15);

    std::string cnonce = hsasl_base64_encode(challenge.nonce);
    std::string var7 = challenge.nonce;
    std::string var8 = cnonce;
    std::string var16;
    var16.append(var18);
    var16.push_back(':');
    var16.append(var7);
    var16.push_back(':');
    var16.append(var8);
    var18 = md5(var16);
    std::string var12 = binary_to_hex(var18);

    std::string nonce_count("00000001");
    std::string var3 = challenge.qop;
    std::string var17;
    var17.append(var12);
    var17.push_back(':');
    var17.append(var7);
    var17.push_back(':');
    var17.append(nonce_count);
    var17.push_back(':');
    var17.append(var8);
    var17.push_back(':');
    var17.append(var3);
    var17.push_back(':');
    var17.append(var13);
    var18 = md5(var17);
    std::string var19 = binary_to_hex(var18);

    std::string response;
    response.append("charset=\"utf-8\",");
    response.append("username=\"");
    response.append(identify);
    response.append("\",");
    response.append("realm=\"");
    response.append(challenge.realm);
    response.append("\",");
    response.append("nonce=\"");
    response.append(challenge.nonce);
    response.append("\",");
    response.append("nc=\"");
    response.append(nonce_count);
    response.append("\",");
    response.append("cnonce=\"");
    response.append(cnonce);
    response.append("\",");
    response.append("digest-uri=\"/");
    response.append(challenge.realm);
    response.append("\",");
    response.append("maxbuf=\"65536\",");
    response.append("response=\"");
    response.append(var19);
    response.append("\",");
    response.append("qop=\"");
    response.append(challenge.qop);
    response.append("\" ");

    *output_len = response.size() + 1;
    *output = (char*)malloc(sizeof(char) * (*output_len));
    memcpy(*output, response.c_str(), response.size());
    (*output)[*output_len - 1] = '\0';
    // std::cout << __FILE__ << ":" << __LINE__ << " " << (*output) << "\n";

    // Set qop in context
    update_gsasl_ctx_qop(sctx, challenge.qop);

    return 0;
}

}
}
