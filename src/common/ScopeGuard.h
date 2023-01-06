
/*
 * Copyright (2022) ByteDance Ltd.
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

#ifndef _HDFS_LIBHDFS3_COMMON_SCOPE_GUARD_H_
#define _HDFS_LIBHDFS3_COMMON_SCOPE_GUARD_H_

#include <utility>

namespace ext
{

template <class F> class scope_guard {
    const F function;

public:
    constexpr scope_guard(const F & function) : function{function} {}
    constexpr scope_guard(F && function) : function{std::move(function)} {}
    ~scope_guard() { function(); }
};

template <class F>
inline scope_guard<F> make_scope_guard(F && function) { return std::forward<F>(function); }

}

#define SCOPE_EXIT_CONCAT(n, ...) \
const auto scope_exit##n = ext::make_scope_guard([&] { __VA_ARGS__; })
#define SCOPE_EXIT_FWD(n, ...) SCOPE_EXIT_CONCAT(n, __VA_ARGS__)
#define SCOPE_EXIT(...) SCOPE_EXIT_FWD(__LINE__, __VA_ARGS__)

#endif