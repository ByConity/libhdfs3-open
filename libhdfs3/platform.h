/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#define THREAD_LOCAL __thread
#define ATTRIBUTE_NORETURN __attribute__ ((noreturn))
#define ATTRIBUTE_NOINLINE __attribute__ ((noinline))

#define GCC_VERSION (__GNUC__ * 10000 \
                     + __GNUC_MINOR__ * 100 \
                     + __GNUC_PATCHLEVEL__)

#define LIBUNWIND_FOUND 1
#define HAVE_DLADDR 1
#define OS_LINUX 1
#define ENABLE_FRAME_POINTER 1
#define NEED_BOOST 1
#define STRERROR_R_RETURN_INT 1
#define HAVE_STEADY_CLOCK 1
#define HAVE_NESTED_EXCEPTION 1
#define HAVE_BOOST_CHRONO 1
#define HAVE_STD_CHRONO 1
#define HAVE_BOOST_ATOMIC 1
#define HAVE_STD_ATOMIC 1

// defined by gcc
#if defined(__ELF__) && defined(OS_LINUX)
# define HAVE_SYMBOLIZE
#elif defined(OS_MACOSX) && defined(HAVE_DLADDR)
// Use dladdr to symbolize.
# define HAVE_SYMBOLIZE
#endif

#define STACK_LENGTH 64
