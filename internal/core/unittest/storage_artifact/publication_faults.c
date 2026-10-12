// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A test-only LD_PRELOAD library. No calls are intercepted until the test arms
// one exact, independently created publication directory. Payload downloads,
// unrelated tests, and staging-file destruction retain normal syscall behavior.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>

static int (*next_rename)(const char*, const char*);
static int (*next_unlink)(const char*);
static const char* test_scope;
static size_t scope_length;
static int rollback_mode;
static int publish_error;
static int rollback_error;
static atomic_int publish_count;
static atomic_int publish_failures;
static atomic_int rollback_failures;

__attribute__((constructor)) static void
resolve_syscalls(void) {
    next_rename = dlsym(RTLD_NEXT, "rename");
    next_unlink = dlsym(RTLD_NEXT, "unlink");
}

void
milvus_artifact_arm_faults(const char* scope,
                           int mode,
                           int publish_errno,
                           int rollback_errno) {
    // Refuse scopes outside the fixture's unique temporary directory.
    if (scope != NULL && (strstr(scope, "/milvus-file-source-") == NULL ||
                          strstr(scope, "/publication-") == NULL)) {
        scope = NULL;
    }
    test_scope = scope;
    scope_length = scope == NULL ? 0 : strlen(scope);
    rollback_mode = mode;
    publish_error = publish_errno;
    rollback_error = rollback_errno;
    atomic_store(&publish_count, 0);
    atomic_store(&publish_failures, 0);
    atomic_store(&rollback_failures, 0);
}

void
milvus_artifact_fault_counts(int* publish, int* rollback) {
    *publish = atomic_load(&publish_failures);
    *rollback = atomic_load(&rollback_failures);
}

static int
in_scope(const char* path) {
    return test_scope != NULL && path != NULL &&
           strncmp(path, test_scope, scope_length) == 0 &&
           path[scope_length] == '/';
}

static const char*
base_name(const char* path) {
    const char* slash = strrchr(path, '/');
    return slash == NULL ? path : slash + 1;
}

static int
has_prefix(const char* value, const char* prefix) {
    return strncmp(value, prefix, strlen(prefix)) == 0;
}

int
rename(const char* source, const char* target) {
    if (in_scope(target)) {
        const char* name = base_name(source);
        const int backup = has_prefix(name, ".milvus-artifact-backup-");
        if (!backup && has_prefix(name, ".milvus-artifact-") &&
            atomic_fetch_add(&publish_count, 1) == 1) {
            atomic_fetch_add(&publish_failures, 1);
            errno = publish_error;
            return -1;
        }
        if (backup && rollback_mode == 2 &&
            atomic_load(&publish_failures) == 1 &&
            atomic_fetch_add(&rollback_failures, 1) == 0) {
            errno = rollback_error;
            return -1;
        }
    }
    return next_rename(source, target);
}

int
unlink(const char* path) {
    if (in_scope(path) && rollback_mode == 1 &&
        atomic_load(&publish_failures) == 1 &&
        !has_prefix(base_name(path), ".milvus-artifact-") &&
        atomic_fetch_add(&rollback_failures, 1) == 0) {
        errno = rollback_error;
        return -1;
    }
    return next_unlink(path);
}
