/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/* This is a minimalistic implementation of a sasl auth */
#ifndef ISASL_H
#define ISASL_H

#include "config.h"
#include "isasl.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    unsigned long len;
    unsigned char data[1];
} sasl_secret_t;

typedef struct {
    unsigned long id;
    int (*proc)(void);
    void *context;
} sasl_callback_t;

/* define the different callback id's we support */
#define SASL_CB_USER 1
#define SASL_CB_AUTHNAME 2
#define SASL_CB_PASS 3
#define SASL_CB_LIST_END 4

/* Define the error codes we support */
#define SASL_OK 1
#define SASL_CONTINUE 2
#define SASL_ERROR 3
#define SASL_BADPARAM 4

typedef struct sasl_conn sasl_conn_t;

#define sasl_client_init(a) SASL_OK

void sasl_done(void);

int sasl_client_new(const char *service,
				const char *serverFQDN,
				const char *iplocalport,
				const char *ipremoteport,
				const sasl_callback_t *prompt_supp,
				unsigned flags,
				sasl_conn_t **pconn);

void sasl_dispose(sasl_conn_t **pconn);

int sasl_client_start(sasl_conn_t *conn,
				const char *mechlist,
				void **prompt_need,
				const char **clientout,
				unsigned *clientoutlen,
				const char **mech);

#define sasl_client_step(a, b, c, d, e, f) SASL_ERROR


#ifdef __cplusplus
}
#endif

#endif
