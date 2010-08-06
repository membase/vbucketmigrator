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
#include "config.h"
#include <string.h>
#include <stdbool.h>

struct sasl_conn {
    const sasl_callback_t *callbacks;
    char *userdata;
};

static const char *plain = "PLAIN";

int sasl_client_new(const char *service,
                    const char *serverFQDN,
                    const char *iplocalport,
                    const char *ipremoteport,
                    const sasl_callback_t *prompt_supp,
                    unsigned flags,
                    sasl_conn_t **pconn) {
    (void)service;
    (void)serverFQDN;
    (void)iplocalport;
    (void)ipremoteport;
    (void)flags;

    struct sasl_conn *conn = calloc(1, sizeof(*conn));
    if (conn == NULL) {
        return SASL_ERROR;
    }

    conn->callbacks = prompt_supp;
    *pconn = conn;
    return SASL_OK;
}

void sasl_dispose(sasl_conn_t **pconn) {
    free((*pconn)->userdata);
    free(*pconn);
    *pconn = NULL;
}

int sasl_client_start(sasl_conn_t *conn,
                      const char *mechlist,
                      void **prompt_need,
                      const char **clientout,
                      unsigned *clientoutlen,
                      const char **mech) {
    (void)prompt_need;

    if (strstr(mechlist, "PLAIN") == NULL) {
        return SASL_ERROR;
    }

    *mech = plain;

    // get the username:
    const char *usernm = NULL;
    unsigned int usernmlen;
    int i = 0;
    bool found = false;

    while (conn->callbacks[i].id != SASL_CB_LIST_END) {
        if (conn->callbacks[i].id == SASL_CB_USER) {
            union {
                int (*get)(void*, int, const char **, unsigned int*);
                int (*proc)(void);
            } hack;

            hack.proc = conn->callbacks[i].proc;

            int r = hack.get(conn->callbacks[i].context, SASL_CB_USER,
                             &usernm, &usernmlen);
            if (r != SASL_OK) {
                return r;
            }
            found = true;
            break;
        }
        ++i;
    }

    if (!found) {
        return SASL_ERROR;
    }

    found = false;
    sasl_secret_t *pass;
    i = 0;
    while (conn->callbacks[i].id != SASL_CB_LIST_END) {
        if (conn->callbacks[i].id == SASL_CB_PASS) {
            union {
                int (*get)(sasl_conn_t *, void *, int, sasl_secret_t **);
                int (*proc)(void);
            } hack;

            hack.proc = conn->callbacks[i].proc;

            int r = hack.get(conn, conn->callbacks[i].context, SASL_CB_PASS,
                             &pass);
            if (r != SASL_OK) {
                return r;
            }
            found = true;
            break;
        }
        ++i;
    }
    if (!found) {
        return SASL_ERROR;
    }

    conn->userdata = calloc(usernmlen + 1 + pass->len + 1, 1);
    if (conn->userdata == NULL) {
        return SASL_ERROR;
    }

    memcpy(conn->userdata + 1, usernm, usernmlen);
    memcpy(conn->userdata + usernmlen + 2, pass->data, pass->len);
    *clientout = conn->userdata;
    *clientoutlen = usernmlen + 2 + pass->len;

    return SASL_OK;
}

void sasl_done(void)
{

}
