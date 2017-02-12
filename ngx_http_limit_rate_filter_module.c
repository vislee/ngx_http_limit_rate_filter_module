/*
 * Copyright wenqiang3@staff.sina.com.cn
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


typedef struct {
    ngx_str_node_t                        sn;
    ngx_queue_t                           queue;
    time_t                                start;
    ngx_uint_t                            conn;
    ngx_queue_t                           req_queue;
    u_char                                key;
} ngx_http_limit_rate_filter_node_t;


typedef struct {
    ngx_queue_t                           rq;
    ngx_http_request_t                   *r;
    off_t                                 sent;
} ngx_http_limit_rate_filter_req_queue_t;


typedef struct {
    ngx_shm_zone_t                       *shm_zone;
    ngx_str_t                             key;
    ngx_http_request_t                   *r;
} ngx_http_limit_rate_filter_cleanup_t;

typedef struct {
    ngx_rbtree_t                          rbtree;
    ngx_rbtree_node_t                     sentinel;
    ngx_queue_t                           queue;
} ngx_http_limit_rate_filter_shctx_t;


typedef struct {
    ngx_http_limit_rate_filter_shctx_t   *sh;
    ngx_slab_pool_t                      *shpool;
} ngx_http_limit_rate_filter_ctx_t;


typedef struct {
    ngx_int_t                      index;
    ngx_str_t                      var;
    ngx_uint_t                     conn;
    size_t                         rate;
    ngx_uint_t                     status_code;
    ngx_shm_zone_t                *shm_zone;
} ngx_http_limit_rate_filter_conf_t;


static ngx_int_t ngx_http_limit_rate_filter_init(ngx_conf_t *cf);
static ngx_int_t ngx_http_limit_rate_filter_ctx_init_zone(ngx_shm_zone_t *shm_zone, void *data);
static char *ngx_http_limit_rate_filter_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_limit_rate_filter_conn(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void *ngx_http_limit_rate_filter_create_conf(ngx_conf_t *cf);
static char *ngx_http_limit_rate_filter_merge_conf(ngx_conf_t *cf, void *parent, void *child);



static ngx_command_t ngx_http_limit_rate_filter_commands[] = {

    { ngx_string("limit_rate_zone_size"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_http_limit_rate_filter_zone,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("limit_rate_conn"),
      NGX_HTTP_LOC_CONF|NGX_CONF_TAKE3,
      ngx_http_limit_rate_filter_conn,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("limit_rate_conn_status"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_limit_rate_filter_conf_t, status_code),
      NULL },

    ngx_null_command
};


static ngx_http_module_t ngx_http_limit_rate_filter_module_ctx = {
    NULL,
    ngx_http_limit_rate_filter_init,

    NULL,
    NULL,

    NULL,
    NULL,

    ngx_http_limit_rate_filter_create_conf,
    ngx_http_limit_rate_filter_merge_conf
};


ngx_module_t ngx_http_limit_rate_filter_module = {
    NGX_MODULE_V1,
    &ngx_http_limit_rate_filter_module_ctx,
    ngx_http_limit_rate_filter_commands,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING
};

static ngx_http_output_body_filter_pt    ngx_http_next_body_filter;



static void *
ngx_http_limit_rate_filter_create_conf(ngx_conf_t *cf)
{
    ngx_http_limit_rate_filter_conf_t    *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_limit_rate_filter_conf_t));
    if (NULL == conf) {
        return NULL;
    }
    conf->index       = NGX_CONF_UNSET;
    conf->status_code = NGX_CONF_UNSET_UINT;
    conf->rate        = NGX_CONF_UNSET_UINT;
    conf->conn        = NGX_CONF_UNSET_UINT;
    conf->shm_zone    = NULL;

    return conf;
}


static char *
ngx_http_limit_rate_filter_merge_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_limit_rate_filter_conf_t  *prev = parent;
    ngx_http_limit_rate_filter_conf_t  *conf = child;

    ngx_conf_merge_value(conf->index, prev->index, NGX_CONF_UNSET);
    ngx_conf_merge_uint_value(conf->rate, prev->rate, 0);
    ngx_conf_merge_uint_value(conf->conn, prev->conn, 0);
    ngx_conf_merge_uint_value(conf->status_code, prev->status_code,
                              NGX_HTTP_SERVICE_UNAVAILABLE);

    if (NULL == conf->shm_zone) {
        if (NULL == prev->shm_zone) {
            ngx_log_error(NGX_LOG_ERR, cf->log, 0, 
              "limit_rate_filter shm_zone null, please check nginx.conf \"sae_limit_rate_zone_size\" ");
            return NGX_CONF_ERROR;
        }
        conf->shm_zone = prev->shm_zone;
    }
    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_limit_rate_filter_free_rq(ngx_http_limit_rate_filter_ctx_t  *ctx,
                                   ngx_http_limit_rate_filter_node_t *node, ngx_http_request_t *r)
{
    ngx_int_t                                  n;
    ngx_queue_t                               *q, *next;
    ngx_http_limit_rate_filter_req_queue_t    *req;

    n = 0;
    for (q = ngx_queue_next(&node->req_queue);
         q != ngx_queue_sentinel(&node->req_queue);
         q = next) {

        next = ngx_queue_next(q);
        req  = ngx_queue_data(q, ngx_http_limit_rate_filter_req_queue_t, rq);
        if (r != NULL && req->r != r) {
            continue;
        }
        n += 1;
        ngx_queue_remove(q);
        ngx_slab_free_locked(ctx->shpool, req);
    }

    return n;
}



static void
ngx_http_limit_rate_filter_cleanup(void *data)
{
    ngx_http_limit_rate_filter_cleanup_t    *lrfln = data;

    uint32_t                                 hash;
    ngx_int_t                                n;
    ngx_http_limit_rate_filter_ctx_t        *ctx;
    ngx_http_limit_rate_filter_node_t       *node;


    ctx = lrfln->shm_zone->data;

    hash = ngx_crc32_short(lrfln->key.data, lrfln->key.len);

    ngx_shmtx_lock(&ctx->shpool->mutex);
    node = (ngx_http_limit_rate_filter_node_t *)
            ngx_str_rbtree_lookup(&ctx->sh->rbtree, &lrfln->key, hash);
    if (node != NULL) {
        n = ngx_http_limit_rate_filter_free_rq(ctx, node, lrfln->r);
        node->conn -= n;
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, lrfln->r->connection->log, 0,
                       "limit rate filter module cleanup conn: %d", node->conn);

        if (node->conn == 0) {
            ngx_queue_remove(&node->queue);
            ngx_rbtree_delete(&ctx->sh->rbtree, &node->sn.node);
            ngx_slab_free_locked(ctx->shpool, node);
        }
    }

    ngx_shmtx_unlock(&ctx->shpool->mutex);
}


static void *
ngx_http_limit_rate_filter_alloc_lru(ngx_http_limit_rate_filter_ctx_t *ctx, size_t len)
{
    ngx_http_limit_rate_filter_node_t    *node;
    ngx_queue_t                          *q;
    ngx_uint_t                            i;
    ngx_int_t                             n;
    void                                 *tmp;


    tmp = ngx_slab_alloc_locked(ctx->shpool, len);
    if (NULL == tmp) {
        for (i = 0; i < 5 && tmp == NULL; i++) {
            if (ngx_queue_empty(&ctx->sh->queue)) {
                break;
            }

            q = ngx_queue_last(&ctx->sh->queue);
            node = ngx_queue_data(q, ngx_http_limit_rate_filter_node_t, queue);
            n = ngx_http_limit_rate_filter_free_rq(ctx, node, NULL);
            if (n != (ngx_int_t)node->conn) {
                ngx_abort();
            }
            ngx_queue_remove(q);
            ngx_rbtree_delete(&ctx->sh->rbtree, &node->sn.node);
            ngx_slab_free_locked(ctx->shpool, node);

            tmp = ngx_slab_alloc_locked(ctx->shpool, len);
        }
    }

    return tmp;
}


static ngx_int_t
ngx_http_limit_rate_filter_handler(ngx_http_request_t *r)
{
    uint32_t                                  hash;
    ngx_str_t                                 key;
    ngx_pool_cleanup_t                       *cln;
    ngx_http_variable_value_t                *vv;
    ngx_http_limit_rate_filter_ctx_t         *ctx;
    ngx_http_limit_rate_filter_node_t        *node;
    ngx_http_limit_rate_filter_conf_t        *lrfcf;
    ngx_http_limit_rate_filter_cleanup_t     *lrfln;
    ngx_http_limit_rate_filter_req_queue_t   *req;


    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "limit rate filter module preaccess handler start");

    lrfcf = ngx_http_get_module_loc_conf(r, ngx_http_limit_rate_filter_module);
    if (NULL == lrfcf) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "ngx_http_get_module_loc_conf ngx_http_limit_rate_filter_module return null");
        return NGX_ERROR;
    }

    if (NULL == lrfcf->shm_zone || lrfcf->index == NGX_CONF_UNSET) {
        return NGX_DECLINED;
    }

    if (0 == lrfcf->conn && 0 == lrfcf->rate) {
        return NGX_DECLINED;
    }

    ctx = lrfcf->shm_zone->data;
    if (NULL == ctx) {
        return NGX_DECLINED;
    }

    vv = ngx_http_get_indexed_variable(r, lrfcf->index);
    if (NULL == vv || vv->not_found) {
        return NGX_DECLINED;
    }

    if (vv->len == 0) {
        return NGX_DECLINED;
    }

    if (vv->len > 1024) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "the value of the \"%V\" variable "
                      "is more than 1024 bytes: \"%v\"",
                      &lrfcf->var, vv);
        return NGX_DECLINED;
    }

    key.len  = vv->len;
    key.data = ngx_pnalloc(r->pool, vv->len);
    if (key.data == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(key.data, vv->data, vv->len);

    cln = ngx_pool_cleanup_add(r->pool, sizeof(ngx_http_limit_rate_filter_cleanup_t));
    if (cln == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "ngx_pool_cleanup_add return null");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    hash = ngx_crc32_short(key.data, key.len);
    ngx_shmtx_lock(&ctx->shpool->mutex);

    node = (ngx_http_limit_rate_filter_node_t *)
            ngx_str_rbtree_lookup(&ctx->sh->rbtree, &key, hash);

    if (NULL == node) {
        node = (ngx_http_limit_rate_filter_node_t *)
                ngx_http_limit_rate_filter_alloc_lru(ctx,
                sizeof(ngx_http_limit_rate_filter_node_t) + key.len);

        if (NULL == node) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "ngx_http_limit_rate_filter_alloc_lru return null");
            goto done;
        }

        ngx_queue_init(&node->req_queue);
        node->sn.node.key = hash;
        ngx_memcpy(&node->key, key.data, key.len);
        node->sn.str.len  = key.len;
        node->sn.str.data = &node->key;
        node->conn        = 1;
        node->start       = r->start_sec;

        req = (ngx_http_limit_rate_filter_req_queue_t *)
               ngx_http_limit_rate_filter_alloc_lru(ctx, sizeof(ngx_http_limit_rate_filter_req_queue_t));
        if (NULL != req) {
            req->r = r;
            ngx_queue_insert_tail(&node->req_queue, &req->rq);
        }

        ngx_rbtree_insert(&ctx->sh->rbtree, &node->sn.node);
        ngx_queue_insert_head(&ctx->sh->queue, &node->queue);

    } else {
        if (lrfcf->conn > 0 && node->conn >= lrfcf->conn) {
            ngx_shmtx_unlock(&ctx->shpool->mutex);
            ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
                          "limiting connetions by zone \"%V\"", &lrfcf->shm_zone->shm.name);
            return lrfcf->status_code;
        }

        req = (ngx_http_limit_rate_filter_req_queue_t *)
               ngx_http_limit_rate_filter_alloc_lru(ctx, sizeof(ngx_http_limit_rate_filter_req_queue_t));
        if (NULL != req) {
            node->conn += 1;
            req->r = r;
            ngx_queue_insert_tail(&node->req_queue, &req->rq);
        }

        ngx_queue_remove(&node->queue);
        ngx_queue_insert_head(&ctx->sh->queue, &node->queue);
    }

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "limit rate filter module handler conn: %d", node->conn);

    cln->handler = ngx_http_limit_rate_filter_cleanup;
    lrfln = cln->data;

    lrfln->shm_zone = lrfcf->shm_zone;
    lrfln->key      = key;
    lrfln->r        = r;


done:
    ngx_shmtx_unlock(&ctx->shpool->mutex);

    if (lrfcf->rate > 0 && r->limit_rate == 0) {
        r->limit_rate = lrfcf->rate;
    }


    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "limit rate filter module preaccess handler end");

    return NGX_DECLINED;
}


static char *
ngx_http_limit_rate_filter_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{

    size_t                              size;
    ngx_str_t                           name;
    ngx_str_t                          *value;
    ngx_shm_zone_t                     *shm_zone;
    ngx_http_limit_rate_filter_ctx_t   *ctx;
    ngx_http_limit_rate_filter_conf_t  *lrfcf;


    value = cf->args->elts;
    size  = ngx_parse_size(&value[1]);
    lrfcf = conf;

    if (NULL != lrfcf->shm_zone) {
        return "is duplicate";
    }

    ctx = ngx_pcalloc(cf->pool, sizeof(ngx_http_limit_rate_filter_ctx_t));
    if (NULL == ctx) {
        return NGX_CONF_ERROR;
    }

    ngx_str_set(&name, "limit_rate_filter_cache_shm_zone");

    shm_zone = ngx_shared_memory_add(cf, &name, size, &ngx_http_limit_rate_filter_module);
    if (NULL == shm_zone) {
        return NGX_CONF_ERROR;
    }

    shm_zone->init  = ngx_http_limit_rate_filter_ctx_init_zone;
    shm_zone->data  = ctx;
    lrfcf->shm_zone = shm_zone;

    return NGX_CONF_OK;
}


static char *
ngx_http_limit_rate_filter_conn(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                             var;
    ngx_str_t                            *value;
    ngx_http_limit_rate_filter_conf_t    *lrfcf;

    lrfcf = conf;
    value = cf->args->elts;

    if ('$' != value[1].data[0]) {
        return NGX_CONF_ERROR;
    }

    if (NGX_CONF_UNSET != lrfcf->index) {
        return "is duplicate";
    }

    lrfcf->var = value[1];

    var.data = value[1].data + 1;
    var.len  = value[1].len -1;

    lrfcf->index = ngx_http_get_variable_index(cf, &var);
    if (NGX_ERROR == lrfcf->index) {
        ngx_log_error(NGX_LOG_ERR, cf->log, 0, 
              "key error, please check nginx.conf \"sae_limit_rate_conn\" ");
        return NGX_CONF_ERROR;
    }

    lrfcf->rate = ngx_parse_size(&value[2]);
    // if (lrfcf->rate < 0) {
    //     ngx_log_error(NGX_LOG_ERR, cf->log, 0, 
    //           "rate error, please check nginx.conf \"sae_limit_rate_conn\" ");
    //     return NGX_CONF_ERROR;
    // }

    lrfcf->conn = ngx_atoi(value[3].data, value[3].len);
    if (lrfcf->conn > 65536) {
        ngx_log_error(NGX_LOG_ERR, cf->log, 0, 
              "conn error, please check nginx.conf \"sae_limit_rate_conn\" ");
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_limit_rate_filter_ctx_init_zone(ngx_shm_zone_t *shm_zone, void *data)
{
    size_t                                   len;
    ngx_http_limit_rate_filter_ctx_t        *ctx;

    ngx_http_limit_rate_filter_ctx_t        *octx = data;

    ctx = shm_zone->data;

    if (NULL != octx) {
        ctx->sh     = octx->sh;
        ctx->shpool = octx->shpool;

        return  NGX_OK;
    }

    ctx->shpool = (ngx_slab_pool_t*) shm_zone->shm.addr;
    if (shm_zone->shm.exists) {
        ctx->sh = ctx->shpool->data;
        return NGX_OK;
    }

    ctx->sh = ngx_slab_alloc(ctx->shpool, sizeof(ngx_http_limit_rate_filter_shctx_t));
    if (NULL == ctx->sh) {
        return NGX_ERROR;
    }

    ctx->shpool->data = ctx->sh;

    ngx_rbtree_init(&ctx->sh->rbtree, &ctx->sh->sentinel,
                    ngx_str_rbtree_insert_value);
    ngx_queue_init(&ctx->sh->queue);

    len = sizeof(" in limit rate filter zone \"\"") + shm_zone->shm.name.len;
    ctx->shpool->log_ctx = ngx_slab_alloc(ctx->shpool, len);
    if (NULL == ctx->shpool->log_ctx) {
        return NGX_ERROR;
    }

    ngx_sprintf(ctx->shpool->log_ctx, " in limit rate filter zone \"%V\"%Z",
                &shm_zone->shm.name);

    return NGX_OK;
}


static ngx_uint_t
ngx_http_limit_rate_filter_sent(ngx_http_limit_rate_filter_node_t *node, ngx_http_request_t *r)
{
    ngx_uint_t                                 sum;
    ngx_queue_t                               *q, *next;
    ngx_http_limit_rate_filter_req_queue_t    *req;

    sum = 0;

    for (q = ngx_queue_next(&node->req_queue);
         q != ngx_queue_sentinel(&node->req_queue);
         q = next) {

        next = ngx_queue_next(q);
        req  = ngx_queue_data(q, ngx_http_limit_rate_filter_req_queue_t, rq);
        if (req->r == r) {
            req->sent = r->connection->sent;
        }
        sum += req->sent;
    }

    return sum;
}



static ngx_int_t
ngx_http_limit_rate_body_filter(ngx_http_request_t *r, ngx_chain_t *in)
{
    time_t                                sec, sec2;
    uint32_t                              hash;
    ngx_str_t                             key;
    ngx_int_t                             rate;
    ngx_uint_t                            sum;
    ngx_http_variable_value_t            *vv;
    ngx_http_limit_rate_filter_ctx_t     *ctx;
    ngx_http_limit_rate_filter_conf_t    *lrfcf;
    ngx_http_limit_rate_filter_node_t    *node;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "limit rate filter module body filter start a:%d", r==r->main);

    lrfcf = ngx_http_get_module_loc_conf(r, ngx_http_limit_rate_filter_module);
    if (lrfcf == NULL || lrfcf->rate == 0 || lrfcf->shm_zone == NULL || r->limit_rate == 0) {
        goto done;
    }

    ctx = lrfcf->shm_zone->data;
    if (ctx == NULL) {
        goto done;
    }

    vv = ngx_http_get_indexed_variable(r, lrfcf->index);
    if (NULL == vv || vv->not_found) {
        goto done;
    }

    key.len = vv->len;
    key.data = vv->data;

    hash = ngx_crc32_short(key.data, key.len);

    ngx_shmtx_lock(&ctx->shpool->mutex);

    node = (ngx_http_limit_rate_filter_node_t *)
            ngx_str_rbtree_lookup(&ctx->sh->rbtree, &key, hash);
    if (NULL == node) {
        r->limit_rate = lrfcf->rate;
        goto fail;
    }

    sum  = ngx_http_limit_rate_filter_sent(node, r);
    sec  = ngx_time() - node->start + 1;
    sec2 = ngx_time() - r->start_sec + 1;
    sec = (sec + sec2) / 2;
    rate = lrfcf->rate - sum / sec;
    rate = rate / node->conn + r->connection->sent / sec2;
    rate = rate > 0? rate: 1024;

    // if (sec2 > 120 && sec2+1 > sec && (size_t)rate < lrfcf->rate/3 && node->conn < 3) {
    //     rate = lrfcf->rate / 3;
    // }

    r->limit_rate = (size_t)rate > lrfcf->rate? lrfcf->rate: (size_t)rate;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "limit rate filter module handler conn: %ud limit: %ud", node->conn, r->limit_rate);

fail:
    ngx_shmtx_unlock(&ctx->shpool->mutex);

done:
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "limit rate filter module body filter done");
    return ngx_http_next_body_filter(r, in);
}


static ngx_int_t
ngx_http_limit_rate_filter_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
    if (NULL == h) {
        return NGX_ERROR;
    }

    *h = ngx_http_limit_rate_filter_handler;

    ngx_http_next_body_filter = ngx_http_top_body_filter;
    ngx_http_top_body_filter = ngx_http_limit_rate_body_filter;

    return NGX_OK;
}
