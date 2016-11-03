
## ngx_http_limit_rate_filter_module

动态限速模块

--------------------------------

**Syntax:** *limit_rate_zone_size size;*

**Default:** -

**Context:** *http*


定义模块共享内存的大小，支持lru.

**Syntax:** *limit_rate_conn key rate count;*

**Default:** -

**Context:** *location*

定义以某个key为单位，速度，最大连接数。key为单位变量

**Syntax:** *limit_rate_conn_status code;*

**Default:** -

**Context:** *http, server, location*

超过最大连接数返回码
