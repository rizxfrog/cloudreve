# ModelScope API 参考

本文档记录 Cloudreve ModelScope 驱动实际使用的上游接口。所有形状以
`pkg/filemanager/driver/modelscope/client.go` 的实现为准；标 **【实测】** 的条目是对
`www.modelscope.cn` / `lfs.modelscope.cn` 真实发起的请求结果（2026-10，`datasets` 类仓库）。

约定：

- 认证 = `Authorization: Bearer <token>` **加上** Cookie `m_session_id=<token>`。
  两者缺一不可，见 `client.go` 的 `auth()`。
- API origin = 配置的 `endpoint`（默认 `https://www.modelscope.cn`）。
- 存储 origin = `lfs` / CDN 域名，**永远不带凭证**。
- 响应统一是双层信封 `{"Code":200,"Success":true,"Data":{...}}`。

---

## 1. LFS batch：申请 blob 上传地址

```
POST {endpoint}/api/v1/repos/{repoType}/{repo}/info/lfs/objects/batch
Content-Type: application/json
Authorization: Bearer <token>
Cookie: m_session_id=<token>

{"operation":"upload","objects":[{"oid":"<sha256>","size":<int>}]}
```

实现：`Validate()`，路由 `repos/{repoType}/{repo}/info/lfs/objects/batch`。

### 1.1 blob 不存在 → 返回上传地址【实测】

```
HTTP 200
{"Code":200,"Data":{"objects":[{"oid":"<sha256>","size":<int>,
  "actions":{"upload":{"href":"https://lfs.modelscope.cn/api/v1/repos/datasets/<owner>/<repo>/blobs/<sha256>/<size>",
  "offset":0,"upload_header":{"Range":"bytes=0-"},"upload_parameters":{}}}}]}}
```

- `href` **没有查询参数**，不是预签名 URL，是仍需认证的 API 地址。
- `upload_header` 恒为 `{"Range":"bytes=0-"}`；驱动断言它必须恰好是这一个键值，否则拒绝
  （`client.go` 的 `Validate` 内 upload_header 校验）。
- `header`、`upload_parameters` 必须为空，`offset` 必须为 0，否则返回 `ErrUpstream`。

### 1.2 blob 已存在 → 省略该对象

ModelScope **不返回任何标志位**，而是把请求的 oid 从 `objects` 中省略：

```
HTTP 200
{"Code":200,"Data":{"objects":[],"reused-objects":[]}}
```

驱动把「oid 不在 `objects` 中」解释为「已存在」，返回空 target，调用方跳过 PUT 只提交指针
（`Validate()` 末尾注释）。空列表是**成功**语义，不是畸形响应。

### 1.3 错误分支

| 上游返回 | 驱动行为 |
|---|---|
| `objects[i].error` 非 null | `ErrUpstream`，附上游 error 内容 |
| `objects[i].size` 与请求不符（且非 0） | `ErrUpstream`，报告双方尺寸 |
| `Code != 200` / `Success == false` | `decodeEnvelope` 判定失败 |
| `href` 非白名单主机或非规范路由 | `ErrUpstream`（拒绝跟随） |

> **注意**：不能用 batch 结果当本地存在性探测。上游按 `oid`+`size` 精确记忆，用**错误的
> size** 询问同一 oid 会重新拿到上传地址。

---

## 2. 上传 blob

```
PUT https://lfs.modelscope.cn/api/v1/repos/{repoType}/{repo}/blobs/{sha256}/{size}
Content-Type: application/octet-stream
Content-Length: <size>
Authorization: Bearer <token>
Cookie: m_session_id=<token>
```

实现：`Put()`。`ContentLength` 显式设置，避免 chunked 编码。

| 情况 | 结果 |
|---|---|
| 无 token | **HTTP 400** `{"Code":10030000001,"Message":"user not logged in","Success":false}` 【实测】 |
| 带 token | **HTTP 200** `{"Code":200,"Data":{},"Message":"success","Success":true}` 【实测】 |

成功时返回信封，驱动要求认证 PUT **必须**返回可解析且确认成功的信封（空体不算成功）。

---

## 3. 提交（commit）：写指针 / 内联文件 / 删除

```
POST {endpoint}/api/v1/repos/{repoType}/{repo}/commit/{revision}
Content-Type: application/json
Authorization: Bearer <token>
Cookie: m_session_id=<token>

{"commit_message":"Cloudreve store","actions":[<action>, ...]}
```

实现：`Commit()`，revision 经 `url.PathEscape`。

### 3.1 action 形状

**LFS 指针**（大文件，blob 已单独 PUT）：

```json
{"action":"create","path":"00/ab/cdef...","type":"lfs",
 "size":123,"sha256":"<digest>","content":"","encoding":""}
```

**内联文件**（≤ 5 MiB，内容随提交同行）：

```json
{"action":"create","path":"00/ab/cdef...","type":"normal",
 "size":123,"sha256":"","content":"<base64>","encoding":"base64"}
```

**删除**：

```json
{"action":"delete","path":"00/ab/cdef..."}
```

### 3.2 约束

- **含不存在路径的 commit 会被整包拒绝**，错误为 `commit rejected by repository policy`。
  因此删除前必须先探测路径存在性，只提交仍存在的路径；全部不存在时视为已达目标态，
  不发请求（`Delete()`）。
- `actions` 为空时 `Commit` 直接返回，不发请求。
- **revision 不能当条件写栅栏**：`commit/{revision}` 只接受分支名，任何 40 位 commit id
  都被拒。

---

## 4. 读取仓库文件

```
GET {endpoint}/api/v1/{repoType}/{repo}/repo?Revision={revision}&FilePath={path}
Authorization: Bearer <token>
Cookie: m_session_id=<token>
```

实现：`startRead()` / `OpenRead()` / `DownloadTarget()`。

> **路径里没有 `repos/` 段。** LFS 与 commit 路由是 `repos/{type}/{repo}/...`，而文件读取是
> `{type}/{repo}/repo`。误用 `repos/` 形式会被边缘镜像以
> **HTTP 421 `mirror self-forwarded loop detected`** 回应，而不是到达 API。
> 详见 `client.go` 中 `startRead` 的注释。

| 上游响应 | 含义 | 驱动行为 |
|---|---|---|
| `2xx` + body | 内联文件，内容直接返回 | 直接作为流；`pos > 0` 时丢弃前 `pos` 字节 |
| `302` | LFS blob，重定向到存储 | 校验 `Location` 在白名单 + 路径含 digest，再**不带凭证**请求 |
| `404` | 对象不存在 | `Exists()` 返回 `false` |
| 其它 | 错误 | `upstreamStatus()` 带上正文摘要 |

### 4.1 签名直链【实测】

`DownloadTarget()` 只发 API 请求、不跟随重定向，返回 302 的 `Location`：

```
HTTP 302
Location: https://cdn-lfs-cn-1.modelscope.cn/prod/lfs-objects/31/7e/b69c1167...?auth_key=1791004592-b4bd5ec0...-0-1794471b...&filename=7eb69c11...&namespace=Romal1223&repository=test01&revision=51df87d8...&tag=dataset
```

处理要点：

- 只保留 `auth_key`；`filename` / `namespace` / `repository` / `revision` / `tag`
  是探针参数，驱动会**删除**它们后再下发。
- `auth_key` 形如 `<ts>-<hex>-<n>-<hex>`，**首段是签发时间戳，不是过期时间**。
- 路径中 digest 被上游拆成多段目录，因此校验时先去掉 `/` 再比对。
- 用该 URL 无凭证请求可成功：**HTTP 206**，且带 `access-control-allow-origin: *`【实测】。

---

## 5. 旧删除端点（已失效）

```
DELETE {endpoint}/api/v1/repos/{repoType}/{repo}/repo
```

**返回 HTTP 400 `the current token no longer supports deletion operations`。**

不可用，必须改用第 3 节的 `{"action":"delete"}` commit。该端点也缺少 `Revision` 参数，
这是早期实现建议 `datasets` 只用 `master` 的原因。

---

## 相关实现位置

| 功能 | 位置 |
|---|---|
| 全部接口实现 | `pkg/filemanager/driver/modelscope/client.go` |
| 驱动上层流程 | `pkg/filemanager/driver/modelscope/modelscope.go` |
| 物理路径映射 | `ObjectPath()` / `HashFromObjectPath()` |
| 策略校验与默认值 | `service/admin/policy.go` 的 `normalizePolicy()` |
| 传输层配置 | `NewClient()` 中的 `apiTransport` / `storageTransport` |
