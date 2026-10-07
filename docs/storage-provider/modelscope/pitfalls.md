# 踩坑记录

本文档汇总 Cloudreve ModelScope 驱动在实现与联调中实际遇到的问题。每条区分：

- **【实测】** — 对真实 `modelscope.cn` / `lfs.modelscope.cn` 发起请求的结果（2026-10）。
- **【代码事实】** — 由实现确定的行为，可复现。
- **【观察】** — 来自早期 FileCabinet 实现的记录，数值为观察值而非契约。

---

## 坑 1：direct 上传走不通，direct 下载可以

这是最容易踩的一个，因为两者的结论**相反**。

### 上传：不可行【实测】

LFS batch 返回的 `href` 是
`https://lfs.modelscope.cn/api/v1/repos/{type}/{owner}/{repo}/blobs/{sha256}/{size}`——
**没有查询参数，不是预签名 URL**，它要求 `Authorization: Bearer` 加 `m_session_id` Cookie。

对一个未携带凭证的 PUT 请求，上游实际返回：

```
HTTP 400
{"Code":10030000001,"Message":"user not logged in","Success":false}
```

带着 token 的同一请求返回 `HTTP 200`。所以浏览器直传永远拿不到授权，**上传必须经服务端中转**。

> 关于状态码：上游给的是 **400 `user not logged in`**。早期 FileCabinet 实现在 direct
> 模式下会**主动拒绝**这种认证型目标，返回它自己的 `422` + 「请使用 UPLOAD_MODE=proxy」
> 提示（`ErrDirectUnsupported`），于是那个项目里看到的是 422。Cloudreve 驱动不再做这层
> 映射——`Token()` 直接返回「ModelScope storage requires relayed uploads」，策略层面强制
> 中转，因此不会走到浏览器直传。

### 下载：可行【实测】

读取路由对 LFS blob 返回 `302`，`Location` 指向 CDN 签名地址：

```
HTTP 302
Location: https://cdn-lfs-cn-1.modelscope.cn/prod/lfs-objects/31/7e/b69c1167...?auth_key=...
```

用该 URL **不带任何凭证**请求可以直接拿到内容：

```
HTTP 206  1024 bytes  Content-Range: bytes 0-1023/874188075
access-control-allow-origin: *
```

因此下载可以配置为直链（关闭「下载中转」），字节不经过 Cloudreve。

### 结论

| 方向 | 直连可行性 | 原因 |
|---|---|---|
| 上传 | **不可** | 上传地址需 Bearer + Cookie 认证 |
| 下载 | **可以** | 302 到自包含签名 URL |

**注意例外**：≤ 5 MiB 的内联对象没有独立 URL，即使开了直链下载也只能经服务端转发。

### 补充：403 出现在哪些场景

上面无凭证上传得到的是 400（`user not logged in`）。**403 是另一回事**——它表示「已认证但
无权操作」，典型是令牌缺少该仓库的写权限：

```
HTTP 403
{"Code":403,"Message":"no write permission to repository"}
```

驱动会把这个状态码与上游解释原样带进错误信息（`diagnostic_test.go` 钉住了这一行为），
因为上传路径会把驱动错误直接展示到管理面板和日志——没有这段正文，只能看到一句无法
排查的失败。

所以排查顺序是：

| 现象 | 判断 |
|---|---|
| PUT 上传返回 **400 `user not logged in`** | 请求没带凭证 → 走了浏览器直传（本不该发生） |
| PUT 上传返回 **403 `no write permission`** | 令牌无效、过期，或对该仓库无写权限 |
| 读取任意对象返回 **403** | 令牌对该仓库无读权限，或仓库类型/ID 配错 |

---

## 坑 2：旧删除端点已失效

```
DELETE {endpoint}/api/v1/repos/{type}/{repo}/repo
```

**返回 HTTP 400 `the current token no longer supports deletion operations`。**

替代方案是走 commit 的删除 action：

```json
{"action":"delete","path":"<objectPath>"}
```

该端点缺少 `Revision` 参数，这是早期实现建议 `datasets` 类型只用 `master` 的历史原因；
Cloudreve 改用 commit 删除 action 后已无此约束。

---

## 坑 3：含不存在路径的 commit 会被整包拒绝

给 commit 提交一个当前修订中不存在的路径，上游不会跳过它，而是**整包拒绝**：

```
commit rejected by repository policy
```

规避方式（`Delete()` 已实现）：删除前先逐个探测路径是否存在，只提交仍然存在的路径；
全部不存在时视为已达成目标态，直接返回成功，不发请求。

---

## 坑 4：HTTP/2 上传慢约 3 倍（性能坑）

Go 的 `http.Transport` 对 HTTPS 默认协商 HTTP/2，而 `lfs.modelscope.cn` 的 **HTTP/2 大包
上传吞吐只有 HTTP/1.1 的三分之一**。ModelScope 官方客户端使用 HTTP/1.1，所以会出现
「同样网络下别人 6 MiB/s、Cloudreve 只有 2 MiB/s」。

同一程序、同一主机、同一内容，仅切换协议【实测】。两种测法结果一致：

| 协议 | 直连存储（32 MiB） | 直连存储（256 MiB） | 经 Cloudreve 中转（256 MiB） |
|---|---|---|---|
| HTTP/2（Go 默认） | 2.01 MiB/s | — | 2.13 MiB/s |
| HTTP/1.1 | 5.52 MiB/s | 5.38 MiB/s | 5.38 MiB/s |

（直连列是绕过 Cloudreve、直接对上游做 PUT 的结果；最后一列是修复前后同一客户端的
端到端结果。Python 客户端走 HTTP/1.1，直连约 5.5–5.8 MiB/s，是参照上限。）

修复方式是把**存储传输层**钉到 HTTP/1.1（`NewClient()`）：

```go
storageTransport.ForceAttemptHTTP2 = false
storageTransport.TLSClientConfig = &tls.Config{NextProtos: []string{"http/1.1"}}
storageTransport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
```

要点：

- 只钉**存储**传输层。API 传输层保持默认 HTTP/2，元数据请求不受影响。
- **读取两个协议同速**，所以同一个传输层可以同时服务上传与下载，无需拆分。
- 三者缺一不可：仅 `ForceAttemptHTTP2 = false` 仍可能在 ALPN 中协商到 h2。

---

## 坑 5：文件读取路由不带 `repos/` 段，误用返回 421

- LFS / commit 路由：`repos/{repoType}/{repo}/...`
- 文件读取路由：`{repoType}/{repo}/repo?...` —— **没有 `repos/`**

把读取请求写成 `repos/` 形式，会被边缘镜像而非 API 处理，返回：

```
HTTP 421  mirror self-forwarded loop detected
```

这个错误很有迷惑性，看起来像网络问题，实际是路由写错。

---

## 坑 6：batch 省略对象 = blob 已存在

LFS batch 对已存在的 blob **不返回任何标志位**，而是把请求的 oid 从 `objects` 中省略：

```json
{"Code":200,"Data":{"objects":[],"reused-objects":[]}}
```

这是**成功**语义（"无需上传"），不是畸形响应。把它当错误处理会导致重复上传或误报失败。

**反过来的陷阱**：不能拿 batch 结果当本地存在性探测。上游按 `oid` + `size` 精确记忆——
用**错误的 size** 询问同一个 oid，会重新拿到上传地址。所以该结果只够用来「跳过 PUT」，
不足以证明本地存在或校验内容。

---

## 坑 7：revision 不能当条件写栅栏

`commit/{revision}` **只接受分支名**，任何 40 位 commit id 都被拒绝。因此无法用
「提交到指定 commit id 才成功」来实现乐观并发控制。

这意味着 ModelScope 的 mutation 缺少 fencing token，多写者并发时旧操作可能迟到覆盖新
操作。规避方式是**每个存储身份单一写者**（Cloudreve 的存储策略本身就是单点配置）。

---

## 坑 8：凭证隔离——哪些请求可以带 token

token 是仓库级凭证，泄露即可读写仓库。

| 目标 | 是否带 token |
|---|---|
| 配置的 ModelScope API origin | ✅ 必须 |
| 精确的 LFS 上传路由 `https://lfs.modelscope.cn/api/v1/repos/{type}/{repo}/blobs/{hash}/{size}` | ✅ 必须 |
| 其它任何存储/CDN 主机（含 `.aliyuncs.com`） | ❌ **绝不** |
| 跟随 302 后的存储读取 | ❌ **绝不** |

两个容易搞错的地方：

1. **主机白名单 ≠ 授权发凭证**。白名单只决定「能不能访问」，不决定「能不能带 token」。
   上传凭证只发给**完全匹配**的那一条 LFS 路由；非规范的 `.modelscope.cn` 上传路径直接拒绝。
2. **跟随重定向时不能继承凭证**。驱动为存储读取新建裸请求，不复制任何认证头。

另外，`m_session_id` Cookie 与 token 同值，所以「只发 Cookie 不发 Authorization」并不安全。

---

## 坑 9：文件名回显污染（`+` 与非法字符）

上游会把 `filename` 参数原样回显到 CDN 响应的 `Content-Disposition` 中。两个坑：

- 查询串编码里空格的 `+` 会被**逐字回显**成 `my+file.apk`，因此必须用 `%20` 而非 `+`。
- 路径分隔符、CR/LF、引号、NUL 等非法值需要回退为 digest，否则会注入到响应头。

驱动还删除 `filename` / `namespace` / `repository` / `revision` / `tag` 这些探针参数，
只保留下发所需的签名参数。

---

## 坑 10：内联对象没有直链

≤ 5 MiB 的对象以 base64 内联在 commit 里，不是 LFS blob，因此：

- `HasPublicSource` 返回 `false`。
- 无法生成直链，只能经服务端读取。
- 即使策略关闭了「下载中转」，这类对象也会自动回退为转发。

排查「为什么这个小文件不能直链」时，先确认大小是否 ≤ 5 MiB。

---

## 坑 11：签名直链不可撤销

`auth_key` 一经签发即成为 bearer 凭证：在它过期前，**任何人**持有该 URL 都能下载，
且无法通过本地删除/撤权使其失效。

- 不要对敏感文件启用直链下载。
- 需要即时撤权时只能使用中转模式。

`auth_key` 形如 `<ts>-<hex>-<n>-<hex>`，其中**首段是签发时间戳，不是过期时间**
（实测签发时刻与当前时间一致），所以无法从 URL 直接读出剩余有效期。早期实现观察到
有效期约 20–30 分钟【观察】，具体以上游实际值为准。

---

## 坑 12：编辑页有几个对 ModelScope 无效的开关

创建向导只暴露 ModelScope 真正支持的字段，但**编辑页是通用表单**，会按通用规则渲染一批
对该驱动无意义的选项：

| 显示项 | 实际行为 |
|---|---|
| 上传中转（`relay`） | 后端 `normalizePolicy()` **强制设为 `true`**，改不动 |
| 分块大小（`chunk_size`） | 驱动完全不读取，无效果 |
| 缩略图 / 媒体元数据 | 驱动 `Thumb` / `MediaMeta` 返回「未实现」，由 Cloudreve 本地生成器处理 |
| 加密 | 由 Cloudreve 本地处理，不经上游；**必须与内容寻址共存**，见坑 15 |

其中**上传中转不是可调项**：对象路径由内容摘要决定，读完内容前无法命名上传目标，所以中转
是结构性要求。在编辑页把它关掉不会有任何效果——保存后仍会被服务端改回 `true`。

排查「为什么我关了中转还是走服务器」时，先看这一条。

---

## 坑 13：已存在对象不传内容，就不重新校验

内容已在仓库中引用、或已存在于上游存储时，会话会被标记为 **prevalidated**，客户端
**不发送任何内容**，上传立即完成（884.6 MiB 的文件实测传输 **0 字节**）。

代价要说清楚：**这条路径不重新校验内容**。

设计依据是内容寻址的语义——对象的物理路径由摘要决定，而摘要来自客户端。服务端接受
「客户端持有该摘要即持有该内容」：同一份内容无论来自哪里，存到上游都解析为同一个
对象，因此重新传输一遍字节不会改变最终状态，只是白白消耗带宽。

需要注意的边界：

- **仅对超过内联阈值的对象生效**。≤ 5 MiB 的内容随 commit 内联提交，没有可跳过的传输。
- **仅当客户端提供 `client_hash` 时生效**。没有摘要就无法寻址，只能走正常接收路径。
- **探测失败不阻断上传**，退回正常传输并记录警告。

因此若部署上要求「每一份进入仓库的内容都必须被服务端逐字节校验」，这条路径与需求冲突，
需要另行处理。

---

## 坑 14：连续提交会被拒绝（提交频率限制）

同一仓库连续 commit 间隔太短时，上游会拒绝：

```
commit rejected by repository policy
```

【现象】批量上传（或上传后立刻删除）时尤为明显，表现为**整个上传失败**，而单文件上传正常。

**规避**：把存储策略的提交模式从「直接提交」改为「排队提交」或「窗口合并提交」。两种模式
互斥，只能选一种：

- **排队提交**：该仓库的每次 commit（上传与删除都算）都经专用队列 `modelscope_commit` 串行
  执行，并在相邻提交之间随机等待配置的间隔（默认 3～5 秒）。
- **窗口合并提交**：随机窗口（默认 3～5 秒）内到达的 commit 合并为一次仓库提交写入，请求数
  远少于逐个提交；调用方仍会等到其 action 真正写入仓库才返回。

要点：

- 串行是**必须**的，不只是限速：commit 是整仓库无栅栏的写操作（见坑 7），两个在途 commit
  可能互相覆盖，只有逐个执行结果才确定。
- 排队间隔从**上一次提交结束**时开始计算，慢提交不会让下一次立刻紧跟。
- 队列可见于「参数设置 → 队列 → 魔搭提交」，可单独调整工作线程数等；线程数 > 1 也不会破坏
  间隔。
- 关闭该模式时，对应的区间/窗口值不参与校验，也不产生任何等待。
- 两者同时开启会被 `service/admin/policy.go` 的 `normalizePolicy()` 拒绝（保存失败）；若历史
  数据中同时开启，驱动按排队提交处理。

**排查**：若开启后仍然偶发相同报错，检查是否有**另一个进程/策略**在写同一仓库——间隔只在
单个 Cloudreve 进程内生效，多实例共用一个仓库时应改用不同 `namespace` 分片。

---

## 坑 15：文件加密与内容寻址冲突（已修复）

ModelScope 的对象路径由内容 SHA-256 决定，因此客户端提供的 `client_hash` 既用来寻址，也用来
校验。而**加密会改变存储实际收到的字节**，两者直接冲突。

### 失败形态【实测】

客户端对**原文件**算摘要，服务端加密后才交给驱动（`manager/upload.go` 的 `m.Upload` 用
cryptor 包裹 `req.File`），驱动按声明摘要寻址并校验收到流：

```
content does not match the declared hash:
  declared 056f7f66…  （明文摘要）
  computed 77677bc7…  （密文摘要）
```

只要策略同时开启「加密」和上传（relay），**大文件（走 `putStreamed`）与小文件（走
`putInlineStreamed`）都会失败**，上传 100% 报错。

### 更危险的是静默损坏路径

同一份内容**先以明文传过**、之后才打开加密时，不会报错，而是产生错误的数据：

1. 客户端仍发送**明文**摘要；
2. `prevalidateDigest` 用它探测，命中已存在的**明文对象**；
3. 会话被标记 prevalidated，客户端**不发内容**；
4. `CommitReference` 写入指向**明文对象**的指针；
5. 但 entity 带上 `encrypt_metadata` → 之后读取时对明文解密 → **得到垃圾，全程无报错**。

触发条件：加密开关**由关变开** + 之前传过相同内容 + 文件 > 5 MiB。「只对新增文件有效」的
语义本身就意味着管理员会来回切这个开关。

### 修复

`dbfs.PrepareUpload` 在生成加密元数据后**清空 `ClientHash`**（`clientHashForUpload`）：
加密上传不再声明摘要，驱动退回缓冲路径，自行对**实际收到的字节**（密文）计算摘要并据此
寻址——自洽且永远正确。

代价：加密上传失去「客户端算哈希、免缓冲」的流式优化，回到中文档描述的一次磁盘缓冲。
两者不能兼得，因为摘要必须描述**存储真正持有的字节**。

要点：

- 开关**只影响新上传**；已按明文摘要入库的对象不受影响，也不需要迁移。
- 清空发生在 `PrepareUpload` 内部，**早于** `CreateUploadSession` 里的 prevalidation 探测，
  因此损坏路径一并堵住。
- 未加密上传不受影响，摘要照常使用。
- 升级前若已踩中损坏路径，那些 entity 指向的是**明文对象却标着已加密**，修复不会回溯改写
  它们；需要重新上传才能恢复正确内容。

---

## 附：状态码速查

| 状态码 | 出现场景 | 来源 |
|---|---|---|
| 400 | 无 token 上传 `user not logged in` | 【实测】上游 |
| 400 | 旧删除端点 `no longer supports deletion operations` | 【实测】上游（记录自 FileCabinet） |
| 400 | `commit rejected by repository policy`（含不存在路径） | 【观察】FileCabinet |
| 401 / 403 | 令牌无权限时出现。403 原文示例：`no write permission to repository`。Cloudreve 侧对非 2xx 一律包装为 `ErrUpstream`，不区分是否可重试——重试策略由上层决定 | 403 正文来自 `diagnostic_test.go` 固定样例；【代码事实】 |
| 404 | 对象不存在（`Exists` 返回 false）；删除 commit 收到 404 视为成功 | 【代码事实】 |
| 421 | 读取路由误用 `repos/` 前缀 → `mirror self-forwarded loop detected` | 【代码事实】 |
| 429 | 限流。Cloudreve 侧同样归入 `ErrUpstream`，无自动退避 | 【代码事实】 |
| 302 | 下载路由重定向到 CDN 签名地址 | 【实测】 |
| 206 | 无凭证访问签名 URL 成功（Range 请求） | 【实测】 |
