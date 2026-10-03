# ModelScope 存储策略

Cloudreve 的 ModelScope 存储策略把**文件内容**存放在魔搭（ModelScope）仓库中，文件名、
目录树等元数据保留在 Cloudreve 数据库。物理对象按内容 SHA-256 寻址。

适用代码：`pkg/filemanager/driver/modelscope/`。

## 文档

| 文档 | 内容 |
|---|---|
| [api.md](api.md) | 上游接口形状、请求/响应示例、实测结果 |
| [best-practices.md](best-practices.md) | 配置建议、上传/下载路径选择、验证清单 |
| [pitfalls.md](pitfalls.md) | 踩坑记录、状态码速查 |

## 一分钟速览

- **只存字节**：目录树与元数据在数据库，上游只有内容寻址的 blob。因此没有 `List`。
- **上传必须中转**：对象路径由内容摘要决定，读完前无法命名上传目标；且上传地址需认证。
- **下载可直链**：读取路由 302 到 CDN 签名 URL，无凭证即可取内容。
- **路径布局**：`<namespace>/<sha256[:2]>/<sha256[2:]>`，`namespace` 是两位数字的分片维度。
- **≤ 5 MiB 内联**：小文件 base64 内联进 commit，没有独立 URL，只能经服务端读取。

## 最容易踩的三个

1. **direct 上传不可行、direct 下载可行** —— 结论相反，别一并处理。见 [pitfalls.md](pitfalls.md#坑-1direct-上传走不通direct-下载可以)。
2. **HTTP/2 上传慢 3 倍** —— 存储传输层必须钉到 HTTP/1.1。见 [pitfalls.md](pitfalls.md#坑-4http2-上传慢约-3-倍性能坑)。
3. **文件读取路由没有 `repos/` 段** —— 写错会得到 421 而非 404。见 [pitfalls.md](pitfalls.md#坑-5文件读取路由不带-repos-段误用返回-421)。

## 主要配置项

| 字段 | 默认值 | 说明 |
|---|---|---|
| 接入点 | `https://www.modelscope.cn` | 必须 HTTPS origin |
| 仓库 ID | — | `owner/repo`，必须已存在且有写权限 |
| 访问令牌 | — | 上传必需，绝不下发浏览器 |
| 仓库类型 | `datasets` | `datasets` 或 `models` |
| 命名空间 | `00` | 两位数字，路径分片维度 |
| 修订版本 | `master` | 提交目标分支 |

上传中转由后端**强制开启**，不可配置。
