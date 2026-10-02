# 目标
添加一个StoragePolic: ModelScope。

# 参考
关于文件中ModelScope上的存储方式，参考 /home/van/github/rizxfrog/FileCabinet 

即：hash取文件的sha256，文件中modelscope上的存储路径是 `<namespace>/hash[:2]/hash[2:]` 例如 `00/24/780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547`

# 要求

你需要使用cloudreve的文件索引系统来构建目录树，通过文件hash可以从modelscope中定位到文件。

即目录结构树（包括文件名等文件meta信息）是存储在数据库中的，modelscope只负责储存文件内容。

配置项： 上传只能proxy,下载可以配置proxy或者direct。（参考FileCabinet下载文件direct的代码逻辑）