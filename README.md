# DFSVS — 分布式文件存储与版本控制系统

> **本副本为「缺陷注入与修复题」基准版本**：在完整可运行系统之上注入了
> 10 个跨层后端缺陷（5 地狱 + 5 困难），对应 `BUG_TASKS.md` 中的 10 道修复题；
> 地狱级缺陷的候选注入位置见 `CANDIDATE_SITES.md`。
> 注入均为「结果错误但流程正常」型：服务可正常启动、页面可正常渲染、
> 接口不返回 5xx、无崩溃/死锁/未捕获异常；前端代码未被修改，仅作观察窗口。
> 若要获取无缺陷的干净实现，请按 `BUG_TASKS.md` 完成修复或对照题目逐项还原。

纯 Python（标准库，零第三方依赖）+ 原生 HTML/CSS/JS 实现的**教学级分布式文件系统**：
模拟 HDFS 风格的 NameNode / DataNode 集群（节点间全 HTTP 通信），
在其上叠加 Git 风格的版本控制（提交 / 分支 / 三方合并 / 检出），
并提供 11 个页面的管理控制台。

代码规模：**约 12,000 行**（后端 ~8,700 行 Python，前端 ~4,400 行 HTML/CSS/JS）。

---

## 1. 快速开始

```bash
# 依赖：Python 3.10+（仅标准库）；无 pip 安装步骤
python3 run.py                  # 启动 NameNode(:8020) + 3 个 DataNode(:8021-8023)，首次自动注入演示数据
python3 run.py --datanodes 4    # 4 个 DataNode
python3 run.py --reset          # 清空 data/ 后重建（重新注入种子数据）
python3 run.py --no-seed        # 空集群启动
```

浏览器打开 <http://127.0.0.1:8020/>（控制台会自动以 `admin/admin123` 登录）。

| 账号 | 口令 | 角色 |
|---|---|---|
| admin | admin123 | 管理员（含故障演练 / 用户 / 权限管理） |
| alice | alice123 | 运维员 |
| bob | bob12345 | 只读用户 |
| carol | carol123 | 运维员（已停用，演示账号状态位） |

把 DataNode 作为独立进程扩容（HTTP 自动注册进集群）：

```bash
python3 -m backend.datanode --id dn5 --port 8025
```

---

## 2. 前端页面（12 个，要求 10 个 + 仪表盘）

| 页面 | 文件 | 内容 |
|---|---|---|
| 仪表盘 | `index.html` | KPI / 容量水位 / 最近提交 / 事件流 / 热点 TOP |
| 文件浏览 | `files.html` | 目录树 + 缩略图网格 + 面包屑 + 块/副本/**EC 分片**详情抽屉 + 文本预览 |
| 上传下载 | `transfer.html` | 分块上传（分片可视化、暂停/续传/混沌模式）、Range 分段下载（断点续传、sha256 校验、副本命中统计） |
| 版本历史 | `versions.html` | 提交时间线（泳道）、分支管理、提交/合并/检出、冲突展示、文件级历史与回滚 |
| 差异对比 | `diff.html` | 版本 diff + 文本 diff 双模式、Myers/Patience/difflib 选择、unified/双栏视图、行内字符级高亮、大文件性能试验台 |
| 节点状态 | `nodes.html` | 节点卡片（心跳/容量/IO/版本向量）、块×节点副本/**EC 分片**矩阵、恢复队列（含分片重建进度）、杀死/复活/注入损坏演练、实时事件流 |
| **冗余策略** | `redundancy.html` | **三副本 vs 纠删码（RS k+m）总览、按目录切换冗余方式、后台无损转换任务进度、EC 分片自动修复队列** |
| 存储统计 | `stats.html` | 容量 donut、副本数分布、块大小直方图、24h 吞吐、容量趋势、类型分布、热度榜（sparkline）、元数据文档表 |
| 用户管理 | `users.html` | 用户 CRUD、角色能力矩阵、活动会话与吊销 |
| 权限设置 | `permissions.html` | 路径前缀 ACL 规则编辑器、默认策略、**判定轨迹测试器** |
| 系统日志 | `logs.html` | 级别/来源/用户/关键字过滤、分页、展开详情、自动刷新、CSV 导出、清空 |
| 回收站 | `recycle.html` | 保留期倒计时、恢复 / 彻底删除 / 清空 |

共享样式 `frontend/css/app.css`（暗色设计系统），共享脚本 `frontend/js/app.js`
（令牌管理、API 封装、导航、吐司/弹窗、SVG 图表库）。

### 2.1 两种冗余方式：三副本 vs 纠删码

| | 三副本（rep，默认） | 纠删码（ec，Reed-Solomon） |
|---|---|---|
| 原理 | 整块复制 3 份 | 块拆 **k 个数据分片 + m 个校验分片**，分散到 k+m 个节点 |
| 空间开销 | 3× | (k+m)/k（如 RS(2,1)=1.5×、RS(2,2)=2×） |
| 容错 | 坏 2 份仍可读 | 任意 m 个分片损坏都能由其余分片 RS 解码还原 |
| 修复 | 拉取整份副本拷贝 | NameNode 取 k 个存活分片在线重建缺失/损坏分片 |
| 适用 | 热数据 | 冷归档、省空间 |

* 策略**按目录**设置并沿目录树就近继承（根默认三副本）；子目录可覆盖；
* 切换目录策略时可对已有文件做**后台转换**：写新冗余块 → 原子替换 inode 引用，
  旧块保留（历史版本可读、GC 宽限期后回收），**切换期间读写不中断**；
* 未转换的旧文件维持原方式 —— **同一目录下两种冗余并存、互不干扰**；
* 文件浏览/节点页可查每个文件的冗余方式、每个分片落在哪个节点及自动修复进度；
* RS 编解码见 `backend/reed_solomon.py`（GF(2^8) Cauchy 系统 MDS 码，纯标准库）。

---

## 3. 架构

```
┌──────────────────────────── 浏览器（11 页面）────────────────────────────┐
│  fetch /api/*（JSON）· /api/download（Range）· /api/thumbnail           │
└───────────────────────────────────┬──────────────────────────────────────┘
                                    │ HTTP（Bearer 令牌 + 路径 ACL）
┌───────────────────────────────────▼──────────────────────────────────────┐
│  NameNode :8020 （backend/namenode.py + http_server.py）                  │
│   · 元数据 9 个 JSON 文档：fs/blocks/versions/users/perms/                │
│     logs/recycle/stats/cluster   —— 原子写 + 版本向量                     │
│   · 块表（genstamp/校验和/副本位置）、EC 条带组（k+m 分片位置/重建）、
│     放置策略、恢复调度、GC、目录冗余策略与后台转换                          │
│   · 上传会话（断点续传暂存）、Range 读路径（副本轮询+故障转移）              │
│   · 版本树 VersionStore（提交/分支/merge/checkout）                        │
└──────┬───────────────────────────────────────────────────▲───────────────┘
       │ PUT /block（流水线复制 X-Forward-To）   心跳/块汇报/事件（JSON）
       │ GET /block（读、Range）                 恢复命令随心跳应答下发
       │                                         cluster 文档按版本向量拉取
┌──────▼───────────────────────────────────────────────────┴───────────────┐
│  DataNode :8021..:8024 （backend/datanode.py，可独立进程运行）              │
│   · blocks/<blk>.dat + node_state.json（原子写）                           │
│   · 心跳线程 / 全量块汇报线程 / 数据巡检(scrub)线程 / 持久化线程             │
│   · 流水线转发、拉取式再复制、静默损坏自发现                                 │
└───────────────────────────────────────────────────────────────────────────┘
```

元数据目录布局（`data/`，全部 JSON，崩溃安全）：

```
data/meta/{fs,blocks,versions,users,perms,logs,recycle,stats,cluster}.json
data/sessions/<upload_id>/piece_000000      # 上传分片暂存
data/datanodes/<node_id>/blocks/<blk>.dat   # 块本体
data/datanodes/<node_id>/node_state.json    # DN 索引（原子写）
data/datanodes/<node_id>/doc_cache/*.json   # DN 同步到的元数据文档
```

---

## 4. 五大难点的实现

### 4.1 分块策略与副本一致性
* **两种分块**（`chunking.py`）：固定 64KiB；CDC 内容定义分块
  （Buzhash 滚动哈希，O(1) 滑窗，min/avg/max 约束）。
  冒烟测试验证：头部插入 16B 后 CDC 11/12 块不变（去重友好），
  固定分块则几乎全部错位。
* **内容去重**：块表维护 `by_checksum` 索引，相同内容块直接复用。
* **流水线复制**：NN PUT → DN1 → DN2 → DN3（`X-Forward-To` 链式头），
  每一跳 sha256 校验；应答嵌套展平后登记副本（含每一跳的 ack）。
* **副本一致性**：每块 `genstamp` 单调递增；块汇报对账时
  未知块/旧 genstamp/校验和不符 → 下发删除命令；
  存活副本 < 期望 → 进入 under-replicated 队列。

### 4.2 节点故障检测与自动恢复
* 心跳 1.5s；>3.6s 标 SUSPECT，>6s 判 DEAD → 其上副本全部失效评估。
* 恢复调度线程每 2s：为缺副本块选「存活好副本(源) → 空闲节点(目标)」，
  命令随该节点下次心跳应答下发；目标 DN 用 HTTP 从源拉取块。
* **坏副本主动替换**：corrupt / stale 副本先下发删除并摘除记录，
  使该节点重新成为复制候选，保证队列可收敛（不会卡死）。
* 节点复活后强制全量块汇报对账；孤儿块（磁盘有、索引无）清理。
* **静默损坏**：DN scrub 线程抽样重算校验和；读路径 NN 侧二次校验 +
  副本故障转移；注入演练见 `POST /api/sim/corrupt`。

### 4.3 版本树冲突合并
* 提交 = 全量快照 + 块引用（块不可变 ⇒ 历史版本天然可读；
  GC 保护集 = 活动 inode ∪ 全部提交快照）。
* 合并：base = LCA；快照级三方状态机（单侧变更采纳 / 双侧同内容采纳 /
  增删冲突保留修改侧）；文本双侧异改 → 行级 diff3（见 4.4），
  冲突写 `<<<<<<< / ======= / >>>>>>>` 标记并提交为待解决状态；
  二进制冲突保留 ours 并记录。合并产生双亲 commit。
* 检出 = 快照物化回活动 inode 树；工作区脏时自动提交保护（不丢数据）；
  快进合并自动识别。

### 4.4 大文件差异对比性能（diff_engine.py）
* **Myers O(ND)**：贪心 + 轨迹回溯，带公共前缀/后缀裁剪；
  轨迹内存保护 D>900 自动降级。
* **Patience**：唯一行锚点 + LIS（O(n log n)）分治，
  锚点间小段回退 Myers / 大段 difflib。
* `auto`：小输入直接 Myers，大输入 Patience。
  实测 20k 行 / 5% 修改 ≈ 85ms（907 个差异段）。
* 三方合并 `merge3`：diff3 语义，闭区间重叠判定（含相邻零宽插入），
  重叠且不同 → 冲突块；相同 → 采纳其一。
* 渲染：unified / 双栏 / 行内字符级高亮；统计与计时返回前端展示。

### 4.5 JSON 元数据多节点同步：原子写 + 版本向量
* **原子写**（`util.atomic_write_*`）：同目录临时文件 → flush → fsync →
  `os.replace` → 目录 fsync；读者永远看到完整旧/新文件。
  块文件、分片暂存、DN 索引同样走原子写。
* **版本向量**：每个文档 `vv = {node_id: 逻辑时钟}`，本地 `touch()` 推进分量；
  DN 心跳携带 `doc_vv`，NN 比较后返回 `pull_docs`；
  DN 拉取信封按 `vv_compare` 判定：
  after→采纳 / before|equal→跳过 / **concurrent→冲突**（cluster 文档以 NN 为权威，
  合并 vv 并留 `doc_synced(conflict-nn-wins)` 审计事件）。
* **锁序纪律**：全局固定 meta → node → health → cmd，
  心跳注册/复活等路径在锁外执行副作用，避免 ABBA 死锁。

### 4.6 纠删码：数据分片 + 校验分片（reed_solomon.py + namenode.py）
* **编码**：块按 k 等分成数据分片 D0..D{k-1}（末片零填充到统一条带 stripe），
  用 `[单位阵 I | Cauchy 校验矩阵]` 点乘 GF(256) 条带生成 m 个校验分片；
  Cauchy 构造保证**任意 k 行都可逆（MDS）** ⇒ 任意 m 个分片损坏可还原。
* **重建**：任取 k 个存活分片（含校验分片），对应 k×k 子矩阵 GF(256)
  高斯-约旦求逆 → 解出全部数据分片 → 再点乘编码矩阵行得到缺失分片。
  `reed_solomon.py` 对 (2,1)/(2,2)/(3,2)/(6,3) 等方案枚举过所有
  「恰好 k 个存活分片」组合验证还原一致。
* **放置**：k+m 个分片各 PUT 到不同节点（沿用跨机架/剩余空间排序），
  节点不足 k+m 时缺失分片临时与其它分片同节点放置，节点扩容/复活后
  由**重平衡**（MRV 完美匹配）迁回独立节点并清理多余副本。
* **读路径**：优先取 k 个数据分片直拼（免编码），含校验分片或缺片时
  RS 重建；逐分片 sha256 校验，失败自动故障转移到同名其它分片。
* **自动修复**：块汇报/scrub/读校验发现分片损坏或节点故障导致存活分片
  < k+m（但 ≥ k）即进入退化队列，NameNode 调度工作线程在线重建并 PUT
  到新节点，页面可查每组 `finished/total` 进度；< k 标记不可解码，
  等节点复活（磁盘分片经块汇报重新挂上）后恢复。
* **目录切换**：冗余策略记在目录 inode 上、子树就近继承；转换任务
  「读旧块→按新冗余写新块→原子换引用」，旧块保留给版本快照与 GC 宽限，
  全程读写不中断；新文件按新策略、未转旧文件维持原方式（同目录并存）。
* **对账一致性**：分片经全量块汇报对账时，连续两次汇报缺失才从 NN 摘除
  （避免 PUT 与汇报快照并发把刚落盘的好分片误判丢失），损坏/孤儿分片
  下发删除；GC 同时回收三副本块与 EC 组分片。

---

## 5. REST API 摘要（节选）

```
POST /api/auth/login|logout      GET /api/auth/me|sessions
GET  /api/fs/tree|list|stat      POST /api/fs/mkdir|rename|move|delete
GET  /api/thumbnail|file/preview|file/blocks
POST /api/upload/begin|chunk|complete      GET /api/upload/status|sessions
GET  /api/download/info|download(Range)
GET  /api/version/branches|commits|graph|diff|working_diff|file_at|history|stats
POST /api/version/commit|branch|branch_delete|checkout|merge|restore|diff_text
GET  /api/nodes|nodes/blocks|nodes/matrix|nodes/block_paths
GET  /api/health/queue           GET /api/sim/events
GET  /api/redundancy/overview|dirs
POST /api/redundancy/policy      GET /api/redundancy/conversions|conversion/<id>
POST /api/sim/kill|revive|corrupt|chaos                （admin）
GET  /api/stats/overview|hotness|timeline
GET|POST /api/users  PUT|DELETE /api/users/<name>      （user_admin）
GET|POST /api/perms  PUT|DELETE /api/perms/<id>        （perm_admin）
POST /api/perms/check
GET  /api/logs|logs/export       POST /api/logs/clear  （admin）
GET  /api/recycle                POST /api/recycle/restore|purge|empty
POST /internal/heartbeat|block_report      GET /internal/meta/<doc>   （集群密钥）
```

---

## 6. 目录结构

```
gsb4/
├── run.py                     # 启动入口
├── backend/
│   ├── config.py              # 全部可调参数（块大小/副本/心跳/保留期…）
│   ├── util.py                # 原子写/版本向量/HTTP 客户端/LRU/环形缓冲
│   ├── chunking.py            # 固定 + CDC 分块、清单、重组校验
│   ├── reed_solomon.py        # GF(2^8) Cauchy RS 纠删码：编码/任意 k 分片重建
│   ├── diff_engine.py         # Myers / Patience / merge3 / 渲染器
│   ├── metadata.py            # JSON 文档仓库（原子写 + vv 同步语义）
│   ├── auth.py                # 用户/口令/会话 + 路径 ACL
│   ├── filesystem.py          # inode 树 + 回收站
│   ├── versioning.py          # 提交/分支/合并/检出/GC 引用集
│   ├── namenode.py            # 块表/EC 条带组/放置/心跳/恢复/重建/冗余转换/上传下载/统计/GC
│   ├── datanode.py            # 块存储/心跳/汇报/scrub/流水线/文档同步
│   ├── http_server.py         # 路由 + 静态页 + 鉴权中间件
│   ├── seed.py                # 演示数据（含冲突合并场景、EC 冷归档目录）
│   └── main.py                # 集群装配
├── frontend/                  # 12 页面 + css/app.css + js/app.js
└── tests/smoke_test.py        # 97 项端到端断言
```

