# -*- coding: utf-8 -*-
"""
namenode.py — NameNode：元数据与集群协调核心
================================================
职责（对应五大难点）：
  * 块表管理：block -> {size, checksum, genstamp, desired, replicas:{node:{...}}}
    副本状态机 ok / corrupt / stale，genstamp 单调递增防止旧副本复活；
  * 副本放置策略：跨机架优先 + 剩余空间优先 + 负载扰动（难点一）；
  * 写路径：分块（chunking）-> 按校验和去重 -> 流水线复制
    （PUT dn1 -> dn1 转发 dn2 -> ... 每跳校验 sha256）；
  * 读路径：副本轮询 + 校验失败自动故障转移（难点一/二）；
  * 心跳管理：注册节点、判活（HEARTBEAT_TIMEOUT）、处理事件
    （replicate_done / corrupt / deleted / doc_synced ...）、下发命令；
  * 块汇报对账：全量比对 DN 库存与块表，未知/过期/损坏块下发删除，
    缺失副本进入恢复队列（难点二：故障检测与自动恢复）；
  * 恢复调度：under-replicated 队列 -> 选择存活源副本 -> 通过心跳命令
    让目标节点 HTTP 拉取复制；
  * 分块上传会话（断点续传）与 Range 下载；
  * GC：引用集 = 活动 inode ∪ 全部提交快照 ∪ 进行中会话，
    未引用块过宽限期后下发删除（难点三配套）；
  * 热度/容量统计（存储统计页数据源）；
  * 元数据文档：MetadataStore（原子写 + 版本向量），cluster 文档
    向 DataNode 同步（难点五）；
  * 审计日志：内存 ring + logs 文档延迟刷盘。
"""

import json
import os
import random
import threading

from . import chunking, config
from . import reed_solomon as rs_code
from .auth import AuthManager, PermissionManager
from .filesystem import FsError, VirtualFS
from .metadata import MetadataStore
from .util import (HttpError, LRU, RateCounter, RingBuffer, b64e, gen_id,
                   guess_mime, hour_key, http_json, http_request,
                   is_text_mime, needs_recovery, canonical_access_op,
                   now, parse_range, sha256_bytes, short_hash, split_multi,
                   vv_compare, vv_merge)
from .versioning import VersionStore


class NNError(Exception):
    pass


class MissingBlockError(NNError):
    pass


_MISSING = object()       # LRU 查询哨兵（区别于缓存的 None）


class NameNode:
    def __init__(self, host=None, port=None, data_dir=None, meta_dir=None,
                 cluster_key=None):
        self.host = host or config.HOST
        self.port = port or config.NAMENODE_PORT
        self.data_dir = data_dir or config.DATA_DIR
        self.meta_dir = meta_dir or config.META_DIR
        self.cluster_key = cluster_key or config.CLUSTER_KEY
        self.node_id = "namenode"
        self.started_at = now()

        os.makedirs(self.meta_dir, exist_ok=True)
        os.makedirs(config.SESSION_DIR, exist_ok=True)

        # ---- 元数据 ----
        self.meta = MetadataStore(self.meta_dir, node_id=self.node_id)
        self.fs = VirtualFS(self.meta)
        self.auth = AuthManager(self.meta)
        self.perms = PermissionManager(self.meta, self.auth)
        self.versions = VersionStore(self)

        # ---- 节点注册表（内存态；摘要持久化到 cluster 文档） ----
        self.nodes = {}                  # node_id -> NodeInfo dict
        self.node_lock = threading.RLock()

        # ---- 命令队列：随下次心跳下发 ----
        self.pending_commands = {}       # node_id -> [cmd]
        self.cmd_lock = threading.Lock()

        # ---- 健康状态 ----
        self.under_replicated = {}       # bid -> {"since": ts, "attempts": n}
        self.corrupt_replicas = {}       # (bid, node) -> info
        self.missing_blocks = set()      # 无任何存活好副本
        self.scheduled = {}              # bid -> {"src","dst","at"}
        # ---- 纠删码（EC）健康与重建状态 ----
        self.ec_degraded = {}            # gid -> {"since","attempts"}
        self.ec_missing = set()          # 存活分片 < k，暂时无法重建
        self.ec_repairs = {}             # gid -> 重建任务进度（供页面查询）
        self.health_lock = threading.RLock()

        # ---- 运行态 ----
        self.events = RingBuffer(config.EVENT_RING_SIZE)
        self.block_cache = LRU(maxsize=512, max_bytes=96 * 1024 * 1024)
        self.sessions = {}               # 上传会话
        self.session_lock = threading.RLock()
        self._rr_counter = 0
        self.api_rate = RateCounter(window=10)
        self.local_datanodes = {}        # 同进程 DN 实例（演练用）
        self.httpd = None
        self._threads = []
        self._stop = threading.Event()
        self.sim_chaos = False           # 前端"混沌模式"：上传随机失败

    # ==================================================================
    # 启动 / 停止
    # ==================================================================
    def start(self, with_http=True):
        self.fs.init_root()
        self.auth.ensure_seed()
        self.perms.ensure_seed()
        self.versions.ensure_head()
        self._init_blocks_doc()
        self._init_stats_doc()
        self.meta.start_flusher()

        for target, name in (
                (self._liveness_loop, "nn-liveness"),
                (self._recovery_loop, "nn-recovery"),
                (self._ec_repair_loop, "nn-ec-repair"),
                (self._convert_loop, "nn-convert"),
                (self._gc_loop, "nn-gc"),
                (self._stats_loop, "nn-stats"),
                (self._trash_loop, "nn-trash"),
                (self._session_gc_loop, "nn-session-gc")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

        if with_http:
            from .http_server import start_namenode_server
            self.httpd = start_namenode_server(self)
        self.log_event("INFO", "namenode", "start", self.node_id, "system",
                       f"NameNode 启动 @ {self.host}:{self.port}")
        self.emit("namenode_start", f"NameNode 启动 @ {self.host}:{self.port}")

    def stop(self):
        self._stop.set()
        if self.httpd:
            try:
                self.httpd.shutdown()
                self.httpd.server_close()
            except Exception:
                pass
        self.meta.stop()
        self.log_event("INFO", "namenode", "stop", self.node_id, "system",
                       "NameNode 停止")

    def _init_blocks_doc(self):
        with self.meta.lock:
            blocks = self.meta.get("blocks")
            blocks.setdefault("blocks", {})
            blocks.setdefault("by_checksum", {})     # 内容去重索引
            blocks.setdefault("groups", {})          # EC 条带组：gid -> 组信息
            blocks.setdefault("ec_by_checksum", {})  # EC 块内容去重索引
            blocks.setdefault("conversions", {})     # 目录冗余方式切换任务
            blocks.setdefault("next_genstamp", 1000)
            # 根目录默认冗余策略（目录可逐级覆盖）
            with self.fs.meta.lock:
                pass
            root = self.fs.get_inode("in_root")
            if root is not None and "redundancy" not in root:
                root["redundancy"] = config.DEFAULT_REDUNDANCY
                root["ec_profile"] = config.EC_PROFILE_DEFAULT
                self.meta.touch("fs", flush=False)
            self.meta.touch("blocks", flush=False)

    def _init_stats_doc(self):
        with self.meta.lock:
            stats = self.meta.get("stats")
            stats.setdefault("access", [])
            stats.setdefault("hourly", {})
            stats.setdefault("capacity_history", [])
            self.meta.touch("stats", flush=False)

    # ==================================================================
    # 日志 / 事件
    # ==================================================================
    def log_event(self, level, source, action, target, user, detail=""):
        entry = {
            "ts": now(), "level": level, "source": source, "action": action,
            "target": target or "", "user": user or "system",
            "detail": (detail or "")[:2000],
        }
        with self.meta.lock:
            logs = self.meta.get("logs")
            items = logs.setdefault("items", [])
            items.append(entry)
            if len(items) > config.LOG_MAX_ENTRIES:
                logs["items"] = items[-config.LOG_MAX_ENTRIES:]
            self.meta.touch("logs", flush=False)
        return entry

    def emit(self, kind, message, **data):
        """集群事件流（节点页实时展示）。"""
        self.events.append({"ts": now(), "kind": kind, "message": message,
                            **data})

    def query_logs(self, level=None, source=None, user=None, q=None,
                   limit=100, offset=0):
        if isinstance(level, str):
            level = split_multi(level, config.LOG_LEVEL_SEP) or None
        with self.meta.lock:
            items = list(self.meta.get("logs").get("items", []))
        items.reverse()
        if level:
            items = [i for i in items if i["level"] in level]
        if source:
            items = [i for i in items if i["source"] == source]
        if user:
            items = [i for i in items if i.get("user") == user]
        if q:
            ql = q.lower()
            items = [i for i in items
                     if ql in json.dumps(i, ensure_ascii=False).lower()]
        total = len(items)
        return {"total": total, "items": items[offset:offset + limit]}

    def clear_logs(self):
        with self.meta.lock:
            self.meta.get("logs")["items"] = []
            self.meta.touch("logs")

    # ==================================================================
    # 节点注册表 / 心跳（难点二：故障检测）
    # ==================================================================
    def handle_heartbeat(self, payload):
        node_id = payload.get("node_id")
        if not node_id:
            raise NNError("缺少 node_id")
        just_registered = False
        just_revived = None
        with self.node_lock:
            node = self.nodes.get(node_id)
            was_state = node["state"] if node else None
            if node is None:
                node = self._register_node(payload)
                just_registered = True
            node.update({
                "rack": payload.get("rack", node.get("rack", "rack-?")),
                "url": payload.get("url", node["url"]),
                "port": payload.get("port", node.get("port")),
                "storage": payload.get("storage", {}),
                "block_count": payload.get("block_count", 0),
                "io": payload.get("io", {}),
                "rates": payload.get("rates", {}),
                "uptime": payload.get("uptime", 0),
                "vv": vv_merge(node.get("vv", {}), payload.get("vv", {})),
                "doc_vv": payload.get("doc_vv", {}),
                "last_seen": now(),
                "hb_count": node.get("hb_count", 0) + 1,
            })
            hb_count = node["hb_count"]
            if node["state"] != "LIVE":
                node["state"] = "LIVE"
                if was_state in ("DEAD", "SUSPECT"):
                    just_revived = dict(node)
            if node.get("killed_flag"):
                node["killed_flag"] = False

        # 锁外执行（保持全局锁序 meta > node > health > cmd，避免 ABBA 死锁）
        if just_registered:
            self._update_cluster_doc()
        if just_revived is not None:
            self._on_node_revived(just_revived)
        if hb_count % 4 == 0:
            self.emit("hb", f"节点 {node_id} 心跳正常 #{hb_count}",
                      node=node_id)

        # 处理捎带事件
        for event in payload.get("events", []):
            self._handle_node_event(node_id, event)

        # 组装应答：命令 + 需要拉取的同步文档
        commands = self._drain_commands(node_id)
        pull_docs = self._docs_to_sync(payload.get("doc_vv", {}))
        return {"ok": True, "nn_time": now(), "commands": commands,
                "pull_docs": pull_docs}

    def _register_node(self, payload):
        node_id = payload["node_id"]
        node = {
            "node_id": node_id,
            "rack": payload.get("rack", "rack-?"),
            "url": payload.get("url", ""),
            "port": payload.get("port"),
            "state": "LIVE",
            "registered_at": now(),
            "last_seen": now(),
            "hb_count": 0,
            "storage": payload.get("storage", {}),
            "block_count": 0,
            "io": {}, "rates": {}, "uptime": 0,
            "vv": payload.get("vv", {}),
            "doc_vv": {},
            "deaths": 0,
            "killed_flag": False,
        }
        self.nodes[node_id] = node
        # 注意：不在 node_lock 内调用 _update_cluster_doc（锁序 meta > node），
        # 由 handle_heartbeat 在释放 node_lock 后统一更新。
        self.log_event("INFO", "namenode", "node_register", node_id, "system",
                       f"DataNode {node_id} 注册 rack={node['rack']}")
        self.emit("node_register", f"节点 {node_id} 注册加入集群",
                  node=node_id)
        return node

    def _on_node_revived(self, node):
        node_id = node["node_id"]
        self.log_event("INFO", "recovery", "node_revived", node_id, "system",
                       f"节点 {node_id} 恢复心跳，重新标记 LIVE，要求全量块汇报")
        self.emit("node_revived", f"节点 {node_id} 复活", node=node_id)
        self._enqueue_command(node_id, {"type": "report"})
        # 该节点上的副本重新纳入健康评估（不持锁调用，check_block_health 自会加锁）
        self._rescan_all_blocks()
        # EC：节点扩容/复活后，把同节点共置的分片重平衡到独立节点
        self._schedule_ec_rebalance()

    def mark_node_dead(self, node_id):
        with self.node_lock:
            node = self.nodes.get(node_id)
            if not node or node["state"] == "DEAD":
                return
            node["state"] = "DEAD"
            node["deaths"] = node.get("deaths", 0) + 1
            node["dead_at"] = now()
        self._update_cluster_doc()
        self.log_event("ERROR", "recovery", "node_dead", node_id, "system",
                       f"心跳超时（>{config.HEARTBEAT_TIMEOUT}s），判定 DEAD；"
                       f"启动副本恢复")
        self.emit("node_dead", f"节点 {node_id} 心跳超时被判定 DEAD",
                  node=node_id)
        self._handle_node_failure(node_id)

    def _handle_node_failure(self, dead_node):
        """节点故障：其上的副本/分片全部失效，低于期望的块进入恢复队列。"""
        affected = 0
        ec_affected = 0
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            for bid, blk in blocks_doc["blocks"].items():
                reps = blk.get("replicas", {})
                if dead_node in reps:
                    affected += 1
                    self.check_block_health(bid)
            for gid, group in blocks_doc.get("groups", {}).items():
                hit = False
                for sh in group.get("shards", {}).values():
                    if dead_node in (sh.get("replicas") or {}):
                        hit = True
                        break
                if hit:
                    ec_affected += 1
                    self.check_ec_group_health(gid)
        self.emit("recovery_start",
                  f"节点 {dead_node} 故障波及 {affected} 个副本块、"
                  f"{ec_affected} 个 EC 组，开始再复制/分片重建",
                  node=dead_node, affected=affected, ec_affected=ec_affected)
        self.log_event("WARN", "recovery", "failure_scan", dead_node, "system",
                       f"故障扫描：{affected} 个副本块、{ec_affected} 个 EC 组")

    def _liveness_loop(self):
        while not self._stop.is_set():
            self._stop.wait(1.0)
            t = now()
            with self.node_lock:
                for node_id, node in self.nodes.items():
                    if node["state"] == "DEAD":
                        continue
                    silence = t - node.get("last_seen", 0)
                    if silence > config.HEARTBEAT_TIMEOUT:
                        self.mark_node_dead(node_id)
                    elif silence > config.HEARTBEAT_TIMEOUT * 0.6 \
                            and node["state"] == "LIVE":
                        node["state"] = "SUSPECT"

    def _handle_node_event(self, node_id, event):
        etype = event.get("type")
        if etype == "replicate_done":
            bid = event.get("block_id")
            with self.meta.lock:
                blk = self.meta.get("blocks")["blocks"].get(bid)
                if blk:
                    self._record_replica(bid, blk, node_id,
                                         event.get("genstamp", blk["genstamp"]),
                                         event.get("checksum", blk["checksum"]),
                                         event.get("size", blk["size"]), "ok")
                    self.meta.touch("blocks", flush=False)
            self.check_block_health(bid)
            self.scheduled.pop(bid, None)
            self.emit("replicate_done",
                      f"块 {short_hash(bid, 12)} 成功复制到 {node_id}",
                      node=node_id, block=bid)
        elif etype == "replicate_failed":
            bid = event.get("block_id")
            self.scheduled.pop(bid, None)
            self.check_block_health(bid)
            self.log_event("WARN", "recovery", "replicate_failed",
                           bid or "", "system",
                           f"{node_id}: {event.get('reason', '')}")
        elif etype == "corrupt":
            bid = event.get("block_id")
            pgid, _pidx = self._parse_shard_id(bid)
            if pgid is not None:
                with self.meta.lock:
                    shard = (self.meta.get("blocks").get("groups", {})
                             .get(pgid, {}).get("shards", {}).get(bid))
                    if shard and node_id in shard.get("replicas", {}):
                        shard["replicas"][node_id]["state"] = "corrupt"
                        shard["replicas"][node_id]["updated_at"] = now()
                        self.meta.touch("blocks", flush=False)
                with self.health_lock:
                    self.corrupt_replicas[(bid, node_id)] = {
                        "reason": event.get("reason", ""), "ts": now()}
                self.check_ec_group_health(pgid)
                self.log_event("ERROR", "ec", "shard_corrupt",
                               f"{bid}@{node_id}", "system",
                               event.get("reason", ""))
                self.emit("corrupt",
                          f"节点 {node_id} 发现 EC 分片 "
                          f"{short_hash(bid, 14)} 损坏",
                          node=node_id, block=pgid)
                return
            with self.meta.lock:
                blk = self.meta.get("blocks")["blocks"].get(bid)
                if blk and node_id in blk.get("replicas", {}):
                    blk["replicas"][node_id]["state"] = "corrupt"
                    blk["replicas"][node_id]["updated_at"] = now()
                    self.meta.touch("blocks", flush=False)
            with self.health_lock:
                self.corrupt_replicas[(bid, node_id)] = {
                    "reason": event.get("reason", ""), "ts": now()}
            self.check_block_health(bid)
            self.log_event("ERROR", "block", "corrupt", f"{bid}@{node_id}",
                           "system", event.get("reason", ""))
            self.emit("corrupt", f"节点 {node_id} 发现块 {short_hash(bid, 12)} 损坏",
                      node=node_id, block=bid)
        elif etype == "deleted":
            bid = event.get("block_id")
            pgid, _pidx = self._parse_shard_id(bid)
            if pgid is not None:
                with self.meta.lock:
                    shard = (self.meta.get("blocks").get("groups", {})
                             .get(pgid, {}).get("shards", {}).get(bid))
                    if shard and node_id in shard.get("replicas", {}):
                        del shard["replicas"][node_id]
                        self.meta.touch("blocks", flush=False)
                self.check_ec_group_health(pgid)
                return
            with self.meta.lock:
                blk = self.meta.get("blocks")["blocks"].get(bid)
                if blk and node_id in blk.get("replicas", {}):
                    del blk["replicas"][node_id]
                    self.meta.touch("blocks", flush=False)
            self.check_block_health(bid)
        elif etype == "doc_synced":
            self.log_event("DEBUG", "sync", "doc_synced",
                           event.get("doc", ""), "system",
                           f"{node_id}: relation={event.get('relation')} "
                           f"vv={event.get('vv')}")
        elif etype == "hb_failed":
            pass    # DN 侧连不上 NN 的记录（NN 收到时已恢复，忽略）
        elif etype == "revived":
            self.emit("node_revived", f"节点 {node_id} 重启完成", node=node_id)

    # ==================================================================
    # 块汇报对账（难点一/二：副本一致性）
    # ==================================================================
    def handle_block_report(self, payload):
        node_id = payload.get("node_id")
        if not node_id:
            raise NNError("缺少 node_id")
        commands = []
        reported = set()
        reported_shards = set()
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = blocks_doc["blocks"]
            groups = blocks_doc.setdefault("groups", {})
            shard_index = blocks_doc.setdefault("shard_index", {})
            touched = False
            for rep in payload.get("blocks", []):
                bid = rep["id"]
                reported.add(bid)
                # ---- EC 分片：id 形如 ecg_xxx__s03 ----
                pgid, pidx = self._parse_shard_id(bid)
                if pgid is not None:
                    group = groups.get(pgid)
                    shard = (group or {}).get("shards", {}).get(bid)
                    if not group or not shard:
                        # 可能是组扩容/重平衡前该节点已有的分片（节点复活场景）
                        commands.append({"type": "delete", "block_id": bid,
                                         "reason": "不属于任何 EC 组的分片"})
                        continue
                    if rep.get("genstamp", 0) < group.get("genstamp", 0):
                        commands.append({"type": "delete", "block_id": bid,
                                         "reason": "EC 分片 genstamp 过期"})
                        continue
                    if rep.get("checksum") != \
                            (shard.get("replicas", {}).get(node_id, {})
                             .get("checksum")) and node_id in \
                            shard.get("replicas", {}):
                        state = "corrupt"
                    else:
                        state = rep.get("state", "ok")
                    if rep.get("checksum") and not shard.get("replicas", {}) \
                            .get(node_id):
                        # NN 重启/复活后重新挂上仍在磁盘上的分片（不删数据）
                        shard.setdefault("replicas", {})[node_id] = {
                            "genstamp": int(rep.get("genstamp",
                                                    group["genstamp"])),
                            "checksum": rep.get("checksum"),
                            "size": rep.get("size", group.get("stripe")),
                            "state": "ok", "updated_at": now(),
                            "reattached_at": now()}
                        shard_index[bid] = pgid
                        touched = True
                        reported_shards.add(pgid)
                    else:
                        old = shard.get("replicas", {}).get(node_id)
                        if rep.get("checksum") != \
                                (old or {}).get("checksum"):
                            state = "corrupt"
                        if old:
                            old["state"] = state
                            old["updated_at"] = now()
                            old.pop("missed_reports", None)
                        if state == "corrupt":
                            commands.append({"type": "delete", "block_id": bid,
                                             "reason": "EC 分片校验和不匹配"})
                        reported_shards.add(pgid)
                    continue
                blk = blocks.get(bid)
                if not blk:
                    commands.append({"type": "delete", "block_id": bid,
                                     "reason": "namenode 块表中不存在"})
                    continue
                if rep.get("genstamp", 0) < blk.get("genstamp", 0):
                    commands.append({"type": "delete", "block_id": bid,
                                     "reason": "genstamp 过期（stale replica）"})
                    continue
                old = blk.get("replicas", {}).get(node_id)
                state = rep.get("state", "ok")
                if rep.get("checksum") != blk.get("checksum"):
                    state = "corrupt"
                if (not old or old.get("checksum") != rep.get("checksum")
                        or old.get("state") != state):
                    touched = True
                self._record_replica(bid, blk, node_id, rep.get("genstamp"),
                                     rep.get("checksum"), rep.get("size"),
                                     state)
                if state == "corrupt":
                    commands.append({"type": "delete", "block_id": bid,
                                     "reason": "校验和不匹配（损坏副本清除）"})
            # 块表认为该节点应有、但汇报中缺失的副本 => 从副本表移除
            for bid, blk in blocks.items():
                if bid in reported:
                    continue
                if node_id in blk.get("replicas", {}):
                    del blk["replicas"][node_id]
                    touched = True
            # EC 分片：该节点持有的分片未在汇报中出现。
            # 全量块汇报与分片 PUT 可能并发（DN 汇报快照早于新分片落盘/入索引），
            # 因此采用「连续两次全量汇报缺失才摘除」的保守策略：
            # 单次缺失只记 missed_reports=1，保留 NN 记录（磁盘分片实际是好的），
            # 下一轮汇报若仍缺失再摘除，避免把刚写好的分片误判为丢失。
            for gid, group in groups.items():
                for sid, sh in group.get("shards", {}).items():
                    if sid in reported:
                        continue
                    reps = sh.get("replicas") or {}
                    rep = reps.get(node_id)
                    if not rep:
                        continue
                    miss = rep.get("missed_reports", 0) + 1
                    if miss >= 2:
                        del reps[node_id]
                        touched = True
                        reported_shards.add(gid)
                    else:
                        rep["missed_reports"] = miss
            # 孤儿块（磁盘有、索引外）
            for bid in payload.get("orphans", []):
                pgid, _i = self._parse_shard_id(bid)
                reason = ("EC 孤儿分片" if pgid is not None else "孤儿块")
                commands.append({"type": "delete", "block_id": bid,
                                 "reason": reason})
            if touched:
                self.meta.touch("blocks", flush=False)
        with self.node_lock:
            node = self.nodes.get(node_id)
            if node:
                node["last_report_at"] = now()
                node["block_count"] = len(payload.get("blocks", []))
        # 汇报后重估相关块健康度
        for bid in reported:
            pgid, _i = self._parse_shard_id(bid)
            if pgid is not None:
                self.check_ec_group_health(pgid)
            else:
                self.check_block_health(bid)
        return {"ok": True, "commands": commands,
                "accepted": len(reported),
                "delete_commands": len(commands)}

    def _record_replica(self, bid, blk, node_id, genstamp, checksum, size,
                        state):
        """写入/更新副本记录（调用方持 meta.lock）。"""
        reps = blk.setdefault("replicas", {})
        reps[node_id] = {
            "genstamp": int(genstamp or blk.get("genstamp", 1)),
            "checksum": checksum or blk.get("checksum"),
            "size": size if size is not None else blk.get("size", 0),
            "state": state,
            "updated_at": now(),
        }

    # ==================================================================
    # 健康度 / 恢复调度（难点二）
    # ==================================================================
    def live_nodes(self):
        with self.node_lock:
            return [n for n in self.nodes.values() if n["state"] == "LIVE"]

    def live_good_replicas(self, blk):
        """存活且状态 ok、genstamp 匹配的副本节点列表。"""
        good = []
        live_ids = {n["node_id"] for n in self.live_nodes()}
        for nid, rep in list((blk.get("replicas") or {}).items()):
            if nid not in live_ids:
                continue
            if rep.get("state") != "ok":
                continue
            if rep.get("genstamp", 0) != blk.get("genstamp", 0):
                continue
            good.append(nid)
        return good

    def check_block_health(self, bid):
        """评估单块健康度，维护 under_replicated / missing 集合。"""
        with self.meta.lock:
            blk = self.meta.get("blocks")["blocks"].get(bid)
        if not blk:
            with self.health_lock:
                self.under_replicated.pop(bid, None)
                self.missing_blocks.discard(bid)
            return None
        good = self.live_good_replicas(blk)
        desired = blk.get("desired", config.DEFAULT_REPLICATION)
        state = "healthy"
        with self.health_lock:
            if not good:
                self.missing_blocks.add(bid)
                self.under_replicated[bid] = self.under_replicated.get(
                    bid, {"since": now(), "attempts": 0})
                state = "missing"
            else:
                self.missing_blocks.discard(bid)
                if needs_recovery(len(good), desired, config.MIN_REPLICATION,
                                  config.RECOVERY_TRIGGER):
                    if bid not in self.under_replicated:
                        self.under_replicated[bid] = {"since": now(),
                                                      "attempts": 0}
                    state = ("critical" if len(good) < config.MIN_REPLICATION
                             else "under")
                else:
                    self.under_replicated.pop(bid, None)
                    self.scheduled.pop(bid, None)
                    # 块已恢复满副本：清理其历史损坏记录
                    for key in [k for k in self.corrupt_replicas
                                if k[0] == bid]:
                        del self.corrupt_replicas[key]
        return {"block": bid, "state": state, "live": len(good),
                "desired": desired}

    def _rescan_all_blocks(self):
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            bids = list(blocks_doc["blocks"].keys())
            gids = list(blocks_doc.get("groups", {}).keys())
        for bid in bids:
            self.check_block_health(bid)
        for gid in gids:
            self.check_ec_group_health(gid)

    # ==================================================================
    # EC 健康度 / 分片重建调度
    # ==================================================================
    def check_ec_group_health(self, gid):
        """评估一个 EC 条带组：维护 ec_degraded / ec_missing。

        健康口径：k+m 个分片全部 ok = healthy；
        存活好分片 < k+m 但 >= k = degraded（可在线重建，容忍再坏 m-1）；
        < k = missing（暂时无法解码，等同块丢失）。
        """
        with self.meta.lock:
            group = self._ec_group(gid)
        if not group:
            with self.health_lock:
                self.ec_degraded.pop(gid, None)
                self.ec_missing.discard(gid)
            return None
        k, m = group["k"], group["m"]
        live_n, _good, corrupt = self.ec_live_shards(group)
        desired = k + m
        with self.health_lock:
            if live_n < k:
                self.ec_missing.add(gid)
                self.ec_degraded[gid] = self.ec_degraded.get(
                    gid, {"since": now(), "attempts": 0})
                state = "missing"
            elif live_n < desired:
                self.ec_missing.discard(gid)
                if gid not in self.ec_degraded:
                    self.ec_degraded[gid] = {"since": now(), "attempts": 0}
                state = "degraded"
            else:
                self.ec_missing.discard(gid)
                self.ec_degraded.pop(gid, None)
                state = "healthy"
                # 分片齐全：清理历史损坏记录与已完成的任务
                for key in [kk for kk in self.corrupt_replicas
                            if self._parse_shard_id(kk[0])[0] == gid]:
                    self.corrupt_replicas.pop(key, None)
                task = self.ec_repairs.get(gid)
                if task and task.get("state") in ("running", "pending"):
                    task["state"] = "done"
                    task["finished_at"] = now()
        return {"group": gid, "state": state, "live": live_n,
                "desired": desired, "k": k, "m": m,
                "corrupt": len(corrupt)}

    def _ec_repair_loop(self):
        """周期扫描退化的 EC 组：在线重建缺失/损坏分片。"""
        while not self._stop.is_set():
            self._stop.wait(config.RECOVERY_SCAN_INTERVAL)
            try:
                self._schedule_ec_repair_once()
            except Exception as e:  # noqa: BLE001
                self.log_event("ERROR", "ec", "repair_loop_error", "",
                               "system", str(e))

    def _schedule_ec_repair_once(self):
        with self.health_lock:
            queue = list(self.ec_degraded.items())
        if not queue:
            return
        live = self.live_nodes()
        if not live:
            return
        live_ids = {n["node_id"] for n in live}
        started = 0
        for gid, info in queue:
            if started >= 32:
                break
            with self.health_lock:
                task = self.ec_repairs.get(gid)
                if task and task.get("state") == "running" and \
                        now() - task.get("at", 0) < config.EC_REPAIR_TIMEOUT:
                    continue
            with self.meta.lock:
                group = self._ec_group(gid)
            if not group:
                with self.health_lock:
                    self.ec_degraded.pop(gid, None)
                continue
            k, m = group["k"], group["m"]
            live_n, good_idx, corrupt = self.ec_live_shards(group)
            if live_n >= k + m:
                with self.health_lock:
                    self.ec_degraded.pop(gid, None)
                continue
            if live_n < k:
                continue   # 无法解码，等待节点复活
            # 1) 先摘除存活节点上的坏分片（删除后该节点重新成为放置候选）
            with self.meta.lock:
                g2 = self._ec_group(gid)
                for sid, nid in corrupt:
                    if nid not in live_ids:
                        continue
                    self._enqueue_command(nid, {
                        "type": "delete", "block_id": sid,
                        "reason": "EC 损坏分片清除后重建"})
                    reps = g2["shards"].get(sid, {}).get("replicas", {})
                    reps.pop(nid, None)
                self.meta.touch("blocks", flush=False)
            # 2) 计算缺失分片 -> 目标节点
            present_idx = dict(good_idx)
            for _sid, sh in group["shards"].items():
                for nid in list((sh.get("replicas") or {}).keys()):
                    if nid in live_ids:
                        present_idx.setdefault(sh["index"], nid)
            missing = [i for i in range(k + m) if i not in present_idx]
            if not missing:
                self.check_ec_group_health(gid)
                continue
            used_nodes = set(present_idx.values())
            stripe = group.get("stripe", 0)
            # 优先把分片放到「没有该组任何分片」的节点（分散度最高）；
            # 节点不足（存活数 < k+m）时退而求其次放到已持有分片的节点，
            # 保证组尽快回到 k+m 满编；待集群扩容后由重平衡迁回独立节点。
            candidates = [n for n in live if n["node_id"] not in used_nodes
                          and n.get("storage", {}).get("free", 0) > stripe]
            colocated = False
            if not candidates:
                candidates = [n for n in live
                              if n.get("storage", {}).get("free", 0) > stripe]
                colocated = bool(candidates)
            if not candidates:
                continue
            chosen = self._rank_targets(candidates, stripe,
                                        min(len(missing), len(candidates)))
            # 缺失分片序号与目标节点一一配对
            plan = list(zip(missing, [n["node_id"] for n in chosen]))
            with self.health_lock:
                self.ec_repairs[gid] = {
                    "group": gid, "state": "running",
                    "k": k, "m": m, "stripe": stripe,
                    "missing": [i for i, _ in plan],
                    "targets": [n for _, n in plan],
                    "colocated": colocated,
                    "total": len(plan), "finished": 0, "failed": [],
                    "at": now(), "attempts": info.get("attempts", 0) + 1}
                info["attempts"] = info.get("attempts", 0) + 1
            t = threading.Thread(
                target=self._repair_ec_group, args=(gid, plan),
                name=f"ec-repair-{gid[-6:]}", daemon=True)
            t.start()
            started += 1
            self.emit("ec_repair_scheduled",
                      f"EC 组 {short_hash(gid, 12)} 启动分片重建："
                      f"缺失 {[i for i, _ in plan]} -> "
                      f"{[n for _, n in plan]}"
                      f"{'（节点不足，临时同节点放置）' if colocated else ''}",
                      block=gid)

    def _repair_ec_group(self, gid, plan):
        """工作线程：拉取 k 个存活分片 -> RS 重建 -> PUT 到目标节点。"""
        with self.meta.lock:
            group = self._ec_group(gid)
        if not group:
            return
        k, m, stripe = group["k"], group["m"], group["stripe"]
        gs = group["genstamp"]
        live_n, good_idx, _corrupt = self.ec_live_shards(group)
        if live_n < k:
            self._repair_fail(gid, plan, f"存活分片 {live_n}<{k}")
            return
        # 拉取 k 个分片（同样带逐片故障转移）
        avail, errors = {}, []
        idxs = sorted(good_idx.keys())
        self._rr_counter += 1
        idxs = idxs[self._rr_counter % len(idxs):] + \
            idxs[:self._rr_counter % len(idxs)]
        for idx in idxs:
            if len(avail) >= k:
                break
            sid = self._shard_id(gid, idx)
            nid = good_idx[idx]
            try:
                avail[idx] = self._fetch_shard_bytes(group, sid, idx, nid)
            except Exception as e:  # noqa: BLE001
                errors.append(str(e))
                self._mark_shard_corrupt(gid, sid, nid, reason=str(e))
        if len(avail) < k:
            self._repair_fail(gid, plan,
                              f"可用分片不足 {len(avail)}/{k}: {errors[:2]}")
            return
        try:
            rebuilt = rs_code.reconstruct(avail, k, m, stripe,
                                          wanted=[i for i, _ in plan])
        except ValueError as e:
            self._repair_fail(gid, plan, f"RS 重建失败: {e}")
            return
        # 逐个 PUT 并登记
        finished = 0
        for idx, target in plan:
            sid = self._shard_id(gid, idx)
            shard = rebuilt.get(idx)
            if shard is None:
                continue
            shard_checksum = sha256_bytes(shard)
            node = self._node_url(target)
            url = (f"{node.rstrip('/')}/block/{sid}?genstamp={gs}"
                   f"&checksum={shard_checksum}&size={stripe}")
            try:
                http_request(url, "PUT", data=shard, headers={
                    "X-Cluster-Key": self.cluster_key,
                    "Content-Type": "application/octet-stream"}, timeout=30)
            except HttpError as e:
                with self.health_lock:
                    task = self.ec_repairs.get(gid)
                    if task:
                        task["failed"].append({"index": idx, "node": target,
                                               "error": str(e)[:160]})
                self.log_event("WARN", "ec", "repair_put_failed",
                               f"{sid}@{target}", "system", str(e)[:160])
                continue
            with self.meta.lock:
                g = self._ec_group(gid)
                if g:
                    g["shards"][sid]["replicas"][target] = {
                        "genstamp": gs, "checksum": shard_checksum,
                        "size": stripe, "state": "ok", "updated_at": now()}
                    self.meta.touch("blocks", flush=False)
            finished += 1
            with self.health_lock:
                task = self.ec_repairs.get(gid)
                if task:
                    task["finished"] = finished
        self.block_cache.invalidate(gid)
        with self.health_lock:
            task = self.ec_repairs.get(gid)
            if task and finished == task.get("total", 0):
                task["state"] = "done"
                task["finished_at"] = now()
        self.check_ec_group_health(gid)
        if finished:
            self.log_event("INFO", "ec", "repair_done", gid, "system",
                           f"重建 {finished} 个分片: "
                           f"{[i for i, _ in plan][:finished]}")
            self.emit("ec_repair_done",
                      f"EC 组 {short_hash(gid, 12)} 完成 {finished} 个分片重建",
                      block=gid)

    def _repair_fail(self, gid, plan, reason):
        with self.health_lock:
            task = self.ec_repairs.get(gid)
            if task:
                task["state"] = "pending"
                task["at"] = 0
                task["last_error"] = reason[:200]
        self.log_event("WARN", "ec", "repair_retry", gid, "system", reason[:200])

    def _schedule_ec_rebalance(self, limit=16):
        """
        EC 重平衡（节点扩容/复活后）：
          * 一个分片在多个节点有副本（块汇报重挂）=> 保留一个、删多余；
          * 同一节点持有两个分片 => 把多余分片迁到空闲独立节点；
        最终恢复「一分片一节点、k+m 个分片互不共置」的最佳分散度。
        """
        live = self.live_nodes()
        moves = 0
        with self.meta.lock:
            groups = list(dict(self._blocks_groups()).values())
        for group in groups:
            if moves >= limit:
                break
            gid = group["id"]
            k, m = group["k"], group["m"]
            if len(live) < k + m:
                continue
            live_ids = {n["node_id"] for n in live}
            stripe = group.get("stripe", 0)
            # 每分片的存活节点集合
            shard_nodes = {}
            for sid, sh in group.get("shards", {}).items():
                shard_nodes[sh["index"]] = sorted(
                    n for n in (sh.get("replicas") or {}) if n in live_ids)
            free_ids = [n["node_id"] for n in live
                        if n.get("storage", {}).get("free", 0) > stripe]
            # MRV 贪心：每轮优先分配「可选节点最少」的分片，尽量沿用现副本，
            # 找不到独占现副本时才把该分片迁移到空闲节点。
            unassigned = set(range(k + m))
            occupied = set()
            assignment = {}
            plan = []
            cleanup = {}
            while unassigned:
                # 计算每个待分配分片的独占候选数
                best, best_cands = None, None
                for idx in unassigned:
                    cands = [n for n in shard_nodes.get(idx, [])
                             if n not in occupied]
                    if best is None or len(cands) < len(best_cands):
                        best, best_cands = idx, cands
                        if len(cands) == 0:
                            break
                if best_cands:
                    chosen = best_cands[0]
                    assignment[best] = chosen
                    occupied.add(chosen)
                    free_ids = [n for n in free_ids if n != chosen]
                elif free_ids:
                    chosen = free_ids.pop(0)
                    assignment[best] = chosen
                    occupied.add(chosen)
                    plan.append((best, chosen))
                # 无候选也无空闲节点（理论上 k+m<=节点数时不发生）=> 跳过
                unassigned.discard(best)
            # 删除「未被选为目标」的副本（同组其它共置 / 多余副本）
            for idx, nodes in shard_nodes.items():
                target = assignment.get(idx)
                for nid in nodes:
                    if target and nid != target:
                        cleanup.setdefault(nid, []).append(
                            self._shard_id(gid, idx))
            if cleanup:
                with self.meta.lock:
                    g = self._ec_group(gid)
                    if g:
                        for nid, sids in cleanup.items():
                            for sid in sids:
                                self._enqueue_command(nid, {
                                    "type": "delete", "block_id": sid,
                                    "reason": "EC 重平衡：清理共置/多余分片"})
                                g["shards"][sid]["replicas"].pop(nid, None)
                        self.meta.touch("blocks", flush=False)
            if not plan:
                if cleanup:
                    self.check_ec_group_health(gid)
                continue
            with self.health_lock:
                self.ec_repairs[gid] = {
                    "group": gid, "state": "running", "k": k, "m": m,
                    "stripe": stripe,
                    "missing": [i for i, _ in plan],
                    "targets": [n for _, n in plan],
                    "total": len(plan), "finished": 0, "failed": [],
                    "rebalance": True,
                    "at": now(), "attempts": 0}
            threading.Thread(
                target=self._rebalance_ec_group, args=(gid, plan),
                name=f"ec-rebal-{gid[-6:]}", daemon=True).start()
            moves += 1
        if moves:
            self.log_event("INFO", "ec", "rebalance_scheduled", "", "system",
                           f"EC 重平衡：{moves} 个组的共置分片将迁回独立节点")

    def _blocks_groups(self):
        return self.meta.get("blocks").get("groups", {}).items()

    def _rebalance_ec_group(self, gid, plan):
        """重平衡：先在新节点重建分片，成功后删除旧节点上的共置副本。"""
        with self.meta.lock:
            group = self._ec_group(gid)
        if not group:
            return
        k, m, stripe, gs = group["k"], group["m"], group["stripe"], \
            group["genstamp"]
        live_n, good_idx, _c = self.ec_live_shards(group)
        if live_n < k:
            return
        avail = {}
        for idx in sorted(good_idx)[:k]:
            sid = self._shard_id(gid, idx)
            try:
                avail[idx] = self._fetch_shard_bytes(
                    group, sid, idx, good_idx[idx])
            except Exception:  # noqa: BLE001
                continue
        if len(avail) < k:
            return
        rebuilt = rs_code.reconstruct(avail, k, m, stripe,
                                      wanted=[i for i, _ in plan])
        finished = 0
        for idx, target in plan:
            sid = self._shard_id(gid, idx)
            shard = rebuilt.get(idx)
            if shard is None:
                continue
            checksum = sha256_bytes(shard)
            url = (f"{self._node_url(target).rstrip('/')}/block/{sid}"
                   f"?genstamp={gs}&checksum={checksum}&size={stripe}")
            try:
                http_request(url, "PUT", data=shard, headers={
                    "X-Cluster-Key": self.cluster_key,
                    "Content-Type": "application/octet-stream"}, timeout=30)
            except HttpError:
                continue
            with self.meta.lock:
                g = self._ec_group(gid)
                if not g:
                    break
                sh = g["shards"][sid]
                # 删除新目标之外节点上该分片的共置副本
                old_nodes = [n for n in (sh.get("replicas") or {})
                             if n != target]
                sh["replicas"][target] = {
                    "genstamp": gs, "checksum": checksum, "size": stripe,
                    "state": "ok", "updated_at": now()}
                for old in old_nodes:
                    self._enqueue_command(old, {
                        "type": "delete", "block_id": sid,
                        "reason": "EC 重平衡：分片迁至独立节点"})
                    sh["replicas"].pop(old, None)
                self.meta.touch("blocks", flush=False)
            finished += 1
            with self.health_lock:
                task = self.ec_repairs.get(gid)
                if task:
                    task["finished"] = finished
        with self.health_lock:
            task = self.ec_repairs.get(gid)
            if task and finished == task.get("total", 0):
                task["state"] = "done"
                task["finished_at"] = now()
        self.check_ec_group_health(gid)
        if finished:
            self.emit("ec_rebalanced",
                      f"EC 组 {short_hash(gid, 12)} 重平衡完成（分片回到独立节点）",
                      block=gid)

    # ==================================================================
    # 目录冗余策略切换 + 后台无损转换
    # ==================================================================
    def set_redundancy_policy(self, path, redundancy, ec_profile=None,
                              recursive=False, user="admin"):
        """
        设置目录的冗余方式（rep/ec）。

          * 只改目录属性：新写入文件立即按新策略落盘；
          * recursive=True：对目录下**已有文件**排队做后台转换；
          * 转换是「写新块 -> 原子换 inode 引用」：旧块保留（历史版本仍可读，
            GC 宽限期后才回收），切换期间读写不中断；
          * 不递归/未转换的旧文件维持原冗余方式 —— 同一目录下两种冗余并存。
        """
        redundancy = redundancy if redundancy in (config.REDUNDANCY_REP,
                                                  config.REDUNDANCY_EC) \
            else config.DEFAULT_REDUNDANCY
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "dir":
                raise FsError(f"不是目录: {path}")
            if path == "/":
                raise FsError("根目录冗余策略不允许修改")
            old = inode.get("redundancy", config.DEFAULT_REDUNDANCY)
            inode["redundancy"] = redundancy
            if redundancy == config.REDUNDANCY_EC:
                pname, _k, _m = self._resolve_ec_profile(ec_profile)
                inode["ec_profile"] = pname
            inode["policy_updated_at"] = now()
            inode["policy_updated_by"] = user
            self.meta.touch("fs")
            job = None
            if recursive:
                job = self._enqueue_conversion(path, redundancy,
                                               inode.get("ec_profile"), user)
        self.log_event("INFO", "convert", "policy_set", path, user,
                       f"{old} -> {redundancy}"
                       f"{('/' + inode.get('ec_profile', '')) if redundancy == 'ec' else ''}"
                       f"{'，并排队后台转换' if recursive else ''}")
        return {"ok": True, "path": path, "redundancy": redundancy,
                "ec_profile": inode.get("ec_profile"),
                "conversion": job}

    def _enqueue_conversion(self, path, redundancy, ec_profile, user):
        """遍历目录，收集需要转换的文件并登记一个转换任务。"""
        with self.meta.lock:
            files = [p for p, inode in self.fs.walk_files(
                self.fs.resolve(path)["id"])]
            conversions = self.meta.get("blocks").setdefault("conversions", {})
            job_id = gen_id("conv")
            items = []
            for p in files:
                inode = self.fs.resolve(p)
                kind, _prof = self.file_redundancy(inode)
                if kind == "empty":
                    continue
                if kind == redundancy:
                    status = "skipped"
                else:
                    status = "pending"
                items.append({"path": p, "status": status,
                              "from": kind, "to": redundancy,
                              "blocks": len(inode.get("block_ids", [])),
                              "size": inode.get("size", 0),
                              "started_at": None, "finished_at": None,
                              "error": None})
            pending = sum(1 for it in items if it["status"] == "pending")
            job = {
                "id": job_id, "path": path, "to": redundancy,
                "ec_profile": ec_profile, "user": user,
                "created_at": now(), "state": "running" if pending else "done",
                "total": len(items), "pending": pending,
                "converted": 0, "failed": 0, "skipped":
                    sum(1 for it in items if it["status"] == "skipped"),
                "items": items,
            }
            conversions[job_id] = job
            self.meta.touch("blocks")
        return self._conversion_view(job)

    def _conversion_view(self, job):
        return {k: job[k] for k in
                ("id", "path", "to", "ec_profile", "user", "created_at",
                 "state", "total", "pending", "converted", "failed",
                 "skipped")}

    def conversion_jobs(self, limit=50):
        with self.meta.lock:
            jobs = list(self.meta.get("blocks").get("conversions", {}).values())
        jobs.sort(key=lambda j: j["created_at"], reverse=True)
        return {"jobs": [self._conversion_view(j) for j in jobs[:limit]],
                "total": len(jobs)}

    def conversion_detail(self, job_id):
        with self.meta.lock:
            job = self.meta.get("blocks").get("conversions", {}).get(job_id)
            if not job:
                raise NNError(f"转换任务不存在: {job_id}")
            return {"job": self._conversion_view(job), "items": job["items"]}

    def _convert_loop(self):
        """后台转换线程：逐文件 写新冗余 -> 原子换 inode block_ids。"""
        while not self._stop.is_set():
            self._stop.wait(config.EC_CONVERT_INTERVAL)
            try:
                self._convert_one_step()
            except Exception as e:  # noqa: BLE001
                self.log_event("ERROR", "convert", "loop_error", "", "system",
                               str(e))

    def _convert_one_step(self):
        with self.meta.lock:
            jobs = [j for j in self.meta.get("blocks")
                    .get("conversions", {}).values()
                    if j["state"] == "running"]
            if not jobs:
                return
            # 最早创建的任务优先；取一个待转换文件（失败的可重试）
            jobs.sort(key=lambda j: j["created_at"])
            job = jobs[0]
            item = next((it for it in job["items"]
                         if it["status"] in ("pending", "failed")), None)
            if item is None:
                job["state"] = "done"
                self.meta.touch("blocks", flush=False)
                return
            first_run = item["status"] == "pending"
            item["status"] = "running"
            item["started_at"] = now()
            path = item["path"]
            to_mode = job["to"]
            ec_profile = job.get("ec_profile")
        # 锁外读取旧数据（旧块全程保留，读路径不受影响）
        try:
            inode = self.fs.resolve(path)
            data = self.read_blocks(inode.get("block_ids", []))
            result = self.store_data_blocks(
                data, redundancy=to_mode, ec_profile=ec_profile,
                author=job.get("user", "system"))
        except Exception as e:  # noqa: BLE001
            with self.meta.lock:
                item["status"] = "failed"
                item["error"] = str(e)[:200]
                if first_run:
                    job["failed"] += 1
                    job["pending"] = max(0, job["pending"] - 1)
                self.meta.touch("blocks")
            self.log_event("ERROR", "convert", "item_failed", path,
                           job.get("user", "system"), str(e)[:200])
            return
        # 原子替换 inode 的块引用（读路径要么全旧、要么全新，不会读到半截）
        with self.meta.lock:
            cur = self.fs.resolve(path, must_exist=False)
            if cur is None:
                item["status"] = "skipped"
                item["error"] = "文件已删除"
                if first_run:
                    job["pending"] = max(0, job["pending"] - 1)
            elif cur.get("content_hash") != result["content_hash"]:
                # 转换期间文件被改写：保留为 running，下轮重新转换
                item["status"] = "pending"
                item["started_at"] = None
            else:
                cur["block_ids"] = result["block_ids"]
                cur["redundancy"] = to_mode
                cur["modified_at"] = now()
                self.meta.touch("fs")
                item["status"] = "done"
                item["finished_at"] = now()
                if first_run:
                    job["converted"] += 1
                    job["pending"] = max(0, job["pending"] - 1)
            if job["pending"] == 0 and not any(
                    it["status"] == "running" for it in job["items"]):
                job["state"] = "done"
            self.meta.touch("blocks")
        self.log_event("INFO", "convert", "item_done", path,
                       job.get("user", "system"),
                       f"{item['from']} -> {to_mode}，"
                       f"{len(result['block_ids'])} 块（旧块保留待 GC）")
        self.emit("convert_progress",
                  f"冗余转换 {short_hash(job['id'], 8)}："
                  f"{job['converted']}/{job['total']} {path} -> {to_mode}",
                  job=job["id"], path=path,
                  converted=job["converted"], total=job["total"])

    def _recovery_loop(self):
        """周期扫描恢复队列：为缺副本的块安排 源->目标 复制命令。"""
        while not self._stop.is_set():
            self._stop.wait(config.RECOVERY_SCAN_INTERVAL)
            try:
                self._schedule_recovery_once()
            except Exception as e:  # noqa: BLE001
                self.log_event("ERROR", "recovery", "loop_error", "", "system",
                               str(e))

    def _schedule_recovery_once(self):
        with self.health_lock:
            queue = list(self.under_replicated.items())
        if not queue:
            return
        live = self.live_nodes()
        if not live:
            return
        live_ids = {n["node_id"] for n in live}
        scheduled_now = 0
        for bid, info in queue:
            if scheduled_now >= 64:      # 单轮限流，防止心跳应答过大
                break
            with self.meta.lock:
                blk = self.meta.get("blocks")["blocks"].get(bid)
            if not blk:
                with self.health_lock:
                    self.under_replicated.pop(bid, None)
                continue
            good = self.live_good_replicas(blk)
            desired = blk.get("desired", config.DEFAULT_REPLICATION)
            if len(good) >= desired:
                with self.health_lock:
                    self.under_replicated.pop(bid, None)
                    self.scheduled.pop(bid, None)
                continue
            # 超时重排
            sched = self.scheduled.get(bid)
            if sched and now() - sched["at"] < config.REPLICATION_TIMEOUT:
                continue
            # 第一步：清理存活节点上的坏副本（corrupt / genstamp 过期），
            # 先下发删除命令并摘除副本记录，使这些节点重新成为复制候选。
            good_set = set(good)
            with self.meta.lock:
                blk2 = self.meta.get("blocks")["blocks"].get(bid)
                if blk2:
                    reps = blk2.get("replicas") or {}
                    for nid in list(reps.keys()):
                        if nid in good_set or nid not in live_ids:
                            continue
                        self._enqueue_command(nid, {
                            "type": "delete", "block_id": bid,
                            "reason": "坏副本（corrupt/stale），删除后重建"})
                        del reps[nid]
                    self.meta.touch("blocks", flush=False)
            have = good_set
            candidates = [n for n in live
                          if n["node_id"] not in have
                          and n.get("storage", {}).get("free", 0)
                          > blk.get("size", 0)]
            if not candidates or not good:
                continue
            need = desired - len(good)
            src = random.choice(good)
            targets = self._rank_targets(candidates, blk.get("size", 0),
                                         count=need)
            src_url = self._node_url(src)
            for tnode in targets:
                cmd = {"type": "replicate", "block_id": bid, "src": src_url,
                       "genstamp": blk.get("genstamp", 1),
                       "checksum": blk.get("checksum"),
                       "size": blk.get("size")}
                self._enqueue_command(tnode["node_id"], cmd)
                scheduled_now += 1
            with self.health_lock:
                self.scheduled[bid] = {"src": src,
                                       "dst": [t["node_id"] for t in targets],
                                       "at": now()}
                info["attempts"] = info.get("attempts", 0) + 1
            self.emit("recovery_scheduled",
                      f"块 {short_hash(bid, 12)} 恢复调度: {src} -> "
                      f"{','.join(t['node_id'] for t in targets)}",
                      block=bid, src=src)

    def _enqueue_command(self, node_id, cmd):
        with self.cmd_lock:
            q = self.pending_commands.setdefault(node_id, [])
            q.append(cmd)
            if len(q) > 500:
                del q[:len(q) - 500]

    def _drain_commands(self, node_id):
        with self.cmd_lock:
            cmds = self.pending_commands.pop(node_id, [])
        return cmds

    # ==================================================================
    # 文档同步（版本向量，难点五）
    # ==================================================================
    def _update_cluster_doc(self):
        """把节点注册表摘要写入 cluster 文档（DataNode 会按 vv 拉取）。"""
        with self.meta.lock:
            cluster = self.meta.get("cluster")
            with self.node_lock:
                cluster["nodes"] = {
                    nid: {"rack": n["rack"], "state": n["state"],
                          "url": n["url"], "registered_at": n["registered_at"]}
                    for nid, n in self.nodes.items()}
            cluster["updated_by"] = self.node_id
            cluster["nn_started_at"] = self.started_at
            cluster["settings"] = {
                "block_size": config.BLOCK_SIZE,
                "replication": config.DEFAULT_REPLICATION,
                "heartbeat_timeout": config.HEARTBEAT_TIMEOUT,
            }
            self.meta.touch("cluster")

    def _docs_to_sync(self, dn_doc_vv):
        """比较版本向量，返回 DN 需要拉取的文档名列表。"""
        pull = []
        with self.meta.lock:
            for doc in config.VERSION_VECTOR_SYNC_DOCS:
                local = self.meta.doc(doc).vv
                remote = (dn_doc_vv or {}).get(doc, {})
                rel = vv_compare(local, remote)
                if rel in ("after", "concurrent"):
                    pull.append(doc)
        return pull

    def get_meta_doc(self, doc):
        if doc not in config.META_DOCS:
            raise NNError(f"未知文档: {doc}")
        return self.meta.export_doc(doc)

    # ==================================================================
    # 副本放置 / 块分配（难点一）
    # ==================================================================
    def _node_url(self, node_id):
        with self.node_lock:
            n = self.nodes.get(node_id)
        return (n or {}).get("url", "")

    def _rank_targets(self, candidates, size, count):
        """跨机架优先 + 剩余空间优先 + 少量随机扰动。"""
        def score(n):
            free = n.get("storage", {}).get("free", 0)
            cap = n.get("storage", {}).get("capacity", 1) or 1
            usage = 1 - (free / cap)
            return (usage + random.random() * 0.05, n.get("rack"))
        ranked = sorted(candidates, key=score)
        chosen, racks = [], set()
        for n in ranked:                       # 第一轮：机架去重
            if len(chosen) >= count:
                break
            if n.get("rack") not in racks:
                chosen.append(n)
                racks.add(n.get("rack"))
        for n in ranked:                       # 第二轮：机架不够再补
            if len(chosen) >= count:
                break
            if n not in chosen:
                chosen.append(n)
        return chosen

    def choose_targets(self, size, count=None, exclude=()):
        count = count or config.DEFAULT_REPLICATION
        live = [n for n in self.live_nodes() if n["node_id"] not in exclude
                and n.get("storage", {}).get("free", 0) > size]
        if not live:
            raise NNError("没有满足空间要求的存活节点，无法放置副本")
        return self._rank_targets(live, size, min(count, len(live)))

    # ==================================================================
    # 冗余策略：三副本（rep）/ 纠删码（ec），按目录继承
    # ==================================================================
    def policy_for_path(self, path):
        """目录树就近继承：沿父目录链找到最近的 redundancy/ec_profile。"""
        with self.meta.lock:
            inode = self.fs.resolve(path, must_exist=False)
            return self._policy_for_inode(inode)

    def _policy_for_inode(self, inode):
        """给定目录 inode（或 None=根），返回 (redundancy, ec_profile)。"""
        inodes = self.fs._inodes()
        cur = inode if inode is not None else inodes.get(self.fs.root_id)
        redundancy = None
        profile = None
        guard = 0
        while cur and guard < 512:
            if redundancy is None and cur.get("redundancy"):
                redundancy = cur["redundancy"]
            if profile is None and cur.get("ec_profile"):
                profile = cur["ec_profile"]
            if redundancy and profile:
                break
            pid = cur.get("parent")
            cur = inodes.get(pid) if pid else None
            guard += 1
        return (redundancy or config.DEFAULT_REDUNDANCY,
                profile or config.EC_PROFILE_DEFAULT)

    def policy_for_write(self, dir_path):
        """上传/写文件时，目标目录的生效策略。"""
        with self.meta.lock:
            dir_inode = self.fs.resolve(dir_path)
            if dir_inode["type"] != "dir":
                raise FsError(f"不是目录: {dir_path}")
            return self._policy_for_inode(dir_inode)

    def file_redundancy(self, inode):
        """根据块表判定文件实际使用的冗余方式（块不可变 ⇒ 以块为准）。"""
        with self.meta.lock:
            groups = self.meta.get("blocks").get("groups", {})
            bids = inode.get("block_ids", [])
            if not bids:
                return "empty", None
            kinds = set()
            for bid in bids:
                kinds.add("ec" if bid in groups else "rep")
            if kinds == {"ec"}:
                g = groups.get(bids[0], {})
                return "ec", f"{g.get('k')}+{g.get('m')}"
            if kinds == {"rep"}:
                return "rep", None
            return "mixed", None

    def _resolve_ec_profile(self, profile_name=None):
        prof = config.EC_PROFILES.get(profile_name or config.EC_PROFILE_DEFAULT)
        if not prof:
            raise NNError(f"未知 EC 方案: {profile_name}")
        k, m = prof["k"], prof["m"]
        if k + m > config.EC_MAX_SHARDS:
            raise NNError(f"分片总数超过上限 {config.EC_MAX_SHARDS}")
        return profile_name or config.EC_PROFILE_DEFAULT, k, m

    def _ec_profile_for_cluster(self, profile_name=None):
        """结合当前存活节点数挑选可落地的 EC 方案（节点不够自动降配）。"""
        name = profile_name or config.EC_PROFILE_DEFAULT
        pname, k, m = self._resolve_ec_profile(name)
        live_count = len(self.live_nodes())
        if live_count >= k + m:
            return pname, k, m
        # 节点不足：选一个能放下的最高冗余方案
        best = None
        for cand_name, cand in config.EC_PROFILES.items():
            if cand["k"] + cand["m"] <= live_count:
                if best is None or (cand["k"] + cand["m"]
                                    > best[1] + best[2]):
                    best = (cand_name, cand["k"], cand["m"])
        if best:
            return best
        raise NNError(
            f"存活节点 {live_count} 个，不足以放置任何 EC 方案"
            f"（至少需要 2 个节点）")

    def allocate_block(self, size, checksum, desired=None, genstamp=None):
        """在块表登记新块（副本随后通过流水线复制填充）。"""
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = blocks_doc["blocks"]
            bid = gen_id("blk")
            while bid in blocks:
                bid = gen_id("blk")
            gs = genstamp or blocks_doc.get("next_genstamp", 1000)
            blocks_doc["next_genstamp"] = gs + 1
            blocks[bid] = {
                "id": bid, "size": size, "checksum": checksum,
                "genstamp": gs,
                "desired": desired or config.DEFAULT_REPLICATION,
                "created_at": now(),
                "replicas": {},
            }
            self.meta.touch("blocks")
            return bid, gs

    def register_existing_checksum(self, checksum):
        """内容去重：同校验和的块已存在则直接复用（配合 CDC 分块更有效）。"""
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            bid = blocks_doc.get("by_checksum", {}).get(checksum)
            if bid and bid in blocks_doc["blocks"]:
                blk = blocks_doc["blocks"][bid]
                if self.live_good_replicas(blk):
                    return bid
            return None

    # ==================================================================
    # 写路径：流水线复制
    # ==================================================================
    def _pipeline_put(self, bid, data, checksum, genstamp, targets):
        """
        PUT 第一个节点并让其链式转发（X-Forward-To）。
        返回 {"stored": [node_id...], "failed": [{node,error}]}
        """
        stored, failed = [], []
        if not targets:
            return {"stored": stored, "failed": failed}
        first = targets[0]
        rest = targets[1:]
        forward = [f"{self._node_url(t['node_id'])}/block/{bid}"
                   f"?genstamp={genstamp}&checksum={checksum}&size={len(data)}"
                   for t in rest]
        url = (f"{first['url'].rstrip('/')}/block/{bid}"
               f"?genstamp={genstamp}&checksum={checksum}&size={len(data)}")
        headers = {"X-Cluster-Key": self.cluster_key,
                   "Content-Type": "application/octet-stream"}
        if forward:
            headers["X-Forward-To"] = ",".join(forward)
        try:
            _s, _h, body = http_request(url, "PUT", data=data,
                                        headers=headers, timeout=30)
            resp = json.loads(body.decode("utf-8")) if body else {}
            stored.append(first["node_id"])

            def flatten(hops):
                """流水线应答是嵌套结构（dn1 -> dn2 -> dn3），递归展平。"""
                for hop in hops:
                    if hop.get("ok"):
                        if hop.get("node"):
                            stored.append(hop["node"])
                        flatten(hop.get("forwarded", []))
                    else:
                        failed.append(hop)

            flatten(resp.get("forwarded", []))
        except HttpError as e:
            failed.append({"node": first["node_id"], "error": str(e)[:200]})
        return {"stored": stored, "failed": failed}

    def _record_stored_replicas(self, bid, blk_genstamp, checksum, size,
                                stored_nodes):
        with self.meta.lock:
            blk = self.meta.get("blocks")["blocks"].get(bid)
            if not blk:
                return
            for nid in stored_nodes:
                if nid:
                    self._record_replica(bid, blk, nid, blk_genstamp,
                                         checksum, size, "ok")
            self.meta.touch("blocks")
        self.check_block_health(bid)

    def store_data_blocks(self, data, desired=None, author="system",
                          redundancy=None, ec_profile=None):
        """
        通用写入：bytes -> 分块 -> 去重 -> 冗余落盘 -> 返回块清单。
        （上传完成 / 合并写回 / 种子数据共用）

        redundancy: "rep" 三副本流水线复制；"ec" 纠删码 k+m 分片。
        两种方式产出的都是块 id 列表，上层 inode/版本树无感知；
        同一目录下不同文件可各自使用不同方式，互不干扰。
        """
        redundancy = redundancy or config.DEFAULT_REDUNDANCY
        content_hash = sha256_bytes(data)
        chunks = chunking.chunk_bytes(data)
        block_ids = []
        dedup_hits = 0
        for ch in chunks:
            if redundancy == config.REDUNDANCY_EC:
                reuse = self.register_existing_ec_checksum(ch.checksum)
            else:
                reuse = self.register_existing_checksum(ch.checksum)
            if reuse:
                block_ids.append(reuse)
                dedup_hits += 1
                continue
            if redundancy == config.REDUNDANCY_EC:
                gid = self.store_ec_block(ch.data, ch.checksum, ec_profile,
                                          author=author)
                block_ids.append(gid)
            else:
                desired = desired or config.DEFAULT_REPLICATION
                bid, gs = self.allocate_block(ch.length, ch.checksum, desired)
                targets = self.choose_targets(ch.length, desired)
                result = self._pipeline_put(bid, ch.data, ch.checksum, gs,
                                            targets)
                self._record_stored_replicas(bid, gs, ch.checksum, ch.length,
                                             result["stored"])
                if not result["stored"]:
                    # 首节点就失败：标记块缺失，交给恢复队列重试
                    self.check_block_health(bid)
                    self.log_event("ERROR", "block", "pipeline_failed", bid,
                                   author, json.dumps(result["failed"])[:400])
                block_ids.append(bid)
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            if redundancy == config.REDUNDANCY_EC:
                by_ck = blocks_doc.setdefault("ec_by_checksum", {})
            else:
                by_ck = blocks_doc.setdefault("by_checksum", {})
            for ch, bid in zip(chunks, block_ids):
                by_ck.setdefault(ch.checksum, bid)
            self.meta.touch("blocks", flush=False)
        manifest = chunking.build_manifest(chunks, total_size=len(data),
                                           content_hash=content_hash)
        manifest["redundancy"] = redundancy
        return {"block_ids": block_ids, "content_hash": content_hash,
                "manifest": manifest, "dedup_hits": dedup_hits,
                "redundancy": redundancy}

    # ------------------------------------------------------------------
    # 纠删码写路径：块 -> RS 编码 -> k+m 个分片分散到不同节点
    # ------------------------------------------------------------------
    @staticmethod
    def _shard_id(gid, idx):
        return f"{gid}__s{idx:02d}"

    @staticmethod
    def _parse_shard_id(sid):
        if "__s" not in sid:
            return None, None
        gid, _, tail = sid.rpartition("__s")
        try:
            return gid, int(tail)
        except ValueError:
            return None, None

    def register_existing_ec_checksum(self, checksum):
        """EC 内容去重：同校验和的条带组已存在且仍可解码则直接复用。"""
        live_ids = {n["node_id"] for n in self.live_nodes()}
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            gid = blocks_doc.get("ec_by_checksum", {}).get(checksum)
            group = blocks_doc.get("groups", {}).get(gid) if gid else None
            if group and self.ec_live_shards(group, live_ids)[0] \
                    >= group.get("k", 1):
                return gid
            return None

    def allocate_ec_group(self, size, checksum, k, m, stripe, profile,
                          genstamp=None):
        """在块表登记一个 EC 条带组（分片随后逐节点 PUT 填充）。"""
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            groups = blocks_doc["groups"]
            gid = gen_id("ecg")
            while gid in groups:
                gid = gen_id("ecg")
            gs = genstamp or blocks_doc.get("next_genstamp", 1000)
            blocks_doc["next_genstamp"] = gs + 1
            shards = {}
            for idx in range(k + m):
                shards[self._shard_id(gid, idx)] = {
                    "index": idx, "kind": "data" if idx < k else "parity",
                    "size": stripe, "replicas": {}}
            groups[gid] = {
                "id": gid, "type": "ec", "size": size,
                "stripe": stripe, "k": k, "m": m, "profile": profile,
                "checksum": checksum, "genstamp": gs,
                "created_at": now(), "shards": shards,
            }
            for sid in shards:
                blocks_doc.setdefault("shard_index", {})[sid] = gid
            self.meta.touch("blocks")
            return gid, gs

    def store_ec_block(self, data, checksum, profile, author="system"):
        """编码一个块并把 k+m 个分片分别 PUT 到不同节点，返回组 id。"""
        pname, k, m = self._ec_profile_for_cluster(profile)
        shards = rs_code.encode(data, k, m)
        stripe = len(shards[0])
        gid, gs = self.allocate_ec_group(len(data), checksum, k, m, stripe,
                                         pname, genstamp=None)
        targets = self.choose_targets(stripe, k + m)
        stored = []
        for idx, shard in enumerate(shards):
            if idx >= len(targets):
                break
            node = targets[idx]
            sid = self._shard_id(gid, idx)
            shard_checksum = sha256_bytes(shard)
            url = (f"{node['url'].rstrip('/')}/block/{sid}"
                   f"?genstamp={gs}&checksum={shard_checksum}"
                   f"&size={stripe}")
            try:
                http_request(url, "PUT", data=shard, headers={
                    "X-Cluster-Key": self.cluster_key,
                    "Content-Type": "application/octet-stream"}, timeout=30)
                stored.append((idx, node["node_id"], shard_checksum))
            except HttpError as e:
                self.log_event("WARN", "ec", "shard_put_failed", sid, author,
                               f"{node['node_id']}: {str(e)[:160]}")
        with self.meta.lock:
            group = self.meta.get("blocks")["groups"].get(gid)
            if group:
                for idx, nid, shard_checksum in stored:
                    sid = self._shard_id(gid, idx)
                    group["shards"][sid]["replicas"][nid] = {
                        "genstamp": gs, "checksum": shard_checksum,
                        "size": stripe, "state": "ok", "updated_at": now()}
                self.meta.touch("blocks")
        self.check_ec_group_health(gid)
        if len(stored) < k:
            self.log_event("ERROR", "ec", "group_underspread", gid, author,
                           f"仅放置 {len(stored)}/{k} 个分片，等待重建")
        else:
            self.emit("ec_stored",
                      f"纠删码组 {short_hash(gid, 12)} 已放置 "
                      f"{len(stored)}/{k + m} 分片（RS {k}+{m}，"
                      f"开销 {(k + m) / k:.2f}×）", block=gid)
        return gid

    def write_file_internal(self, path, data, author="admin", mime=None,
                            owner=None, redundancy=None, ec_profile=None):
        """
        写文件（内部 API）：确保父目录存在 -> 按目录冗余策略存块 -> 建/覆盖 inode。
        path 为完整文件路径；data 可以是 bytes 或 str（按 UTF-8 编码）。

        redundancy/ec_profile 显式给出时优先（目录切换转换流程使用）；
        否则按父目录就近继承的策略执行。
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        path = os.path.normpath(path).replace("\\", "/")
        if not path.startswith("/"):
            path = "/" + path
        dir_path = "/".join(path.split("/")[:-1]) or "/"
        name = path.split("/")[-1]
        mime = mime or guess_mime(name)
        with self.meta.lock:
            self.fs.mkdirs(dir_path, owner or author)
            if redundancy is None:
                redundancy, ec_profile = self._policy_for_inode(
                    self.fs.resolve(dir_path))
            result = self.store_data_blocks(
                data, author=author, redundancy=redundancy,
                ec_profile=ec_profile)
            inode = self.fs.create_file(dir_path, name, len(data),
                                        result["content_hash"],
                                        result["block_ids"], mime,
                                        owner or author)
            inode["redundancy"] = redundancy
        self._record_hourly("uploads", 1)
        self._record_hourly("bytes_in", len(data))
        return {"path": path, "inode_id": inode["id"],
                "size": len(data), "mime": mime,
                "content_hash": result["content_hash"],
                "block_ids": result["block_ids"],
                "chunks": len(result["block_ids"]),
                "dedup_hits": result["dedup_hits"],
                "redundancy": redundancy}

    # ==================================================================
    # 读路径（副本轮询 + 故障转移 / 纠删码解码）
    # ==================================================================
    def read_blocks(self, block_ids, verify=True):
        """按块表顺序拼接读取（版本合并/预览/diff 使用）。

        块 id 既可能是三副本块（blocks[bid]），也可能是 EC 条带组
        （groups[gid]）——两种冗余方式对上层透明。
        """
        out = []
        for bid in block_ids:
            data = self.read_block_unified(bid, verify=verify)
            out.append(data)
        return b"".join(out)

    def read_block_unified(self, bid, verify=True):
        """按块 id 的实际冗余方式读取并返回重组后的原始块字节。"""
        with self.meta.lock:
            is_group = bid in self.meta.get("blocks").get("groups", {})
        if is_group:
            return self.read_ec_block(bid, verify=verify)[0]
        data, _m, _n = self.read_block(bid, verify=verify)
        return data

    # ------------------------------------------------------------------
    # 纠删码读路径
    # ------------------------------------------------------------------
    def _ec_group(self, gid):
        return self.meta.get("blocks").get("groups", {}).get(gid)

    def ec_live_shards(self, group, live_ids=None):
        """返回 (存活好分片数, {idx: node_id}, 损坏分片 [(sid,nid)])。

        注意锁序 meta > node：调用方若已持有 meta.lock，必须从锁外
        预取 live_ids 传入，避免本方法再获取 node_lock 造成 ABBA 死锁。
        """
        if live_ids is None:
            live_ids = {n["node_id"] for n in self.live_nodes()}
        good = {}
        corrupt = []
        for sid, sh in (group.get("shards") or {}).items():
            for nid, rep in (sh.get("replicas") or {}).items():
                if nid not in live_ids:
                    continue
                if rep.get("state") != "ok":
                    corrupt.append((sid, nid))
                    continue
                if rep.get("genstamp", 0) != group.get("genstamp", 0):
                    continue
                good[sh["index"]] = nid
        return len(good), good, corrupt

    def _fetch_shard_bytes(self, group, sid, idx, nid):
        """从某节点拉一个分片并校验 sha256；失败抛异常（由调用方故障转移）。"""
        meta = group["shards"][sid]["replicas"][nid]
        url = f"{self._node_url(nid).rstrip('/')}/block/{sid}"
        _s, _h, data = http_request(
            url, "GET", headers={"X-Cluster-Key": self.cluster_key},
            timeout=15)
        if data is None or sha256_bytes(data) != meta.get("checksum"):
            raise NNError(f"分片 {sid}@{nid} 校验和不匹配")
        if len(data) != group.get("stripe"):
            raise NNError(f"分片 {sid}@{nid} 长度不符")
        return data

    def read_ec_block(self, gid, verify=True):
        """
        读取一个 EC 条带组：任取 k 个存活分片，优先用数据分片直拼；
        含校验分片或缺片时走 RS 重建。校验失败自动切换其它分片。
        返回 (data, group, served_nodes)。
        """
        cached = self.block_cache.get(gid, _MISSING)
        if cached is not _MISSING:
            with self.meta.lock:
                group = self._ec_group(gid)
            return cached, group, "cache"
        with self.meta.lock:
            group = self._ec_group(gid)
        if not group:
            raise MissingBlockError(f"EC 条带组不存在: {gid}")
        k, m, stripe = group["k"], group["m"], group["stripe"]
        live_count, good_idx, corrupt = self.ec_live_shards(group)
        if live_count < k:
            raise MissingBlockError(
                f"EC 组 {gid} 存活分片 {live_count}<k={k}，暂时无法解码")

        # 轮询起点打乱，分散读压力；优先数据分片（可免编码直拼）
        idxs = list(good_idx.keys())
        self._rr_counter += 1
        n_idxs = len(idxs)
        idxs.sort(key=lambda idx: (idx >= k,
                                   (idx + self._rr_counter) % n_idxs))
        chosen = idxs[:k]
        shard_bytes = {}
        served = []
        errors = []
        for idx in chosen:
            sid = self._shard_id(gid, idx)
            nid = good_idx[idx]
            try:
                shard_bytes[idx] = self._fetch_shard_bytes(group, sid, idx, nid)
                served.append(nid)
            except Exception as e:  # noqa: BLE001
                errors.append(f"{sid}@{nid}:{e}")
                self._mark_shard_corrupt(gid, sid, nid, reason=str(e))
                # 用备用分片替换
                for alt in idxs:
                    if alt in shard_bytes or alt in chosen:
                        continue
                    asid = self._shard_id(gid, alt)
                    anid = good_idx.get(alt)
                    if not anid:
                        continue
                    try:
                        shard_bytes[alt] = self._fetch_shard_bytes(
                            group, asid, alt, anid)
                        served.append(anid)
                        chosen.append(alt)
                        break
                    except Exception as e2:  # noqa: BLE001
                        errors.append(f"{asid}@{anid}:{e2}")
                        self._mark_shard_corrupt(gid, asid, anid,
                                                 reason=str(e2))
        if len(shard_bytes) < k:
            self.check_ec_group_health(gid)
            raise MissingBlockError(
                f"EC 组 {gid} 可用分片不足: {errors}")

        data_idxs = sorted(i for i in shard_bytes if i < k)
        if len(data_idxs) == k:
            ordered = [shard_bytes[i] for i in range(k)]
            data = rs_code.join_data_shards(
                [bytearray(x) for x in ordered], k, group["size"])
        else:
            avail = dict(shard_bytes)
            rebuilt = rs_code.reconstruct(avail, k, m, stripe,
                                          wanted=list(range(k)))
            full = dict(avail)
            full.update(rebuilt)
            data = rs_code.join_data_shards(
                [bytearray(full[i]) for i in range(k)], k, group["size"])
        if verify and sha256_bytes(data) != group["checksum"]:
            # 极少见：多个分片静默错误且恰好凑齐 k 个；触发健康评估
            self.check_ec_group_health(gid)
            raise MissingBlockError(f"EC 组 {gid} 重组后校验和不匹配")
        self.block_cache.put(gid, data)
        return data, group, served

    def _mark_shard_corrupt(self, gid, sid, nid, reason=""):
        with self.meta.lock:
            group = self._ec_group(gid)
            rep = (group or {}).get("shards", {}).get(sid, {}) \
                .get("replicas", {}).get(nid)
            if rep:
                rep["state"] = "corrupt"
                rep["updated_at"] = now()
                self.meta.touch("blocks", flush=False)
        with self.health_lock:
            self.corrupt_replicas[(sid, nid)] = {"reason": reason,
                                                  "ts": now()}
        self.check_ec_group_health(gid)
        self.log_event("WARN", "ec", "shard_read_failover",
                       f"{sid}@{nid}", "system", reason[:200])

    def read_block(self, bid, start=None, end=None, verify=True,
                   use_cache=True):
        """
        读一个块：存活好副本轮询，校验失败自动切换下一副本。
        返回 (data, block_meta, served_by)。
        """
        if use_cache and start is None:
            cached = self.block_cache.get(bid, _MISSING)
            if cached is not _MISSING:
                return cached, self._block_meta(bid), "cache"
        with self.meta.lock:
            blk = self._block_meta(bid)
        if not blk:
            raise MissingBlockError(f"块表中不存在: {bid}")
        candidates = self.live_good_replicas(blk)
        if not candidates:
            # 放宽：任何持有该块且存活的节点（读时校验兜底）
            live_ids = {n["node_id"] for n in self.live_nodes()}
            candidates = [nid for nid, r in (blk.get("replicas") or {}).items()
                          if nid in live_ids]
        if not candidates:
            raise MissingBlockError(
                f"块 {bid} 无可用副本（missing），文件暂不可读")
        # 轮询起点
        self._rr_counter += 1
        order = candidates[self._rr_counter % len(candidates):] + \
            candidates[:self._rr_counter % len(candidates)]
        errors = []
        for nid in order:
            url = f"{self._node_url(nid).rstrip('/')}/block/{bid}"
            headers = {"X-Cluster-Key": self.cluster_key}
            if start is not None:
                headers["Range"] = f"bytes={start}-{end}"
            try:
                _s, _h, data = http_request(url, "GET", headers=headers,
                                            timeout=15)
                if verify and start is None:
                    actual = sha256_bytes(data)
                    if actual != blk["checksum"]:
                        raise NNError("校验和不匹配")
                if start is None and use_cache:
                    self.block_cache.put(bid, data)
                return data, blk, nid
            except Exception as e:  # noqa: BLE001
                errors.append(f"{nid}:{e}")
                # 通知该节点此副本可疑
                with self.meta.lock:
                    b2 = self.meta.get("blocks")["blocks"].get(bid)
                    if b2 and nid in b2.get("replicas", {}):
                        b2["replicas"][nid]["state"] = "corrupt"
                        self.meta.touch("blocks", flush=False)
                self.block_cache.invalidate(bid)
                self.check_block_health(bid)
                self.log_event("WARN", "block", "read_failover", bid, "system",
                               f"副本 {nid} 读取失败，切换下一副本: {e}")
        raise MissingBlockError(f"块 {bid} 所有副本读取失败: {errors}")

    def _block_meta(self, bid):
        blk = self.meta.get("blocks")["blocks"].get(bid)
        return dict(blk) if blk else None

    def read_file_range(self, path, offset=0, length=None, user=None):
        """
        文件级 Range 读：把 [offset, offset+length) 映射到块区间逐块读取。
        返回 (data, info)。
        """
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "file":
                raise FsError(f"不是文件: {path}")
            block_ids = list(inode.get("block_ids", []))
            size = inode.get("size", 0)
        offset = max(0, min(offset, size))
        end = size - 1 if length is None else min(size - 1, offset + length - 1)
        if size == 0 or offset > end:
            return b"", {"size": size, "start": offset, "end": offset,
                         "nodes": [], "blocks_touched": 0}
        out = []
        nodes = []
        touched = 0
        pos = offset
        # 逐块定位
        block_starts = []
        acc = 0
        with self.meta.lock:
            groups = self.meta.get("blocks").get("groups", {})
            for bid in block_ids:
                blk = (self._block_meta(bid)
                       if bid not in groups else self._ec_group(bid))
                bsize = (blk or {}).get("size", 0)
                block_starts.append((bid, acc, bsize))
                acc += bsize
        for bid, bstart, bsize in block_starts:
            bend = bstart + bsize - 1
            if bend < pos or bstart > end:
                continue
            s = max(pos, bstart) - bstart
            e = min(end, bend) - bstart
            if bid in groups:
                # 纠删码块按整组解码（教学规模 64KiB；解码后在内存切片）
                data, _blk, node = self.read_ec_block(bid)
                if s != 0 or e != bsize - 1:
                    data = data[s:e + 1]
            else:
                full = (s == 0 and e == bsize - 1)
                data, _blk, node = self.read_block(
                    bid, None if full else s, None if full else e)
            out.append(data)
            if isinstance(node, list):
                nodes.extend(node)
            else:
                nodes.append(node)
            touched += 1
        data = b"".join(out)
        # 热度记录
        self.record_access(path, "download", user, len(data),
                           nodes[0] if nodes else None)
        return data, {"size": size, "start": pos, "end": end,
                      "nodes": sorted(set(nodes)), "blocks_touched": touched}

    # ==================================================================
    # 上传会话（分块上传 + 断点续传）
    # ==================================================================
    def upload_begin(self, path, filename, size, session_id=None,
                     piece_size=None, user="anonymous"):
        with self.session_lock:
            self._prune_sessions_nolock()
            if session_id and session_id in self.sessions:
                sess = self.sessions[session_id]
                if sess["filename"] == filename and sess["size"] == size:
                    sess["last_active"] = now()
                    return self._session_view(sess)
            if len(self.sessions) >= config.UPLOAD_SESSION_MAX:
                raise NNError("上传会话过多，请稍后再试")
            sess_id = session_id or gen_id("up")
            stage_dir = os.path.join(config.SESSION_DIR, sess_id)
            os.makedirs(stage_dir, exist_ok=True)
            piece = piece_size or config.UPLOAD_PIECE_SIZE
            total_pieces = max(1, (size + piece - 1) // piece) if size else 1
            sess = {
                "id": sess_id, "path": path, "filename": filename,
                "size": size, "piece_size": piece,
                "total_pieces": total_pieces,
                "received": {},            # idx -> {size, checksum, ts}
                "user": user, "created_at": now(), "last_active": now(),
                "stage_dir": stage_dir, "completed": False, "result": None,
            }
            self.sessions[sess_id] = sess
        self.log_event("INFO", "upload", "begin", f"{path}/{filename}", user,
                       f"size={size} piece={piece} pieces={total_pieces}")
        return self._session_view(sess)

    def _session_view(self, sess):
        return {
            "session": sess["id"], "path": sess["path"],
            "filename": sess["filename"], "size": sess["size"],
            "piece_size": sess["piece_size"],
            "total_pieces": sess["total_pieces"],
            "received": sorted(int(i) for i in sess["received"]),
            "received_count": len(sess["received"]),
            "completed": sess["completed"],
            "created_at": sess["created_at"],
            "expires_in": max(0, sess["last_active"] +
                              config.UPLOAD_SESSION_TTL - now()),
        }

    def upload_chunk(self, session_id, index, data_b64, checksum=None,
                     simulate_fail=False):
        import base64
        with self.session_lock:
            sess = self.sessions.get(session_id)
            if not sess:
                raise NNError("会话不存在或已过期")
            if sess["completed"]:
                raise NNError("会话已完成")
        # 混沌模式：服务端随机失败，演练前端重试
        flaky = simulate_fail or self.sim_chaos
        if flaky and random.random() < config.UPLOAD_FLAKY_RATE_CHAOS:
            raise NNError("模拟网络故障：分片写入失败（请重试）")
        try:
            data = base64.b64decode(data_b64)
        except Exception:
            raise NNError("分片 base64 解码失败")
        actual = sha256_bytes(data)
        if checksum and checksum != actual:
            raise NNError(f"分片 {index} 校验和不匹配，请重传")
        index = int(index)
        if index < 0 or index >= sess["total_pieces"]:
            raise NNError(f"非法分片序号: {index}")
        piece_path = os.path.join(sess["stage_dir"], f"piece_{index:06d}")
        from .util import atomic_write_bytes
        atomic_write_bytes(piece_path, data)     # 分片暂存也原子写
        with self.session_lock:
            sess["received"][str(index)] = {"size": len(data),
                                            "checksum": actual, "ts": now()}
            sess["last_active"] = now()
            done = len(sess["received"])
        return {"ok": True, "index": index, "checksum": actual,
                "received_count": done, "total_pieces": sess["total_pieces"],
                "complete": done == sess["total_pieces"]}

    def upload_complete(self, session_id, user="anonymous"):
        with self.session_lock:
            sess = self.sessions.get(session_id)
            if not sess:
                raise NNError("会话不存在或已过期")
            missing = [i for i in range(sess["total_pieces"])
                       if str(i) not in sess["received"]]
            if missing:
                raise NNError(f"仍有 {len(missing)} 个分片未上传: "
                              f"{missing[:10]}")
            if sess["completed"]:
                return sess["result"]
        # 读取全部分片 -> 拼接 -> 校验总大小
        datas = []
        for i in range(sess["total_pieces"]):
            piece_path = os.path.join(sess["stage_dir"], f"piece_{i:06d}")
            with open(piece_path, "rb") as f:
                datas.append(f.read())
        data = b"".join(datas)
        if sess["size"] and len(data) != sess["size"]:
            raise NNError(f"拼接后大小不符: {len(data)} != {sess['size']}")
        t0 = now()
        full_path = sess["path"].rstrip("/") + "/" + sess["filename"]
        info = self.write_file_internal(full_path, data, user)
        elapsed = now() - t0
        result = {
            "ok": True, "file": info, "elapsed_s": round(elapsed, 3),
            "throughput_mbps": round(len(data) / max(elapsed, 1e-6) / 1e6, 2),
            "pieces": sess["total_pieces"],
            "blocks": info["chunks"],
            "dedup_hits": info["dedup_hits"],
        }
        with self.session_lock:
            sess["completed"] = True
            sess["result"] = result
        self._cleanup_session_dir(sess)
        self.log_event("INFO", "upload", "complete", full_path, user,
                       f"{len(data)} 字节 / {sess['total_pieces']} 分片 / "
                       f"{info['chunks']} 块 / 去重命中 {info['dedup_hits']} / "
                       f"{result['elapsed_s']}s")
        self.emit("upload_complete",
                  f"{sess['filename']} 上传完成（{len(data)} B, "
                  f"{info['chunks']} 块）", path=full_path)
        return result

    def _cleanup_session_dir(self, sess):
        import shutil
        try:
            shutil.rmtree(sess["stage_dir"], ignore_errors=True)
        except Exception:
            pass

    def upload_status(self, session_id):
        with self.session_lock:
            sess = self.sessions.get(session_id)
            if not sess:
                raise NNError("会话不存在或已过期")
            return self._session_view(sess)

    def list_sessions(self):
        with self.session_lock:
            return [self._session_view(s) for s in self.sessions.values()]

    def _prune_sessions_nolock(self):
        t = now()
        for sid in [k for k, s in self.sessions.items()
                    if t - s["last_active"] > config.UPLOAD_SESSION_TTL]:
            sess = self.sessions.pop(sid)
            self._cleanup_session_dir(sess)

    def _session_gc_loop(self):
        while not self._stop.is_set():
            self._stop.wait(60)
            with self.session_lock:
                self._prune_sessions_nolock()

    # ==================================================================
    # 下载 / 预览 / 缩略图
    # ==================================================================
    def download_info(self, path):
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "file":
                raise FsError(f"不是文件: {path}")
            groups = self.meta.get("blocks").get("groups", {})
            blk_metas = [self._block_meta(b) if b not in groups
                         else self._ec_group(b)
                         for b in inode.get("block_ids", [])]
        return {
            "path": path, "name": inode["name"], "size": inode.get("size", 0),
            "content_hash": inode.get("content_hash"),
            "mime": inode.get("mime"),
            "redundancy": inode.get("redundancy", config.DEFAULT_REDUNDANCY),
            "blocks": [({"id": b["id"], "size": b["size"],
                         "checksum": b["checksum"][:16],
                         "genstamp": b["genstamp"],
                         "replicas": sorted(b.get("replicas", {}).keys())}
                        if b.get("type") != "ec" else
                        {"id": b["id"], "size": b["size"],
                         "checksum": b["checksum"][:16],
                         "genstamp": b["genstamp"], "type": "ec",
                         "k": b["k"], "m": b["m"],
                         "profile": b.get("profile"),
                         "stripe": b["stripe"],
                         "shards": self._shard_view(b)})
                       for b in blk_metas if b],
        }

    def _shard_view(self, group):
        """EC 组分片 -> 前端可渲染的落位列表。"""
        live_ids = {n["node_id"] for n in self.live_nodes()}
        out = []
        for sid, sh in sorted(group.get("shards", {}).items()):
            reps = sh.get("replicas") or {}
            nodes = []
            for nid, r in sorted(reps.items()):
                nodes.append({"node": nid, "state": r.get("state"),
                              "live": nid in live_ids,
                              "rack": (self.nodes.get(nid) or {}).get("rack")})
            out.append({"index": sh["index"], "id": sid,
                        "kind": sh.get("kind"), "size": sh.get("size"),
                        "short": short_hash(sid, 16),
                        "nodes": nodes})
        return out

    def preview_file(self, path):
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "file":
                raise FsError(f"不是文件: {path}")
            mime = inode.get("mime", "")
            size = inode.get("size", 0)
        data, info = self.read_file_range(
            path, 0, min(size, config.PREVIEW_MAX_BYTES))
        text = data.decode("utf-8", "replace") if not \
            (data[:8192].find(b"\x00") >= 0) else None
        return {"path": path, "mime": mime, "size": size,
                "truncated": size > len(data),
                "is_text": text is not None, "content": text,
                "nodes": info["nodes"]}

    def thumbnail(self, path):
        """图片文件读取原始字节作为缩略图（SVG/PNG 等浏览器自渲染）。"""
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "file":
                raise FsError(f"不是文件: {path}")
            mime = inode.get("mime", "")
            size = inode.get("size", 0)
            if not mime.startswith("image/"):
                raise FsError("非图片文件")
            if size > config.THUMB_MAX_BYTES:
                raise FsError("图片过大，不生成缩略图")
        data, info = self.read_file_range(path, 0, size)
        self.record_access(path, "thumb", None, len(data),
                           info["nodes"][0] if info["nodes"] else None)
        return data, mime

    # ==================================================================
    # 热度 / 统计
    # ==================================================================
    def record_access(self, path, op, user, nbytes, node=None):
        entry = {"ts": now(), "path": path,
                 "op": canonical_access_op(op, config.ACCESS_OP_CANON),
                 "user": user or "",
                 "bytes": nbytes or 0, "node": node or ""}
        with self.meta.lock:
            stats = self.meta.get("stats")
            access = stats.setdefault("access", [])
            access.append(entry)
            if len(access) > config.ACCESS_LOG_CAP:
                stats["access"] = access[-config.ACCESS_LOG_CAP:]
            self.meta.touch("stats", flush=False)
        # inode 上的计数
        try:
            with self.meta.lock:
                inode = self.fs.resolve(path, must_exist=False)
                if inode and inode["type"] == "file":
                    inode["access_count"] = inode.get("access_count", 0) + 1
                    inode["last_access"] = now()
                    self.meta.touch("fs", flush=False)
        except FsError:
            pass

    def _record_hourly(self, key, amount):
        hour = hour_key(now(), config.STATS_HOUR_OFFSET)
        with self.meta.lock:
            hourly = self.meta.get("stats").setdefault("hourly", {})
            bucket = hourly.setdefault(hour, {"uploads": 0, "downloads": 0,
                                              "bytes_in": 0, "bytes_out": 0,
                                              "reads": 0})
            bucket[key] = bucket.get(key, 0) + amount
            self.meta.touch("stats", flush=False)

    def _stats_loop(self):
        while not self._stop.is_set():
            self._stop.wait(config.STATS_INTERVAL)
            try:
                self._append_capacity_history()
                self.meta.flush_dirty()
            except Exception:
                pass

    def _append_capacity_history(self):
        with self.node_lock:
            used = sum(n.get("storage", {}).get("used", 0)
                       for n in self.nodes.values())
            cap = sum(n.get("storage", {}).get("capacity", 0)
                      for n in self.nodes.values())
        with self.meta.lock:
            fs_stats = self.fs.global_stats()
            hist = self.meta.get("stats").setdefault("capacity_history", [])
            hist.append({"ts": now(), "used": used, "capacity": cap,
                         "files": fs_stats["files"],
                         "blocks": len(self.meta.get("blocks")["blocks"])})
            if len(hist) > config.CAPACITY_HISTORY_CAP:
                self.meta.get("stats")["capacity_history"] = \
                    hist[-config.CAPACITY_HISTORY_CAP:]
            self.meta.touch("stats", flush=False)

    def overview_stats(self):
        with self.meta.lock:
            blocks = list(self.meta.get("blocks")["blocks"].values())
            fs_stats = self.fs.global_stats()
            with self.health_lock:
                under = len(self.under_replicated)
                corrupt = len(self.corrupt_replicas)
                missing = len(self.missing_blocks)
        with self.node_lock:
            nodes = list(self.nodes.values())
        total_cap = sum(n.get("storage", {}).get("capacity", 0) for n in nodes)
        total_used = sum(n.get("storage", {}).get("used", 0) for n in nodes)
        rep_dist = {}
        size_hist = {}
        ec_groups = 0
        ec_degraded_n = 0
        for blk in blocks:
            live = len(self.live_good_replicas(blk))
            rep_dist[str(live)] = rep_dist.get(str(live), 0) + 1
            bucket = chunking.size_bucket(blk.get("size", 0))
            size_hist[bucket] = size_hist.get(bucket, 0) + 1
        with self.meta.lock:
            groups = self.meta.get("blocks").get("groups", {})
        for g in groups.values():
            ec_groups += 1
            live_n = self.ec_live_shards(g)[0]
            if live_n < g["k"] + g["m"]:
                ec_degraded_n += 1
        logical = fs_stats["bytes"]
        return {
            "files": fs_stats["files"],
            "dirs": fs_stats["dirs"],
            "logical_bytes": logical,
            "physical_bytes": total_used,
            "replication_overhead": round(total_used / logical, 2)
            if logical else 0,
            "blocks": len(blocks),
            "ec_groups": ec_groups,
            "ec_degraded": ec_degraded_n,
            "block_size_hist": size_hist,
            "replica_dist": rep_dist,
            "capacity": total_cap,
            "used": total_used,
            "free": max(0, total_cap - total_used),
            "nodes_total": len(nodes),
            "nodes_live": sum(1 for n in nodes if n["state"] == "LIVE"),
            "nodes_dead": sum(1 for n in nodes if n["state"] == "DEAD"),
            "under_replicated": under,
            "corrupt_replicas": corrupt,
            "missing_blocks": missing,
            "ext_bytes": fs_stats["ext_bytes"],
            "ext_count": fs_stats["ext_count"],
            "trash": self.fs.trash_stats(),
            "versions": self.versions.repo_stats(),
            "cache": self.block_cache.stats(),
            "api_rate": round(self.api_rate.rate(), 2),
            "nn_uptime": now() - self.started_at,
            "meta": {k: {"vv": v["vv"], "bytes": v["bytes"]}
                     for k, v in self.meta.stats()["docs"].items()},
            "meta_stats": {k: v for k, v in self.meta.stats().items()
                           if k != "docs"},
        }

    def hotness(self, limit=12):
        """指数衰减热度榜 + 每文件访问 sparkline。"""
        import math
        with self.meta.lock:
            access = list(self.meta.get("stats").get("access", []))
        t = now()
        hl = config.HOTNESS_DECAY_HALF_LIFE
        per_file = {}
        for e in access:
            if e.get("op") not in ("download", "preview", "thumb"):
                continue
            age = max(0.0, t - e["ts"])
            weight = 0.5 ** (age / hl)
            f = per_file.setdefault(e["path"], {
                "path": e["path"], "score": 0.0, "count": 0, "bytes": 0,
                "last": 0, "ops": {}, "buckets": [0] * 12})
            f["score"] += weight
            f["count"] += 1
            f["bytes"] += e.get("bytes", 0)
            f["last"] = max(f["last"], e["ts"])
            f["ops"][e["op"]] = f["ops"].get(e["op"], 0) + 1
            # 最近 12 个时间桶（按访问记录窗口均分）
            bucket = int(age // (config.HOTNESS_DECAY_HALF_LIFE / 2))
            if bucket < 12:
                f["buckets"][11 - bucket] += 1
        top = sorted(per_file.values(), key=lambda x: -x["score"])[:limit]
        for f in top:
            f["score"] = round(f["score"], 3)
        return top

    def timeline_stats(self, hours=24):
        import time as _time
        with self.meta.lock:
            hourly = dict(self.meta.get("stats").get("hourly", {}))
            hist = list(self.meta.get("stats").get("capacity_history", []))
        buckets = []
        base = int(now() // 3600) * 3600
        for i in range(hours - 1, -1, -1):
            ts = base - i * 3600
            key = _time.strftime("%Y-%m-%dT%H", _time.localtime(ts))
            b = hourly.get(key, {})
            buckets.append({"hour": key,
                            "uploads": b.get("uploads", 0),
                            "downloads": b.get("downloads", 0),
                            "bytes_in": b.get("bytes_in", 0),
                            "bytes_out": b.get("bytes_out", 0)})
        return {"hourly": buckets, "capacity_history": hist[-300:]}

    # ==================================================================
    # 节点视图 / 演练
    # ==================================================================
    def nodes_view(self):
        with self.node_lock:
            nodes = [dict(n) for n in self.nodes.values()]
        per_node_blocks = {n["node_id"]: 0 for n in nodes}
        per_node_bytes = {n["node_id"]: 0 for n in nodes}
        per_node_corrupt = {n["node_id"]: 0 for n in nodes}
        per_node_shards = {n["node_id"]: 0 for n in nodes}
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = list(blocks_doc["blocks"].values())
            groups = list(blocks_doc.get("groups", {}).values())
        for blk in blocks:
            for nid, rep in list((blk.get("replicas") or {}).items()):
                if nid in per_node_blocks:
                    per_node_blocks[nid] += 1
                    per_node_bytes[nid] += rep.get("size", 0)
                    if rep.get("state") == "corrupt":
                        per_node_corrupt[nid] += 1
        for group in groups:
            for sh in group.get("shards", {}).values():
                for nid, rep in (sh.get("replicas") or {}).items():
                    if nid in per_node_shards:
                        per_node_shards[nid] += 1
                        per_node_bytes[nid] += rep.get("size", 0)
                        if rep.get("state") == "corrupt":
                            per_node_corrupt[nid] += 1
        out = []
        for n in sorted(nodes, key=lambda x: x["node_id"]):
            nid = n["node_id"]
            out.append({
                "node_id": nid, "rack": n.get("rack"), "url": n.get("url"),
                "state": n.get("state"), "registered_at": n.get("registered_at"),
                "last_seen": n.get("last_seen"),
                "last_seen_ago": now() - n.get("last_seen", 0),
                "last_report_at": n.get("last_report_at"),
                "hb_count": n.get("hb_count", 0),
                "deaths": n.get("deaths", 0),
                "dead_at": n.get("dead_at"),
                "storage": n.get("storage", {}),
                "io": n.get("io", {}), "rates": n.get("rates", {}),
                "uptime": n.get("uptime", 0),
                "vv": n.get("vv", {}), "doc_vv": n.get("doc_vv", {}),
                "nn_block_count": per_node_blocks.get(nid, 0),
                "nn_shard_count": per_node_shards.get(nid, 0),
                "nn_block_bytes": per_node_bytes.get(nid, 0),
                "corrupt": per_node_corrupt.get(nid, 0),
                "pending_commands": len(self.pending_commands.get(nid, [])),
            })
        with self.health_lock:
            health = {
                "under_replicated": len(self.under_replicated),
                "corrupt_replicas": len(self.corrupt_replicas),
                "missing_blocks": len(self.missing_blocks),
                "scheduled": len(self.scheduled),
                "ec_degraded": len(self.ec_degraded),
                "ec_missing": len(self.ec_missing),
                "ec_repairing": sum(
                    1 for t in self.ec_repairs.values()
                    if t.get("state") == "running"),
            }
        return {"nodes": out, "health": health,
                "summary": {
                    "total": len(out),
                    "live": sum(1 for n in out if n["state"] == "LIVE"),
                    "suspect": sum(1 for n in out if n["state"] == "SUSPECT"),
                    "dead": sum(1 for n in out if n["state"] == "DEAD"),
                }}

    def node_blocks(self, node_id, limit=200, offset=0):
        """从 DataNode 实时拉取其块清单（HTTP 同步演示）。"""
        dn = self.local_datanodes.get(node_id)
        if dn:
            return dn.block_list(limit, offset)
        # 远程节点：走块表反查（三副本块 + EC 分片）
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            items = [(bid, b, None) for bid, b in blocks_doc["blocks"].items()
                     if node_id in (b.get("replicas") or {})]
            groups = blocks_doc.get("groups", {})
            for _gid, g in groups.items():
                for sid, sh in g.get("shards", {}).items():
                    if node_id in (sh.get("replicas") or {}):
                        items.append((sid, sh["replicas"][node_id],
                                      sh.get("kind")))
        total = len(items)
        out = []
        for bid, rep, ec_kind in items[offset:offset + limit]:
            pgid, pidx = self._parse_shard_id(bid)
            out.append({"id": bid, "genstamp": rep["genstamp"],
                        "size": rep["size"], "state": rep["state"],
                        "checksum": rep["checksum"][:16],
                        "stored_at": rep.get("updated_at"),
                        "ec_group": pgid, "ec_index": pidx,
                        "ec_kind": ec_kind})
        return {"total": total, "blocks": out}

    def replica_matrix(self, limit=60):
        """块 x 节点 分布矩阵（节点页可视化）。

        三副本块每节点一格（副本状态）；EC 组每格可含 0/1/多个分片，
        用 "s<idx>" 标注数据/校验分片，损坏分片标记为 corrupt。
        """
        with self.node_lock:
            node_ids = sorted(self.nodes.keys())
            live_ids = {nid for nid, n in self.nodes.items()
                        if n["state"] == "LIVE"}
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = dict(blocks_doc["blocks"])
            groups = dict(blocks_doc.get("groups", {}))

        def good_count(blk):
            n = 0
            for nid, rep in (blk.get("replicas") or {}).items():
                if nid in live_ids and rep.get("state") == "ok" and \
                        rep.get("genstamp", 0) == blk.get("genstamp", 0):
                    n += 1
            return n

        # 优先展示有问题的块
        def blk_score(bid_b):
            bid, blk = bid_b
            return (good_count(blk) - blk.get("desired", 3),
                    -len(blk.get("replicas", {})))

        rep_sorted = sorted(blocks.items(), key=blk_score)
        rows = []
        for bid, blk in rep_sorted:
            cells = {}
            for nid in node_ids:
                rep = (blk.get("replicas") or {}).get(nid)
                if not rep:
                    cells[nid] = "-"
                elif rep.get("state") == "ok" and \
                        rep.get("genstamp") == blk.get("genstamp"):
                    cells[nid] = "ok"
                elif rep.get("state") == "corrupt":
                    cells[nid] = "corrupt"
                else:
                    cells[nid] = "stale"
            live = good_count(blk)
            rows.append({"block": bid,
                         "short": short_hash(bid.replace("blk_", ""), 8),
                         "kind": "rep", "size": blk.get("size", 0),
                         "desired": blk.get("desired"),
                         "live": live, "cells": cells,
                         "status": ("missing" if live == 0 else
                                    "under" if live < blk.get("desired", 3)
                                    else "ok")})

        # EC 组：问题组优先
        def ec_score(item):
            gid, g = item
            live_n = self.ec_live_shards(g)[0]
            return (live_n - (g["k"] + g["m"]), -len(g.get("shards", {})))

        ec_sorted = sorted(groups.items(), key=ec_score)
        for gid, g in ec_sorted:
            live_n, good_idx, _c = self.ec_live_shards(g)
            desired = g["k"] + g["m"]
            # 每节点 -> 该节点持有的分片序号集合（含状态）
            cells = {nid: {"shards": []} for nid in node_ids}
            for sid, sh in g.get("shards", {}).items():
                for nid, rep in (sh.get("replicas") or {}).items():
                    if nid not in cells:
                        cells[nid] = {"shards": []}
                    cells[nid]["shards"].append({
                        "index": sh["index"], "sid": sid,
                        "kind": sh.get("kind"),
                        "state": rep.get("state"),
                        "live": nid in live_ids})
            rows.append({"block": gid,
                         "short": short_hash(gid.replace("ecg_", ""), 8),
                         "kind": "ec", "size": g.get("size", 0),
                         "desired": desired, "live": live_n,
                         "k": g["k"], "m": g["m"],
                         "profile": g.get("profile"),
                         "cells": cells,
                         "status": ("missing" if live_n < g["k"] else
                                    "degraded" if live_n < desired else "ok")})
        # 不健康的排前
        order = {"missing": 0, "degraded": 1, "under": 1, "ok": 2}
        rows.sort(key=lambda r: order.get(r["status"], 3))
        total_blocks = len(blocks) + len(groups)
        return {"nodes": node_ids, "rows": rows[:limit],
                "total_blocks": total_blocks,
                "total_rep": len(blocks), "total_ec": len(groups)}

    def health_queue(self):
        with self.health_lock:
            under = dict(self.under_replicated)
            corrupt = {f"{b}@{n}": dict(v) for (b, n), v
                       in self.corrupt_replicas.items()}
            missing = set(self.missing_blocks)
            scheduled = dict(self.scheduled)
            ec_degraded = dict(self.ec_degraded)
            ec_missing = set(self.ec_missing)
            ec_repairs = {gid: dict(t) for gid, t in self.ec_repairs.items()}
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            groups = self.meta.get("blocks").get("groups", {})
            under_items = []
            for bid, info in list(under.items())[:80]:
                blk = blocks.get(bid, {})
                under_items.append({
                    "block": bid, "desired": blk.get("desired"),
                    "live": len(self.live_good_replicas(blk)) if blk else 0,
                    "since": info["since"], "attempts": info.get("attempts", 0),
                    "scheduled": scheduled.get(bid),
                    "size": blk.get("size", 0),
                })
            ec_items = []
            for gid, info in list(ec_degraded.items())[:80]:
                g = groups.get(gid, {})
                live_n = self.ec_live_shards(g)[0] if g else 0
                task = ec_repairs.get(gid, {})
                ec_items.append({
                    "group": gid, "k": g.get("k"), "m": g.get("m"),
                    "profile": g.get("profile"),
                    "desired": (g.get("k", 0) + g.get("m", 0)),
                    "live": live_n,
                    "since": info["since"], "attempts": info.get("attempts", 0),
                    "state": task.get("state"),
                    "finished": task.get("finished", 0),
                    "total": task.get("total", 0),
                    "missing": task.get("missing", []),
                    "targets": task.get("targets", []),
                    "last_error": task.get("last_error"),
                    "size": g.get("size", 0),
                })
        return {"under_replicated": under_items, "corrupt": corrupt,
                "missing": sorted(missing)[:80],
                "ec_groups": ec_items,
                "ec_missing": sorted(ec_missing)[:80],
                "counts": {"under": len(under), "corrupt": len(corrupt),
                           "missing": len(missing),
                           "scheduled": len(scheduled),
                           "ec_degraded": len(ec_degraded),
                           "ec_missing": len(ec_missing),
                           "ec_repairing": sum(
                               1 for t in ec_repairs.values()
                               if t.get("state") == "running")}}

    # ---- 演练 ----
    def sim_kill_node(self, node_id):
        dn = self.local_datanodes.get(node_id)
        if not dn:
            raise NNError(f"节点不在本进程管理内: {node_id}")
        dn.kill_sim()
        with self.node_lock:
            n = self.nodes.get(node_id)
            if n:
                n["killed_flag"] = True
        self.log_event("WARN", "sim", "kill_node", node_id, "admin",
                       "故障演练：手动杀死节点")
        self.emit("sim_kill", f"演练：节点 {node_id} 已被杀死", node=node_id)
        return {"ok": True}

    def sim_revive_node(self, node_id):
        dn = self.local_datanodes.get(node_id)
        if not dn:
            raise NNError(f"节点不在本进程管理内: {node_id}")
        dn.revive_sim()
        self.log_event("INFO", "sim", "revive_node", node_id, "admin",
                       "故障演练：节点复活")
        self.emit("sim_revive", f"演练：节点 {node_id} 已复活", node=node_id)
        return {"ok": True}

    def sim_corrupt_block(self, block_id, node_id):
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blk = blocks_doc["blocks"].get(block_id)
            pgid, _pidx = self._parse_shard_id(block_id)
            shard = None
            if not blk and pgid is not None:
                shard = (blocks_doc.get("groups", {}).get(pgid, {})
                         .get("shards", {}).get(block_id))
        if not blk and not shard:
            raise NNError(f"块/分片不存在: {block_id}")
        if blk and node_id not in (blk.get("replicas") or {}):
            raise NNError(f"{node_id} 上没有块 {block_id} 的副本")
        if shard and node_id not in (shard.get("replicas") or {}):
            raise NNError(f"{node_id} 上没有分片 {block_id}")
        dn = self.local_datanodes.get(node_id)
        if not dn:
            raise NNError(f"节点不在本进程管理内: {node_id}")
        dn.corrupt_block_sim(block_id)
        if shard:
            self.log_event("WARN", "sim", "corrupt_shard",
                           f"{block_id}@{node_id}", "admin",
                           "故障演练：注入 EC 分片静默损坏（等待巡检/读取发现）")
            self.emit("sim_corrupt",
                      f"演练：EC 分片 {short_hash(block_id, 16)}@{node_id} "
                      f"已注入损坏", node=node_id, block=pgid)
        else:
            self.log_event("WARN", "sim", "corrupt_block",
                           f"{block_id}@{node_id}", "admin",
                           "故障演练：注入静默数据损坏（等待巡检/读取发现）")
            self.emit("sim_corrupt",
                      f"演练：块 {short_hash(block_id, 12)}@{node_id} 已注入损坏",
                      node=node_id, block=block_id)
        return {"ok": True}

    def sim_chaos_mode(self, enabled):
        self.sim_chaos = bool(enabled)
        self.log_event("WARN", "sim", "chaos_mode", str(enabled), "admin", "")
        return {"ok": True, "enabled": self.sim_chaos}

    # ==================================================================
    # GC（版本树保护下的块回收）
    # ==================================================================
    def referenced_blocks(self):
        refs = set()
        with self.meta.lock:
            for _p, inode in self.fs.all_files():
                refs.update(inode.get("block_ids", []))
            trash_root = self.fs.get_inode(self.fs.trash_id)
            if trash_root:
                stack = list(trash_root.get("children", []))
                inodes = self.fs._inodes()
                while stack:
                    cur = stack.pop()
                    node = inodes.get(cur)
                    if not node:
                        continue
                    refs.update(node.get("block_ids", []))
                    stack.extend(node.get("children", []))
        refs |= self.versions.all_referenced_blocks()
        with self.session_lock:
            for sess in self.sessions.values():
                refs.update(sess.get("result", {}).get("file", {})
                            .get("block_ids", []) if sess.get("result") else [])
        return refs

    def gc_blocks(self):
        """回收未被引用的块（宽限期防止误删刚写的块）。

        三副本块：下发各副本删除；EC 组：下发各分片所在节点删除。
        """
        refs = self.referenced_blocks()
        t = now()
        deleted = 0
        ec_deleted = 0
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = blocks_doc["blocks"]
            groups = blocks_doc.get("groups", {})
            by_ck = blocks_doc.get("by_checksum", {})
            ec_by_ck = blocks_doc.get("ec_by_checksum", {})
            shard_index = blocks_doc.get("shard_index", {})
            to_delete = []
            for bid, blk in blocks.items():
                if bid in refs:
                    continue
                if t - blk.get("created_at", t) < config.GC_GRACE_SECONDS:
                    continue
                to_delete.append(bid)
            ec_to_delete = []
            for gid, group in groups.items():
                if gid in refs:
                    continue
                if t - group.get("created_at", t) < config.GC_GRACE_SECONDS:
                    continue
                ec_to_delete.append(gid)
            for bid in to_delete:
                blk = blocks.pop(bid)
                ck = blk.get("checksum")
                if by_ck.get(ck) == bid:
                    by_ck.pop(ck, None)
                for nid in (blk.get("replicas") or {}):
                    self._enqueue_command(nid, {
                        "type": "delete", "block_id": bid,
                        "reason": "GC：无引用的孤儿块"})
                deleted += 1
                with self.health_lock:
                    self.under_replicated.pop(bid, None)
                    self.missing_blocks.discard(bid)
            for gid in ec_to_delete:
                group = groups.pop(gid)
                if ec_by_ck.get(group.get("checksum")) == gid:
                    ec_by_ck.pop(group["checksum"], None)
                targets = set()
                for sid, sh in group.get("shards", {}).items():
                    shard_index.pop(sid, None)
                    for nid in (sh.get("replicas") or {}):
                        targets.add(nid)
                    # 兜底：即使副本表为空，也向所有曾放置节点下发删除
                self.cache_invalidate_group(gid)
                for sid in group.get("shards", {}):
                    for nid in targets:
                        self._enqueue_command(nid, {
                            "type": "delete", "block_id": sid,
                            "reason": "GC：无引用的 EC 分片"})
                ec_deleted += 1
                with self.health_lock:
                    self.ec_degraded.pop(gid, None)
                    self.ec_missing.discard(gid)
                    self.ec_repairs.pop(gid, None)
            if deleted or ec_deleted:
                self.meta.touch("blocks")
        if deleted or ec_deleted:
            self.log_event("INFO", "gc", "gc_blocks", "", "system",
                           f"回收 {deleted} 个副本块、{ec_deleted} 个 EC 组")
        return deleted + ec_deleted

    def cache_invalidate_group(self, gid):
        self.block_cache.invalidate(gid)

    def _gc_loop(self):
        while not self._stop.is_set():
            self._stop.wait(config.GC_INTERVAL)
            try:
                self.gc_blocks()
            except Exception as e:  # noqa: BLE001
                self.log_event("ERROR", "gc", "loop_error", "", "system", str(e))

    def _trash_loop(self):
        while not self._stop.is_set():
            self._stop.wait(config.TRASH_EXPIRE_CHECK_INTERVAL)
            try:
                expired, freed = self.fs.purge_expired()
                if expired:
                    self.log_event("INFO", "fs", "trash_expire", "", "system",
                                   f"回收站过期清理 {len(expired)} 项（单位 "
                                   f"{config.TRASH_RETENTION_UNIT}），"
                                   f"释放 {len(freed)} 个块引用")
            except Exception:
                pass

    # ==================================================================
    # 块详情（文件详情页：块 -> 副本 -> 节点）
    # ==================================================================
    def file_blocks_detail(self, path):
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "file":
                raise FsError(f"不是文件: {path}")
            out = []
            for bid in inode.get("block_ids", []):
                if bid in self.meta.get("blocks").get("groups", {}):
                    out.append(self._ec_group_detail(bid))
                    continue
                blk = self._block_meta(bid)
                if not blk:
                    out.append({"id": bid, "missing": True})
                    continue
                live = self.live_good_replicas(blk)
                out.append({
                    "id": bid, "type": "rep",
                    "short": short_hash(bid.replace("blk_", ""), 8),
                    "size": blk["size"],
                    "checksum": blk["checksum"][:16],
                    "genstamp": blk["genstamp"],
                    "desired": blk.get("desired"),
                    "live": len(live),
                    "status": ("missing" if not live else
                               "under" if len(live) < blk.get("desired", 3)
                               else "ok"),
                    "replicas": [
                        {"node": nid, "state": r.get("state"),
                         "genstamp": r.get("genstamp"),
                         "size": r.get("size"),
                         "updated_at": r.get("updated_at"),
                         "rack": (self.nodes.get(nid) or {}).get("rack"),
                         "live": nid in {n["node_id"] for n in self.live_nodes()}}
                        for nid, r in sorted((blk.get("replicas") or {}).items())],
                })
            redundancy, profile = self.file_redundancy(inode)
            # 该文件各 EC 组的修复进度汇总
            repair = []
            with self.health_lock:
                for b in out:
                    task = self.ec_repairs.get(b.get("id"))
                    if task and task.get("state") in ("running", "pending"):
                        repair.append({
                            "group": b["id"], "state": task["state"],
                            "finished": task.get("finished", 0),
                            "total": task.get("total", 0),
                            "targets": task.get("targets", []),
                            "missing": task.get("missing", [])})
            return {"path": path, "size": inode.get("size", 0),
                    "content_hash": inode.get("content_hash"),
                    "redundancy": inode.get("redundancy",
                                           config.DEFAULT_REDUNDANCY),
                    "redundancy_actual": redundancy,
                    "ec_profile": profile,
                    "repairs": repair,
                    "blocks": out}

    def _ec_group_detail(self, gid):
        group = self._ec_group(gid)
        if not group:
            return {"id": gid, "missing": True, "type": "ec"}
        k, m = group["k"], group["m"]
        live_n, _good, corrupt = self.ec_live_shards(group)
        desired = k + m
        status = ("missing" if live_n < k else
                  "degraded" if live_n < desired else "ok")
        with self.health_lock:
            task = self.ec_repairs.get(gid)
        return {
            "id": gid, "type": "ec",
            "short": short_hash(gid.replace("ecg_", ""), 8),
            "size": group["size"], "stripe": group["stripe"],
            "checksum": group["checksum"][:16],
            "genstamp": group["genstamp"],
            "k": k, "m": m, "profile": group.get("profile"),
            "desired": desired, "live": live_n, "status": status,
            "repair": ({"state": task.get("state"),
                        "finished": task.get("finished", 0),
                        "total": task.get("total", 0),
                        "targets": task.get("targets", []),
                        "missing": task.get("missing", [])}
                       if task and task.get("state") in
                       ("running", "pending", "done") else None),
            "shards": self._shard_view(group),
        }

    def block_paths(self, bid):
        """反查引用某块/EC组的文件路径（节点页/健康队列展示用）。"""
        with self.meta.lock:
            paths = [p for p, inode in self.fs.all_files()
                     if bid in inode.get("block_ids", [])]
        return paths

    # ==================================================================
    # 冗余总览（冗余管理页 / 统计）
    # ==================================================================
    def redundancy_overview(self):
        """全集群按文件统计两种冗余方式的用量与健康度。"""
        files_rep = files_ec = files_mixed = 0
        bytes_rep = bytes_ec = 0
        groups_rep_bytes = groups_ec_bytes = 0
        ec_profiles = {}
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            groups = blocks_doc.get("groups", {})
            rep_blocks = blocks_doc["blocks"]
            # 物理占用：EC 分片按 stripe*(k+m) 估算（仅统计已落位分片）
            for gid, g in groups.items():
                placed = sum(len(sh.get("replicas") or {})
                             for sh in g.get("shards", {}).values())
                groups_ec_bytes += placed * g.get("stripe", 0)
                p = g.get("profile", config.EC_PROFILE_DEFAULT)
                ec_profiles[p] = ec_profiles.get(p, 0) + 1
            for bid, b in rep_blocks.items():
                groups_rep_bytes += len(b.get("replicas") or {}) \
                    * b.get("size", 0)
            for _p, inode in self.fs.all_files():
                kind, prof = self.file_redundancy(inode)
                size = inode.get("size", 0)
                if kind == "ec":
                    files_ec += 1
                    bytes_ec += size
                elif kind == "rep":
                    files_rep += 1
                    bytes_rep += size
                elif kind == "mixed":
                    files_mixed += 1
        with self.health_lock:
            ec_health = {"degraded": len(self.ec_degraded),
                         "missing": len(self.ec_missing),
                         "repairing": sum(1 for t in self.ec_repairs.values()
                                         if t.get("state") == "running")}
        return {
            "files_rep": files_rep, "files_ec": files_ec,
            "files_mixed": files_mixed,
            "logical_rep": bytes_rep, "logical_ec": bytes_ec,
            "physical_rep": groups_rep_bytes,
            "physical_ec": groups_ec_bytes,
            "ec_profiles": ec_profiles,
            "ec_health": ec_health,
            "profiles": config.EC_PROFILES,
        }

    def redundancy_dirs(self):
        """列出显式设置过冗余策略的目录（含根的默认值）。"""
        out = []
        with self.meta.lock:
            inodes = self.fs._inodes()
            for iid, node in inodes.items():
                if node.get("type") != "dir":
                    continue
                if iid == self.fs.root_id or node.get("redundancy"):
                    path = self.fs.path_of(iid)
                    stats = self.fs.dir_stats(node)
                    eff, prof = self._policy_for_inode(node)
                    out.append({
                        "id": iid, "path": path,
                        "redundancy": node.get("redundancy",
                                              config.DEFAULT_REDUNDANCY),
                        "effective": eff, "ec_profile":
                            node.get("ec_profile", prof),
                        "explicit": bool(node.get("redundancy"))
                        or iid == self.fs.root_id,
                        "is_root": iid == self.fs.root_id,
                        "files": stats["files"], "bytes": stats["bytes"],
                        "updated_by": node.get("policy_updated_by"),
                        "updated_at": node.get("policy_updated_at")})
        out.sort(key=lambda x: x["path"])
        return out
