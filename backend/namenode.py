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

from . import chunking, config, erasure
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
        # ---- 纠删码（EC）组健康状态 ----
        self.ec_degraded = set()         # 可用分片 < k+m 但仍 >= k
        self.ec_critical = set()         # 可用分片 < k（再坏就丢数据）
        self.ec_rebuilding = {}          # gid -> {"targets":[n],"at":ts}
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
        self._init_ec_doc()
        self._init_stats_doc()
        self.meta.start_flusher()

        for target, name in (
                (self._liveness_loop, "nn-liveness"),
                (self._recovery_loop, "nn-recovery"),
                (self._gc_loop, "nn-gc"),
                (self._stats_loop, "nn-stats"),
                (self._trash_loop, "nn-trash"),
                (self._session_gc_loop, "nn-session-gc")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

        # 启动后做一次 EC 组全量健康评估（恢复上次运行中的重建状态）
        self._rescan_ec_groups()

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
            blocks.setdefault("next_genstamp", 1000)
            self.meta.touch("blocks", flush=False)

    def _init_ec_doc(self):
        with self.meta.lock:
            ec = self.meta.get("ec_groups")
            ec.setdefault("groups", {})
            self.meta.touch("ec_groups", flush=False)

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
            "ec_report_due": True,
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
        with self.node_lock:
            self.nodes[node_id]["revived_at"] = now()
            self.nodes[node_id]["ec_report_due"] = True
        self.log_event("INFO", "recovery", "node_revived", node_id, "system",
                       f"节点 {node_id} 恢复心跳，重新标记 LIVE，要求全量块汇报")
        self.emit("node_revived", f"节点 {node_id} 复活", node=node_id)
        self._enqueue_command(node_id, {"type": "report"})
        # 该节点上的副本重新纳入健康评估（不持锁调用，check_block_health 自会加锁）
        self._rescan_all_blocks()

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
        """节点故障：其上的副本全部失效，低于期望副本数的块进入恢复队列。"""
        affected = 0
        ec_affected = set()
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            for bid, blk in blocks.items():
                reps = blk.get("replicas", {})
                if dead_node in reps:
                    gid = blk.get("ec_group")
                    if gid:
                        ec_affected.add(gid)
                    else:
                        affected += 1
                        self.check_block_health(bid)
        for gid in ec_affected:
            self.check_ec_group(gid)
        self.emit("recovery_start",
                  f"节点 {dead_node} 故障波及 {affected} 个副本块、"
                  f"{len(ec_affected)} 个纠删码组，开始自动修复",
                  node=dead_node, affected=affected, ec_groups=len(ec_affected))
        self.log_event("WARN", "recovery", "failure_scan", dead_node, "system",
                       f"故障扫描：{affected} 个副本块 / {len(ec_affected)} 个 EC 组受影响")

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
            gid = None
            with self.meta.lock:
                blk = self.meta.get("blocks")["blocks"].get(bid)
                if blk:
                    gid = blk.get("ec_group")
                    if gid:
                        # EC 分片重建完成：登记副本并做单组归一化（去重复制）
                        self._record_replica(
                            bid, blk, node_id,
                            event.get("genstamp", blk["genstamp"]),
                            event.get("checksum", blk["checksum"]),
                            event.get("size", blk["size"]), "ok")
                        self._normalize_ec_group(gid)
                        self.meta.touch("blocks", flush=False)
                    else:
                        self._record_replica(
                            bid, blk, node_id,
                            event.get("genstamp", blk["genstamp"]),
                            event.get("checksum", blk["checksum"]),
                            event.get("size", blk["size"]), "ok")
                        self.meta.touch("blocks", flush=False)
            if gid:
                self._on_ec_shard_done(gid, bid, node_id,
                                       event.get("indexes", []))
            self.check_block_health(bid)
            self.scheduled.pop(bid, None)
            if not gid:
                self.emit("replicate_done",
                          f"块 {short_hash(bid, 12)} 成功复制到 {node_id}",
                          node=node_id, block=bid)
        elif etype == "replicate_failed":
            bid = event.get("block_id")
            self.scheduled.pop(bid, None)
            with self.meta.lock:
                blk = self.meta.get("blocks")["blocks"].get(bid)
                gid = blk.get("ec_group") if blk else None
            if gid:
                self._on_ec_rebuild_failed(
                    gid, node_id, event.get("indexes", []),
                    event.get("reason", ""))
            else:
                self.check_block_health(bid)
            self.log_event("WARN", "recovery", "replicate_failed",
                           bid or "", "system",
                           f"{node_id}: {event.get('reason', '')}")
        elif etype == "corrupt":
            bid = event.get("block_id")
            gid = None
            with self.meta.lock:
                blk = self.meta.get("blocks")["blocks"].get(bid)
                if blk and node_id in blk.get("replicas", {}):
                    blk["replicas"][node_id]["state"] = "corrupt"
                    blk["replicas"][node_id]["updated_at"] = now()
                    gid = blk.get("ec_group")
                    self.meta.touch("blocks", flush=False)
            with self.health_lock:
                self.corrupt_replicas[(bid, node_id)] = {
                    "reason": event.get("reason", ""), "ts": now()}
            if gid:
                self.check_ec_group(gid)
            else:
                self.check_block_health(bid)
            self.log_event("ERROR", "block", "corrupt", f"{bid}@{node_id}",
                           "system", event.get("reason", ""))
            self.emit("corrupt", f"节点 {node_id} 发现块 {short_hash(bid, 12)} 损坏",
                      node=node_id, block=bid)
        elif etype == "deleted":
            bid = event.get("block_id")
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
        elif etype == "ec_rebuild_report":
            gid = event.get("group")
            self.log_event("INFO", "ec", "rebuild_report", gid or "", "system",
                           f"{node_id} 重建分片 {event.get('indexes')}")

    # ==================================================================
    # 块汇报对账（难点一/二：副本一致性）
    # ==================================================================
    def handle_block_report(self, payload):
        node_id = payload.get("node_id")
        if not node_id:
            raise NNError("缺少 node_id")
        commands = []
        reported = set()
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = blocks_doc["blocks"]
            touched = False
            for rep in payload.get("blocks", []):
                bid = rep["id"]
                reported.add(bid)
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
            # 孤儿块（磁盘有、索引外）
            for bid in payload.get("orphans", []):
                commands.append({"type": "delete", "block_id": bid,
                                 "reason": "孤儿块"})
            if touched:
                self.meta.touch("blocks", flush=False)
        with self.node_lock:
            node = self.nodes.get(node_id)
            if node:
                node["last_report_at"] = now()
                node["block_count"] = len(payload.get("blocks", []))
                node.pop("ec_report_due", None)
        # 汇报后重估相关块健康度（EC 分片块内部转 EC 组评估 + 副本归一化）
        for bid in reported:
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

    def _node_ready_for_ec(self, node):
        """
        节点是否可作为 EC 重建目标：
        新注册 / 刚复活的节点在完成一次全量块汇报前，NN 块表不完整
        （它磁盘上可能已持有某些分片），排除之，避免把两个分片重建到
        同一节点。用显式 ec_report_due 标记判定：注册/复活置位，
        收到一次全量块汇报后清除。
        """
        return not node.get("ec_report_due", False)

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
        # EC 分片块：健康度由所属纠删码组统一评估（单分片不做三副本队列）
        gid = blk.get("ec_group")
        if gid:
            self.check_ec_group(gid)
            return {"block": bid, "state": "ec",
                    "group": gid,
                    "live": len(self.live_good_replicas(blk))}
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
            bids = list(self.meta.get("blocks")["blocks"].keys())
        for bid in bids:
            self.check_block_health(bid)
        self._rescan_ec_groups()

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
        # 先调度纠删码组分片重建，再调度普通三副本再复制
        try:
            self._ec_schedule_once()
        except Exception as e:  # noqa: BLE001
            self.log_event("ERROR", "ec", "schedule_error", "", "system",
                           str(e))
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
            if blk.get("ec_group"):
                # EC 分片块不进三副本队列（由 EC 调度器统一重建）
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
                "ec_profiles": {n: {"k": p["k"], "m": p["m"]}
                                for n, p in config.EC_PROFILES.items()},
                "default_storage_policy": config.DEFAULT_STORAGE_POLICY,
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
    # 纠删码（Erasure Coding）：组健康 / 自动重建 / 读写
    # ==================================================================
    def _ec_doc(self):
        return self.meta.get("ec_groups")

    @staticmethod
    def _ec_profile(group):
        p = config.EC_PROFILES.get(group.get("profile"))
        if p:
            return p
        return {"k": group.get("k"), "m": group.get("m")}

    def _ec_group(self, gid):
        with self.meta.lock:
            return self._ec_doc()["groups"].get(gid)

    def _ec_shard_map(self, group, live_only=True):
        """
        返回 {index: [(node, rep), ...]}（ok 且 genstamp 匹配的分片副本）。
        存活好分片的节点集合 = shard_nodes。
        """
        live_ids = ({n["node_id"] for n in self.live_nodes()}
                    if live_only else None)
        shards = {}
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            for idx_s, bid in group["shards"].items():
                idx = int(idx_s)
                blk = blocks.get(bid)
                if not blk:
                    shards[idx] = []
                    continue
                locs = []
                for nid, rep in (blk.get("replicas") or {}).items():
                    if live_ids is not None and nid not in live_ids:
                        continue
                    if rep.get("state") != "ok":
                        continue
                    if rep.get("genstamp", 0) != blk.get("genstamp", 0):
                        continue
                    locs.append((nid, rep))
                shards[idx] = locs
        return shards

    def _ec_available(self, group):
        """
        评估组健康：
          返回 dict: index -> {"nodes": [nid...], "bid": bid, "state": ok/corrupt/missing}
        """
        k, m = self._ec_profile(group)["k"], self._ec_profile(group)["m"]
        n = k + m
        live_ids = {x["node_id"] for x in self.live_nodes()}
        out = {}
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            for idx in range(n):
                bid = group["shards"].get(str(idx))
                blk = blocks.get(bid) if bid else None
                nodes = []
                state = "missing"
                if blk:
                    corrupt = False
                    for nid, rep in (blk.get("replicas") or {}).items():
                        if nid not in live_ids:
                            continue
                        if rep.get("genstamp", 0) != blk.get("genstamp", 0):
                            continue
                        if rep.get("state") == "ok":
                            nodes.append(nid)
                        elif rep.get("state") == "corrupt":
                            corrupt = True
                    state = "ok" if nodes else ("corrupt" if corrupt
                                                else "missing")
                out[idx] = {"bid": bid, "nodes": nodes, "state": state}
        return out

    def _normalize_ec_group(self, gid):
        """
        保证"不同分片互不共址、每分片只保留一个好副本"。语义见下；
        返回摘除的重复副本数。调用方需持有 meta.lock。
        """
        group = self._ec_doc()["groups"].get(gid)
        if not group:
            return 0
        # 存活节点信息在 meta 锁外取快照（遵守 meta -> node 锁序）
        live_snap = self.live_nodes()
        live_ids = {n["node_id"] for n in live_snap}
        blocks = self.meta.get("blocks")["blocks"]
        removed = 0

        # 统计每个节点持有的"不同分片"好副本
        node_shards = {}
        shard_good = {}
        for idx_s, bid in group["shards"].items():
            idx = int(idx_s)
            blk = blocks.get(bid)
            good = []
            if blk:
                for nid, rep in (blk.get("replicas") or {}).items():
                    if nid in live_ids and rep.get("state") == "ok" \
                            and rep.get("genstamp", 0) == blk.get("genstamp", 0):
                        good.append(nid)
            shard_good[idx] = (bid, blk, good)
            for nid in good:
                node_shards.setdefault(nid, []).append(idx)

        # 1) 同一分片的多份好副本：只保留一个
        planned = set(group.get("nodes", []))
        for idx, (bid, blk, good) in shard_good.items():
            if len(good) <= 1:
                continue
            keep = next((n for n in good if n in planned), good[0])
            for nid in good:
                if nid == keep:
                    continue
                self._enqueue_command(nid, {
                    "type": "delete", "block_id": bid,
                    "reason": "EC 分片重复副本归一化（每分片仅保留一处）"})
                del blk["replicas"][nid]
                node_shards[nid].remove(idx)
                removed += 1

        # 2) 不同分片共址：组冗余充足且有空闲就绪节点时，迁出冲突分片
        for nid, idxs in list(node_shards.items()):
            if len(set(idxs)) <= 1:
                continue
            total = len(group["shards"])
            good_count = sum(1 for g in shard_good.values() if g[2])
            occupied = {x for xs in node_shards.values() for x in xs}
            free_ready = [nd["node_id"] for nd in live_snap
                          if nd["node_id"] not in occupied
                          and self._node_ready_for_ec(nd)
                          and nd.get("storage", {}).get("free", 0)
                          >= group.get("shard_size", 0)]
            # 仅在当前布局已能满足容错（好分片数 > k）且确有空闲节点时迁出
            if not free_ready or good_count <= group["k"]:
                continue
            # 迁出该节点上索引最大的分片（保留较小/数据分片优先）
            move_idx = sorted(set(idxs))[-1]
            bid, blk, _g = shard_good[move_idx]
            if blk and nid in blk.get("replicas", {}):
                self._enqueue_command(nid, {
                    "type": "delete", "block_id": bid,
                    "reason": "EC 分片共址自愈：迁移到空闲节点"})
                del blk["replicas"][nid]
                node_shards[nid].remove(move_idx)
                removed += 1
        return removed

    def ec_group_state_readonly(self, gid):
        """只读评估组健康（不写元数据、不发事件），供列表/视图使用。"""
        with self.meta.lock:
            group = self._ec_doc()["groups"].get(gid)
            if not group:
                return None
            prof = self._ec_profile(group)
            k, m = prof["k"], prof["m"]
            avail_map = self._ec_available(group)
        good = sum(1 for v in avail_map.values() if v["nodes"])
        return {"group": gid, "k": k, "m": m, "available": good,
                "missing": [i for i, v in avail_map.items()
                            if not v["nodes"]],
                "shards": avail_map,
                "state": ("healthy" if good >= k + m else
                          "degraded" if good >= k else "critical")}

    def check_ec_group(self, gid):
        """评估单个 EC 组，维护 ec_degraded / ec_critical，并触发重建。"""
        with self.meta.lock:
            group = self._ec_doc()["groups"].get(gid)
            if not group:
                with self.health_lock:
                    self.ec_degraded.discard(gid)
                    self.ec_critical.discard(gid)
                    self.ec_rebuilding.pop(gid, None)
                return None
            normalized = self._normalize_ec_group(gid)
            prof = self._ec_profile(group)
            k, m = prof["k"], prof["m"]
            avail_map = self._ec_available(group)
            good_idx = [i for i, v in avail_map.items() if v["nodes"]]
            avail = len(good_idx)
            if normalized:
                # 仅在确实摘除了重复副本时落盘（查询轮询不产生写放大）
                self.meta.touch("blocks", flush=False)

        # 清理已恢复分片的历史损坏记录
        with self.health_lock:
            if avail >= k + m:
                self.ec_degraded.discard(gid)
                self.ec_critical.discard(gid)
            elif avail >= k:
                self.ec_critical.discard(gid)
                if gid not in self.ec_degraded:
                    self.emit("ec_degraded",
                              f"纠删码组 {short_hash(gid, 10)} 丢失 "
                              f"{k + m - avail} 个分片（仍可还原，排队重建）",
                              group=gid, available=avail, k=k, m=m)
                self.ec_degraded.add(gid)
            else:
                self.ec_degraded.add(gid)
                if gid not in self.ec_critical:
                    self.emit("ec_critical",
                              f"纠删码组 {short_hash(gid, 10)} 仅剩 "
                              f"{avail}/{k} 个分片，超过容错上限，数据有丢失风险",
                              group=gid, available=avail, k=k, m=m)
                self.ec_critical.add(gid)
        return {"group": gid, "available": avail, "k": k, "m": m,
                "missing": [i for i, v in avail_map.items()
                            if not v["nodes"]],
                "shards": avail_map,
                "state": ("healthy" if avail >= k + m else
                          "degraded" if avail >= k else "critical")}

    def _rescan_ec_groups(self):
        with self.meta.lock:
            gids = list(self._ec_doc()["groups"].keys())
        for gid in gids:
            self.check_ec_group(gid)

    # ---------------- 重建调度 ----------------
    def _ec_schedule_once(self):
        with self.health_lock:
            critical = sorted(self.ec_critical)
            degraded = sorted(g for g in self.ec_degraded
                              if g not in self.ec_critical)
            rebuilding = dict(self.ec_rebuilding)
        slots = config.EC_MAX_REBUILDING_GROUPS - len(rebuilding)
        if slots <= 0:
            return
        for gid in (critical + degraded):
            if slots <= 0:
                break
            if gid in rebuilding:
                info = rebuilding[gid]
                stale = False
                # 目标节点已死亡/不再就绪 => 立即重排（不必等超时）
                with self.node_lock:
                    for tnode in info.get("targets", []):
                        tn = self.nodes.get(tnode)
                        if not tn or tn["state"] != "LIVE" or \
                                tn.get("ec_report_due"):
                            stale = True
                if not stale and now() - info.get("at", 0) \
                        < config.EC_RECOVERY_TIMEOUT:
                    continue
                if stale:
                    with self.health_lock:
                        self.ec_rebuilding.pop(gid, None)
                    rebuilding.pop(gid, None)
                    # 清掉组内已无意义的 recovering 标记，让计划器重选目标
                    with self.meta.lock:
                        grp = self._ec_doc()["groups"].get(gid)
                        if grp and grp.get("rebuild"):
                            grp["rebuild"]["recovering"] = {}
                            self.meta.touch("ec_groups", flush=False)
            try:
                planned = self._ec_plan_group_rebuild(gid)
            except Exception as e:  # noqa: BLE001
                self.log_event("ERROR", "ec", "plan_error", gid, "system",
                               str(e))
                continue
            if planned:
                slots -= 1

    def _ec_plan_group_rebuild(self, gid):
        """
        为一个组安排重建：
          * 找到缺失分片（无存活好副本）；
          * 校验分片（发现损坏先删）；
          * 存活分片 >= k 才可重建；
          * 每个缺失分片选一个未持有任何组分片的目标节点，下发
            ec_rebuild 命令（目标节点拉取任意 k 个存活分片，本地解码/重算）。
        """
        # 存活节点快照在 meta 锁外获取（遵守 meta -> node 锁序）
        live_snap = self.live_nodes()
        live_ids = {x["node_id"] for x in live_snap}
        with self.meta.lock:
            group = self._ec_doc()["groups"].get(gid)
            if not group:
                return 0
            prof = self._ec_profile(group)
            k, m = prof["k"], prof["m"]
            n = k + m
            shard_len = group.get("shard_size", 0)
            blocks = self.meta.get("blocks")["blocks"]

            # 1) 摘除并删除存活节点上的损坏分片，使目标节点可被重新选择
            good_nodes = {}
            for idx in range(n):
                bid = group["shards"].get(str(idx))
                blk = blocks.get(bid) if bid else None
                good = []
                if blk:
                    for nid in list((blk.get("replicas") or {}).keys()):
                        if nid not in live_ids:
                            continue
                        rep = blk["replicas"][nid]
                        if rep.get("state") == "ok" and \
                                rep.get("genstamp", 0) == blk.get("genstamp", 0):
                            good.append(nid)
                        else:
                            self._enqueue_command(nid, {
                                "type": "delete", "block_id": bid,
                                "reason": "EC 坏分片删除后重建"})
                            del blk["replicas"][nid]
                good_nodes[idx] = good

            missing = [idx for idx in range(n) if not good_nodes.get(idx)]
            if not missing:
                self.meta.touch("blocks", flush=False)
                with self.health_lock:
                    self.ec_degraded.discard(gid)
                    self.ec_critical.discard(gid)
                    self.ec_rebuilding.pop(gid, None)
                return 0
            avail_count = n - len(missing)
            if avail_count < k:
                self.meta.touch("blocks", flush=False)
                return 0    # 不足 k 个分片，无法重建（critical 状态等待节点复活）

            occupied = {nid for locs in good_nodes.values() for nid in locs}
            # 2) 为每个缺失分片选目标节点（互不相同，且不持有组分片）。
            #    仅选用"已完成复活后首次块汇报"的节点，防止在 NN 块表
            #    尚未补齐时把两个分片重建到同一节点。
            candidates = [nd for nd in live_snap
                          if nd["node_id"] not in occupied
                          and self._node_ready_for_ec(nd)
                          and nd.get("storage", {}).get("free", 0)
                          >= shard_len]
            targets = self._rank_targets(candidates, shard_len,
                                         min(len(missing), len(candidates)))
            assignments = {}
            for idx, tnode in zip(missing, targets):
                assignments[idx] = tnode["node_id"]
            if not assignments:
                self.meta.touch("blocks", flush=False)
                return 0

            # 3) 组装命令（每个目标：源为任意 k 个存活分片）
            source_idx = [i for i in range(n) if good_nodes.get(i)][:k]
            cmd_targets = []
            planned_nodes = list(group.get("nodes", []))
            for idx, tnode_id in assignments.items():
                bid = group["shards"][str(idx)]
                blk = blocks.get(bid)
                sources = []
                for sidx in source_idx:
                    src_node = good_nodes[sidx][0]
                    src_bid = group["shards"][str(sidx)]
                    src_blk = blocks.get(src_bid)
                    sources.append({
                        "index": sidx,
                        "node": src_node,
                        "block_id": src_bid,
                        "url": f"{self._node_url(src_node).rstrip('/')}"
                               f"/block/{src_bid}",
                        "checksum": (src_blk or {}).get("checksum"),
                    })
                cmd = {
                    "type": "ec_rebuild",
                    "group": gid,
                    "k": k, "m": m,
                    "profile": group.get("profile"),
                    "shard_size": shard_len,
                    "assignments": [{
                        "index": idx, "block_id": bid,
                        "genstamp": (blk or {}).get("genstamp",
                                                    config.GENSTAMP_INITIAL),
                        "checksum": (blk or {}).get("checksum"),
                        "size": shard_len,
                    }],
                    "sources": sources,
                }
                self._enqueue_command(tnode_id, cmd)
                cmd_targets.append(tnode_id)
                if tnode_id not in planned_nodes:
                    planned_nodes.append(tnode_id)
            group["nodes"] = planned_nodes
            group["rebuild"] = {
                "at": now(),
                "missing_total": len(missing),
                "recovering": {str(i): {"node": tid, "at": now()}
                               for i, tid in assignments.items()},
                "done": [], "failed": 0,
            }
            self.meta.touch("ec_groups")
            self.meta.touch("blocks", flush=False)

        with self.health_lock:
            self.ec_rebuilding[gid] = {"targets": cmd_targets, "at": now()}
        self.log_event("WARN", "ec", "rebuild_scheduled", gid, "system",
                       f"重建分片 {missing} -> 目标 {cmd_targets}")
        self.emit("ec_rebuild_start",
                  f"纠删码组 {short_hash(gid, 10)} 开始重建 {len(missing)} "
                  f"个分片（{k}+{m}）",
                  group=gid, missing=missing, targets=cmd_targets)
        return len(assignments)

    def _on_ec_shard_done(self, gid, bid, node_id, indexes):
        """DN 完成一个（或多个）重建分片后更新组进度。"""
        done_idx = None
        with self.meta.lock:
            group = self._ec_doc()["groups"].get(gid)
            if group:
                rb = group.setdefault("rebuild", {})
                rec = rb.get("recovering", {})
                # 通过 block_id 反查分片序号（兼容 DN 未回传 indexes 的情况）
                for idx_s, sbid in group["shards"].items():
                    if sbid == bid:
                        done_idx = int(idx_s)
                if indexes:
                    done_idx = int(indexes[0])
                if done_idx is not None and str(done_idx) in rec:
                    del rec[str(done_idx)]
                done = rb.setdefault("done", [])
                if done_idx is not None and done_idx not in done:
                    done.append(done_idx)
                total = rb.get("missing_total", 0)
                rb["progress"] = f"{len(done)}/{total}"
                self.meta.touch("ec_groups", flush=False)
        st = self.check_ec_group(gid)
        if st and st["state"] == "healthy":
            with self.meta.lock:
                g = self._ec_doc()["groups"].get(gid)
                if g:
                    g["rebuild"] = None
                    self.meta.touch("ec_groups")
            with self.health_lock:
                self.ec_rebuilding.pop(gid, None)
            self.log_event("INFO", "ec", "rebuild_done", gid, "system",
                           "组分片全部修复")
            self.emit("ec_rebuild_done",
                      f"纠删码组 {short_hash(gid, 10)} 修复完成，冗余已恢复",
                      group=gid)
        else:
            self.emit("ec_rebuild_progress",
                      f"纠删码组 {short_hash(gid, 10)} 分片 "
                      f"{short_hash(bid, 10)} 已在 {node_id} 重建",
                      group=gid, block=bid, node=node_id)

    def _on_ec_rebuild_failed(self, gid, node_id, indexes, reason):
        with self.meta.lock:
            group = self._ec_doc()["groups"].get(gid)
            if group:
                rb = group.setdefault("rebuild", {})
                rb["failed"] = rb.get("failed", 0) + 1
                rb.setdefault("last_error",
                              f"{node_id}: {(reason or '')[:160]}")
                self.meta.touch("ec_groups", flush=False)
        with self.health_lock:
            self.ec_rebuilding.pop(gid, None)   # 放开重排
        self.log_event("WARN", "ec", "rebuild_failed", gid, "system",
                       f"{node_id}: {reason}")
        self.emit("ec_rebuild_failed",
                  f"纠删码组 {short_hash(gid, 10)} 在 {node_id} 重建失败，将重试",
                  group=gid, node=node_id)
        self.check_ec_group(gid)

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

    def allocate_block(self, size, checksum, desired=None, genstamp=None,
                       ec_info=None):
        """
        在块表登记新块（副本随后通过流水线复制填充）。
        ec_info: {"group": gid, "index": i} 时登记为 EC 分片块
                 （单副本、由纠删码组管理，不参与三副本队列）。
        """
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = blocks_doc["blocks"]
            bid = gen_id("blk")
            while bid in blocks:
                bid = gen_id("blk")
            gs = genstamp or blocks_doc.get("next_genstamp", 1000)
            blocks_doc["next_genstamp"] = gs + 1
            record = {
                "id": bid, "size": size, "checksum": checksum,
                "genstamp": gs,
                "desired": desired or config.DEFAULT_REPLICATION,
                "created_at": now(),
                "replicas": {},
            }
            if ec_info:
                record["desired"] = 1
                record["ec_group"] = ec_info["group"]
                record["ec_index"] = ec_info["index"]
            blocks[bid] = record
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
    # 存储策略（目录冗余方式：三副本 / 纠删码）
    # ==================================================================
    def resolve_storage_policy(self, path):
        """
        沿目录树向上找最近显式设置 storage_policy 的祖先目录。
        返回 {"policy": replica|ec, "profile": "ec-2+1"|...,
              "inherited_from": 路径, "explicit": bool}。
        """
        path = os.path.normpath(path).replace("\\", "/")
        segs = [s for s in path.split("/") if s]
        with self.meta.lock:
            inodes = self.fs._inodes()
            cur = inodes.get(self.fs.root_id)
            found = None
            if cur and cur.get("storage_policy"):
                found = ("/", cur.get("storage_policy"),
                         cur.get("ec_profile"))
            cur_path = ""
            for i, seg in enumerate(segs):
                cur_path += "/" + seg
                node = self.fs.resolve(cur_path, must_exist=False)
                if node and node.get("storage_policy"):
                    found = (cur_path, node.get("storage_policy"),
                             node.get("ec_profile"))
            if found:
                return {"policy": found[1],
                        "profile": found[2] or config.EC_DEFAULT_PROFILE,
                        "inherited_from": found[0], "explicit": True}
            return {"policy": config.DEFAULT_STORAGE_POLICY,
                    "profile": config.EC_DEFAULT_PROFILE,
                    "inherited_from": None, "explicit": False}

    def get_storage_policy_view(self, path):
        """目录冗余策略视图（含可用方案表与当前集群能否放下）。"""
        path = path or "/"
        inode = self.fs.resolve(path)
        if inode["type"] != "dir":
            raise FsError(f"不是目录: {path}")
        eff = self.resolve_storage_policy(path)
        live_n = len(self.live_nodes())
        profiles = []
        for name, p in config.EC_PROFILES.items():
            need = p["k"] + p["m"]
            profiles.append({
                "name": name, "k": p["k"], "m": p["m"],
                "label": p["label"], "desc": p["desc"],
                "nodes_required": need,
                "fits": live_n >= need,
                "overhead": round((p["k"] + p["m"]) / p["k"], 2),
            })
        best = erasure.best_profile(live_n)
        return {
            "path": path,
            "policy": inode.get("storage_policy", eff["policy"]),
            "is_inherited": not inode.get("storage_policy"),
            "effective": eff,
            "ec_profile": inode.get("ec_profile", eff["profile"]),
            "live_nodes": live_n,
            "profiles": profiles,
            "auto_best": best[0] if best else None,
            "replication": config.DEFAULT_REPLICATION,
            "replication_overhead": float(config.DEFAULT_REPLICATION),
        }

    def set_storage_policy(self, path, policy, profile=None, actor="admin"):
        """
        设置/清除目录的冗余策略。
          * 只影响之后新写入/覆盖的文件；旧文件保持原冗余（两种并存、互不干扰）；
          * 不移动任何已有数据，因此读写不中断。
        policy 传 "replica" / "ec" / "inherit"（清除显式设置）。
        """
        path = path or "/"
        inode = self.fs.resolve(path)
        if inode["type"] != "dir":
            raise FsError(f"不是目录: {path}")
        policy = (policy or "").strip()
        if policy not in (config.POLICY_REPLICA, config.POLICY_EC,
                          "inherit", ""):
            raise NNError(f"未知冗余策略: {policy}")
        profile = profile or config.EC_DEFAULT_PROFILE
        if policy == config.POLICY_EC and profile not in config.EC_PROFILES:
            raise NNError(f"未知纠删码方案: {profile}")
        with self.meta.lock:
            if policy in ("inherit", ""):
                inode.pop("storage_policy", None)
                inode.pop("ec_profile", None)
                action = "reset"
            else:
                inode["storage_policy"] = policy
                if policy == config.POLICY_EC:
                    inode["ec_profile"] = profile
                else:
                    inode.pop("ec_profile", None)
                action = "set"
            self.meta.touch("fs")
        label = {"replica": f"三副本（×{config.DEFAULT_REPLICATION}）",
                 "ec": f"纠删码 {profile}"}.get(policy, "继承上级")
        self.log_event("INFO", "fs", "storage_policy", path, actor,
                       f"目录冗余策略 -> {label}（仅影响新文件，旧数据保留）")
        self.emit("storage_policy", f"目录 {path} 冗余策略切换为 {label}",
                  path=path, policy=policy, profile=profile)
        return {"ok": True, "path": path, "action": action,
                "view": self.get_storage_policy_view(path)}

    def _pick_ec_profile(self, requested, live_n):
        """
        决定写入时使用的 EC 方案：
          requested 为具体方案时要求节点足够；
          "auto" / None 时挑集群放得下的最省方案；都放不下抛错。
        """
        if requested and requested != "auto":
            p = config.EC_PROFILES.get(requested)
            if not p:
                raise NNError(f"未知纠删码方案: {requested}")
            if live_n < p["k"] + p["m"]:
                raise NNError(
                    f"纠删码方案 {requested} 需要 {p['k'] + p['m']} 个存活节点"
                    f"（{p['k']} 数据 + {p['m']} 校验），当前只有 {live_n} 个")
            return requested, p
        best = erasure.best_profile(live_n)
        if not best:
            raise NNError(
                f"存活节点仅 {live_n} 个，不足以放置纠删码分片（至少 3 个）")
        return best[0], best[1]

    # ==================================================================
    # 写路径：纠删码（EC）
    # ==================================================================
    def store_ec_data(self, data, profile=None, author="system"):
        """
        EC 写入：data -> 切条带组 -> 每组 k+m 分片 -> 每分片一个节点。
        返回 {data_block_ids, groups:[gid...], content_hash, profile, k, m}。
        data_block_ids 按顺序为每个组的数据分片块 id（供 inode 引用，
        顺序拼接即逻辑字节流）。
        """
        live_n = len(self.live_nodes())
        prof_name, prof = self._pick_ec_profile(profile, live_n)
        k, m = prof["k"], prof["m"]
        n = k + m
        content_hash = sha256_bytes(data)
        total = len(data)
        max_data = max(k, config.EC_GROUP_MAX_DATA)

        # 切组：每组数据长度按 k 对齐，最后一组可能较短（分片内补零）
        bounds = list(range(0, total, max_data)) or [0]
        groups = []
        data_block_ids = []
        for gi, start in enumerate(bounds):
            chunk = data[start:start + max_data]
            shards, pad = erasure.encode(chunk, k, m)
            shard_len = len(shards[0])
            gid = gen_id("ecg")
            shard_bids = []
            shard_checksums = [sha256_bytes(s) for s in shards]
            # 为每个分片选不同节点（每分片一个，共 k+m 个）
            targets = self.choose_targets(shard_len, n)
            if len(targets) < n:
                raise NNError(
                    f"存活节点不足：{prof_name} 需要 {n} 个，可用 {len(targets)} 个")
            target_nodes = [t["node_id"] for t in targets]
            for idx, (shard, csum) in enumerate(zip(shards, shard_checksums)):
                bid, gs = self.allocate_block(
                    shard_len, csum, desired=1,
                    ec_info={"group": gid, "index": idx})
                shard_bids.append(bid)
                # EC 分片：单节点直 PUT（不走三副本流水线）
                t = targets[idx]
                url = (f"{t['url'].rstrip('/')}/block/{bid}"
                       f"?genstamp={gs}&checksum={csum}&size={shard_len}")
                try:
                    http_request(
                        url, "PUT", data=shard,
                        headers={"X-Cluster-Key": self.cluster_key,
                                 "Content-Type": "application/octet-stream"},
                        timeout=30)
                    self._record_stored_replicas(bid, gs, csum, shard_len,
                                                 [t["node_id"]])
                except HttpError as e:
                    self.log_event("ERROR", "ec", "shard_put_failed", bid,
                                   author, f"{t['node_id']}: {e}")
                    self.check_ec_group(gid)
            with self.meta.lock:
                group = {
                    "id": gid,
                    "profile": prof_name, "k": k, "m": m,
                    "data_len": len(chunk), "shard_size": shard_len,
                    "pad": pad,
                    "index": gi, "offset": start,
                    "shards": {str(i): b for i, b in enumerate(shard_bids)},
                    "nodes": target_nodes,
                    "created_at": now(),
                    "content_hash": sha256_bytes(chunk),
                    "rebuild": None,
                }
                self._ec_doc()["groups"][gid] = group
                self.meta.touch("ec_groups")
            groups.append(gid)
            data_block_ids.extend(shard_bids[:k])
            self.check_ec_group(gid)
            self.log_event("INFO", "ec", "group_written", gid, author,
                           f"{prof_name} 组 #{gi}：{len(chunk)} B 数据，"
                           f"{n} 分片分布到 {','.join(target_nodes)}")
        return {"block_ids": data_block_ids,
                "data_block_ids": data_block_ids, "groups": groups,
                "content_hash": content_hash,
                "profile": prof_name, "k": k, "m": m,
                "dedup_hits": 0,
                "storage": config.POLICY_EC}

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

    def store_data_blocks(self, data, desired=None, author="system"):
        """
        通用写入：bytes -> 分块 -> 去重 -> 流水线复制 -> 返回块清单。
        （上传完成 / 合并写回 / 种子数据共用）
        """
        desired = desired or config.DEFAULT_REPLICATION
        content_hash = sha256_bytes(data)
        chunks = chunking.chunk_bytes(data)
        block_ids = []
        dedup_hits = 0
        for ch in chunks:
            reuse = self.register_existing_checksum(ch.checksum)
            if reuse:
                block_ids.append(reuse)
                dedup_hits += 1
                continue
            bid, gs = self.allocate_block(ch.length, ch.checksum, desired)
            targets = self.choose_targets(ch.length, desired)
            result = self._pipeline_put(bid, ch.data, ch.checksum, gs, targets)
            self._record_stored_replicas(bid, gs, ch.checksum, ch.length,
                                         result["stored"])
            if not result["stored"]:
                # 首节点就失败：标记块缺失，交给恢复队列重试
                self.check_block_health(bid)
                self.log_event("ERROR", "block", "pipeline_failed", bid,
                               author, json.dumps(result["failed"])[:400])
            block_ids.append(bid)
        with self.meta.lock:
            by_ck = self.meta.get("blocks").setdefault("by_checksum", {})
            for ch, bid in zip(chunks, block_ids):
                by_ck.setdefault(ch.checksum, bid)
            self.meta.touch("blocks", flush=False)
        manifest = chunking.build_manifest(chunks, total_size=len(data),
                                           content_hash=content_hash)
        return {"block_ids": block_ids, "content_hash": content_hash,
                "manifest": manifest, "dedup_hits": dedup_hits}

    def write_file_internal(self, path, data, author="admin", mime=None,
                            owner=None, storage=None, ec_profile=None):
        """
        写文件（内部 API）：确保父目录存在 -> 存块 -> 建/覆盖 inode。
        path 为完整文件路径；data 可以是 bytes 或 str（按 UTF-8 编码）。

        storage:
          None/"auto" -> 按路径所属目录的存储策略决定（三副本 / 纠删码）；
          "replica"   -> 强制三副本；
          "ec"        -> 强制纠删码（ec_profile 指定方案，None 用目录/默认）。
        切换目录策略后，旧文件仍保留原有块布局，新文件按新策略写——
        两种冗余方式在同一目录下并存、互不干扰。
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
            if storage in (None, "auto"):
                policy = self.resolve_storage_policy(dir_path)
                storage = policy["policy"]
                if not ec_profile:
                    ec_profile = policy["profile"]
            if storage == config.POLICY_EC:
                result = self.store_ec_data(data, ec_profile, author=author)
            else:
                result = self.store_data_blocks(data, author=author)
            inode = self.fs.create_file(dir_path, name, len(data),
                                        result["content_hash"],
                                        result["block_ids"], mime,
                                        owner or author,
                                        storage=result.get(
                                            "storage", config.POLICY_REPLICA),
                                        ec_groups=result.get("groups"),
                                        ec_profile=result.get("profile"))
        self._record_hourly("uploads", 1)
        self._record_hourly("bytes_in", len(data))
        return {"path": path, "inode_id": inode["id"],
                "size": len(data), "mime": mime,
                "storage": inode.get("storage", config.POLICY_REPLICA),
                "ec_profile": inode.get("ec_profile"),
                "ec_groups": inode.get("ec_groups", []),
                "content_hash": result["content_hash"],
                "block_ids": result["block_ids"],
                "chunks": len(result["block_ids"]),
                "dedup_hits": result.get("dedup_hits", 0)}

    # ==================================================================
    # 读路径（副本轮询 + 故障转移；纠删码组重建读取）
    # ==================================================================
    def _fetch_shard(self, bid, node_id, blk):
        """从指定节点拉取一个分片/块字节并校验（失败抛异常）。"""
        url = f"{self._node_url(node_id).rstrip('/')}/block/{bid}"
        _s, _h, data = http_request(
            url, "GET", headers={"X-Cluster-Key": self.cluster_key},
            timeout=20)
        if sha256_bytes(data) != blk["checksum"]:
            raise NNError(f"{node_id} 分片校验和不匹配")
        return data

    def read_ec_group(self, gid, want_indices=None, verify=True):
        """
        读一个纠删码组：从存活好分片所在节点拉取（任意 k 个即可），
        本地 Reed-Solomon 重建需要的数据分片。
          want_indices None -> 返回全部数据分片（bytes 列表，按 0..k-1）；
          指定集合 -> 返回 {index: bytes}。
        拉到坏分片自动剔除并尝试下一个；存活好分片不足 k 个抛 MissingBlockError。
        """
        with self.meta.lock:
            group = self._ec_doc()["groups"].get(gid)
            if not group:
                raise MissingBlockError(f"纠删码组不存在: {gid}")
            prof = self._ec_profile(group)
            k, m = prof["k"], prof["m"]
            shard_meta = {}
            for idx_s, bid in group["shards"].items():
                blk = self.meta.get("blocks")["blocks"].get(bid)
                shard_meta[int(idx_s)] = (bid, blk)
        avail = self._ec_available(group)
        good = [i for i, v in avail.items() if v["nodes"]]
        if len(good) < k:
            raise MissingBlockError(
                f"纠删码组 {short_hash(gid, 10)} 仅 {len(good)}/{k} 个"
                f"存活分片，暂时无法读取（等待自动重建）")
        # 优先数据分片，再补校验分片，凑够 k 个
        preferred = sorted(good, key=lambda i: (i >= k, i))
        chosen = preferred[:k]
        available = {}
        used_nodes = []
        errors = []
        for idx in chosen:
            bid, blk = shard_meta.get(idx, (None, None))
            if not blk:
                errors.append(f"#{idx}:块表缺失")
                continue
            nodes = list(avail[idx]["nodes"])
            random.shuffle(nodes)
            for nid in nodes:
                try:
                    data = self._fetch_shard(bid, nid, blk)
                    available[idx] = data
                    used_nodes.append(nid)
                    break
                except Exception as e:  # noqa: BLE001
                    errors.append(f"#{idx}@{nid}:{e}")
                    with self.meta.lock:
                        b2 = self.meta.get("blocks")["blocks"].get(bid)
                        if b2 and nid in b2.get("replicas", {}):
                            b2["replicas"][nid]["state"] = "corrupt"
                            self.meta.touch("blocks", flush=False)
                    self.check_ec_group(gid)
        if len(available) < k:
            raise MissingBlockError(
                f"纠删码组 {short_hash(gid, 10)} 读取失败（仅获取 "
                f"{len(available)}/{k} 分片）: {errors[:4]}")
        if want_indices is None:
            rebuilt = erasure.reconstruct(available, k, m,
                                          want=set(range(k)))
            return [rebuilt[i] for i in range(k)], used_nodes
        want = set(want_indices)
        return erasure.reconstruct(available, k, m, want=want), used_nodes

    def read_blocks(self, block_ids, verify=True):
        """按块表顺序拼接读取（版本合并/预览/diff 使用，自动识别 EC 分片序列）。"""
        if not block_ids:
            return b""
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            gids = []
            for bid in block_ids:
                blk = blocks_doc["blocks"].get(bid)
                gid = blk.get("ec_group") if blk else None
                if gid and gid not in gids:
                    gids.append(gid)
        if not gids:
            out = []
            for bid in block_ids:
                data, _meta, _node = self.read_block(bid, verify=verify)
                out.append(data)
            return b"".join(out)
        # EC 文件：按组解码（block_ids 为各组数据分片，顺序与组列表一致）
        with self.meta.lock:
            ordered = []
            for bid in block_ids:
                blk = self.meta.get("blocks")["blocks"].get(bid)
                gid = blk.get("ec_group") if blk else None
                if gid and gid not in ordered:
                    ordered.append(gid)
        out = []
        for gid in ordered:
            shards, _nodes = self.read_ec_group(gid)
            with self.meta.lock:
                grp = self._ec_doc()["groups"][gid]
                out.append(b"".join(shards)[:grp["data_len"]])
        return b"".join(out)

    def read_block(self, bid, start=None, end=None, verify=True,
                   use_cache=True):
        """
        读一个块：存活好副本轮询，校验失败自动切换下一副本。
        返回 (data, block_meta, served_by)。
        """
        if use_cache and start is None:
            cached = self.block_cache.get(bid)
            if cached is not None:
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
        三副本文件走块副本轮询；纠删码文件走组重建（按组定位偏移）。
        返回 (data, info)。
        """
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "file":
                raise FsError(f"不是文件: {path}")
            block_ids = list(inode.get("block_ids", []))
            size = inode.get("size", 0)
            is_ec = inode.get("storage") == config.POLICY_EC
            ec_group_ids = list(inode.get("ec_groups", []))
        offset = max(0, min(offset, size))
        end = size - 1 if length is None else min(size - 1, offset + length - 1)
        if size == 0 or offset > end:
            return b"", {"size": size, "start": offset, "end": offset,
                         "nodes": [], "blocks_touched": 0, "storage":
                         inode.get("storage", "replica")}

        if is_ec and ec_group_ids:
            data, nodes, touched = self._read_ec_file_range(
                ec_group_ids, size, offset, end)
            self.record_access(path, "download", user, len(data),
                               nodes[0] if nodes else None)
            return data, {"size": size, "start": offset, "end": end,
                          "nodes": sorted(set(nodes)),
                          "blocks_touched": touched,
                          "storage": "ec",
                          "ec_profile": inode.get("ec_profile")}

        out = []
        nodes = []
        touched = 0
        pos = offset
        # 逐块定位
        block_starts = []
        acc = 0
        with self.meta.lock:
            for bid in block_ids:
                blk = self._block_meta(bid)
                bsize = (blk or {}).get("size", 0)
                block_starts.append((bid, acc, bsize))
                acc += bsize
        for bid, bstart, bsize in block_starts:
            bend = bstart + bsize - 1
            if bend < pos or bstart > end:
                continue
            s = max(pos, bstart) - bstart
            e = min(end, bend) - bstart
            full = (s == 0 and e == bsize - 1)
            data, _blk, node = self.read_block(
                bid, None if full else s, None if full else e)
            out.append(data)
            nodes.append(node)
            touched += 1
        data = b"".join(out)
        # 热度记录
        self.record_access(path, "download", user, len(data),
                           nodes[0] if nodes else None)
        return data, {"size": size, "start": pos, "end": end,
                      "nodes": sorted(set(nodes)), "blocks_touched": touched,
                      "storage": "replica"}

    def _read_ec_file_range(self, group_ids, size, start, end):
        """EC 文件的范围读：定位覆盖 [start,end] 的组，逐组解码后切片。"""
        out = []
        nodes = []
        touched = 0
        with self.meta.lock:
            plan = []
            acc = 0
            for gid in group_ids:
                grp = self._ec_doc()["groups"].get(gid)
                if not grp:
                    raise MissingBlockError(f"纠删码组缺失: {gid}")
                dlen = grp["data_len"]
                plan.append((gid, acc, dlen))
                acc += dlen
        for gid, goff, dlen in plan:
            gend = goff + dlen - 1
            if gend < start or goff > end:
                continue
            cache_key = f"ecg:{gid}"
            decoded = self.block_cache.get(cache_key)
            if decoded is None:
                shards, used = self.read_ec_group(gid)
                with self.meta.lock:
                    grp = self._ec_doc()["groups"][gid]
                    decoded = b"".join(shards)[:grp["data_len"]]
                self.block_cache.put(cache_key, decoded)
                nodes.extend(used)
            else:
                nodes.append("cache")
            s = max(start, goff) - goff
            e = min(end, gend) - goff
            out.append(decoded[s:e + 1])
            touched += 1
        return b"".join(out), nodes, touched

    # ==================================================================
    # 上传会话（分块上传 + 断点续传）
    # ==================================================================
    def upload_begin(self, path, filename, size, session_id=None,
                     piece_size=None, user="anonymous", storage=None,
                     ec_profile=None):
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
            # 上传时按目标目录策略确定本文件冗余方式（显式参数可覆盖）
            pol = self.resolve_storage_policy(path)
            storage = storage or pol["policy"]
            ec_profile = ec_profile or pol["profile"]
            sess = {
                "id": sess_id, "path": path, "filename": filename,
                "size": size, "piece_size": piece,
                "total_pieces": total_pieces,
                "received": {},            # idx -> {size, checksum, ts}
                "user": user, "created_at": now(), "last_active": now(),
                "stage_dir": stage_dir, "completed": False, "result": None,
                "storage": storage, "ec_profile": ec_profile,
            }
            self.sessions[sess_id] = sess
        self.log_event("INFO", "upload", "begin", f"{path}/{filename}", user,
                       f"size={size} piece={piece} pieces={total_pieces} "
                       f"storage={storage}"
                       + (f"/{ec_profile}" if storage == "ec" else ""))
        view = self._session_view(sess)
        view["storage"] = storage
        view["ec_profile"] = ec_profile if storage == "ec" else None
        return view

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
        info = self.write_file_internal(
            full_path, data, user,
            storage=sess.get("storage"),
            ec_profile=sess.get("ec_profile"))
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
            blk_metas = [self._block_meta(b) for b in inode.get("block_ids", [])]
            is_ec = inode.get("storage") == config.POLICY_EC
        if is_ec:
            return {
                "path": path, "name": inode["name"],
                "size": inode.get("size", 0),
                "content_hash": inode.get("content_hash"),
                "mime": inode.get("mime"),
                "storage": "ec",
                "ec_profile": inode.get("ec_profile"),
                "ec_groups": inode.get("ec_groups", []),
                "blocks": [],
            }
        return {
            "path": path, "name": inode["name"], "size": inode.get("size", 0),
            "content_hash": inode.get("content_hash"),
            "mime": inode.get("mime"),
            "storage": "replica",
            "blocks": [{"id": b["id"], "size": b["size"],
                        "checksum": b["checksum"][:16],
                        "genstamp": b["genstamp"],
                        "replicas": sorted(b.get("replicas", {}).keys())}
                       for b in blk_metas if b],
        }

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
        ec_shard_count = 0
        ec_groups = 0
        for blk in blocks:
            if blk.get("ec_group"):
                ec_shard_count += 1
                continue
            live = len(self.live_good_replicas(blk))
            rep_dist[str(live)] = rep_dist.get(str(live), 0) + 1
            bucket = chunking.size_bucket(blk.get("size", 0))
            size_hist[bucket] = size_hist.get(bucket, 0) + 1
        with self.meta.lock:
            ec_groups = len(self._ec_doc()["groups"])
            ec_profiles = {}
            for g in self._ec_doc()["groups"].values():
                p = g.get("profile", "?")
                ec_profiles[p] = ec_profiles.get(p, 0) + 1
        logical = fs_stats["bytes"]
        return {
            "files": fs_stats["files"],
            "dirs": fs_stats["dirs"],
            "logical_bytes": logical,
            "physical_bytes": total_used,
            "replication_overhead": round(total_used / logical, 2)
            if logical else 0,
            "blocks": len(blocks),
            "replica_blocks": len(blocks) - ec_shard_count,
            "ec_shard_blocks": ec_shard_count,
            "ec_groups": ec_groups,
            "ec_profiles": ec_profiles,
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
        per_node_ec = {n["node_id"]: 0 for n in nodes}
        with self.meta.lock:
            blocks = list(self.meta.get("blocks")["blocks"].values())
        for blk in blocks:
            is_ec = bool(blk.get("ec_group"))
            for nid, rep in list((blk.get("replicas") or {}).items()):
                if nid in per_node_blocks:
                    per_node_blocks[nid] += 1
                    per_node_bytes[nid] += rep.get("size", 0)
                    if is_ec:
                        per_node_ec[nid] += 1
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
                "nn_block_bytes": per_node_bytes.get(nid, 0),
                "nn_ec_shards": per_node_ec.get(nid, 0),
                "corrupt": per_node_corrupt.get(nid, 0),
                "pending_commands": len(self.pending_commands.get(nid, [])),
            })
        with self.health_lock:
            health = {
                "under_replicated": len(self.under_replicated),
                "corrupt_replicas": len(self.corrupt_replicas),
                "missing_blocks": len(self.missing_blocks),
                "scheduled": len(self.scheduled),
                "ec_groups": self.ec_stats_counts(),
            }
        return {"nodes": out, "health": health,
                "summary": {
                    "total": len(out),
                    "live": sum(1 for n in out if n["state"] == "LIVE"),
                    "suspect": sum(1 for n in out if n["state"] == "SUSPECT"),
                    "dead": sum(1 for n in out if n["state"] == "DEAD"),
                }}

    def ec_stats_counts(self):
        """EC 组计数（调用方持 health_lock 或容忍近似）。"""
        return {"total": len(self._ec_doc()["groups"]),
                "degraded": len(self.ec_degraded),
                "critical": len(self.ec_critical),
                "rebuilding": len(self.ec_rebuilding)}

    def ec_groups_view(self, limit=200):
        """EC 组列表（节点页/存储策略页展示：方案、落点、健康、修复进度）。"""
        with self.meta.lock:
            gids = list(self._ec_doc()["groups"].keys())[:limit]
        rows = []
        for gid in gids:
            st = self.check_ec_group(gid)
            if not st:
                continue
            group = self._ec_group(gid)
            shard_brief = []
            with self.meta.lock:
                blocks = self.meta.get("blocks")["blocks"]
                for idx, v in st["shards"].items():
                    shard_brief.append({
                        "index": idx,
                        "kind": "data" if idx < st["k"] else "parity",
                        "nodes": v["nodes"], "state": v["state"],
                        "checksum": ((blocks.get(v["bid"]) or {})
                                     .get("checksum") or "")[:10],
                    })
            rec = (group.get("rebuild") or {}).get("recovering", {})
            rows.append({
                "id": gid,
                "short": short_hash(gid.replace("ecg_", ""), 10),
                "profile": group.get("profile"),
                "k": st["k"], "m": st["m"],
                "data_len": group.get("data_len"),
                "shard_size": group.get("shard_size"),
                "available": st["available"],
                "status": st["state"],
                "missing": st["missing"],
                "shards": shard_brief,
                "recovering": rec,
                "nodes": group.get("nodes", []),
                "created_at": group.get("created_at"),
                "rebuild": group.get("rebuild"),
                "paths": self.block_paths(
                    group["shards"].get("0"), ec_group_only=True),
            })
        rows.sort(key=lambda r: ({"critical": 0, "degraded": 1,
                                  "healthy": 2}[r["status"]],
                                 -r.get("data_len", 0)))
        return {"groups": rows, "counts": {
            "total": len(rows),
            "degraded": sum(1 for r in rows if r["status"] == "degraded"),
            "critical": sum(1 for r in rows if r["status"] == "critical"),
            "rebuilding": sum(1 for r in rows if r.get("rebuild"))}}

    def node_blocks(self, node_id, limit=200, offset=0):
        """从 DataNode 实时拉取其块清单（HTTP 同步演示）。"""
        dn = self.local_datanodes.get(node_id)
        if dn:
            return dn.block_list(limit, offset)
        # 远程节点：走块表反查
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            items = [(bid, b) for bid, b in blocks.items()
                     if node_id in (b.get("replicas") or {})]
        total = len(items)
        out = []
        for bid, b in items[offset:offset + limit]:
            rep = b["replicas"][node_id]
            out.append({"id": bid, "genstamp": rep["genstamp"],
                        "size": rep["size"], "state": rep["state"],
                        "checksum": rep["checksum"][:16],
                        "stored_at": rep.get("updated_at")})
        return {"total": total, "blocks": out}

    def replica_matrix(self, limit=60, include_ec=False):
        """块 x 节点 副本分布矩阵（节点页可视化，默认只列三副本块）。"""
        with self.node_lock:
            node_ids = sorted(self.nodes.keys())
            live_ids = {nid for nid, n in self.nodes.items()
                        if n["state"] == "LIVE"}
        with self.meta.lock:
            all_blocks = self.meta.get("blocks")["blocks"]
            blocks = {bid: b for bid, b in all_blocks.items()
                      if include_ec or not b.get("ec_group")}

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

        items = sorted(blocks.items(), key=blk_score)[:limit]
        rows = []
        for bid, blk in items:
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
            rows.append({"block": bid, "short": short_hash(bid.replace("blk_", ""), 8),
                         "size": blk.get("size", 0),
                         "desired": blk.get("desired"),
                         "live": live, "cells": cells,
                         "status": ("missing" if live == 0 else
                                    "under" if live < blk.get("desired", 3)
                                    else "ok")})
        return {"nodes": node_ids, "rows": rows, "total_blocks": len(blocks)}

    def health_queue(self):
        with self.health_lock:
            under = dict(self.under_replicated)
            corrupt = {f"{b}@{n}": dict(v) for (b, n), v
                       in self.corrupt_replicas.items()}
            missing = set(self.missing_blocks)
            scheduled = dict(self.scheduled)
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            under_items = []
            for bid, info in list(under.items())[:80]:
                blk = blocks.get(bid, {})
                if blk.get("ec_group"):
                    continue
                under_items.append({
                    "block": bid, "desired": blk.get("desired"),
                    "live": len(self.live_good_replicas(blk)) if blk else 0,
                    "since": info["since"], "attempts": info.get("attempts", 0),
                    "scheduled": scheduled.get(bid),
                    "size": blk.get("size", 0),
                })
        # EC 组修复进度
        ec_groups = []
        for g in self.ec_groups_view(limit=120)["groups"]:
            if g["status"] != "healthy" or g.get("rebuild"):
                ec_groups.append(g)
        return {"under_replicated": under_items, "corrupt": corrupt,
                "missing": sorted(missing)[:80],
                "ec_groups": ec_groups,
                "counts": {"under": len(under_items),
                           "corrupt": len(corrupt),
                           "missing": len(missing),
                           "scheduled": len(scheduled),
                           "ec_degraded": len(self.ec_degraded),
                           "ec_critical": len(self.ec_critical),
                           "ec_rebuilding": len(self.ec_rebuilding)}}

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
            blk = self.meta.get("blocks")["blocks"].get(block_id)
        if not blk:
            raise NNError(f"块不存在: {block_id}")
        if node_id not in (blk.get("replicas") or {}):
            raise NNError(f"{node_id} 上没有块 {block_id} 的副本")
        dn = self.local_datanodes.get(node_id)
        if not dn:
            raise NNError(f"节点不在本进程管理内: {node_id}")
        dn.corrupt_block_sim(block_id)
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
                # EC 文件的校验分片不在 inode.block_ids 内，由组表补入保护集
                for gid in inode.get("ec_groups", []):
                    grp = self._ec_doc()["groups"].get(gid)
                    if grp:
                        refs.update(grp["shards"].values())
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
                    for gid in node.get("ec_groups", []):
                        grp = self._ec_doc()["groups"].get(gid)
                        if grp:
                            refs.update(grp["shards"].values())
                    stack.extend(node.get("children", []))
        refs |= self.versions.all_referenced_blocks()
        refs |= self.all_ec_blocks_in_version_refs()
        with self.session_lock:
            for sess in self.sessions.values():
                refs.update(sess.get("result", {}).get("file", {})
                            .get("block_ids", []) if sess.get("result") else [])
        return refs

    def all_ec_blocks_in_version_refs(self):
        """旧快照可能只记了数据分片块；按块表 ec_group 反查补齐同组全部分片。"""
        extra = set()
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            groups = self._ec_doc()["groups"]
            for bid in self.versions.all_referenced_blocks():
                blk = blocks.get(bid)
                if blk and blk.get("ec_group"):
                    grp = groups.get(blk["ec_group"])
                    if grp:
                        extra.update(grp["shards"].values())
        return extra

    def _referenced_ec_groups(self):
        """仍被活动文件 / 回收站 / 版本快照引用的 EC 组 id 集合。"""
        gids = set()
        with self.meta.lock:
            blocks = self.meta.get("blocks")["blocks"]
            for _p, inode in self.fs.all_files():
                gids.update(inode.get("ec_groups", []))
            trash_root = self.fs.get_inode(self.fs.trash_id)
            if trash_root:
                stack = list(trash_root.get("children", []))
                inodes = self.fs._inodes()
                while stack:
                    cur = stack.pop()
                    node = inodes.get(cur)
                    if not node:
                        continue
                    gids.update(node.get("ec_groups", []))
                    stack.extend(node.get("children", []))
            # 版本快照（新快照存 ec_groups；旧快照靠块反查）
            for c in self.versions._v()["commits"].values():
                for e in c.get("snapshot", {}).values():
                    gids.update(e.get("ec_groups", []))
                    for bid in e.get("block_ids", []):
                        blk = blocks.get(bid)
                        if blk and blk.get("ec_group"):
                            gids.add(blk["ec_group"])
        return gids

    def gc_blocks(self):
        """回收未被引用的块（宽限期防止误删刚写的块）。"""
        refs = self.referenced_blocks()
        live_groups = self._referenced_ec_groups()
        t = now()
        deleted = 0
        ec_groups_deleted = 0
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            blocks = blocks_doc["blocks"]
            by_ck = blocks_doc.get("by_checksum", {})
            ec_doc = self._ec_doc()
            # 1) 回收无引用的 EC 组（连带其全部分片块）
            for gid in list(ec_doc["groups"].keys()):
                grp = ec_doc["groups"][gid]
                if gid in live_groups:
                    continue
                if t - grp.get("created_at", t) < config.GC_GRACE_SECONDS:
                    continue
                for bid in grp["shards"].values():
                    blk = blocks.pop(bid, None)
                    if blk:
                        for nid in (blk.get("replicas") or {}):
                            self._enqueue_command(nid, {
                                "type": "delete", "block_id": bid,
                                "reason": "GC：无引用的 EC 分片"})
                        deleted += 1
                del ec_doc["groups"][gid]
                ec_groups_deleted += 1
                with self.health_lock:
                    self.ec_degraded.discard(gid)
                    self.ec_critical.discard(gid)
                    self.ec_rebuilding.pop(gid, None)
            # 2) 回收普通无引用块（EC 分片块统一随组回收，这里跳过）
            to_delete = []
            for bid, blk in blocks.items():
                if blk.get("ec_group"):
                    continue
                if bid in refs:
                    continue
                if t - blk.get("created_at", t) < config.GC_GRACE_SECONDS:
                    continue
                to_delete.append(bid)
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
            if deleted or ec_groups_deleted:
                self.meta.touch("blocks")
                self.meta.touch("ec_groups")
        if deleted:
            self.log_event("INFO", "gc", "gc_blocks", "", "system",
                           f"回收 {deleted} 个未引用块（含 {ec_groups_deleted} 个 EC 组）")
        return deleted

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
            is_ec = inode.get("storage") == config.POLICY_EC
            if is_ec:
                return self._ec_file_detail(inode, path)
            out = []
            for bid in inode.get("block_ids", []):
                blk = self._block_meta(bid)
                if not blk:
                    out.append({"id": bid, "missing": True})
                    continue
                live = self.live_good_replicas(blk)
                out.append({
                    "id": bid,
                    "short": short_hash(bid.replace("blk_", ""), 8),
                    "size": blk["size"],
                    "checksum": blk["checksum"][:16],
                    "genstamp": blk["genstamp"],
                    "desired": blk.get("desired"),
                    "live": len(live),
                    "storage": "replica",
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
            return {"path": path, "size": inode.get("size", 0),
                    "content_hash": inode.get("content_hash"),
                    "storage": "replica",
                    "blocks": out}

    def _ec_file_detail(self, inode, path):
        """EC 文件详情：逐组列出 k+m 个分片的落点节点 / 健康 / 重建进度。"""
        groups_out = []
        live_all = {n["node_id"] for n in self.live_nodes()}
        worst = "healthy"
        with self.meta.lock:
            blocks_doc = self.meta.get("blocks")
            ec_doc = self._ec_doc()
            for gi, gid in enumerate(inode.get("ec_groups", [])):
                grp = ec_doc["groups"].get(gid)
                if not grp:
                    groups_out.append({"id": gid, "missing": True, "index": gi})
                    worst = "critical"
                    continue
                k, m = grp["k"], grp["m"]
                avail = 0
                shards = []
                recovering = (grp.get("rebuild") or {}).get("recovering", {})
                for idx in range(k + m):
                    bid = grp["shards"].get(str(idx))
                    blk = blocks_doc["blocks"].get(bid) if bid else None
                    reps = []
                    ok_nodes = []
                    rep_items = (blk.get("replicas") or {}).items() if blk else []
                    for nid, r in sorted(rep_items):
                        live = nid in live_all
                        good = live and r.get("state") == "ok" and \
                            r.get("genstamp", 0) == blk.get("genstamp", 0)
                        if good:
                            ok_nodes.append(nid)
                        reps.append({
                            "node": nid, "state": r.get("state"),
                            "rack": (self.nodes.get(nid) or {}).get("rack"),
                            "live": live})
                    if ok_nodes:
                        avail += 1
                    kind = "data" if idx < k else "parity"
                    shards.append({
                        "index": idx, "kind": kind,
                        "id": bid,
                        "short": short_hash((bid or "").replace("blk_", ""), 8),
                        "size": (blk or {}).get("size", grp.get("shard_size")),
                        "checksum": ((blk or {}).get("checksum") or "")[:12],
                        "nodes": ok_nodes,
                        "replicas": reps,
                        "recovering_to": (recovering.get(str(idx)) or {})
                        .get("node"),
                        "state": "ok" if ok_nodes else
                        ("recovering" if str(idx) in recovering else "missing"),
                    })
                status = ("healthy" if avail >= k + m else
                          "degraded" if avail >= k else "critical")
                if status == "critical":
                    worst = "critical"
                elif status == "degraded" and worst != "critical":
                    worst = "degraded"
                groups_out.append({
                    "id": gid,
                    "short": short_hash(gid.replace("ecg_", ""), 10),
                    "index": gi,
                    "profile": grp.get("profile"),
                    "k": k, "m": m,
                    "data_len": grp.get("data_len"),
                    "shard_size": grp.get("shard_size"),
                    "pad": grp.get("pad", 0),
                    "available": avail,
                    "status": status,
                    "shards": shards,
                    "rebuild": grp.get("rebuild"),
                })
        return {"path": path, "size": inode.get("size", 0),
                "content_hash": inode.get("content_hash"),
                "storage": "ec",
                "ec_profile": inode.get("ec_profile"),
                "status": worst,
                "groups": groups_out,
                "blocks": []}

    def block_paths(self, bid, ec_group_only=False):
        """反查引用某块（或 EC 组的任一分片）的文件路径（节点页/健康队列展示用）。"""
        with self.meta.lock:
            if ec_group_only:
                group = self._ec_doc()["groups"].get(bid)
                bids = set(group["shards"].values()) if group else {bid}
            else:
                bids = {bid}
            paths = [p for p, inode in self.fs.all_files()
                     if bids & set(inode.get("block_ids", []))
                     or bids & self._inode_ec_shard_set(inode)]
        return paths

    def _inode_ec_shard_set(self, inode):
        out = set()
        groups = self._ec_doc()["groups"]
        for gid in inode.get("ec_groups", []):
            grp = groups.get(gid)
            if grp:
                out.update(grp["shards"].values())
        return out
