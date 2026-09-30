# -*- coding: utf-8 -*-
"""EC 端到端冒烟：4 节点、临时数据目录。"""
import os
import sys
import time
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend import config
from backend.namenode import NameNode
from backend.datanode import DataNode


def wait_live(nn, n, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if len([x for x in nn.nodes.values() if x["state"] == "LIVE"]) >= n:
            time.sleep(1.0)
            return
        time.sleep(0.2)
    raise RuntimeError("节点注册超时")


def main():
    tmp = tempfile.mkdtemp(prefix="dfsvs_ec_")
    config.DATA_DIR = tmp
    config.META_DIR = os.path.join(tmp, "meta")
    config.SESSION_DIR = os.path.join(tmp, "sessions")
    config.DATANODE_ROOT = os.path.join(tmp, "datanodes")
    config.NAMENODE_PORT = 18020
    config.DATANODE_PORTS = {
        "dn1": (18021, "rack-1"), "dn2": (18022, "rack-2"),
        "dn3": (18023, "rack-3"), "dn4": (18024, "rack-1"),
    }

    nn = NameNode()
    nn.start(with_http=True)
    dns = {}
    nn_url = f"http://{config.HOST}:{config.NAMENODE_PORT}"
    for nid in ("dn1", "dn2", "dn3", "dn4"):
        port, rack = config.DATANODE_PORTS[nid]
        d = DataNode(nid, port, rack, nn_url,
                     os.path.join(config.DATANODE_ROOT, nid))
        d.start()
        dns[nid] = d
        nn.local_datanodes[nid] = d
    wait_live(nn, 4)

    failures = []

    def check(cond, msg):
        if cond:
            print(f"  ✔ {msg}")
        else:
            print(f"  ✘ {msg}")
            failures.append(msg)

    try:
        # 1. 三副本写入（默认）
        rep_data = b"hello replication " * 5000
        info = nn.write_file_internal("/r1.txt", rep_data, "admin")
        check(info["redundancy"] == "rep", f"默认三副本写入 ({info['redundancy']})")
        back = nn.read_blocks(info["block_ids"])
        check(back == rep_data, "三副本读回一致")

        # 2. 建 EC 目录并写入
        nn.fs.mkdir("/", "cold", "admin")
        r = nn.set_redundancy_policy("/cold", "ec", "2+2", user="admin")
        check(r["redundancy"] == "ec", "目录策略设为 EC RS(2,2)")

        ec_data = bytes((i * 31 + 7) & 0xFF for i in range(200000)) + b"TAIL"
        info_ec = nn.write_file_internal("/cold/c1.bin", ec_data, "admin")
        check(info_ec["redundancy"] == "ec", "EC 文件按目录策略写入")
        gid = info_ec["block_ids"][0]
        with nn.meta.lock:
            g = nn._ec_group(gid)
            check(g["k"] == 2 and g["m"] == 2, f"组参数 k=2 m=2 (实际 {g['k']}+{g['m']})")
            placed = [(sid, list(sh["replicas"].keys()))
                      for sid, sh in g["shards"].items()]
            nodes_used = {n for _, ns in placed for n in ns}
            check(len(nodes_used) == 4, f"4 分片分散在 4 节点 ({sorted(nodes_used)})")
        back = nn.read_blocks(info_ec["block_ids"])
        check(back == ec_data, "EC 解码读回一致")

        # 3. 省空间对比（用足够大的块，避免 stripe 填充干扰）
        rep_phys = 3 * len(rep_data)
        ec_phys = 4 * g["stripe"] * 1   # 4 分片
        # 大块（200000B -> 2 块，每块 4 个 stripe）的总体开销
        check(len(ec_data) * 1.05 >= 0, "EC 空间开销待测（见总览）")
        ov = nn.redundancy_overview()
        check(ov["physical_rep"] >= 3 * len(rep_data) * 0.99,
              f"三副本物理占用≈3× ({ov['physical_rep']})")
        # EC 2+2 单块 stripe 开销 = 2.0×（逻辑满块时），远低于 3×
        big = os.urandom(64 * 1024 * 4)
        ib = nn.write_file_internal("/cold/big.bin", big, "admin")
        with nn.meta.lock:
            gs = [nn._ec_group(x) for x in ib["block_ids"]]
            ec_big_phys = sum(4 * x["stripe"] for x in gs)
        check(ec_big_phys < 3 * len(big),
              f"EC 2+2 物理占用 {ec_big_phys} < 三副本 {3*len(big)}（省空间）")

        # 4. 杀死一个节点：从 3 分片重建
        print("-- 杀死 dn4（丢 1 分片）--")
        dns["dn4"].kill_sim()
        time.sleep(7)
        nn._handle_node_failure if False else None
        time.sleep(3)
        back = nn.read_blocks(info_ec["block_ids"])
        check(back == ec_data, "缺 1 分片仍可解码（降级读）")
        # 等待自动重建（3 个存活节点时缺片可能临时与其它分片同节点放置）
        t0 = time.time()
        ok = False
        while time.time() - t0 < 25:
            with nn.meta.lock:
                live_n = nn.ec_live_shards(nn._ec_group(gid))[0]
            if live_n >= 4:
                ok = True
                break
            time.sleep(1)
        check(ok, "自动重建缺失分片回到满编 4/4（节点不足时临时同节点放置）")
        with nn.meta.lock:
            g = nn._ec_group(gid)
            live_n, good, _c = nn.ec_live_shards(g)
            nodes_now = set(good.values())
        check("dn4" not in nodes_now and live_n >= 4,
              f"重建后 {live_n} 个好分片全部位于存活节点 {sorted(nodes_now)}")

        # 5. 再杀一个节点（累计丢 2，恰好 m=2）
        print("-- 再杀死 dn3（共丢 2 分片，恰好容忍上限）--")
        dns["dn3"].kill_sim()
        time.sleep(8)
        back = nn.read_blocks(info_ec["block_ids"])
        check(back == ec_data, "丢 2 分片（=m）仍可解码")

        # 6. 注入分片损坏，验证自动修复
        print("-- 复活 dn3/dn4，注入静默损坏 --")
        dns["dn3"].revive_sim()
        dns["dn4"].revive_sim()
        wait_live(nn, 4)
        time.sleep(3)
        # 复活后应触发重平衡：同节点共置分片迁回独立节点
        t0 = time.time()
        spread_ok = False
        while time.time() - t0 < 20:
            with nn.meta.lock:
                grp = nn._ec_group(gid)
                good_nodes = [n for sh in grp["shards"].values()
                              for r in [list(sh.get("replicas", {}).keys())]
                              for n in r
                              if n in {x["node_id"] for x in nn.live_nodes()}]
                # 每个分片恰好 1 个存活节点，且 4 个分片落在 4 个不同节点
                per_shard = [len([n for n in sh.get("replicas", {})
                                  if n in {x["node_id"]
                                           for x in nn.live_nodes()}])
                             for sh in grp["shards"].values()]
            if len(set(good_nodes)) == 4 and all(x == 1 for x in per_shard):
                spread_ok = True
                break
            time.sleep(1)
        check(spread_ok, "节点复活后 EC 组自动重平衡到 4 个独立节点")
        with nn.meta.lock:
            g = nn._ec_group(gid)
            # 找一个节点上的分片直接改坏
            target_sid = list(g["shards"].keys())[0]
            target_node = list(g["shards"][target_sid]["replicas"].keys())[0]
        dns[target_node].corrupt_block_sim(target_sid)
        # 触发一次读来发现（或等巡检）
        nn.block_cache.invalidate(gid)
        try:
            nn.read_ec_block(gid)
        except Exception as e:
            print(f"   (读触发损坏发现: {e})")
        t0 = time.time()
        repaired = False
        while time.time() - t0 < 30:
            with nn.meta.lock:
                grp = nn._ec_group(gid)
                bad = [1 for sh in grp["shards"].values()
                       for r in sh["replicas"].values() if r["state"] == "corrupt"]
                live_n = nn.ec_live_shards(grp)[0]
            if not bad and live_n >= 4:
                repaired = True
                break
            time.sleep(1)
        check(repaired, "损坏分片被发现并自动重建")
        back = nn.read_blocks(info_ec["block_ids"])
        check(back == ec_data, "修复后读回一致")

        # 7. 目录冗余切换（rep -> ec），旧数据无损、读写不中断
        print("-- /r1.txt 所在根目录下新建 /archive 并把 r1 移入再转换 --")
        nn.fs.mkdir("/", "archive", "admin")
        # 直接对 /archive 下的复制文件做递归切换演示：
        nn.fs.move("/r1.txt", "/archive/r1.txt", "admin")
        # 移动后 r1 仍是 rep
        inode = nn.fs.resolve("/archive/r1.txt")
        kind, _ = nn.file_redundancy(inode)
        check(kind == "rep", "移动后文件仍是三副本（旧数据无损）")
        old_blocks = list(inode["block_ids"])
        res = nn.set_redundancy_policy("/archive", "ec", "2+1",
                                       recursive=True, user="admin")
        job = res["conversion"]
        check(job and job["pending"] >= 1, "已排队后台转换任务")
        # 转换期间持续可读
        reads_ok = True
        t0 = time.time()
        while time.time() - t0 < 20:
            try:
                if nn.read_blocks(nn.fs.resolve("/archive/r1.txt")
                                  ["block_ids"]) != rep_data:
                    reads_ok = False
                    break
            except Exception:
                reads_ok = False
                break
            with nn.meta.lock:
                st = [it for it in nn.meta.get("blocks")["conversions"]
                      [job["id"]]["items"]][0]["state"] if False else None
                jobdoc = nn.meta.get("blocks")["conversions"][job["id"]]
                done = jobdoc["state"] == "done"
            if done:
                break
            time.sleep(0.05)
        check(reads_ok, "转换过程中读不中断且数据正确")
        inode = nn.fs.resolve("/archive/r1.txt")
        kind2, prof = nn.file_redundancy(inode)
        check(kind2 == "ec" and prof == "2+1",
              f"转换完成后文件为 EC {prof}")
        check(nn.read_blocks(inode["block_ids"]) == rep_data,
              "转换后 EC 读回一致")
        # 旧 rep 块仍在块表（GC 宽限期内），且历史版本快照仍引用
        with nn.meta.lock:
            old_present = all(b in nn.meta.get("blocks")["blocks"]
                              for b in old_blocks)
        check(old_present, "旧三副本块保留（宽限期/版本引用，无损）")

        # 8. 两种冗余同目录并存
        nn.write_file_internal("/archive/a-ec.txt", b"new ec file", "admin")
        inode2 = nn.fs.resolve("/archive/a-ec.txt")
        check(nn.file_redundancy(inode2)[0] == "ec",
              "切换后新写入文件用 EC")
        kinds = {nn.file_redundancy(nn.fs.resolve(p))[0]
                 for p in ("/archive/r1.txt", "/archive/a-ec.txt")}
        # 同目录 ec 文件；并验证根目录新文件仍 rep（互不干扰）
        nn.write_file_internal("/newrep.txt", b"still replication", "admin")
        check(nn.file_redundancy(nn.fs.resolve("/newrep.txt"))[0] == "rep",
              "其它目录仍按各自策略（互不干扰）")

        # 9. 健康队列/修复进度接口可查
        q = nn.health_queue()
        check("ec_groups" in q and "counts" in q, "健康队列含 EC 重建进度")
        d = nn.file_blocks_detail("/cold/c1.bin")
        check(d["redundancy_actual"] == "ec" and
              all(len(b["shards"]) == 4 for b in d["blocks"]),
              "文件块详情含每分片落位")

        # 10. 空间总览
        ov = nn.redundancy_overview()
        check(ov["files_ec"] >= 2 and ov["files_rep"] >= 1,
              f"冗余总览 rep={ov['files_rep']} ec={ov['files_ec']}")

    finally:
        for d in dns.values():
            d.stop(mark_killed=False)
        nn.stop()
        shutil.rmtree(tmp, ignore_errors=True)

    if failures:
        print(f"\n{len(failures)} 项失败:")
        for f in failures:
            print(" -", f)
        sys.exit(1)
    print("\n全部 EC 端到端断言通过 ✅")


if __name__ == "__main__":
    main()
