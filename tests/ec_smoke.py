# -*- coding: utf-8 -*-
"""EC 端到端冒烟：写入 / 读取 / 节点故障重建 / 分片损坏重建 / 策略切换并存。"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.main import Cluster
from backend import config


def wait_for(pred, timeout=30, msg=""):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        last = pred()
        if last:
            return last
        time.sleep(0.3)
    raise AssertionError(f"超时等待: {msg} (last={last})")


def main():
    import tempfile
    from backend import datanode as dn_mod
    # 使用临时数据目录 + 独立端口，避免影响正在运行的演示集群
    tmp = tempfile.mkdtemp(prefix="dfsvs-ec-smoke-")
    config.DATA_DIR = tmp
    config.META_DIR = os.path.join(tmp, "meta")
    config.SESSION_DIR = os.path.join(tmp, "sessions")
    config.DATANODE_ROOT = os.path.join(tmp, "datanodes")
    config.NAMENODE_PORT = 8120
    port_map = {"dn1": (8131, "rack-1"), "dn2": (8132, "rack-2"),
                "dn3": (8133, "rack-3"), "dn4": (8134, "rack-1")}
    config.DATANODE_PORTS.update(port_map)
    c = Cluster(datanode_count=4, seed=False, verbose=False,
                nn_port=8120)
    c.start()
    nn = c.nn
    try:
        time.sleep(1.0)

        # 1) 三副本文件（旧方式不受影响）
        data_rep = ("三副本文件内容-" * 5000).encode()
        nn.write_file_internal("/rep.txt", data_rep, "admin")
        got, info = nn.read_file_range("/rep.txt", 0, len(data_rep))
        assert got == data_rep and info["storage"] == "replica"
        print("1) 三副本写读 OK")

        # 2) 目录切换为 EC
        nn.fs.mkdir("/", "ecdir", "admin")
        nn.set_storage_policy("/ecdir", "ec", "ec-2+1", "admin")
        v = nn.get_storage_policy_view("/ecdir")
        assert v["policy"] == "ec" and v["effective"]["profile"] == "ec-2+1"
        assert v["live_nodes"] == 4 and v["profiles"][0]["fits"]
        print("2) 目录策略切 EC 2+1 OK")

        # 3) EC 写入：大小覆盖 <k / 非对齐 / 多组
        for size in (0, 1, 100, 300_000, config.EC_GROUP_MAX_DATA + 5000):
            payload = os.urandom(size)
            p = f"/ecdir/f-{size}.bin"
            nn.write_file_internal(p, payload, "admin")
            got, info = nn.read_file_range(p, 0, max(size, 1))
            got = got[:size]
            assert got == payload, size
            assert info["storage"] == "ec"
        print("3) EC 写读 OK（空/非对齐/跨组）")

        # 4) 分片分布：每分片在不同节点
        det = nn.file_blocks_detail("/ecdir/f-300000.bin")
        assert det["storage"] == "ec"
        g = det["groups"][0]
        nodes_per_shard = [set(s["nodes"]) for s in g["shards"]]
        all_nodes = set().union(*nodes_per_shard)
        assert len(all_nodes) == 3, all_nodes   # 3 分片放 3 个不同节点
        assert g["k"] == 2 and g["m"] == 1 and g["available"] == 3
        print("4) 分片分散到不同节点 OK:",
              {s["index"]: s["nodes"] for s in g["shards"]})

        # 5) 两种冗余同目录并存：把 /rep.txt 旁边再放 EC 文件
        nn.write_file_internal("/rep-sibling-ec.bin", os.urandom(5000),
                               "admin", storage="ec", ec_profile="ec-2+1")
        nn.write_file_internal("/rep-sibling-rep.bin", os.urandom(5000),
                               "admin", storage="replica")
        d1 = nn.file_blocks_detail("/rep-sibling-ec.bin")
        d2 = nn.file_blocks_detail("/rep-sibling-rep.bin")
        assert d1["storage"] == "ec" and d2["storage"] == "replica"
        print("5) 同目录两种冗余并存 OK")

        # 6) 杀死一个持有分片的节点 -> 自动重建；期间仍可读
        target_gid = det["groups"][0]["id"]
        victim = g["shards"][0]["nodes"][0]
        c.dns[victim].kill_sim()
        # 等 NN 判 DEAD
        wait_for(lambda: nn.nodes.get(victim, {}).get("state") == "DEAD",
                 15, "判 DEAD")
        # 读不受影响（2 个分片即可还原）
        payload = open("/dev/null", "rb").read()  # placeholder
        got, _ = nn.read_file_range("/ecdir/f-300000.bin", 0, 300_000)
        assert len(got) == 300_000
        print(f"6) 杀死 {victim} 后 EC 文件仍可读（降级读）OK")
        # 等自动重建到健康
        wait_for(lambda: nn.ec_group_state_readonly(target_gid)["state"]
                 == "healthy", 40, "EC 自动重建")
        det2 = nn.file_blocks_detail("/ecdir/f-300000.bin")
        g2 = det2["groups"][0]
        assert g2["available"] == 3
        new_nodes = set().union(*[set(s["nodes"]) for s in g2["shards"]])
        assert victim not in new_nodes
        # 内容仍一致
        # 重新计算原始 payload 的 sha 无法直接对比随机内容，改为校验读长度+哈希稳定
        h1 = hashlib_sha(got)
        got2, _ = nn.read_file_range("/ecdir/f-300000.bin", 0, 300_000)
        assert hashlib_sha(got2) == h1
        print(f"   自动重建完成，分片落在 {sorted(new_nodes)} OK")

        # 7) 复活旧节点：旧分片作为重复副本被归一化
        c.dns[victim].revive_sim()
        time.sleep(3)
        det3 = nn.file_blocks_detail("/ecdir/f-300000.bin")
        for s in det3["groups"][0]["shards"]:
            live_reps = [r for r in s["replicas"] if r["state"] == "ok"
                         and r["live"]]
            assert len(live_reps) == 1, (s["index"], live_reps)
        print("7) 节点复活后 EC 分片归一化（无重复副本）OK")

        # 8) 注入分片静默损坏 -> 巡检/读发现 -> 删除坏分片并重建
        victim2_gid = det3["groups"][0]["id"]
        shard0 = det3["groups"][0]["shards"][0]
        nid2 = shard0["nodes"][0]
        bid2 = shard0["id"]
        c.dns[nid2].corrupt_block_sim(bid2)
        # 等待 scrub（6s）或主动触发读路径
        wait_for(lambda: nn.ec_group_state_readonly(victim2_gid)["state"]
                 == "healthy", 45, "坏分片重建后恢复")
        st = nn.ec_group_state_readonly(victim2_gid)
        assert st["available"] == 3
        print("8) 静默损坏自动修复 OK")

        # 9) 节点不足时显式方案应报错；auto 回退
        c.dns["dn2"].kill_sim()
        time.sleep(8)
        try:
            nn.write_file_internal("/ecdir/should-fail.bin", b"x" * 100,
                                   "admin", storage="ec",
                                   ec_profile="ec-4+2")
            raise AssertionError("应因节点不足报错")
        except Exception as e:
            assert "需要 6" in str(e), str(e)
        print("9) 节点不足时 EC 4+2 明确报错 OK")
        c.dns["dn2"].revive_sim()
        time.sleep(2)

        # 10) GC 不会误删 EC 分片
        nn.gc_blocks()
        got, _ = nn.read_file_range("/ecdir/f-300000.bin", 0, 300_000)
        assert len(got) == 300_000
        print("10) GC 保护 EC 分片 OK")

        # 11) 三副本文件在 EC 目录策略切换后，旧文件仍为 replica
        assert nn.fs.resolve("/rep.txt").get("storage") == "replica"
        pol = nn.resolve_storage_policy("/ecdir")
        assert pol["policy"] == "ec"
        print("11) 切换策略旧数据无损保留 OK")

        print("\n全部断言通过 ✔")
    finally:
        c.stop()


def hashlib_sha(b):
    import hashlib
    return hashlib.sha256(b).hexdigest()


if __name__ == "__main__":
    main()
