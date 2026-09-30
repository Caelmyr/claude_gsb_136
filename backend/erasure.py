# -*- coding: utf-8 -*-
"""
erasure.py — 纠删码（Reed-Solomon over GF(2^8)）
==================================================
把一段数据切成 k 个等长数据分片 D0..D{k-1}，再线性编码出 m 个校验分片
P0..P{m-1}；k+m 个分片分散放在不同节点上，**任意 k 个分片**即可无损还原。

空间开销 1 + m/k（如 2+1 为 ×1.5），明显低于三副本（×3）。

数学：
  * 有限域 GF(2^8)，本原多项式 0x11d（x^8+x^4+x^3+x^2+1），
    加减法即字节 XOR；乘除法用 log/exp 表完成。
  * 编码矩阵 [ I | P ]：上半部 k×k 单位阵（数据分片原样），下半部为
    Cauchy 矩阵 P[i][j] = 1/(x_i XOR y_j)，其中 x_i、y_j 取互不相交的域元素。
    Cauchy 矩阵的任意方子矩阵都可逆 —— 因而丢失任意分片（含数据分片），
    都能用存活的任意 k 行做高斯消元反解出全部数据分片。
  * 重建：取存活分片对应的 k 个行向量组成 A，左乘 A^{-1}，得到
    [D0..D{k-1}]；若只需补某个丢失分片，再用编码矩阵对应行做一次线性组合。

性能（教学规模，纯标准库）：
  域乘 mul(a,b) 经 bytes.translate 的 256 字节查表 + int XOR 完成，
  按 EC_CELL_SIZE 单元流式处理，避免 O(n·k) 的 Python 字节循环。
"""

from . import config

# ---------------------------------------------------------------------------
# GF(2^8) 对数/指数表（本原多项式 0x11d）
# ---------------------------------------------------------------------------

_FIELD = 256
_EXP = [0] * 512
_LOG = [0] * _FIELD


def _init_tables():
    x = 1
    for i in range(_FIELD - 1):
        _EXP[i] = x
        _LOG[x] = i
        x <<= 1
        if x & 0x100:
            x ^= 0x11D
    for i in range(_FIELD - 1, 512):
        _EXP[i] = _EXP[i - (_FIELD - 1)]


_init_tables()


def gf_mul(a, b):
    """GF(2^8) 单字节乘法（标量路径，构造矩阵用）。"""
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def gf_div(a, b):
    """GF(2^8) 单字节除法。"""
    if a == 0:
        return 0
    if b == 0:
        raise ZeroDivisionError("GF(256) 除零")
    return _EXP[(_LOG[a] - _LOG[b]) % (_FIELD - 1)]


def gf_inv(a):
    """GF(2^8) 乘法逆元。"""
    if a == 0:
        raise ZeroDivisionError("0 无乘法逆元")
    return _EXP[(_FIELD - 1) - _LOG[a]]


# ---------------------------------------------------------------------------
# 矩阵工具（元素均为 GF(2^8) 域元素）
# ---------------------------------------------------------------------------

def _cauchy_matrix(rows, cols, x_off=0, y_off=128):
    """
    生成 rows×cols Cauchy 矩阵：P[i][j] = 1 / (x_i XOR y_j)。
    x 取 [x_off, x_off+rows)，y 取 [y_off, y_off+cols)，
    rows, cols 均很小（<= 16），整数域内不会重叠。
    """
    return [[gf_inv((x_off + i) ^ (y_off + j)) for j in range(cols)]
            for i in range(rows)]


def _identity(n):
    return [[1 if i == j else 0 for j in range(n)] for i in range(n)]


def coding_matrix(k, m):
    """系统编码矩阵 (k+m)×k = [I_k ; P_cauchy]。"""
    return _identity(k) + _cauchy_matrix(m, k)


def _invert_matrix(mat):
    """
    GF(2^8) 上的高斯-若尔当消元求逆（n 很小）。
    Cauchy 矩阵保证本模块取到的任意子方阵都可逆。
    """
    n = len(mat)
    aug = [row[:] + eye_row[:] for row, eye_row in zip(mat, _identity(n))]
    for col in range(n):
        pivot = next((r for r in range(col, n) if aug[r][col]), None)
        if pivot is None:
            raise ValueError("矩阵在 GF(256) 上不可逆（分片组合不可解）")
        if pivot != col:
            aug[col], aug[pivot] = aug[pivot], aug[col]
        inv_p = gf_inv(aug[col][col])
        aug[col] = [gf_mul(v, inv_p) for v in aug[col]]
        for r in range(n):
            if r == col or aug[r][col] == 0:
                continue
            factor = aug[r][col]
            aug[r] = [v ^ gf_mul(factor, pv)
                      for v, pv in zip(aug[r], aug[col])]
    return [row[n:] for row in aug]


# ---------------------------------------------------------------------------
# 分片级线性组合（bytes.translate 查表，逐单元流式执行）
# ---------------------------------------------------------------------------

def _mul_tables():
    """a -> 长度 256 的乘法查表（bytes.translate 用），惰性缓存。"""
    tbl = [None] * _FIELD
    tbl[0] = bytes(_FIELD)
    for a in range(1, _FIELD):
        base = _LOG[a]
        tbl[a] = bytes(0 if j == 0 else _EXP[base + _LOG[j]]
                       for j in range(_FIELD))
    return tbl


_MUL_TABLES = _mul_tables()


def _linear_combo(coeffs, shards, cell_size, out_len):
    """
    计算 sum_i coeffs[i] * shards[i]（GF(2^8) 上的字节级线性组合）。
    coeffs[i] == 0 的分片跳过。

    查表乘法走 bytes.translate（C 速度）；GF 加法 = XOR，
    用 int.from_bytes 把整条分片转大整数后逐位 XOR（比 Python 层
    逐字节 zip 快约两个数量级），再转回 bytes。
    教学规模分片 ≤ 4MiB，整数运算内存开销可接受。
    """
    pairs = [(c, s) for c, s in zip(coeffs, shards) if c]
    if not pairs:
        return b"\x00" * out_len
    acc = 0
    for c, s in pairs:
        acc ^= int.from_bytes(s.translate(_MUL_TABLES[c]), "big")
    return acc.to_bytes(out_len, "big")


# ---------------------------------------------------------------------------
# 编码 / 重建
# ---------------------------------------------------------------------------

def encode(data, k, m, cell_size=None):
    """
    数据 -> (shards, pad)：
      shards 长度 k+m，前 k 个为数据分片（等长，末尾补零），
      后 m 个为校验分片；pad 为末尾填充字节数（还原时裁掉）。
    """
    cell_size = cell_size or config.EC_CELL_SIZE
    if k <= 0 or m <= 0:
        raise ValueError("k、m 必须为正整数")
    pad = (-len(data)) % k
    shard_len = (len(data) + pad) // k
    if shard_len == 0:
        padded = data + b"\x00" * pad
    else:
        padded = data + b"\x00" * pad
    shards = [bytes(padded[i * shard_len:(i + 1) * shard_len])
              for i in range(k)]
    p = _cauchy_matrix(m, k)
    for row in p:
        shards.append(_linear_combo(row, shards[:k], cell_size, shard_len))
    return shards, pad


def reconstruct(available, k, m, want=None, cell_size=None):
    """
    用存活分片重建。
      available: {index: shard_bytes}，至少包含 k 个分片；
      want:      需要输出的分片索引集合（None 表示全部数据分片 0..k-1）。
    返回 {index: shard_bytes}。

    做法：取存活分片中任意 k 个，其编码矩阵行组成 A；
    解 A·D = S 得数据分片 D；wanted 中的校验分片再由 P 线性组合算出。
    """
    cell_size = cell_size or config.EC_CELL_SIZE
    if len(available) < k:
        raise ValueError(
            f"存活分片不足：{len(available)} < {k}，无法重建（超过容错上限）")
    have = sorted(available.keys())[:k]
    n = k + m
    if any(i < 0 or i >= n for i in have):
        raise ValueError("分片索引越界")
    shard_len = len(available[have[0]])
    if any(len(available[i]) != shard_len for i in have):
        raise ValueError("存活分片长度不一致")
    cm = coding_matrix(k, m)
    a_mat = [cm[i] for i in have]
    a_inv = _invert_matrix(a_mat)
    have_shards = [available[i] for i in have]
    data_shards = []
    for row in a_inv:
        data_shards.append(_linear_combo(row, have_shards, cell_size,
                                         shard_len))
    want = set(range(k)) if want is None else set(want)
    out = {}
    for idx in want:
        if idx in available:
            out[idx] = available[idx]
        elif idx < k:
            out[idx] = data_shards[idx]
        else:
            out[idx] = _linear_combo(cm[idx], data_shards, cell_size,
                                     shard_len)
    return out


def decode(available, k, m, total_size, cell_size=None):
    """用任意 k+ 个存活分片还原原始数据（自动裁掉填充）。"""
    data = reconstruct(available, k, m, want=set(range(k)),
                       cell_size=cell_size)
    raw = b"".join(data[i] for i in range(k))
    return raw[:total_size]


def profile_ok(k, m, nodes):
    """给定存活节点数，该方案能否放置（每分片一个不同节点）。"""
    return nodes >= k + m


def best_profile(nodes):
    """按节点数挑一个放得下的最省空间方案；都放不下返回 None。"""
    best = None
    for name, p in config.EC_PROFILES.items():
        if nodes >= p["k"] + p["m"]:
            if best is None or (p["k"] + p["m"]) > (best[1]["k"] + best[1]["m"]):
                best = (name, p)
    return best
