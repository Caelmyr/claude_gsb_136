# -*- coding: utf-8 -*-
"""
reed_solomon.py — Reed-Solomon 纠删码（纯标准库，GF(2^8)）
=============================================================
在「三副本」之外提供一种更省空间的冗余方式：

    文件 → 切块（chunking）→ 每个块再拆成 k 个数据分片 D0..D{k-1}
                             + m 个校验分片 P0..P{m-1}

  * 存储空间开销 (k+m)/k（如 2+1 为 1.5×，2+2 为 2×），低于三副本 3×；
  * k+m 个分片分散到不同节点，任意 m 个分片损坏/丢失都能还原；
  * 编码矩阵采用 [单位阵 I | Vandermonde 校验行]；
  * 重建缺失分片：任取 k 个存活分片组成 k×k 子矩阵 A，
    在 GF(256) 上高斯-约旦求逆，A^-1·存活分片 = 全部分片，
    再用编码矩阵对应行点乘得到缺失分片。

有限域 GF(2^8)：本原多项式 x^8+x^4+x^3+x^2+1（0x11d），
指数/对数表 O(1) 乘法，按字节条带（stripe）做向量点积。

只依赖 Python 标准库；规模按教学集群设计（块 64KiB、k+m ≤ 8）。
"""

# ----------------------------------------------------------------------------
# GF(2^8) 域运算（本原多项式 0x11d）
# ----------------------------------------------------------------------------

_EXP = [0] * 512
_LOG = [0] * 256


def _init_tables(prim=0x11D):
    x = 1
    for i in range(255):
        _EXP[i] = x
        _LOG[x] = i
        x <<= 1
        if x & 0x100:
            x ^= prim
    for i in range(255, 512):
        _EXP[i] = _EXP[i - 255]


_init_tables()


def gf_mul(a, b):
    """GF(256) 乘法：对数相加（0 元素单独处理）。"""
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def _gf_vec_mul_add(c, vec, stripe, out):
    """out ^= c · vec（按字节条带）。c=0 时为空操作。"""
    if c == 0:
        return
    if c == 1:
        for i in range(stripe):
            out[i] ^= vec[i]
        return
    log_c = _LOG[c]
    for i in range(stripe):
        v = vec[i]
        if v:
            out[i] ^= _EXP[log_c + _LOG[v]]


# ----------------------------------------------------------------------------
# 编码矩阵：[ I (k×k) | Cauchy 校验行 (m×k) ]
# ----------------------------------------------------------------------------
# 选 Cauchy 而不是朴素 Vandermonde：[单位阵 | Cauchy] 的任意 k 行都可逆
# （删掉单位阵行/列后，剩下的仍是 Cauchy 子矩阵，det ≠ 0），这是
# systematic MDS 码的标准构造，保证「任意 m 个分片损坏都能还原」。

def gf_div(a, b):
    """GF(256) 除法（b 必须非 0）。"""
    if a == 0:
        return 0
    return _EXP[_LOG[a] - _LOG[b] + 255] if _LOG[a] < _LOG[b] \
        else _EXP[_LOG[a] - _LOG[b]]


def coding_matrix(k, m):
    """返回 (k+m)×k 的编码矩阵：前 k 行单位阵，后 m 行 Cauchy 矩阵。

    校验行 r、列 c 的系数 = 1 / (x_r + y_c)，
    取 x_r = r+1，y_c = m+1+c（两组小整数互不相交 ⇒ x+y ≠ 0）。
    k+m ≤ 8，字段值均在 1..8 内，安全。
    """
    n = k + m
    mat = [[0] * k for _ in range(n)]
    for i in range(k):
        mat[i][i] = 1                       # 数据分片：单位阵
    for r in range(m):
        row = mat[k + r]
        x = r + 1
        for c in range(k):
            row[c] = gf_div(1, x ^ (m + 1 + c))   # GF 加法 = 异或
    return mat


def _invert_matrix(mat):
    """GF(256) 上的高斯-约旦求逆（方阵，调用方保证可逆）。"""
    n = len(mat)
    a = [row[:] + [1 if i == j else 0 for j in range(n)]
         for i, row in enumerate(mat)]
    for col in range(n):
        pivot = next((r for r in range(col, n) if a[r][col]), None)
        if pivot is None:
            raise ValueError("编码矩阵奇异，无法重建（存活分片不足 k 个？）")
        if pivot != col:
            a[col], a[pivot] = a[pivot], a[col]
        inv_p = _EXP[255 - _LOG[a[col][col]]]
        a[col] = [gf_mul(v, inv_p) for v in a[col]]
        for r in range(n):
            if r == col:
                continue
            factor = a[r][col]
            if factor:
                a[r] = [v ^ gf_mul(factor, pv)
                        for v, pv in zip(a[r], a[col])]
    return [row[n:] for row in a]


# ----------------------------------------------------------------------------
# 分片 / 编码 / 重建
# ----------------------------------------------------------------------------

def split_shards(data, k):
    """把一块等长字节连续切成 k 个数据分片（末片零填充到统一条带长度）。

    返回 (shards, stripe)。stripe = ceil(len(data)/k)，
    原始长度由调用方（块表 size 字段）记录，重组时按 size 截尾。
    """
    stripe = (len(data) + k - 1) // k
    shards = [bytearray(data[i * stripe:(i + 1) * stripe]) for i in range(k)]
    for sh in shards:
        if len(sh) < stripe:
            sh.extend(b"\x00" * (stripe - len(sh)))
    return shards, stripe


def encode(data, k, m):
    """数据 → (k+m) 个等长分片 bytes（前 k 数据，后 m 校验）。"""
    data_shards, stripe = split_shards(data, k)
    matrix = coding_matrix(k, m)
    out = [bytes(s) for s in data_shards]
    for r in range(k, k + m):
        parity = bytearray(stripe)
        row = matrix[r]
        for c in range(k):
            _gf_vec_mul_add(row[c], data_shards[c], stripe, parity)
        out.append(bytes(parity))
    return out


def reconstruct(available, k, m, stripe, wanted=None):
    """
    用存活分片重建任意缺失分片。

    available: {index: bytes}，至少包含 k 个分片（数据或校验均可）；
    wanted:    需要重建的分片序号集合；None 表示重建全部缺失序号。
    返回 {index: bytes}（仅包含新重建出的分片）。
    """
    if len(available) < k:
        raise ValueError(
            f"存活分片不足：{len(available)} < k={k}，无法重建")
    have = sorted(available)[:k]                  # 任取 k 个即可
    matrix = coding_matrix(k, m)
    sub = [matrix[i] for i in have]               # k×k 子矩阵
    inv = _invert_matrix(sub)                     # A^-1
    surv = [available[i] for i in have]

    # 解码出全部 k 个数据分片
    decoded = [bytearray(stripe) for _ in range(k)]
    for r in range(k):
        inv_row = inv[r]
        for c in range(k):
            _gf_vec_mul_add(inv_row[c], surv[c], stripe, decoded[r])

    if wanted is None:
        wanted = [i for i in range(k + m) if i not in available]
    result = {}
    for idx in wanted:
        if idx in available:
            continue
        row = matrix[idx]
        out = bytearray(stripe)
        for c in range(k):
            _gf_vec_mul_add(row[c], decoded[c], stripe, out)
        result[idx] = bytes(out)
    return result


def join_data_shards(shards, k, original_size):
    """把 k 个数据分片连续拼回（split_shards 的逆运算），截掉末片填充。"""
    stripe = len(shards[0])
    data = bytearray(stripe * k)
    for idx, sh in enumerate(shards):
        base = idx * stripe
        data[base:base + len(sh)] = sh
    return bytes(data[:original_size])
