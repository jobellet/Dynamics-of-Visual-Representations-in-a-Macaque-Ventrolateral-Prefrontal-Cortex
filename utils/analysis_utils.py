import numpy as np
from scipy.stats import rankdata as scipy_rankdata
from scipy.spatial.distance import pdist
import jax.numpy as jnp
from jax import jit

rng_global = np.random.default_rng(42)

def get_upper_indices(n):
    return np.triu_indices(n, k=1)

def pairwise_euclidean_distance(X, i_upper, j_upper):
    diff = X[:, None, :] - X[None, :, :]
    return jnp.sum(diff**2, axis=-1)[i_upper, j_upper]

def spearman_corr_ranked(x, y):
    x_mean = x.mean()
    y_mean = y.mean()
    num = ((x - x_mean) * (y - y_mean)).sum()
    den = np.sqrt(((x - x_mean)**2).sum() * ((y - y_mean)**2).sum())
    return float(num / den)

def rank_data(arr):
    try:
        return scipy_rankdata(arr, axis=0)
    except Exception:
        return np.vstack([scipy_rankdata(arr[:, i]) for i in range(arr.shape[1])]).T

def rank_jaccard_rdm(x):
    x = np.asarray(x)
    if x.ndim == 1:
        x = x[:, None]
    N, M = x.shape
    out = np.empty_like(x, dtype=float)
    for j in range(M):
        col = x[:, j]
        n0 = np.count_nonzero(col == 0)
        if n0 == 0 or n0 == N:
            out[:, j] = (N + 1) / 2.0
        else:
            r0 = (n0 + 1) / 2.0
            r1 = (n0 + 1 + N) / 2.0
            out[:, j] = np.where(col == 0, r0, r1)
    return out.squeeze()

def pairwise_cosine_distances(X):
    X_norm = X / np.linalg.norm(X, axis=1, keepdims=True)
    sim = X_norm @ X_norm.T
    dmat = 1.0 - sim
    return dmat[np.triu_indices(X.shape[0], k=1)]

def jaccard_distance(labels):
    diff = labels[:, None] != labels[None, :]
    return diff[np.triu_indices(labels.size, k=1)].astype(float)

def robust_rdm(feat_list):
    dmats = [pdist(f, 'euclidean') for f in feat_list]
    normed = np.stack(
        [(d - np.median(d)) / (np.median(np.abs(d - np.median(d))) + 1e-9) for d in dmats],
        axis=0,
    )
    return scipy_rankdata(normed.mean(axis=0))

def rank_data_batch(arr, n_batch):
    n = arr.shape[1]
    out = np.empty_like(arr)
    for i in range(0, n, n_batch):
        out[:, i:i + n_batch] = rank_data(arr[:, i:i + n_batch])
    out[:, -n % n_batch:] = rank_data(arr[:, -n % n_batch:])
    return out

def condensed(mat):
    return pdist(mat, metric="euclidean")

@jit
def sqeucl(mat, vec):
    """row-wise mean squared distance to vec"""
    return jnp.mean((mat - vec)**2, axis=1)

def training_kind(name: str) -> str:
    lo = name.lower()
    if "clip" in lo:
        return "Language Aligned"
    if any(k in lo for k in ("dino", "mae", "moco", "simmim", "self")):
        return "Self-supervised"
    return "Supervised"

def round_robin_pairs(n: int, rng):
    players, rounds = list(range(n)), []
    for _ in range(n - 1):
        pairs = [(players[i], players[-i-1]) for i in range(n // 2)]
        rng.shuffle(pairs)
        rounds.append(pairs)
        players = [players[0]] + [players[-1]] + players[1:-1]
    return rounds

def pairs_to_batches(pairs_round, batch_size=16):
    flat = [j for p in pairs_round for j in p]
    return [
        np.array(flat[i : i + batch_size], int)
        for i in range(0, len(flat), batch_size)
        if i + batch_size <= len(flat)
    ]

def perm_signflip_onesample(vec, n_perm, greater=True):
    vec, obs, rnd = np.asarray(vec), vec.mean(), np.empty(n_perm)
    for i in range(n_perm):
        rnd[i] = (vec * rng_global.choice([-1, 1], size=vec.shape[0])).mean()
    p = (
        ((rnd >= obs).sum() + 1) / (n_perm + 1)
        if greater
        else ((np.abs(rnd) >= abs(obs)).sum() + 1) / (n_perm + 1)
    )
    return obs, rnd, p

def perm_diff_independent(x, y, n_perm, two_sided=True):
    x, y = np.asarray(x), np.asarray(y)
    obs  = x.mean() - y.mean()
    combined, n_x, rnd = np.concatenate([x, y]), x.shape[0], np.empty(n_perm)
    for i in range(n_perm):
        perm = rng_global.permutation(combined)
        rnd[i] = perm[:n_x].mean() - perm[n_x:].mean()
    if two_sided:
        p = ((np.abs(rnd) >= abs(obs)).sum() + 1) / (n_perm + 1)
    else:
        p = ((rnd >= obs).sum() + 1) / (n_perm + 1)
    return obs, rnd, p

def zscore(obs, perm):
    mu, sd = perm.mean(0), perm.std(0, ddof=0)
    return (obs - mu) / sd, (perm - mu) / sd
