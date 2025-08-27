import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.lines import Line2D
from matplotlib.patches import PathPatch
from matplotlib.path import Path as MplPath
from matplotlib.legend_handler import HandlerPatch
from scipy.stats import gaussian_kde

def q_to_stars(q):
    return "****" if q<1e-4 else "***" if q<1e-3 else "**" if q<1e-2 else "*" if q<.05 else ""

def clusters(vals, gap=2.):
    if not vals: return []
    cur, out = [vals[0]], []
    for v in vals[1:]:
        (cur if v-cur[-1] < gap else out.append(cur) or (cur:=[v]))
    out.append(cur)
    return out

def gaussian_patch():
    x = np.linspace(0,1,50)
    y = np.exp(-((x-.5)/.25)**2)
    verts = np.column_stack([x,y])
    codes = [MplPath.MOVETO] + [MplPath.LINETO]*(len(x)-1)
    path = MplPath(verts, codes)
    return PathPatch(path, lw=1.3, facecolor="none", edgecolor="black")

class HandlerGaussian(HandlerPatch):
    def create_artists(self, legend, tup, xdescent, ydescent, width, height, fontsize, trans_):
        patch = gaussian_patch()
        import matplotlib.transforms as transforms
        patch.set_transform(trans_ +
            transforms.Affine2D().scale(width, height*0.8).translate(xdescent, ydescent))
        return [patch]

VERT_HANDLE = Line2D([0],[0], ls="none", marker='|', ms=10, mec="black", mfc="black", mew=1.3)
GAUSS_HANDLE = gaussian_patch()

def plot_rotated_density_single(ax, title, obs, perm, p_value,
                                color=(0.8,0.8,0.8), line_color="firebrick"):
    """One‐sided rotated KDE for a single distribution + obs line."""
    kde = gaussian_kde(perm)
    # x-grid over central bulk
    lo, hi = np.percentile(perm, [0.1, 99.9])
    pad = (hi - lo)*0.02
    xs = np.linspace(lo-pad, hi+pad, 200)
    dens = kde(xs)
    dens[dens < 0.005*dens.max()] = 0

    # plot above zero
    ax.fill_between(xs, 0, dens, color=color, alpha=0.6, linewidth=0)
    ax.plot(xs, dens, color=color, lw=1)

    # observed line
    ax.plot([obs, obs], [0, dens.max()*1.1],
            color=line_color, lw=2.5, zorder=5)

    # significance star
    if p_value < 0.05:
        ax.text(obs, dens.max()*1.2, "**",
                ha="center", va="bottom", fontsize=14, color=line_color)

    # styling
    ax.axhline(0, color="black", lw=0.8, alpha=0.3)
    ax.set_ylabel(title, rotation=0, ha="right", va="center")
    ax.yaxis.set_label_coords(-0.12, 0.5)
    ax.set_xticks(ax.get_xticks())
    ax.set_xlabel("Mean squared distance")
    for spine in ax.spines.values():
        spine.set_visible(False)
    # custom x-axis ticks
    y0 = 0
    ax.plot(ax.get_xlim(), [y0,y0], color="black", lw=1, clip_on=False)
    tick_len = (ax.get_ylim()[1]-ax.get_ylim()[0])*0.035
    for xt in ax.get_xticks():
        ax.plot([xt, xt], [y0, y0+tick_len], color="black", lw=0.8, clip_on=False)

def plot_rotated_density_dual(ax, title, obs_vals, perm_vals, colors, p_cat, p_pos):
    """Two‐sided KDE: perm_vals = [perm1, perm2], obs_vals = [obs1, obs2]."""
    perm1, perm2 = perm_vals
    obs1, obs2   = obs_vals
    kde1 = gaussian_kde(perm1)
    kde2 = gaussian_kde(perm2)

    # shared x-grid
    allp = np.concatenate([perm1, perm2])
    lo, hi = np.percentile(allp, [0.1, 99.9])
    pad = (hi - lo)*0.02
    xs = np.linspace(lo-pad, hi+pad, 200)

    d1 = kde1(xs); d2 = kde2(xs)
    d1[d1 < 0.005*d1.max()] = 0
    d2[d2 < 0.005*d2.max()] = 0
    m = max(d1.max(), d2.max(), 1e-8)

    # fill lower & upper
    ax.fill_between(xs, 0, -d1, color=colors[0], alpha=0.6, linewidth=0)
    ax.plot(xs, -d1, color=colors[0], lw=1)
    ax.fill_between(xs, 0, d2, color=colors[1], alpha=0.6, linewidth=0)
    ax.plot(xs, d2, color=colors[1], lw=1)

    # zero line
    ax.axhline(0, color="black", lw=0.8, alpha=0.3)

    # obs lines
    ax.plot([obs1]*2, [0, -m*1.1], color=colors[0], lw=2.5, zorder=5)
    ax.plot([obs2]*2, [0,  m*1.1], color=colors[1], lw=2.5, zorder=5)

    # stars
    if p_cat < 0.05:
        ax.text(obs1, -m*1.2, "*" if p_cat<0.05 else "",
                ha="center", va="top", fontsize=14, color=colors[0])
    if p_pos < 0.05:
        ax.text(obs2,  m*1.2, "*" if p_pos<0.05 else "",
                ha="center", va="bottom", fontsize=14, color=colors[1])

    # labels & styling
    ax.set_ylabel(title, rotation=0, ha="right", va="center")
    ax.yaxis.set_label_coords(-0.12, 0.5)
    ax.set_xlabel("Prediction-error gain")
    for spine in ax.spines.values():
        spine.set_visible(False)
    y0 = 0
    ax.plot(ax.get_xlim(), [y0,y0], color="black", lw=1, clip_on=False)
    tick_len = (ax.get_ylim()[1]-ax.get_ylim()[0])*0.035
    for xt in ax.get_xticks():
        ax.plot([xt, xt], [y0, y0+tick_len], color="black", lw=0.8, clip_on=False)
