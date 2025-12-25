import numpy as np
import pandas as pd



def bootstrap_diff_means(a, b, n_boot=2000, seed=0): 


    a = pd.to_numeric(a, errors='coerce').dropna().to_numpy()
    b = pd.to_numeric(b, errors='coerce').dropna().to_numpy()

    rng = np.random.default_rng(seed)
    boot_diffs = []
    
    for _ in range(n_boot):
        a_sample = rng.choice(a, size=len(a), replace=True)
        b_sample = rng.choice(b, size=len(b), replace=True)
        boot_diffs.append(np.mean(a_sample) - np.mean(b_sample))
    
    boot_diffs = pd.Series(boot_diffs)

    return{
        "diff_mean": float(a.mean() - b.mean()),
        "ci_lower": np.quantile(boot_diffs,0.025),
        "ci_upper": float(np.quantile(boot_diffs,0.975)),
    }
    