import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def plot_conf_matrix(cmx):
    cmap = mpl.colors.ListedColormap(['green'])
    cmap1 = mpl.colors.ListedColormap(['red'])
    mask1 = (cmx.isin([cmx.iloc[0,0],cmx.iloc[1,1]]))
    
    f, ax = plt.subplots(figsize = (5,3))
    sns.heatmap(cmx, annot=True, fmt = 'g', cmap = cmap,
            cbar = False, annot_kws={"size": 20},
            ax=ax)
    sns.heatmap(cmx, mask=mask1 , cmap=cmap1, cbar=False)

    ax.set_ylabel('Predicted label', fontsize = 15)
    ax.set_xlabel('True label', fontsize = 15)
    ax.set_title("Confusion Matrix", fontsize = 20)
    plt.show()