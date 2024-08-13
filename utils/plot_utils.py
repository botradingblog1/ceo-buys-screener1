import matplotlib.pyplot as plt
from config import *
import os


def plot_candidates(candidates_df):
    # Sort the candidates by ownership change and price drop
    candidates_df = candidates_df.nlargest(10, 'ownership_change')

    # Plotting
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(candidates_df['price_drop'], candidates_df['ownership_change'],
                          s=candidates_df['ownership_change'] * 50, alpha=0.6, edgecolors="w", linewidth=2)

    for i, row in candidates_df.iterrows():
        plt.text(row['price_drop'], row['ownership_change'], row['symbol'], fontsize=10, ha='right')

    plt.title('Top 10 Stocks by CEO Ownership Change and Price Drop')
    plt.xlabel('Price Drop (%)')
    plt.ylabel('Ownership Change (%)')
    plt.grid(True)

    # Store image file
    os.makedirs(PLOTS_DIR, exist_ok=True)
    path = os.path.join(PLOTS_DIR, "top_stocks_ceo_buys.png")
    plt.savefig(path, format="png")