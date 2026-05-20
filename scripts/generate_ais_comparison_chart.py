import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def main():
    # Connect to the DuckDB catalog
    db_path = 'Data/catalog.duckdb'
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    con = duckdb.connect(db_path)

    # Specific target sites and their mappings to user options
    target_sites = {
        'Dudgeon': 'Option 1: Dudgeon (UK MDE)',
        'Trianel Windpark Borkum 2': 'Option 2: Borkum II (Zenodo)',
        'Westermost Rough': 'Option 4a: Westermost Rough (Ørsted)',
        'Anholt': 'Option 4b: Anholt (Ørsted)',
        'Wikinger': 'Active Study: Wikinger (Core Farm)',
        'Baltic Eagle': 'Active Study: Baltic Eagle (Peak Farm)',
        'Trianel Windpark Borkum 1': 'Borkum I (Ref)'
    }

    # Query matching wind farms from our dwell events database
    query = """
    SELECT wind_farm, COUNT(*) as event_count 
    FROM dwell_events 
    WHERE wind_farm IN ('Dudgeon', 'Trianel Windpark Borkum 2', 'Westermost Rough', 'Anholt', 'Wikinger', 'Baltic Eagle', 'Trianel Windpark Borkum 1')
    GROUP BY wind_farm
    """
    df = con.execute(query).df()

    # Manually add Levenmouth (Option 3) since it has 0 events and is not in the database
    df = pd.concat([df, pd.DataFrame([{'wind_farm': 'Levenmouth', 'event_count': 0}])], ignore_index=True)
    target_sites['Levenmouth'] = 'Option 3: Levenmouth LDT (ORE)'

    # Map to labels
    df['Label'] = df['wind_farm'].map(target_sites).fillna(df['wind_farm'])

    # Sort descending
    df = df.sort_values(by='event_count', ascending=False)

    # Style the plot for a premium aesthetic
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, ax = plt.subplots(figsize=(12, 6.5))
    
    # Establish a premium, cohesive color palette
    # Option 4b Anholt gets the highlight color (vibrant emerald-teal)
    # Other options get a cool slate-blue
    # Our active study farms get a deep plum
    # Standard references get a muted gray
    palette = []
    for label in df['Label']:
        if 'Option 4b: Anholt' in label:
            palette.append('#10b981')  # Vibrant Emerald for the winning option
        elif 'Option' in label:
            palette.append('#0284c7')  # Premium Sky Blue for other options
        elif 'Active Study' in label:
            palette.append('#6366f1')  # Indigo-Purple for our active study farms
        else:
            palette.append('#94a3b8')  # Slate Gray for reference farms

    # Draw the horizontal bar chart
    bars = ax.barh(df['Label'], df['event_count'], color=palette, edgecolor='#e2e8f0', height=0.6)

    # Title & Labels
    ax.set_title('AIS Dwell Event Readings by Wind Farm (Comparing Pivot Options)', fontsize=15, fontweight='bold', pad=20, color='#1e293b')
    ax.set_xlabel('Number of Dwell Events (SOG < 0.5 kn, Duration ≥ 15 min)', fontsize=12, labelpad=12, fontweight='semibold', color='#334155')
    ax.tick_params(axis='both', which='major', labelsize=11, labelcolor='#475569')

    # Add values on the end of each bar
    for bar in bars:
        width = bar.get_width()
        ax.text(
            width + (20 if width > 0 else 5), 
            bar.get_y() + bar.get_height()/2, 
            f'{int(width):,}', 
            va='center', 
            ha='left', 
            fontsize=11, 
            fontweight='bold', 
            color='#1e293b'
        )

    # Spacing and boundaries
    ax.set_xlim(0, max(df['event_count']) * 1.12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(True)
    ax.spines['left'].set_color('#cbd5e1')
    ax.spines['bottom'].set_visible(True)
    ax.spines['bottom'].set_color('#cbd5e1')
    ax.grid(axis='x', linestyle='--', alpha=0.5, color='#cbd5e1')
    ax.grid(axis='y', visible=False)

    plt.tight_layout()

    # Save to workspace figures directory
    os.makedirs('reports/figures', exist_ok=True)
    workspace_fig_path = 'reports/figures/ais_option_comparison.png'
    plt.savefig(workspace_fig_path, dpi=300)
    print(f"Saved workspace figure to: {workspace_fig_path}")

    # Save to artifacts directory (for rendering in model response/artifacts)
    artifact_dir = '/Users/rafewatson/.gemini/antigravity/brain/46b87963-ae9a-46df-8e22-d89c80ed2a9d/artifacts'
    os.makedirs(artifact_dir, exist_ok=True)
    artifact_fig_path = os.path.join(artifact_dir, 'ais_option_comparison.png')
    plt.savefig(artifact_fig_path, dpi=300)
    print(f"Saved artifact figure to: {artifact_fig_path}")

    con.close()

if __name__ == '__main__':
    main()
