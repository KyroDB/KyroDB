#!/usr/bin/env python3
"""
KyroDB Large-Scale Benchmark Plot Generator
Generates comprehensive visualizations for benchmark results
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import argparse
import json

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

class BenchmarkPlotter:
    def __init__(self, results_dir):
        self.results_dir = Path(results_dir)
        self.output_dir = self.results_dir / "plots"
        self.output_dir.mkdir(exist_ok=True)
        
    def load_summary_data(self):
        """Load summary data from CSV"""
        summary_file = self.results_dir / "summary.csv"
        if not summary_file.exists():
            print(f"Warning: Summary file not found: {summary_file}")
            return None
            
        df = pd.read_csv(summary_file)
        # Convert scale to millions for better readability
        df['scale_m'] = df['scale'] / 1_000_000
        return df
    
    def plot_latency_by_scale(self, df):
        """Plot latency percentiles by scale"""
        if df is None or df.empty:
            return
            
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('KyroDB Latency Performance by Scale', fontsize=16, fontweight='bold')
        
        metrics = ['p50_us', 'p95_us', 'p99_us']
        metric_names = ['P50 Latency (μs)', 'P95 Latency (μs)', 'P99 Latency (μs)']
        
        for i, (metric, name) in enumerate(zip(metrics, metric_names)):
            ax = axes[i // 2, i % 2]
            
            # Plot by distribution
            for dist in df['distribution'].unique():
                dist_data = df[df['distribution'] == dist]
                if dist == 'uniform':
                    ax.plot(dist_data['scale_m'], dist_data[metric], 
                           marker='o', linewidth=2, markersize=8, label=f'{dist.title()}')
                else:
                    # For zipf, show different thetas
                    for theta in dist_data['zipf_theta'].unique():
                        theta_data = dist_data[dist_data['zipf_theta'] == theta]
                        ax.plot(theta_data['scale_m'], theta_data[metric], 
                               marker='s', linewidth=2, markersize=8, 
                               label=f'{dist.title()} (θ={theta})')
            
            ax.set_xlabel('Dataset Size (millions of keys)')
            ax.set_ylabel(name)
            ax.set_title(f'{name} by Scale')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Use log scale for better visualization
            if metric == 'p99_us':
                ax.set_yscale('log')
        
        # Throughput plot
        ax = axes[1, 1]
        for dist in df['distribution'].unique():
            dist_data = df[df['distribution'] == dist]
            if dist == 'uniform':
                ax.plot(dist_data['scale_m'], dist_data['rps'], 
                       marker='o', linewidth=2, markersize=8, label=f'{dist.title()}')
            else:
                for theta in dist_data['zipf_theta'].unique():
                    theta_data = dist_data[dist_data['zipf_theta'] == theta]
                    ax.plot(theta_data['scale_m'], theta_data['rps'], 
                           marker='s', linewidth=2, markersize=8, 
                           label=f'{dist.title()} (θ={theta})')
        
        ax.set_xlabel('Dataset Size (millions of keys)')
        ax.set_ylabel('Throughput (RPS)')
        ax.set_title('Throughput by Scale')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'latency_by_scale.png', dpi=300, bbox_inches='tight')
        plt.close()
        
    def plot_distribution_comparison(self, df):
        """Compare uniform vs zipf distributions"""
        if df is None or df.empty:
            return
            
        # Filter to 10M and 50M for comparison
        comparison_df = df[df['scale'].isin([10_000_000, 50_000_000])].copy()
        if comparison_df.empty:
            return
            
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Uniform vs Zipf Distribution Performance', fontsize=16, fontweight='bold')
        
        metrics = ['p50_us', 'p95_us', 'p99_us', 'rps']
        metric_names = ['P50 Latency (μs)', 'P95 Latency (μs)', 'P99 Latency (μs)', 'Throughput (RPS)']
        
        for i, (metric, name) in enumerate(zip(metrics, metric_names)):
            ax = axes[i // 2, i % 2]
            
            # Create grouped bar chart
            scales = comparison_df['scale'].unique()
            x = np.arange(len(scales))
            width = 0.35
            
            # Uniform data
            uniform_data = comparison_df[comparison_df['distribution'] == 'uniform']
            uniform_values = [uniform_data[uniform_data['scale'] == scale][metric].iloc[0] 
                            if not uniform_data[uniform_data['scale'] == scale].empty else 0 
                            for scale in scales]
            
            # Zipf data (average across thetas)
            zipf_data = comparison_df[comparison_df['distribution'] == 'zipf']
            zipf_values = []
            for scale in scales:
                scale_data = zipf_data[zipf_data['scale'] == scale]
                if not scale_data.empty:
                    zipf_values.append(scale_data[metric].mean())
                else:
                    zipf_values.append(0)
            
            ax.bar(x - width/2, uniform_values, width, label='Uniform', alpha=0.8)
            ax.bar(x + width/2, zipf_values, width, label='Zipf (avg)', alpha=0.8)
            
            ax.set_xlabel('Dataset Size')
            ax.set_ylabel(name)
            ax.set_title(f'{name} Comparison')
            ax.set_xticks(x)
            ax.set_xticklabels([f'{scale//1_000_000}M' for scale in scales])
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            if metric == 'rps':
                ax.set_yscale('log')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'distribution_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        
    def plot_zipf_theta_impact(self, df):
        """Show impact of Zipf theta parameter"""
        if df is None or df.empty:
            return
            
        zipf_df = df[df['distribution'] == 'zipf'].copy()
        if zipf_df.empty:
            return
            
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Zipf Distribution: Impact of Theta Parameter', fontsize=16, fontweight='bold')
        
        metrics = ['p50_us', 'p95_us', 'p99_us', 'rps']
        metric_names = ['P50 Latency (μs)', 'P95 Latency (μs)', 'P99 Latency (μs)', 'Throughput (RPS)']
        
        for i, (metric, name) in enumerate(zip(metrics, metric_names)):
            ax = axes[i // 2, i % 2]
            
            for scale in zipf_df['scale'].unique():
                scale_data = zipf_df[zipf_df['scale'] == scale]
                ax.plot(scale_data['zipf_theta'], scale_data[metric], 
                       marker='o', linewidth=2, markersize=8, 
                       label=f'{scale//1_000_000}M keys')
            
            ax.set_xlabel('Zipf Theta (θ)')
            ax.set_ylabel(name)
            ax.set_title(f'{name} vs Zipf Theta')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            if metric == 'rps':
                ax.set_yscale('log')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'zipf_theta_impact.png', dpi=300, bbox_inches='tight')
        plt.close()
        
    def plot_performance_summary(self, df):
        """Create a comprehensive performance summary"""
        if df is None or df.empty:
            return
            
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('KyroDB Performance Summary', fontsize=16, fontweight='bold')
        
        # 1. Throughput heatmap
        ax = axes[0, 0]
        pivot_rps = df.pivot_table(values='rps', index='scale', columns='distribution', aggfunc='mean')
        sns.heatmap(pivot_rps, annot=True, fmt='.0f', cmap='YlOrRd', ax=ax)
        ax.set_title('Throughput (RPS) Heatmap')
        ax.set_xlabel('Distribution')
        ax.set_ylabel('Scale')
        
        # 2. P99 latency heatmap
        ax = axes[0, 1]
        pivot_p99 = df.pivot_table(values='p99_us', index='scale', columns='distribution', aggfunc='mean')
        sns.heatmap(pivot_p99, annot=True, fmt='.1f', cmap='RdYlBu_r', ax=ax)
        ax.set_title('P99 Latency (μs) Heatmap')
        ax.set_xlabel('Distribution')
        ax.set_ylabel('Scale')
        
        # 3. Latency distribution
        ax = axes[0, 2]
        latency_data = []
        labels = []
        for _, row in df.iterrows():
            latency_data.extend([row['p50_us'], row['p95_us'], row['p99_us']])
            labels.extend([f"{row['scale']//1_000_000}M-{row['distribution']}-P50",
                         f"{row['scale']//1_000_000}M-{row['distribution']}-P95",
                         f"{row['scale']//1_000_000}M-{row['distribution']}-P99"])
        
        ax.bar(range(len(latency_data)), latency_data, alpha=0.7)
        ax.set_title('Latency Distribution Across All Tests')
        ax.set_ylabel('Latency (μs)')
        ax.set_xticks(range(len(latency_data)))
        ax.set_xticklabels(labels, rotation=45, ha='right')
        
        # 4. Scale vs Performance
        ax = axes[1, 0]
        for dist in df['distribution'].unique():
            dist_data = df[df['distribution'] == dist]
            ax.scatter(dist_data['scale_m'], dist_data['rps'], 
                      s=100, alpha=0.7, label=dist.title())
        ax.set_xlabel('Scale (millions)')
        ax.set_ylabel('Throughput (RPS)')
        ax.set_title('Scale vs Throughput')
        ax.legend()
        ax.set_yscale('log')
        
        # 5. Latency vs Throughput
        ax = axes[1, 1]
        scatter = ax.scatter(df['p99_us'], df['rps'], 
                           c=df['scale_m'], s=100, alpha=0.7, cmap='viridis')
        ax.set_xlabel('P99 Latency (μs)')
        ax.set_ylabel('Throughput (RPS)')
        ax.set_title('Latency vs Throughput Trade-off')
        ax.set_yscale('log')
        ax.set_xscale('log')
        plt.colorbar(scatter, ax=ax, label='Scale (millions)')
        
        # 6. Performance ratio (uniform vs zipf)
        ax = axes[1, 2]
        uniform_data = df[df['distribution'] == 'uniform']
        zipf_data = df[df['distribution'] == 'zipf']
        
        if not uniform_data.empty and not zipf_data.empty:
            # Calculate ratio for common scales
            common_scales = set(uniform_data['scale']) & set(zipf_data['scale'])
            ratios = []
            scale_labels = []
            
            for scale in sorted(common_scales):
                uniform_rps = uniform_data[uniform_data['scale'] == scale]['rps'].iloc[0]
                zipf_avg_rps = zipf_data[zipf_data['scale'] == scale]['rps'].mean()
                ratio = uniform_rps / zipf_avg_rps
                ratios.append(ratio)
                scale_labels.append(f'{scale//1_000_000}M')
            
            ax.bar(scale_labels, ratios, alpha=0.7, color='orange')
            ax.set_xlabel('Scale')
            ax.set_ylabel('Uniform/Zipf Throughput Ratio')
            ax.set_title('Performance Ratio: Uniform vs Zipf')
            ax.axhline(y=1, color='red', linestyle='--', alpha=0.7)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'performance_summary.png', dpi=300, bbox_inches='tight')
        plt.close()
        
    def generate_report(self, df):
        """Generate a text report with key findings"""
        if df is None or df.empty:
            return
            
        report_file = self.output_dir / "benchmark_report.txt"
        
        with open(report_file, 'w') as f:
            f.write("KyroDB Large-Scale Benchmark Report\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Total benchmarks run: {len(df)}\n")
            f.write(f"Scales tested: {sorted(df['scale'].unique())}\n")
            f.write(f"Distributions tested: {sorted(df['distribution'].unique())}\n\n")
            
            # Best performance
            best_rps = df.loc[df['rps'].idxmax()]
            f.write(f"Best throughput: {best_rps['rps']:.0f} RPS\n")
            f.write(f"  - Scale: {best_rps['scale']:,} keys\n")
            f.write(f"  - Distribution: {best_rps['distribution']}\n")
            f.write(f"  - P99 latency: {best_rps['p99_us']:.1f} μs\n\n")
            
            # Worst performance
            worst_rps = df.loc[df['rps'].idxmin()]
            f.write(f"Lowest throughput: {worst_rps['rps']:.0f} RPS\n")
            f.write(f"  - Scale: {worst_rps['scale']:,} keys\n")
            f.write(f"  - Distribution: {worst_rps['distribution']}\n")
            f.write(f"  - P99 latency: {worst_rps['p99_us']:.1f} μs\n\n")
            
            # Scale impact
            f.write("Scale Impact Analysis:\n")
            for scale in sorted(df['scale'].unique()):
                scale_data = df[df['scale'] == scale]
                avg_rps = scale_data['rps'].mean()
                avg_p99 = scale_data['p99_us'].mean()
                f.write(f"  {scale:,} keys: {avg_rps:.0f} RPS avg, {avg_p99:.1f} μs P99 avg\n")
            
            f.write("\nDistribution Impact Analysis:\n")
            for dist in df['distribution'].unique():
                dist_data = df[df['distribution'] == dist]
                avg_rps = dist_data['rps'].mean()
                avg_p99 = dist_data['p99_us'].mean()
                f.write(f"  {dist.title()}: {avg_rps:.0f} RPS avg, {avg_p99:.1f} μs P99 avg\n")
            
            # Zipf theta analysis
            zipf_data = df[df['distribution'] == 'zipf']
            if not zipf_data.empty:
                f.write("\nZipf Theta Impact:\n")
                for theta in sorted(zipf_data['zipf_theta'].unique()):
                    theta_data = zipf_data[zipf_data['zipf_theta'] == theta]
                    avg_rps = theta_data['rps'].mean()
                    avg_p99 = theta_data['p99_us'].mean()
                    f.write(f"  θ={theta}: {avg_rps:.0f} RPS avg, {avg_p99:.1f} μs P99 avg\n")
        
        print(f"Report generated: {report_file}")
        
    def create_all_plots(self):
        """Generate all plots"""
        print("Loading benchmark data...")
        df = self.load_summary_data()
        
        if df is None or df.empty:
            print("No benchmark data found. Run benchmarks first.")
            return
        
        print(f"Loaded {len(df)} benchmark results")
        print("Generating plots...")
        
        self.plot_latency_by_scale(df)
        print("✓ Latency by scale plot generated")
        
        self.plot_distribution_comparison(df)
        print("✓ Distribution comparison plot generated")
        
        self.plot_zipf_theta_impact(df)
        print("✓ Zipf theta impact plot generated")
        
        self.plot_performance_summary(df)
        print("✓ Performance summary plot generated")
        
        self.generate_report(df)
        print("✓ Benchmark report generated")
        
        print(f"\nAll plots saved to: {self.output_dir}")

def main():
    parser = argparse.ArgumentParser(description='Generate KyroDB benchmark plots')
    parser.add_argument('--results-dir', default='bench/results', 
                       help='Directory containing benchmark results')
    parser.add_argument('--commit', help='Specific commit hash to plot')
    
    args = parser.parse_args()
    
    if args.commit:
        results_dir = Path(args.results_dir) / args.commit
    else:
        # Find the most recent results directory
        results_base = Path(args.results_dir)
        if not results_base.exists():
            print(f"Results directory not found: {results_base}")
            return
        
        # Find the most recent commit directory
        commit_dirs = [d for d in results_base.iterdir() if d.is_dir()]
        if not commit_dirs:
            print(f"No benchmark results found in {results_base}")
            return
        
        results_dir = max(commit_dirs, key=lambda x: x.stat().st_mtime)
    
    if not results_dir.exists():
        print(f"Results directory not found: {results_dir}")
        return
    
    print(f"Using results directory: {results_dir}")
    
    plotter = BenchmarkPlotter(results_dir)
    plotter.create_all_plots()

if __name__ == "__main__":
    main() 