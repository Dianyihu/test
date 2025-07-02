#!/usr/bin/env python3
"""
Individual Wafer Gantt Chart Generator
Creates separate Gantt charts for each wafer showing their complete processing journey.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def create_individual_wafer_gantts(csv_file: str = "wafer_processing_logs.csv"):
    """Create individual Gantt charts for each wafer"""
    
    # Load data
    df = pd.read_csv(csv_file)
    
    # Prepare data
    df['start_datetime'] = pd.to_datetime(df['start_time'], unit='s')
    df['end_datetime'] = pd.to_datetime(df['end_time'], unit='s')
    df['process_step'] = df['unit_id'] + ' (seq ' + df['seq_id'].astype(str) + ')'
    df['duration_minutes'] = df['duration'] / 60
    
    # Color mapping for units
    units = df['unit_id'].unique()
    color_palette = px.colors.qualitative.Set3 + px.colors.qualitative.Pastel
    unit_colors = {unit: color_palette[i % len(color_palette)] for i, unit in enumerate(units)}
    
    # Create output directory
    output_dir = "individual_wafer_gantts"
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all unique wafers
    wafers = sorted(df['wafer_id'].unique())
    
    print(f"Creating individual Gantt charts for {len(wafers)} wafers...")
    
    for wafer_id in wafers:
        wafer_data = df[df['wafer_id'] == wafer_id].copy()
        wafer_data = wafer_data.sort_values('seq_id')
        
        # Create Gantt chart for this wafer
        fig = px.timeline(
            wafer_data,
            x_start="start_datetime",
            x_end="end_datetime",
            y="process_step",
            color="unit_id",
            title=f"Processing Timeline for {wafer_id} (Lot: {wafer_data.iloc[0]['lot_id']})",
            labels={
                "process_step": "Process Step",
                "unit_id": "Processing Unit",
                "start_datetime": "Time",
                "end_datetime": "Time"
            },
            hover_data={
                "duration_minutes": ":.1f",
                "resource_id": True
            },
            color_discrete_map=unit_colors
        )
        
        # Order process steps by sequence
        process_order = []
        for seq_id in sorted(wafer_data['seq_id'].astype(int).unique()):
            steps = wafer_data[wafer_data['seq_id'] == str(seq_id)]['process_step'].unique()
            process_order.extend(steps)
        
        fig.update_yaxes(categoryorder="array", categoryarray=process_order)
        
        # Customize layout
        fig.update_layout(
            height=max(400, len(process_order) * 40),
            showlegend=True,
            xaxis_title="Time",
            yaxis_title="Process Step",
            font=dict(size=11),
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )
        
        # Add detailed hover information
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>" +
                         "Duration: %{customdata[0]:.1f} min<br>" +
                         "Resource: %{customdata[1]}<br>" +
                         "Start: %{x}<br>" +
                         "End: %{x2}<extra></extra>",
            customdata=wafer_data[['duration_minutes', 'resource_id']].values
        )
        
        # Save individual chart
        filename = f"{output_dir}/{wafer_id}_gantt.html"
        fig.write_html(filename)
        print(f"  ✓ {wafer_id} → {filename}")
    
    # Create a summary chart showing all wafers side by side
    create_all_wafers_summary(df, unit_colors, output_dir)
    
    print(f"\nAll individual Gantt charts saved in '{output_dir}/' directory")

def create_all_wafers_summary(df, unit_colors, output_dir):
    """Create a summary showing all wafers in one view"""
    
    fig = px.timeline(
        df,
        x_start="start_datetime",
        x_end="end_datetime",
        y="wafer_id",
        color="unit_id",
        title="All Wafers Processing Timeline Summary",
        labels={
            "wafer_id": "Wafer ID",
            "unit_id": "Processing Unit",
            "start_datetime": "Time",
            "end_datetime": "Time"
        },
        hover_data={
            "process_step": True,
            "duration_minutes": ":.1f",
            "resource_id": True,
            "lot_id": True
        },
        color_discrete_map=unit_colors
    )
    
    # Customize layout
    fig.update_layout(
        height=max(600, len(df['wafer_id'].unique()) * 25),
        showlegend=True,
        xaxis_title="Time",
        yaxis_title="Wafer ID",
        font=dict(size=10),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    # Order wafers
    fig.update_yaxes(categoryorder="array", categoryarray=sorted(df['wafer_id'].unique()))
    
    # Enhanced hover template
    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>" +
                     "Unit: %{customdata[0]}<br>" +
                     "Duration: %{customdata[1]:.1f} min<br>" +
                     "Resource: %{customdata[2]}<br>" +
                     "Lot: %{customdata[3]}<br>" +
                     "Start: %{x}<br>" +
                     "End: %{x2}<extra></extra>",
        customdata=df[['process_step', 'duration_minutes', 'resource_id', 'lot_id']].values
    )
    
    # Save summary
    filename = f"{output_dir}/all_wafers_summary.html"
    fig.write_html(filename)
    print(f"  ✓ Summary chart → {filename}")

def create_wafer_statistics_table():
    """Create a statistics table for all wafers"""
    
    df = pd.read_csv("wafer_processing_logs.csv")
    
    # Calculate statistics per wafer
    wafer_stats = df.groupby('wafer_id').agg({
        'start_time': 'min',
        'end_time': 'max',
        'duration': 'sum',
        'lot_id': 'first'
    }).reset_index()
    
    wafer_stats['cycle_time'] = wafer_stats['end_time'] - wafer_stats['start_time']
    wafer_stats['cycle_time_hours'] = wafer_stats['cycle_time'] / 3600
    wafer_stats['processing_time_hours'] = wafer_stats['duration'] / 3600
    wafer_stats['utilization'] = (wafer_stats['duration'] / wafer_stats['cycle_time'] * 100).round(1)
    
    # Round for display
    wafer_stats['cycle_time_hours'] = wafer_stats['cycle_time_hours'].round(2)
    wafer_stats['processing_time_hours'] = wafer_stats['processing_time_hours'].round(2)
    
    # Save to CSV
    output_stats = wafer_stats[['wafer_id', 'lot_id', 'cycle_time_hours', 'processing_time_hours', 'utilization']]
    output_stats.columns = ['Wafer_ID', 'Lot_ID', 'Cycle_Time_Hours', 'Processing_Time_Hours', 'Utilization_Percent']
    output_stats.to_csv('wafer_statistics.csv', index=False)
    
    print(f"Wafer statistics saved to wafer_statistics.csv")
    print("\nWafer Processing Statistics:")
    print(output_stats.to_string(index=False))

def main():
    """Main function"""
    print("=== Individual Wafer Gantt Chart Generator ===\n")
    
    # Create individual charts
    create_individual_wafer_gantts()
    
    # Create statistics
    create_wafer_statistics_table()
    
    print("\n=== Generation Complete ===")
    print("Files created:")
    print("1. individual_wafer_gantts/ - Directory with individual wafer charts")
    print("2. wafer_statistics.csv - Statistical summary of all wafers")

if __name__ == "__main__":
    main() 