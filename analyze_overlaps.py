#!/usr/bin/env python3
"""
Analyze Resource Overlaps
This script identifies overlapping resource usage periods in the simulation data.
"""

import pandas as pd
import numpy as np

def analyze_resource_overlaps(csv_file: str = "wafer_processing_logs.csv"):
    """Analyze overlapping resource usage periods"""
    
    df = pd.read_csv(csv_file)
    
    print("=== Resource Overlap Analysis ===\n")
    
    # Group by resource to check for overlaps
    resources = df['resource_id'].unique()
    
    overlaps_found = []
    
    for resource in resources:
        resource_data = df[df['resource_id'] == resource].copy()
        resource_data = resource_data.sort_values('start_time')
        
        print(f"\n--- Resource: {resource} ---")
        print(f"Total uses: {len(resource_data)}")
        
        # Check for overlaps
        overlaps = []
        for i in range(len(resource_data) - 1):
            current = resource_data.iloc[i]
            next_item = resource_data.iloc[i + 1]
            
            # Check if current end time is after next start time (overlap)
            if current['end_time'] > next_item['start_time']:
                overlap_duration = current['end_time'] - next_item['start_time']
                overlaps.append({
                    'resource': resource,
                    'wafer1': current['wafer_id'],
                    'wafer2': next_item['wafer_id'],
                    'wafer1_end': current['end_time'],
                    'wafer2_start': next_item['start_time'],
                    'overlap_duration': overlap_duration,
                    'seq1': current['seq_id'],
                    'seq2': next_item['seq_id']
                })
        
        if overlaps:
            print(f"⚠️  OVERLAPS FOUND: {len(overlaps)}")
            for overlap in overlaps[:5]:  # Show first 5 overlaps
                print(f"  - {overlap['wafer1']} (seq {overlap['seq1']}) ends at {overlap['wafer1_end']:.1f}")
                print(f"    {overlap['wafer2']} (seq {overlap['seq2']}) starts at {overlap['wafer2_start']:.1f}")
                print(f"    Overlap: {overlap['overlap_duration']:.1f} seconds")
                print()
            overlaps_found.extend(overlaps)
        else:
            print("✅ No overlaps found")
    
    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"Total overlaps found: {len(overlaps_found)}")
    
    if overlaps_found:
        # Group by resource
        overlap_by_resource = {}
        for overlap in overlaps_found:
            resource = overlap['resource']
            if resource not in overlap_by_resource:
                overlap_by_resource[resource] = []
            overlap_by_resource[resource].append(overlap)
        
        print("\nOverlaps by resource:")
        for resource, resource_overlaps in overlap_by_resource.items():
            print(f"  {resource}: {len(resource_overlaps)} overlaps")
    
    return overlaps_found

def check_simulation_logic():
    """Check the simulation logic for resource handling"""
    
    print("\n=== SIMULATION LOGIC ANALYSIS ===")
    
    # Check if the issue is in the simulator
    print("Potential causes of overlapping resources:")
    print("1. Multiple resources with same name but different capacities")
    print("2. Resource not properly being seized/released")
    print("3. Simulation time synchronization issues")
    print("4. Multiple instances of same resource type")
    
    # Check the JSON configuration
    import json
    with open('metal_tool_by_unit.json', 'r') as f:
        config = json.load(f)
    
    unit_capability = config['tin_tool_config']['unit_capbility']
    
    print(f"\nResource configuration from JSON:")
    for unit_id, resources in unit_capability.items():
        print(f"  {unit_id}: {resources}")
    
    # Check if VTR01 appears in multiple units
    vtr01_units = []
    for unit_id, resources in unit_capability.items():
        if 'VTR01' in resources:
            vtr01_units.append(unit_id)
    
    print(f"\nVTR01 appears in units: {vtr01_units}")
    
    if len(vtr01_units) > 1:
        print("⚠️  VTR01 appears in multiple units - this could cause overlaps!")
        print("   Each unit should have its own resource instance.")

def main():
    """Main analysis function"""
    overlaps = analyze_resource_overlaps()
    check_simulation_logic()
    
    if overlaps:
        print(f"\n⚠️  CONCLUSION: Resource overlaps detected!")
        print("The simulation is not properly handling resource constraints.")
        print("This explains why the Gantt chart shows overlapping timelines.")
    else:
        print(f"\n✅ CONCLUSION: No resource overlaps found.")

if __name__ == "__main__":
    main() 