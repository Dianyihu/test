#!/usr/bin/env python3
"""
Semiconductor Tool Simulator
Based on the metal tool configuration and flow diagram.
Simulates wafer processing through various tool stages.
"""

import simpy
import random
import json
import numpy as np
from anylogic_flow_units import *
from typing import Dict, List, Any
import pandas as pd

class WaferAgent(Agent):
    """Specialized agent for wafer processing"""
    
    def __init__(self, wafer_id: str, lot_id: str, creation_time: float):
        super().__init__(wafer_id, creation_time)
        self.lot_id = lot_id
        self.current_seq_id = 1  # Start with seq_id 1 (ATR), skip LOADPORT (seq_id 0)
        self.processing_history = []
        self.start_time = creation_time
        
    def log_processing_step(self, unit_id: str, seq_id: str, start_time: float, end_time: float, resource_id: str = None):
        """Log a processing step"""
        self.processing_history.append({
            'wafer_id': self.id,
            'lot_id': self.lot_id,
            'unit_id': unit_id,
            'seq_id': seq_id,
            'resource_id': resource_id,
            'start_time': start_time,
            'end_time': end_time,
            'duration': end_time - start_time
        })

class SemiconductorToolUnit(FlowUnit):
    """Specialized flow unit for semiconductor tool processing"""
    
    def __init__(self, env: simpy.Environment, unit_id: str, resources: List[str], 
                 unit_flow_steps: List[Dict], flow_sequence: Dict, is_transfer_unit: bool = False):
        super().__init__(env, unit_id)
        self.unit_id = unit_id
        self.resources = {}
        self.unit_flow_steps = {step['seq_id']: step for step in unit_flow_steps if step['unit_id'] == unit_id}
        self.flow_sequence = flow_sequence
        self.is_transfer_unit = is_transfer_unit
        
        # Create resources for this unit
        for resource_name in resources:
            self.resources[resource_name] = simpy.Resource(env, capacity=1)
        
        # For units with multiple resources, use round-robin selection
        self.resource_names = list(resources)
        self.resource_index = 0
        
    def get_next_resource(self) -> str:
        """Get next resource using round-robin"""
        if len(self.resource_names) == 1:
            return self.resource_names[0]
        
        resource_name = self.resource_names[self.resource_index]
        self.resource_index = (self.resource_index + 1) % len(self.resource_names)
        return resource_name
    
    def get_processing_time(self, seq_id: str) -> float:
        """Get processing time based on normal distribution"""
        if seq_id not in self.unit_flow_steps:
            return 0.0
            
        step = self.unit_flow_steps[seq_id]
        if step['duration_mean'] == 0:
            return 0.0
            
        # Use normal distribution with min constraint
        duration = max(
            step['duration_min'],
            np.random.normal(step['duration_mean'], step['duration_std'])
        )
        return duration
    
    def receive_agent(self, agent: WaferAgent):
        """Process wafer through this unit"""
        self.agents_entered += 1
        
        # Find the current processing step for this agent
        current_step = None
        for step in self.unit_flow_steps.values():
            if int(step['seq_id']) == agent.current_seq_id:
                current_step = step
                break
        
        if not current_step:
            logger.warning(f"No step found for agent {agent.id} at seq_id {agent.current_seq_id} in unit {self.unit_id}")
            # If no step found, check if we should skip this unit
            agent.current_seq_id += 1
            self.agents_exited += 1
            self.send_agent(agent)
            return
            
        yield from self._process_wafer(agent, current_step)
    
    def _process_wafer(self, agent: WaferAgent, step: Dict):
        """Process wafer through the unit"""
        start_time = self.env.now
        
        # Skip resource allocation for zero-duration steps
        if step['duration_mean'] == 0:
            agent.log_processing_step(
                self.unit_id, step['seq_id'], start_time, start_time, "N/A"
            )
            agent.current_seq_id += 1
            self.agents_exited += 1
            self.send_agent(agent)
            return
        
        # Get resource and processing time
        resource_name = self.get_next_resource()
        resource = self.resources[resource_name]
        processing_time = self.get_processing_time(step['seq_id'])
        
        # Request resource
        with resource.request() as request:
            yield request
            
            # Process
            logger.info(f"Unit {self.unit_id} processing wafer {agent.id} on {resource_name} for {processing_time:.1f}s")
            yield self.env.timeout(processing_time)
            
            # Log the processing step
            end_time = self.env.now
            agent.log_processing_step(
                self.unit_id, step['seq_id'], start_time, end_time, resource_name
            )
            
            # Update agent state
            agent.current_seq_id += 1
            self.agents_exited += 1
            
            # Forward to next unit
            self.send_agent(agent)

class WaferSource(Source):
    """Specialized source for wafer generation"""
    
    def __init__(self, env: simpy.Environment, name: str, lot_size: int = 25, 
                 lot_interval: float = 3600, max_lots: int = None):
        super().__init__(env, name)
        self.lot_size = lot_size
        self.lot_interval = lot_interval  # Time between lot arrivals
        self.max_lots = max_lots
        self.lot_count = 0
        
    def _generation_process(self):
        """Generate lots of wafers"""
        while True:
            if self.max_lots and self.lot_count >= self.max_lots:
                break
                
            # Generate a lot of wafers
            lot_id = f"LOT_{self.lot_count:03d}"
            
            for wafer_idx in range(self.lot_size):
                wafer_id = f"{lot_id}_W{wafer_idx:02d}"
                wafer = WaferAgent(wafer_id, lot_id, self.env.now)
                
                self.generated_count += 1
                self.agents_exited += 1
                
                logger.info(f"Generated wafer {wafer_id} from {lot_id} at time {self.env.now}")
                self.send_agent(wafer)
                
                # Small delay between wafers in the same lot
                yield self.env.timeout(random.uniform(10, 30))
            
            self.lot_count += 1
            
            # Wait for next lot
            if self.max_lots is None or self.lot_count < self.max_lots:
                yield self.env.timeout(self.lot_interval)

class SemiconductorToolSimulator:
    """Main simulator for semiconductor tool"""
    
    def __init__(self, config_file: str):
        self.env = simpy.Environment()
        self.model = FlowModel(self.env)
        self.units = {}
        self.wafer_logs = []
        
        # Load configuration
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.unit_capability = self.config['tin_tool_config']['unit_capbility']
        self.unit_flow = self.config['tin_tool_config']['unit_flow']
        
        # Create flow sequence mapping
        self.flow_sequence = self._create_flow_sequence()
        
        self._create_flow_units()
        self._connect_flow_units()
    
    def _create_flow_sequence(self):
        """Create a mapping of seq_id to next unit"""
        sorted_flow = sorted(self.unit_flow, key=lambda x: int(x['seq_id']))
        sequence = {}
        
        for i, step in enumerate(sorted_flow):
            seq_id = int(step['seq_id'])
            unit_id = step['unit_id']
            
            # Find next unit (skip LOADPORT at end)
            next_unit = None
            if i < len(sorted_flow) - 1:
                next_step = sorted_flow[i + 1]
                if next_step['unit_id'] != 'LOADPORT':
                    next_unit = next_step['unit_id']
            
            sequence[seq_id] = {
                'unit_id': unit_id,
                'next_unit': next_unit,
                'step_data': step
            }
        
        return sequence
    
    def _create_flow_units(self):
        """Create all flow units based on configuration"""
        
        # Create wafer source
        self.wafer_source = WaferSource(
            self.env, "WaferSource", 
            lot_size=25, 
            lot_interval=3600,  # 1 hour between lots
            max_lots=5
        )
        self.model.add_unit(self.wafer_source)
        
        # Create processing units based on capability
        capability_map = {}
        for cap in self.unit_capability:
            for unit_id, resources in cap.items():
                capability_map[unit_id] = resources
        
        # Create units
        for unit_id, resources in capability_map.items():
            if unit_id == "LOADPORT":
                continue  # Skip LOADPORT as it's handled by source/sink
                
            # Determine if this is a transfer unit
            is_transfer = any(step['transfer'] for step in self.unit_flow if step['unit_id'] == unit_id)
            
            unit = SemiconductorToolUnit(
                self.env, unit_id, resources, self.unit_flow, self.flow_sequence, is_transfer
            )
            self.units[unit_id] = unit
            self.model.add_unit(unit)
        
        # Create sink
        self.wafer_sink = Sink(self.env, "WaferSink")
        self.model.add_unit(self.wafer_sink)
    
    def _connect_flow_units(self):
        """Connect flow units based on sequence"""
        # Sort flow steps by sequence ID
        sorted_flow = sorted(self.unit_flow, key=lambda x: int(x['seq_id']))
        
        # Connect source to first processing unit (ATR)
        first_unit = next((step['unit_id'] for step in sorted_flow if step['unit_id'] != 'LOADPORT'), None)
        if first_unit and first_unit in self.units:
            self.wafer_source.connect_to(self.units[first_unit])
        
        # Create a sequential connection chain
        prev_unit = None
        for step in sorted_flow:
            current_unit = step['unit_id']
            
            # Skip LOADPORT
            if current_unit == 'LOADPORT':
                continue
                
            if current_unit in self.units:
                if prev_unit and prev_unit != current_unit and prev_unit in self.units:
                    # Connect previous unit to current unit
                    self.units[prev_unit].connect_to(self.units[current_unit])
                prev_unit = current_unit
        
        # Connect last unit to sink
        if prev_unit and prev_unit in self.units:
            self.units[prev_unit].connect_to(self.wafer_sink)
    
    def run_simulation(self, duration: float = 86400):  # 24 hours default
        """Run the simulation"""
        print(f"Starting semiconductor tool simulation for {duration/3600:.1f} hours...")
        
        # Start the simulation
        self.model.run_simulation(duration)
        
        # Collect wafer processing logs
        self._collect_wafer_logs()
        
        print(f"Simulation completed at time {self.env.now:.1f}")
        
    def _collect_wafer_logs(self):
        """Collect processing logs from completed wafers"""
        if hasattr(self.wafer_sink, 'completed_agents'):
            for wafer in self.wafer_sink.completed_agents:
                if hasattr(wafer, 'processing_history'):
                    self.wafer_logs.extend(wafer.processing_history)
    
    def get_statistics(self):
        """Get comprehensive simulation statistics"""
        stats = self.model.get_all_statistics()
        
        # Add wafer-specific statistics
        if self.wafer_logs:
            df = pd.DataFrame(self.wafer_logs)
            
            # Calculate throughput statistics
            completed_wafers = len(df['wafer_id'].unique()) if not df.empty else 0
            total_time = self.env.now
            throughput = completed_wafers / (total_time / 3600) if total_time > 0 else 0
            
            # Calculate cycle time statistics
            if not df.empty:
                wafer_cycle_times = df.groupby('wafer_id').agg({
                    'start_time': 'min',
                    'end_time': 'max'
                })
                wafer_cycle_times['cycle_time'] = wafer_cycle_times['end_time'] - wafer_cycle_times['start_time']
                
                avg_cycle_time = wafer_cycle_times['cycle_time'].mean()
                min_cycle_time = wafer_cycle_times['cycle_time'].min()
                max_cycle_time = wafer_cycle_times['cycle_time'].max()
            else:
                avg_cycle_time = min_cycle_time = max_cycle_time = 0
            
            stats['simulation_summary'] = {
                'total_wafers_completed': completed_wafers,
                'simulation_time_hours': total_time / 3600,
                'throughput_wafers_per_hour': throughput,
                'average_cycle_time_minutes': avg_cycle_time / 60,
                'min_cycle_time_minutes': min_cycle_time / 60,
                'max_cycle_time_minutes': max_cycle_time / 60
            }
        
        return stats
    
    def print_statistics(self):
        """Print detailed simulation statistics"""
        stats = self.get_statistics()
        
        print("\n" + "="*60)
        print("SEMICONDUCTOR TOOL SIMULATION RESULTS")
        print("="*60)
        
        # Print simulation summary
        if 'simulation_summary' in stats:
            summary = stats['simulation_summary']
            print(f"\nSIMULATION SUMMARY:")
            print(f"  Simulation Time: {summary['simulation_time_hours']:.1f} hours")
            print(f"  Wafers Completed: {summary['total_wafers_completed']}")
            print(f"  Throughput: {summary['throughput_wafers_per_hour']:.2f} wafers/hour")
            print(f"  Average Cycle Time: {summary['average_cycle_time_minutes']:.1f} minutes")
            print(f"  Min Cycle Time: {summary['min_cycle_time_minutes']:.1f} minutes")
            print(f"  Max Cycle Time: {summary['max_cycle_time_minutes']:.1f} minutes")
        
        # Print unit statistics
        print(f"\nUNIT STATISTICS:")
        for unit_name, unit_stats in stats.items():
            if unit_name == 'simulation_summary':
                continue
                
            print(f"\n{unit_name}:")
            for key, value in unit_stats.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}")
    
    def save_wafer_logs(self, filename: str = "wafer_processing_logs.csv"):
        """Save wafer processing logs to CSV"""
        if self.wafer_logs:
            df = pd.DataFrame(self.wafer_logs)
            df.to_csv(filename, index=False)
            print(f"Wafer processing logs saved to {filename}")
        else:
            print("No wafer logs to save")

def main():
    """Main function to run the semiconductor tool simulation"""
    
    # Create and run simulation
    simulator = SemiconductorToolSimulator("metal_tool_by_unit.json")
    
    # Run for 24 hours
    simulator.run_simulation(duration=24 * 3600)
    
    # Print results
    simulator.print_statistics()
    
    # Save logs
    simulator.save_wafer_logs()

if __name__ == "__main__":
    main() 