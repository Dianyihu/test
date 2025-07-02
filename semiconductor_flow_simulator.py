#!/usr/bin/env python3
"""
Simplified Semiconductor Tool Simulator
Properly handles sequential flow through semiconductor processing units.
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
        self.current_seq_id = 1  # Start with seq_id 1 (ATR), skip LOADPORT
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

class FlowController(FlowUnit):
    """Controls wafer flow through the entire semiconductor process"""
    
    def __init__(self, env: simpy.Environment, name: str, config: Dict):
        super().__init__(env, name)
        self.config = config
        self.unit_capability = config['tin_tool_config']['unit_capbility']
        self.unit_flow = config['tin_tool_config']['unit_flow']
        
        # Create flow sequence
        self.flow_sequence = sorted(self.unit_flow, key=lambda x: int(x['seq_id']))
        
        # Create resource pools for each unit
        self.resource_pools = {}
        self._create_resource_pools()
        
    def _create_resource_pools(self):
        """Create resource pools for each unit"""
        capability_map = {}
        for cap in self.unit_capability:
            for unit_id, resources in cap.items():
                capability_map[unit_id] = resources
        
        for unit_id, resources in capability_map.items():
            if unit_id != "LOADPORT":
                # Create a resource pool for this unit
                capacity = len(resources)
                self.resource_pools[unit_id] = simpy.Resource(self.env, capacity=capacity)
    
    def receive_agent(self, agent: WaferAgent):
        """Start processing wafer through the complete flow"""
        self.agents_entered += 1
        yield from self._process_wafer_flow(agent)
    
    def _process_wafer_flow(self, agent: WaferAgent):
        """Process wafer through the complete semiconductor flow"""
        
        for step in self.flow_sequence:
            seq_id = int(step['seq_id'])
            unit_id = step['unit_id']
            
            # Skip LOADPORT steps
            if unit_id == "LOADPORT":
                continue
            
            # Get processing parameters
            duration_mean = step['duration_mean']
            duration_std = step['duration_std']
            duration_min = step['duration_min']
            is_transfer = step['transfer']
            
            # Skip zero-duration steps
            if duration_mean == 0:
                agent.log_processing_step(unit_id, str(seq_id), self.env.now, self.env.now, "N/A")
                continue
            
            # Calculate processing time
            if duration_std > 0:
                processing_time = max(duration_min, np.random.normal(duration_mean, duration_std))
            else:
                processing_time = duration_mean
            
            # Get resource
            if unit_id in self.resource_pools:
                resource = self.resource_pools[unit_id]
                
                start_time = self.env.now
                
                # Request resource
                with resource.request() as request:
                    yield request
                    
                    # Process
                    logger.info(f"Processing wafer {agent.id} in {unit_id} (seq {seq_id}) for {processing_time:.1f}s")
                    yield self.env.timeout(processing_time)
                    
                    end_time = self.env.now
                    agent.log_processing_step(unit_id, str(seq_id), start_time, end_time, unit_id)
        
        # Wafer completed processing
        self.agents_exited += 1
        self.send_agent(agent)

class WaferSource(Source):
    """Specialized source for wafer generation"""
    
    def __init__(self, env: simpy.Environment, name: str, lot_size: int = 25, 
                 lot_interval: float = 3600, max_lots: int = None):
        super().__init__(env, name)
        self.lot_size = lot_size
        self.lot_interval = lot_interval
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

class SemiconductorFlowSimulator:
    """Main simulator for semiconductor tool flow"""
    
    def __init__(self, config_file: str):
        self.env = simpy.Environment()
        self.model = FlowModel(self.env)
        self.wafer_logs = []
        
        # Load configuration
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self._create_flow_model()
    
    def _create_flow_model(self):
        """Create the flow model"""
        
        # Create wafer source
        self.wafer_source = WaferSource(
            self.env, "WaferSource", 
            lot_size=25, 
            lot_interval=3600,  # 1 hour between lots
            max_lots=3  # Reduced for testing
        )
        self.model.add_unit(self.wafer_source)
        
        # Create flow controller
        self.flow_controller = FlowController(self.env, "FlowController", self.config)
        self.model.add_unit(self.flow_controller)
        
        # Create sink
        self.wafer_sink = Sink(self.env, "WaferSink")
        self.model.add_unit(self.wafer_sink)
        
        # Connect the flow
        self.wafer_source.connect_to(self.flow_controller)
        self.flow_controller.connect_to(self.wafer_sink)
    
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
            
            # Unit utilization statistics
            unit_stats = {}
            for unit_id in df['unit_id'].unique():
                unit_data = df[df['unit_id'] == unit_id]
                total_processing_time = unit_data['duration'].sum()
                utilization = total_processing_time / total_time if total_time > 0 else 0
                
                unit_stats[unit_id] = {
                    'wafers_processed': len(unit_data),
                    'total_processing_time': total_processing_time,
                    'average_processing_time': unit_data['duration'].mean(),
                    'utilization': utilization
                }
            
            stats['simulation_summary'] = {
                'total_wafers_completed': completed_wafers,
                'simulation_time_hours': total_time / 3600,
                'throughput_wafers_per_hour': throughput,
                'average_cycle_time_minutes': avg_cycle_time / 60,
                'min_cycle_time_minutes': min_cycle_time / 60,
                'max_cycle_time_minutes': max_cycle_time / 60,
                'unit_statistics': unit_stats
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
            if 'unit_statistics' in summary:
                print(f"\nUNIT UTILIZATION:")
                for unit_id, unit_stats in summary['unit_statistics'].items():
                    print(f"  {unit_id}:")
                    print(f"    Wafers Processed: {unit_stats['wafers_processed']}")
                    print(f"    Avg Processing Time: {unit_stats['average_processing_time']:.1f}s")
                    print(f"    Utilization: {unit_stats['utilization']:.1%}")
        
        # Print flow unit statistics
        print(f"\nFLOW UNIT STATISTICS:")
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
    
    def create_gantt_chart(self, filename: str = "wafer_gantt_chart.html"):
        """Create a Gantt chart of wafer processing"""
        if not self.wafer_logs:
            print("No wafer logs available for Gantt chart")
            return
        
        try:
            import plotly.express as px
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
            
            df = pd.DataFrame(self.wafer_logs)
            
            # Create Gantt chart
            fig = px.timeline(df, 
                            x_start="start_time", 
                            x_end="end_time",
                            y="resource_id", 
                            color="wafer_id",
                            title="Semiconductor Tool Wafer Processing Gantt Chart",
                            labels={"resource_id": "Resource", "wafer_id": "Wafer"})
            
            fig.update_yaxes(categoryorder="total ascending")
            fig.update_layout(height=800)
            
            fig.write_html(filename)
            print(f"Gantt chart saved to {filename}")
            
        except ImportError:
            print("Plotly not available. Cannot create Gantt chart.")

def main():
    """Main function to run the semiconductor tool simulation"""
    
    # Create and run simulation
    simulator = SemiconductorFlowSimulator("metal_tool_by_unit.json")
    
    # Run for 8 hours (shorter for testing)
    simulator.run_simulation(duration=8 * 3600)
    
    # Print results
    simulator.print_statistics()
    
    # Save logs and create visualizations
    simulator.save_wafer_logs()
    simulator.create_gantt_chart()

if __name__ == "__main__":
    main() 