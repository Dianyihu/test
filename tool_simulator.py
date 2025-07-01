import simpy
import random
import json
from collections import defaultdict
import pandas as pd
import plotly.express as px


class MetalTool:
    """Represents the Metal Tool environment, its tools, and their states."""
    def __init__(self, env, unit_capability):
        self.env = env
        self.unit_to_parts = defaultdict(list)
        
        # Create a single, shared resource for each unique physical part.
        all_parts = set(p for unit in unit_capability for parts in unit.values() for p in parts)
        self.shared_resources = {part_name: simpy.Resource(env, capacity=1) for part_name in all_parts}

        for unit_info in unit_capability:
            for unit_id, part_names in unit_info.items():
                self.unit_to_parts[unit_id] = [
                    {"part_name": part_name, "resource": self.shared_resources[part_name]}
                    for part_name in part_names
                ]


def wafer_process(env, tool, wafer_id, unit_flow, start_time_tracker, end_time_tracker, wafer_log, start_at_step=None):
    """Simulates a single wafer's journey with continuous timeline (hold-and-wait)."""
    if not start_at_step:
        start_time_tracker[wafer_id] = env.now
    
    # Filter out transfer steps and sort by sequence ID
    sorted_flow = sorted([step for step in unit_flow if not step.get("transfer")], key=lambda x: int(x["seq_id"]))
    
    if not sorted_flow:
        end_time_tracker[wafer_id] = env.now
        return

    current_resource = None
    current_request = None
    current_part_name = None
    resource_start_time = None
    
    try:
        for idx, step in enumerate(sorted_flow):
            unit_id = step["unit_id"]
            unit_available = tool.unit_to_parts.get(unit_id, [])
            
            if not unit_available:
                print(f"Error: No tools available for unit '{unit_id}' for wafer {wafer_id}.")
                continue

            # Get the next resource
            if current_resource is None:
                # First step - acquire initial resource
                requests = [part["resource"].request() for part in unit_available]
                
                # Add timeout to prevent infinite waiting
                timeout_event = env.timeout(1000)  # 1000 time units timeout
                result = yield env.any_of(requests + [timeout_event])
                
                if timeout_event in result:
                    # Timeout occurred, cancel all requests and exit
                    for req in requests:
                        if not req.triggered:
                            req.cancel()
                    print(f"Timeout: Wafer {wafer_id} could not acquire initial resource for {unit_id}")
                    end_time_tracker[wafer_id] = env.now
                    return
                
                acquired_request = None
                for req in requests:
                    if req in result:
                        acquired_request = req
                        break
                
                if acquired_request is None:
                    print(f"Error: No request was acquired for wafer {wafer_id}")
                    end_time_tracker[wafer_id] = env.now
                    return
                
                for i, req in enumerate(requests):
                    if req == acquired_request:
                        current_resource = unit_available[i]["resource"]
                        current_part_name = unit_available[i]["part_name"]
                        current_request = acquired_request
                        resource_start_time = env.now
                    elif not req.triggered:
                        req.cancel()
            else:
                # Hold-and-wait: acquire next resource before releasing current
                requests = [part["resource"].request() for part in unit_available]
                
                # Add timeout to prevent infinite waiting
                timeout_event = env.timeout(1000)  # 1000 time units timeout
                result = yield env.any_of(requests + [timeout_event])
                
                if timeout_event in result:
                    # Timeout occurred, cancel all requests and continue with current resource
                    for req in requests:
                        if not req.triggered:
                            req.cancel()
                    print(f"Timeout: Wafer {wafer_id} could not acquire next resource for {unit_id}, continuing...")
                    # Process on current resource
                    yield env.timeout(step["duration_min"])
                    continue
                
                acquired_request = None
                for req in requests:
                    if req in result:
                        acquired_request = req
                        break
                
                if acquired_request is None:
                    print(f"Error: No next request was acquired for wafer {wafer_id}")
                    # Process on current resource
                    yield env.timeout(step["duration_min"])
                    continue
                
                next_resource = None
                next_part_name = None
                
                for i, req in enumerate(requests):
                    if req == acquired_request:
                        next_resource = unit_available[i]["resource"]
                        next_part_name = unit_available[i]["part_name"]
                    elif not req.triggered:
                        req.cancel()
                
                # Log the previous resource usage (from when it was acquired until now)
                wafer_log.append({
                    "WaferID": wafer_id, 
                    "UnitID": sorted_flow[idx-1]["unit_id"], 
                    "SeqID": sorted_flow[idx-1]['seq_id'],
                    "PartID": current_part_name, 
                    "Start": resource_start_time,
                    "Finish": env.now
                })
                
                # Release current resource and switch to next
                current_resource.release(current_request)
                current_resource = next_resource
                current_request = acquired_request
                current_part_name = next_part_name
                resource_start_time = env.now
            
            # Process for the required duration
            yield env.timeout(step["duration_min"])

        # Log the final resource usage
        if current_resource is not None:
            wafer_log.append({
                "WaferID": wafer_id, 
                "UnitID": sorted_flow[-1]["unit_id"], 
                "SeqID": sorted_flow[-1]['seq_id'],
                "PartID": current_part_name, 
                "Start": resource_start_time,
                "Finish": env.now
            })
            current_resource.release(current_request)

    except Exception as e:
        print(f"Error in wafer_process for {wafer_id}: {e}")
        # Clean up any held resources
        if current_resource is not None and current_request is not None:
            try:
                current_resource.release(current_request)
            except:
                pass
    
    end_time_tracker[wafer_id] = env.now


def lot_process(env, tool, lot_id, num_wafers, unit_flow, wafer_log):
    """Manages the processing of a lot of wafers and calculates its cycle time."""
    print(f"Starting LOT: {lot_id} with {num_wafers} wafers.")
    start_time = env.now
    
    wafer_start_times = {}
    wafer_end_times = {}
    
    processes = [
        env.process(wafer_process(env, tool, f"{lot_id}-W{i+1}", unit_flow, wafer_start_times, wafer_end_times, wafer_log))
        for i in range(num_wafers)
    ]
    
    yield simpy.AllOf(env, processes)

    end_time = env.now
    print(f"Finished LOT: {lot_id}. Total processing time: {(end_time - start_time)/60:.2f} min")
    return (end_time - start_time) / 60


if __name__ == "__main__":
    with open('metal_tool_by_unit.json', 'r') as f:
        config = json.load(f)['tin_tool_config']
    
    unit_capability = config['unit_capbility']
    unit_flow = config['unit_flow']

    env = simpy.Environment()
    tool = MetalTool(env, unit_capability)
    wafer_processing_log = []

    incoming_lot_id = "INCOMING_LOT"
    incoming_lot_size = 25
    
    print("--- Starting Simulation ---")
    lot_proc_event = env.process(lot_process(env, tool, incoming_lot_id, incoming_lot_size, unit_flow, wafer_processing_log))
    
    # Set a maximum simulation time to prevent infinite loops
    max_sim_time = 10000  # Maximum simulation time units
    
    try:
        env.run(until=simpy.events.AnyOf(env, [lot_proc_event, env.timeout(max_sim_time)]))
        
        if env.now >= max_sim_time:
            print(f"Simulation reached maximum time limit of {max_sim_time} time units")
        else:
            print("--- Simulation Finished ---")
            
    except Exception as e:
        print(f"Simulation error: {e}")
        print("--- Simulation Terminated ---")

    df = pd.DataFrame.from_records(wafer_processing_log)
    
    # Use the pre-calculated Finish times from the simulation
    if not df.empty:
        print(f"Generated {len(df)} processing records")
        # Convert to datetime for plotting
        df['Start'] = pd.to_datetime(df['Start'], unit='s')
        df['Finish'] = pd.to_datetime(df['Finish'], unit='s')

        fig = px.timeline(df, x_start="Start", x_end="Finish", y="WaferID", color="PartID", hover_name="PartID",
                          title="Wafer Processing Gantt Chart")
        fig.update_yaxes(categoryorder="total ascending")
        fig.show()
    else:
        print("No processing records generated - simulation may have failed")
