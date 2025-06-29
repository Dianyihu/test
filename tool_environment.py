import simpy
import random
import json
from collections import defaultdict


class MetalTool:
    """Represents the Metal Tool environment, its tools, and their states."""
    def __init__(self, env, unit_capbility, status):
        self.env = env
        self.parts = {}  
        self.part_to_unit_map = {}
        self.unit_to_parts = defaultdict(list)

        for unit_info in unit_capbility:
            for unit_id, part_names in unit_info.items():
                self.unit_to_parts[unit_id] = []
                for part_name in part_names:
                    self.part_to_unit_map[part_name] = unit_id
                    if status.get(part_name) != 'down':
                        resource = simpy.Resource(env, capacity=1)
                        self.parts[part_name] = resource
                        self.unit_to_parts[unit_id].append(resource)

    def pre_occupy_part(self, resource, part_name):
        """A process to simulate a tool being occupied at the start."""
        # Assumption: The tool is busy for a random time, representing remaining work.
        hold_time = random.uniform(50, 250)
        with resource.request() as req:
            yield req
            # print(f"{self.env.now:.2f}: {tool_name} is pre-occupied for {hold_time:.2f}s.")
            yield self.env.timeout(hold_time)
            # print(f"{self.env.now:.2f}: {tool_name} is now free.")


def wafer_process(env, tool, wafer_id, unit_flow, start_time_tracker, end_time_tracker, start_at_step=None):
    """Simulates a single wafer's journey, with an optional starting step."""
    if not start_at_step:
        start_time_tracker[wafer_id] = env.now
    
    sorted_flow = sorted(unit_flow, key=lambda x: int(x["seq_id"]))
    start_index = 0

    if start_at_step:
        # Find the index of the starting step
        try:
            start_index = next(i for i, step in enumerate(sorted_flow) if step['seq_id'] == start_at_step['seq_id'])
        except StopIteration:
            print(f"Error: start_at_step {start_at_step['seq_id']} not found in flow for wafer {wafer_id}.")
            return

        # Handle the first, partially-completed step
        first_step = sorted_flow[start_index]
        unit_id = first_step["unit_id"]
        
        # This assumes the specific occupied tool is known and passed implicitly.
        # A more robust solution might pass the specific tool resource.
        available_tools = tool.unit_to_parts.get(unit_id, [])
        if not available_tools:
            return # No available tools for this unit.
            
        # For simplicity, we just grab the first tool of the unit.
        res = available_tools[0]
        with res.request() as req:
            yield req
            
            # Simulate remaining time
            if first_step["recipe"]:
                remaining_time = random.uniform(0, first_step["recipe_time"])
            else:
                remaining_time = random.uniform(0, first_step["time_mean"])
            
            yield env.timeout(remaining_time)
            res.release(req)
        
        # Advance index to start the loop at the next step
        start_index += 1

    for step in sorted_flow[start_index:]:
        unit_id = step["unit_id"]
        
        available_tools = tool.unit_to_parts.get(unit_id)
        if not available_tools:
            print(f"Error: No tools available for unit '{unit_id}' for wafer {wafer_id}.")
            return

        reqs = [res.request() for res in available_tools]
        result = yield env.any_of(reqs)
        
        req = list(result.keys())[0]
        res = req.resource
        
        for r in reqs:
            if r != req: r.cancel()
        
        if step["recipe"]:
            proc_time = step["recipe_time"]
        else:
            proc_time = max(0, random.normalvariate(step["time_mean"], step["time_std"]))
        
        yield env.timeout(proc_time)
        res.release(req)

    end_time_tracker[wafer_id] = env.now


def lot_process(env, tool, lot_id, num_wafers, unit_flow):
    """Manages the processing of a lot of wafers and calculates its cycle time."""
    print(f"Starting LOT: {lot_id} with {num_wafers} wafers.")
    start_time = env.now
    
    wafer_start_times = {}
    wafer_end_times = {}

    for i in range(num_wafers):
        wafer_id = f"{lot_id}-W{i+1}"
        env.process(wafer_process(env, tool, wafer_id, unit_flow, wafer_start_times, wafer_end_times))

    while len(wafer_end_times) < num_wafers:
        yield env.timeout(1)

    end_time = env.now
    print(f"Finished LOT: {lot_id}. Total processing time: {(end_time - start_time)/60:.2f} min")
    return (end_time - start_time)/60


def initialize_factory_state(env, tool, status, unit_flow):
    """Creates 'ghost' wafers for tools that are initially occupied."""
    ghost_wafer_ends = {}
    for tool_name, tool_status in status.items():
        if tool_status == 'occupied' and tool_name in tool.parts:
            unit_id = tool.part_to_unit_map.get(tool_name)
            if not unit_id: continue

            # Find possible steps in the flow for this unit
            possible_steps = [s for s in unit_flow if s['unit_id'] == unit_id]
            if not possible_steps: continue
            
            # Randomly pick a step for this ghost wafer to be in
            current_step = random.choice(possible_steps)
            ghost_wafer_id = f"GHOST-{tool_name}"
            
            # Start a process for this ghost wafer from its current step
            env.process(wafer_process(env, tool, ghost_wafer_id, unit_flow, {}, ghost_wafer_ends, start_at_step=current_step))


if __name__ == "__main__":
    with open('metal_tool_by_unit.json', 'r') as f:
        config = json.load(f)['tin_tool_config']
    
    unit_capbility = config['unit_capbility']
    unit_flow = config['unit_flow']

    status = {
        "LOADPORT": [
            {"LOT_ID": "LOT.01", "WAITING_NUM": 8, "STATUS": "PROCESSING"}, 
            {"LOT_ID": "LOT.02", "WAITING_NUM": 10, "STATUS": "WAITING"},
        ],
        "ATR.01": "occupied", "ALIGNER.01": "occupied", 
        "LA": "available", "LB": "available",
        "VTR01.01": "occupied", "VTR01.02": "occupied", 
        "CHA": "occupied", "CHB": "occupied", "CHC": "available", "CHD": "available", 
        "VTR02.01": "occupied", "VTR02.02": "occupied", 
        "CH2": "occupied", "CH3": "occupied", "CH4": "available", "CH5": "occupied",
    }
    
    env = simpy.Environment()
    tool = MetalTool(env, unit_capbility, status)

    # Create ghost wafers for occupied tools
    initialize_factory_state(env, tool, status, unit_flow)

    # Start processes for lots already at the loadport
    for lot_info in status["LOADPORT"]:
        env.process(lot_process(env, tool, lot_info["LOT_ID"], lot_info["WAITING_NUM"], unit_flow))
        
    incoming_lot_id = "INCOMING_LOT"
    incoming_lot_size = 10
    
    print("--- Starting Simulation ---")
    incoming_lot_process = env.process(lot_process(env, tool, incoming_lot_id, incoming_lot_size, unit_flow))
    
    env.run(until=incoming_lot_process)
    print("--- Simulation Finished ---")
