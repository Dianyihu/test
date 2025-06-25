import simpy
import random
from collections import namedtuple

# Data structure for a lot
Lot = namedtuple("Lot", "name num_wafers")
# Data structure for lot status
LotStatus = namedtuple("LotStatus", "lot wafers_waiting wafers_processing wafers_finished")


class MetalToolSimulator:
    """A simulator for a semiconductor metal tool."""

    def __init__(self, env, num_loadports, num_processing_slots, num_chambers, num_degas_chambers, process_time_dist, degas_time_dist):
        """Initialize the simulator with machine parameters."""
        self.env = env
        self.loadports = simpy.Resource(env, capacity=num_loadports)
        self.processing_slots = simpy.Resource(env, capacity=num_processing_slots)
        self.degas_chambers = simpy.Resource(env, capacity=num_degas_chambers)
        self.chambers = simpy.Resource(env, capacity=num_chambers)
        self.process_time_dist = process_time_dist
        self.degas_time_dist = degas_time_dist

    def _occupy_resource_on_init(self, resource, name):
        """A process to occupy a resource at the beginning of the simulation."""
        print(f"0.00: A {name} is initially occupied.")
        yield resource.request()

    def degas_wafer(self, lot_name, wafer_name):
        """Simulate a single wafer undergoing degas."""
        print(f"{self.env.now:7.2f}: {lot_name} - {wafer_name} requesting degas chamber.")
        with self.degas_chambers.request() as request:
            yield request
            print(f"{self.env.now:7.2f}: {lot_name} - {wafer_name} got degas chamber, starting degas.")
            degas_duration = max(0, self.degas_time_dist())
            yield self.env.timeout(degas_duration)
            print(f"{self.env.now:7.2f}: {lot_name} - {wafer_name} finished degas (took {degas_duration:.2f} min), releasing degas chamber.")

    def process_wafer(self, lot_name, wafer_name):
        """Simulate a single wafer processing in a chamber."""
        print(f"{self.env.now:7.2f}: {lot_name} - {wafer_name} requesting chamber.")
        with self.chambers.request() as request:
            yield request
            print(f"{self.env.now:7.2f}: {lot_name} - {wafer_name} got chamber, starting process.")
            process_duration = max(0, self.process_time_dist())
            yield self.env.timeout(process_duration)
            print(f"{self.env.now:7.2f}: {lot_name} - {wafer_name} finished process (took {process_duration:.2f} min), releasing chamber.")

    def process_wafer_full_path(self, lot_name, wafer_name):
        """Simulate the full path for a single wafer (degas and process)."""
        yield self.env.process(self.degas_wafer(lot_name, wafer_name))
        yield self.env.process(self.process_wafer(lot_name, wafer_name))

    def process_wip_lot(self, lot_status):
        """Simulate the process for a lot that is already work-in-progress."""
        lot = lot_status.lot
        print(f"0.00: {lot.name} is a WIP lot at the tool.")
        arrival_time = 0

        with self.loadports.request() as request:
            yield request
            track_in_time = 0
            print(f"0.00: {lot.name} is occupying a loadport (Track in).")

            with self.processing_slots.request() as ps_request:
                yield ps_request
                processing_slot_acquired_time = 0
                print(f"0.00: {lot.name} is occupying a processing slot, starting wafer processing.")

                wafer_processes = []
                # Start processes for wafers that are already 'processing'
                for i in range(lot_status.wafers_processing):
                    wafer_name = f"Wafer_{lot_status.wafers_finished + i + 1}"
                    wafer_processes.append(self.env.process(self.process_wafer(lot.name, wafer_name)))
                
                # Start processes for wafers that are 'waiting'
                for i in range(lot_status.wafers_waiting):
                    wafer_name = f"Wafer_{lot_status.wafers_finished + lot_status.wafers_processing + i + 1}"
                    wafer_processes.append(self.env.process(self.process_wafer_full_path(lot.name, wafer_name)))

                yield self.env.all_of(wafer_processes)
                
                process_end_time = self.env.now
                print(f"{self.env.now:7.2f}: {lot.name} finished all wafers (Process End), releasing processing slot.")

        print(f"{self.env.now:7.2f}: {lot.name} released loadport.")
        
        total_time = self.env.now - arrival_time
        print(f"--- {lot.name} Summary ---")
        print(f"  Total time in system: {total_time:.2f} minutes.")
        print(f"  Queueing for Loadport: {track_in_time - arrival_time:.2f} minutes.")
        print(f"  Queueing for Processing Slot: {processing_slot_acquired_time - track_in_time:.2f} minutes.")
        print(f"  Wafers Processing Time: {process_end_time - processing_slot_acquired_time:.2f} minutes.")
        print(f"----------------------\n")

    def process_lot(self, lot):
        """Simulate the entire process flow for a single lot."""
        print(f"{self.env.now:7.2f}: {lot.name} arrives at the tool (Step in).")
        arrival_time = self.env.now

        with self.loadports.request() as request:
            yield request
            track_in_time = self.env.now
            print(f"{self.env.now:7.2f}: {lot.name} acquired a loadport (Track in).")

            print(f"{self.env.now:7.2f}: {lot.name} waiting for a processing slot.")
            with self.processing_slots.request() as ps_request:
                yield ps_request
                processing_slot_acquired_time = self.env.now
                print(f"{self.env.now:7.2f}: {lot.name} acquired a processing slot, starting wafer processing.")

                # Process all wafers in the lot concurrently
                wafer_processes = [
                    self.env.process(self.process_wafer_full_path(lot.name, f"Wafer_{i+1}"))
                    for i in range(lot.num_wafers)
                ]
                
                yield self.env.all_of(wafer_processes)
                
                process_end_time = self.env.now
                print(f"{self.env.now:7.2f}: {lot.name} finished all wafers (Process End), releasing processing slot.")

        print(f"{self.env.now:7.2f}: {lot.name} released loadport.")
        
        total_time = self.env.now - arrival_time
        print(f"--- {lot.name} Summary ---")
        print(f"  Total time in system: {total_time:.2f} minutes.")
        print(f"  Queueing for Loadport: {track_in_time - arrival_time:.2f} minutes.")
        print(f"  Queueing for Processing Slot: {processing_slot_acquired_time - track_in_time:.2f} minutes.")
        print(f"  Wafers Processing Time: {process_end_time - processing_slot_acquired_time:.2f} minutes.")
        print(f"----------------------\n")


def run_simulation():
    """Set up and run the simulation."""
    # --- Configuration ---
    NUM_LOADPORTS = 3
    NUM_PROCESSING_SLOTS = 2
    NUM_CHAMBERS = 4
    NUM_DEGAS_CHAMBERS = 2

    mean_time = 5
    std_dev = 1
    process_time_dist = lambda: random.normalvariate(mean_time, std_dev)
    degas_time_dist = lambda: 2  # Constant 2 minutes for degas

    # WIP Information
    initial_lots = [
        LotStatus(lot=Lot(name="WIP_Lot_1", num_wafers=25), wafers_waiting=10, wafers_processing=5, wafers_finished=10),
        LotStatus(lot=Lot(name="WIP_Lot_2", num_wafers=15), wafers_waiting=15, wafers_processing=0, wafers_finished=0),
    ]
    occupied_process_chambers = 1
    occupied_degas_chambers = 1
    
    new_lot_to_estimate = Lot(name="NEW_LOT", num_wafers=20)
    # --- End Configuration ---

    print("--- Starting Semiconductor Metal Tool Simulation ---")
    print(f"Configuration: {NUM_LOADPORTS} Loadports ({NUM_PROCESSING_SLOTS} concurrently), {NUM_CHAMBERS} Chambers, {NUM_DEGAS_CHAMBERS} Degas Chambers, Custom Process Time Distribution\n")

    env = simpy.Environment()
    simulator = MetalToolSimulator(env, NUM_LOADPORTS, NUM_PROCESSING_SLOTS, NUM_CHAMBERS, NUM_DEGAS_CHAMBERS, process_time_dist, degas_time_dist)

    # Occupy chambers based on initial state
    for _ in range(occupied_process_chambers):
        env.process(simulator._occupy_resource_on_init(simulator.chambers, "Process Chamber"))
    for _ in range(occupied_degas_chambers):
        env.process(simulator._occupy_resource_on_init(simulator.degas_chambers, "Degas Chamber"))
        
    # Start processes for initial lots
    for lot_status in initial_lots:
        env.process(simulator.process_wip_lot(lot_status))
    
    # Start the process for the new lot slightly after to ensure it queues
    env.process(simulator.process_lot(new_lot_to_estimate))

    env.run()


if __name__ == "__main__":
    run_simulation() 