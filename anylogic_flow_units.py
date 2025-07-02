import simpy
import random
from typing import Any, Callable, Optional, List, Dict, Union
from collections import deque, defaultdict
from dataclasses import dataclass, field
from enum import Enum
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Agent:
    """Represents an agent (entity) flowing through the system"""
    id: str
    creation_time: float
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not hasattr(self, 'current_time'):
            self.current_time = self.creation_time

class FlowUnit:
    """Base class for all flow units"""
    
    def __init__(self, env: simpy.Environment, name: str = None):
        self.env = env
        self.name = name or f"{self.__class__.__name__}_{id(self)}"
        self.input_port = None
        self.output_port = None
        self.agents_entered = 0
        self.agents_exited = 0
        self.statistics = defaultdict(list)
        
    def connect_to(self, next_unit: 'FlowUnit'):
        """Connect this unit to the next unit in the flow"""
        self.output_port = next_unit
        next_unit.input_port = self
        
    def send_agent(self, agent: Agent):
        """Send agent to the next unit"""
        if self.output_port:
            self.env.process(self.output_port.receive_agent(agent))
        else:
            logger.warning(f"Agent {agent.id} reached end of flow at {self.name}")
    
    def receive_agent(self, agent: Agent):
        """Receive agent from previous unit - to be implemented by subclasses"""
        raise NotImplementedError
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get unit statistics"""
        return {
            'name': self.name,
            'agents_entered': self.agents_entered,
            'agents_exited': self.agents_exited,
            'current_population': self.agents_entered - self.agents_exited
        }

class Source(FlowUnit):
    """Generates agents at specified intervals"""
    
    def __init__(self, env: simpy.Environment, name: str = None, 
                 arrival_rate: float = 1.0, max_arrivals: int = None,
                 agent_factory: Callable = None):
        super().__init__(env, name)
        self.arrival_rate = arrival_rate  # agents per time unit
        self.max_arrivals = max_arrivals
        self.agent_factory = agent_factory or self._default_agent_factory
        self.generated_count = 0
        
    def _default_agent_factory(self) -> Agent:
        """Default agent factory"""
        return Agent(
            id=f"Agent_{self.generated_count}",
            creation_time=self.env.now
        )
    
    def start_generation(self):
        """Start generating agents"""
        return self.env.process(self._generation_process())
    
    def _generation_process(self):
        """Process that generates agents"""
        while True:
            if self.max_arrivals and self.generated_count >= self.max_arrivals:
                break
                
            # Generate inter-arrival time (exponential distribution)
            inter_arrival_time = random.expovariate(self.arrival_rate)
            yield self.env.timeout(inter_arrival_time)
            
            # Create and send new agent
            agent = self.agent_factory()
            self.generated_count += 1
            self.agents_exited += 1
            
            logger.info(f"Source {self.name} generated {agent.id} at time {self.env.now}")
            self.send_agent(agent)
    
    def receive_agent(self, agent: Agent):
        """Sources don't receive agents"""
        pass

class Queue(FlowUnit):
    """Queue flow unit - stores agents with optional capacity limit"""
    
    def __init__(self, env: simpy.Environment, name: str = None, 
                 capacity: int = float('inf'), priority_func: Callable = None):
        super().__init__(env, name)
        self.capacity = capacity
        self.priority_func = priority_func
        self.queue = deque() if not priority_func else []
        self.waiting_agents = []
        
    def receive_agent(self, agent: Agent):
        """Receive and queue an agent"""
        self.agents_entered += 1
        agent.current_time = self.env.now
        
        if len(self.queue) < self.capacity:
            if self.priority_func:
                # Priority queue - insert in sorted order
                priority = self.priority_func(agent)
                inserted = False
                for i, (stored_agent, stored_priority) in enumerate(self.queue):
                    if priority < stored_priority:  # Lower number = higher priority
                        self.queue.insert(i, (agent, priority))
                        inserted = True
                        break
                if not inserted:
                    self.queue.append((agent, priority))
            else:
                # FIFO queue
                self.queue.append(agent)
            
            logger.info(f"Queue {self.name} received {agent.id} at time {self.env.now}")
            
            # Try to immediately forward if output is available
            yield from self._try_forward_agent()
        else:
            logger.warning(f"Queue {self.name} at capacity, rejecting {agent.id}")
    
    def _try_forward_agent(self):
        """Try to forward the next agent in queue"""
        if self.queue and self.output_port:
            if self.priority_func:
                agent, _ = self.queue.pop(0)
            else:
                agent = self.queue.popleft()
            
            self.agents_exited += 1
            logger.info(f"Queue {self.name} forwarding {agent.id} at time {self.env.now}")
            self.send_agent(agent)
        yield self.env.timeout(0)  # Make this a generator
    
    def get_queue_length(self) -> int:
        """Get current queue length"""
        return len(self.queue)
    
    def get_statistics(self) -> Dict[str, Any]:
        stats = super().get_statistics()
        stats.update({
            'current_queue_length': self.get_queue_length(),
            'capacity': self.capacity,
            'utilization': self.get_queue_length() / self.capacity if self.capacity != float('inf') else 0
        })
        return stats

class Delay(FlowUnit):
    """Delay flow unit - holds agents for a specified duration"""
    
    def __init__(self, env: simpy.Environment, name: str = None,
                 delay_time: Union[float, Callable] = 1.0, capacity: int = float('inf')):
        super().__init__(env, name)
        self.delay_time = delay_time
        self.capacity = capacity
        self.processing_agents = []
        
    def receive_agent(self, agent: Agent):
        """Receive agent and start delay process"""
        if len(self.processing_agents) >= self.capacity:
            logger.warning(f"Delay {self.name} at capacity, rejecting {agent.id}")
            return
            
        self.agents_entered += 1
        agent.current_time = self.env.now
        self.processing_agents.append(agent)
        
        logger.info(f"Delay {self.name} received {agent.id} at time {self.env.now}")
        
        # Start delay process
        yield from self._delay_process(agent)
    
    def _delay_process(self, agent: Agent):
        """Process that delays the agent"""
        # Calculate delay time
        if callable(self.delay_time):
            delay = self.delay_time()
        else:
            delay = self.delay_time
            
        # Wait for the delay
        yield self.env.timeout(delay)
        
        # Remove from processing and forward
        self.processing_agents.remove(agent)
        self.agents_exited += 1
        
        logger.info(f"Delay {self.name} completed processing {agent.id} at time {self.env.now}")
        self.send_agent(agent)
    
    def get_statistics(self) -> Dict[str, Any]:
        stats = super().get_statistics()
        stats.update({
            'agents_in_process': len(self.processing_agents),
            'capacity': self.capacity,
            'utilization': len(self.processing_agents) / self.capacity if self.capacity != float('inf') else 0
        })
        return stats

class ResourcePool(FlowUnit):
    """Resource pool - agents seize resources, get processed, then release"""
    
    def __init__(self, env: simpy.Environment, name: str = None,
                 capacity: int = 1, service_time: Union[float, Callable] = 1.0,
                 priority_func: Callable = None):
        super().__init__(env, name)
        self.resource = simpy.PriorityResource(env, capacity=capacity) if priority_func else simpy.Resource(env, capacity=capacity)
        self.service_time = service_time
        self.priority_func = priority_func
        self.capacity = capacity
        
    def receive_agent(self, agent: Agent):
        """Receive agent and start resource process"""
        self.agents_entered += 1
        agent.current_time = self.env.now
        
        logger.info(f"ResourcePool {self.name} received {agent.id} at time {self.env.now}")
        
        # Start resource seizure process
        yield from self._resource_process(agent)
    
    def _resource_process(self, agent: Agent):
        """Process that seizes resource, processes, and releases"""
        # Request resource
        if self.priority_func:
            priority = self.priority_func(agent)
            request = self.resource.request(priority=priority)
        else:
            request = self.resource.request()
        
        # Wait for resource
        yield request
        
        logger.info(f"ResourcePool {self.name} seized resource for {agent.id} at time {self.env.now}")
        
        # Calculate service time
        if callable(self.service_time):
            service = self.service_time()
        else:
            service = self.service_time
            
        # Process (service time)
        yield self.env.timeout(service)
        
        # Release resource
        self.resource.release(request)
        self.agents_exited += 1
        
        logger.info(f"ResourcePool {self.name} completed processing {agent.id} at time {self.env.now}")
        self.send_agent(agent)
    
    def get_statistics(self) -> Dict[str, Any]:
        stats = super().get_statistics()
        stats.update({
            'resource_capacity': self.capacity,
            'resource_utilization': (self.capacity - len(self.resource.users)) / self.capacity,
            'queue_length': len(self.resource.queue)
        })
        return stats

class Service(FlowUnit):
    """Service unit - combines queue and resource processing like AnyLogic Service block"""
    
    def __init__(self, env: simpy.Environment, name: str = None,
                 capacity: int = 1, service_time: Union[float, Callable] = 1.0,
                 queue_capacity: int = float('inf'), priority_func: Callable = None,
                 setup_time: Union[float, Callable] = 0.0, 
                 resource_schedule: Dict = None):
        super().__init__(env, name)
        self.capacity = capacity
        self.service_time = service_time
        self.setup_time = setup_time
        self.queue_capacity = queue_capacity
        self.priority_func = priority_func
        self.resource_schedule = resource_schedule or {}
        
        # Create resource and queue
        self.resource = simpy.PriorityResource(env, capacity=capacity) if priority_func else simpy.Resource(env, capacity=capacity)
        self.queue = deque() if not priority_func else []
        self.processing_agents = []
        self.rejected_agents = 0
        self.total_queue_time = 0
        self.total_service_time = 0
        
        # Statistics tracking
        self.queue_length_samples = []
        self.service_start_times = {}
        self.queue_start_times = {}
        
    def receive_agent(self, agent: Agent):
        """Receive agent and either queue or reject based on capacity"""
        self.agents_entered += 1
        agent.current_time = self.env.now
        
        # Check queue capacity
        if len(self.queue) >= self.queue_capacity:
            self.rejected_agents += 1
            logger.warning(f"Service {self.name} queue full, rejecting {agent.id} at time {self.env.now}")
            return
        
        # Add to queue
        self.queue_start_times[agent.id] = self.env.now
        if self.priority_func:
            priority = self.priority_func(agent)
            # Insert in priority order (lower number = higher priority)
            inserted = False
            for i, (stored_agent, stored_priority) in enumerate(self.queue):
                if priority < stored_priority:
                    self.queue.insert(i, (agent, priority))
                    inserted = True
                    break
            if not inserted:
                self.queue.append((agent, priority))
        else:
            self.queue.append(agent)
        
        logger.info(f"Service {self.name} queued {agent.id} at time {self.env.now}, queue length: {len(self.queue)}")
        
        # Start service process
        yield from self._service_process()
    
    def _service_process(self):
        """Main service process - handles queue and resource management"""
        while self.queue:
            # Get next agent from queue
            if self.priority_func:
                agent, _ = self.queue.pop(0)
            else:
                agent = self.queue.popleft()
            
            # Track queue time
            if agent.id in self.queue_start_times:
                queue_time = self.env.now - self.queue_start_times[agent.id]
                self.total_queue_time += queue_time
                del self.queue_start_times[agent.id]
            
            # Request resource
            request = self.resource.request()
            yield request
            
            # Agent starts service
            self.processing_agents.append(agent)
            self.service_start_times[agent.id] = self.env.now
            
            logger.info(f"Service {self.name} started processing {agent.id} at time {self.env.now}")
            
            # Setup time (if any)
            if callable(self.setup_time):
                setup = self.setup_time()
            else:
                setup = self.setup_time
            
            if setup > 0:
                yield self.env.timeout(setup)
            
            # Service time
            if callable(self.service_time):
                service = self.service_time()
            else:
                service = self.service_time
            
            yield self.env.timeout(service)
            
            # Complete service
            self.processing_agents.remove(agent)
            if agent.id in self.service_start_times:
                service_duration = self.env.now - self.service_start_times[agent.id]
                self.total_service_time += service_duration
                del self.service_start_times[agent.id]
            
            # Release resource
            self.resource.release(request)
            self.agents_exited += 1
            
            logger.info(f"Service {self.name} completed processing {agent.id} at time {self.env.now}")
            self.send_agent(agent)
            
            # Break if no more agents in queue
            if not self.queue:
                break
    
    def get_current_utilization(self) -> float:
        """Get current resource utilization"""
        if self.capacity == 0:
            return 0.0
        return len(self.resource.users) / self.capacity
    
    def get_queue_length(self) -> int:
        """Get current queue length"""
        return len(self.queue)
    
    def is_resource_available(self) -> bool:
        """Check if resource is available"""
        return len(self.resource.users) < self.capacity
    
    def get_statistics(self) -> Dict[str, Any]:
        """Enhanced statistics for service unit"""
        stats = super().get_statistics()
        
        # Calculate averages
        avg_queue_time = self.total_queue_time / max(1, self.agents_entered - len(self.queue)) if self.agents_entered > 0 else 0
        avg_service_time = self.total_service_time / max(1, self.agents_exited) if self.agents_exited > 0 else 0
        
        stats.update({
            'resource_capacity': self.capacity,
            'current_utilization': self.get_current_utilization(),
            'queue_capacity': self.queue_capacity,
            'current_queue_length': self.get_queue_length(),
            'agents_in_service': len(self.processing_agents),
            'rejected_agents': self.rejected_agents,
            'average_queue_time': avg_queue_time,
            'average_service_time': avg_service_time,
            'total_queue_time': self.total_queue_time,
            'total_service_time': self.total_service_time,
            'resource_busy': not self.is_resource_available(),
            'throughput': self.agents_exited / max(0.001, self.env.now) if hasattr(self.env, 'now') and self.env.now > 0 else 0
        })
        return stats

class SelectOutput(FlowUnit):
    """Select Output - routes agents to different outputs based on condition"""
    
    def __init__(self, env: simpy.Environment, name: str = None,
                 condition_func: Callable = None, true_probability: float = 0.5):
        super().__init__(env, name)
        self.condition_func = condition_func or (lambda agent: random.random() < true_probability)
        self.true_output = None
        self.false_output = None
        
    def connect_true_output(self, unit: FlowUnit):
        """Connect the true branch output"""
        self.true_output = unit
        
    def connect_false_output(self, unit: FlowUnit):
        """Connect the false branch output"""
        self.false_output = unit
    
    def receive_agent(self, agent: Agent):
        """Receive agent and route based on condition"""
        self.agents_entered += 1
        agent.current_time = self.env.now
        
        # Evaluate condition
        if self.condition_func(agent):
            if self.true_output:
                logger.info(f"SelectOutput {self.name} routing {agent.id} to TRUE branch at time {self.env.now}")
                self.env.process(self.true_output.receive_agent(agent))
            else:
                logger.warning(f"SelectOutput {self.name} has no TRUE output for {agent.id}")
        else:
            if self.false_output:
                logger.info(f"SelectOutput {self.name} routing {agent.id} to FALSE branch at time {self.env.now}")
                self.env.process(self.false_output.receive_agent(agent))
            else:
                logger.warning(f"SelectOutput {self.name} has no FALSE output for {agent.id}")
        
        self.agents_exited += 1
        yield self.env.timeout(0)  # Immediate routing

class Combine(FlowUnit):
    """Combine - waits for agents from multiple inputs before forwarding"""
    
    def __init__(self, env: simpy.Environment, name: str = None, 
                 required_agents: int = 2, timeout: float = None):
        super().__init__(env, name)
        self.required_agents = required_agents
        self.timeout = timeout
        self.waiting_agents = []
        self.batch_count = 0
        
    def receive_agent(self, agent: Agent):
        """Receive agent and check if batch is complete"""
        self.agents_entered += 1
        agent.current_time = self.env.now
        self.waiting_agents.append(agent)
        
        logger.info(f"Combine {self.name} received {agent.id}, waiting agents: {len(self.waiting_agents)}")
        
        # Check if we have enough agents to form a batch
        if len(self.waiting_agents) >= self.required_agents:
            yield from self._process_batch()
        elif self.timeout:
            # Start timeout process if specified
            self.env.process(self._timeout_process())
            
        yield self.env.timeout(0)
    
    def _process_batch(self):
        """Process a complete batch"""
        batch = self.waiting_agents[:self.required_agents]
        self.waiting_agents = self.waiting_agents[self.required_agents:]
        self.batch_count += 1
        
        logger.info(f"Combine {self.name} processing batch {self.batch_count} with agents: {[a.id for a in batch]}")
        
        # Forward all agents in the batch
        for agent in batch:
            self.agents_exited += 1
            self.send_agent(agent)
        
        yield self.env.timeout(0)
    
    def _timeout_process(self):
        """Handle timeout for incomplete batches"""
        initial_count = len(self.waiting_agents)
        yield self.env.timeout(self.timeout)
        
        # If we still have the same agents waiting, process them
        if len(self.waiting_agents) == initial_count and self.waiting_agents:
            logger.info(f"Combine {self.name} timeout reached, processing incomplete batch")
            yield from self._process_batch()

class Sink(FlowUnit):
    """Sink - final destination for agents"""
    
    def __init__(self, env: simpy.Environment, name: str = None):
        super().__init__(env, name)
        self.completed_agents = []
        
    def receive_agent(self, agent: Agent):
        """Receive and terminate agent"""
        self.agents_entered += 1
        agent.current_time = self.env.now
        self.completed_agents.append(agent)
        
        logger.info(f"Sink {self.name} received {agent.id} at time {self.env.now}")
        yield self.env.timeout(0)
    
    def get_statistics(self) -> Dict[str, Any]:
        stats = super().get_statistics()
        if self.completed_agents:
            cycle_times = [agent.current_time - agent.creation_time for agent in self.completed_agents]
            stats.update({
                'total_completed': len(self.completed_agents),
                'average_cycle_time': sum(cycle_times) / len(cycle_times),
                'min_cycle_time': min(cycle_times),
                'max_cycle_time': max(cycle_times)
            })
        return stats

class FlowModel:
    """Container for managing flow units and running simulations"""
    
    def __init__(self, env: simpy.Environment):
        self.env = env
        self.flow_units = []
        self.sources = []
        
    def add_unit(self, unit: FlowUnit):
        """Add a flow unit to the model"""
        self.flow_units.append(unit)
        if isinstance(unit, Source):
            self.sources.append(unit)
    
    def run_simulation(self, duration: float):
        """Run the simulation"""
        # Start all sources
        for source in self.sources:
            source.start_generation()
        
        # Run simulation
        self.env.run(until=duration)
    
    def get_all_statistics(self) -> Dict[str, Any]:
        """Get statistics from all flow units"""
        return {unit.name: unit.get_statistics() for unit in self.flow_units}
    
    def print_statistics(self):
        """Print statistics for all flow units"""
        print("\n=== Flow Model Statistics ===")
        for unit_name, stats in self.get_all_statistics().items():
            print(f"\n{unit_name}:")
            for key, value in stats.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}") 