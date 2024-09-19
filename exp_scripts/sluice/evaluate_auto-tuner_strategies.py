import numpy as np
import matplotlib.pyplot as plt


# Simulate different monotonic increasing P99 latency behaviors under different workload patterns
def simulate_p99_latency(bar, workload_type, workload_variability):
    if workload_type == "exponential_growth":
        base_latency = 100 * np.exp(0.002 * bar)  # Exponential growth
    elif workload_type == "logarithmic_growth":
        base_latency = 50 * np.log(bar + 1) + 100  # Logarithmic growth
    elif workload_type == "inverse_decay":
        base_latency = 1000 / (2000 - bar + 1) + 200  # Inverse decay as bar increases
    elif workload_type == "linear_growth":
        base_latency = 100 + 0.5 * bar  # Linear growth
    else:
        base_latency = 100 + 0.5 * bar  # Default to linear growth

    noise = np.random.normal(0, workload_variability)
    return max(0, base_latency + noise)


# Auto tuner with different initial bar choosing strategies
class AutoTuner:
    def __init__(self, strategy, user_limit, initial_strategy, workload_type, workload_variability):
        self.strategy = strategy
        self.user_limit = user_limit
        self.bar = self.choose_initial_bar(initial_strategy, workload_type, workload_variability)
        self.history = []
        self.converged = False
        self.time_to_converge = 0
        self.time_to_meet_limit = None
        self.meet_limit = False

    def choose_initial_bar(self, initial_strategy, workload_type, workload_variability):
        if initial_strategy == "fixed_high_start":
            return self.user_limit + 100  # Start with a higher than user limit
        elif initial_strategy == "proportional_to_user_limit":
            return self.user_limit * 1.2  # Proportional start
        elif initial_strategy == "random_start":
            return np.random.uniform(0.5 * self.user_limit, 1.5 * self.user_limit)
        elif initial_strategy == "historical_analysis":
            # For simplicity, use a rough estimate of the workload's impact on bar
            return simulate_p99_latency(500, workload_type,
                                        workload_variability)  # Assume historical data points to 500
        elif initial_strategy == "workload_based_estimation":
            # Conduct a simple trial to get an initial estimate
            trial_bar = 200
            trial_latency = simulate_p99_latency(trial_bar, workload_type, workload_variability)
            return trial_bar if trial_latency < self.user_limit else self.user_limit + 100  # Simple heuristic
        else:
            return self.user_limit + 100  # Default to a conservative start

    def step(self, p99_latency):
        if self.strategy == "conservative_gradient_descent":
            step_size = 0.5 * (p99_latency - self.user_limit) + 25
            if p99_latency > self.user_limit:
                self.bar = max(0, self.bar - step_size)
            else:
                self.bar += 0.1 * 500  # Assuming safety margin is 500ms

        elif self.strategy == "exponential_decay":
            decay_factor = 0.9
            if p99_latency > self.user_limit:
                self.bar = max(0, decay_factor * self.bar)
            else:
                self.bar += 0.1 * 500

        elif self.strategy == "pid_controller":
            # Simple P-only controller for demonstration
            Kp = 0.1
            error = p99_latency - self.user_limit
            self.bar = max(0, self.bar - Kp * error)

        elif self.strategy == "binary_search":
            low, high = 0, 2000  # Assume initial range for bar
            if p99_latency > self.user_limit:
                high = self.bar
            else:
                low = self.bar
            self.bar = (low + high) / 2

        elif self.strategy == "adaptive_gradient_descent_with_momentum":
            # Simulate momentum with a moving average of changes
            step_size = 0.1 * (p99_latency - self.user_limit)
            momentum = 0.9  # Decay factor for momentum
            if not hasattr(self, 'velocity'):
                self.velocity = 0
            self.velocity = momentum * self.velocity - step_size
            self.bar = max(0, self.bar + self.velocity)

        self.history.append(self.bar)

        # Check if P99 latency < user limit
        if p99_latency <= self.user_limit and not self.meet_limit:
            self.meet_limit = True
            self.time_to_meet_limit = len(self.history)

        # Check convergence
        if not self.converged and self.has_converged():
            self.converged = True
            self.time_to_converge = len(self.history)

    def has_converged(self, threshold=10):
        if len(self.history) < 10:
            return False
        # Check if the last 10 values have converged within a threshold
        recent_bars = self.history[-10:]
        return np.std(recent_bars) < threshold


# Simulation parameters
user_latency_limit = 500
workload_variability = 50  # Variability in the workload affecting P99 latency

# Define different workload types
workload_types = [
    "exponential_growth",
    "logarithmic_growth",
    "inverse_decay",
    "linear_growth"
]

# Initial bar choosing strategies
initial_strategies = [
    "fixed_high_start",
    "proportional_to_user_limit",
    "random_start",
    "historical_analysis",
    "workload_based_estimation"
]

# Tuning strategies
tuning_strategies = [
    "conservative_gradient_descent",
    "exponential_decay",
    "pid_controller",
    "binary_search",
    "adaptive_gradient_descent_with_momentum"
]

# Evaluate each strategy
results = []
optimal_bar = 400  # Assume optimal bar for evaluation purposes

for workload_type in workload_types:
    print(f"Workload type: {workload_type}")
    for initial_strategy in initial_strategies:
        for tuning_strategy in tuning_strategies:
            tuner = AutoTuner(tuning_strategy, user_latency_limit, initial_strategy, workload_type,
                              workload_variability)
            print(f"  Initial Strategy: {initial_strategy} | Tuning Strategy: {tuning_strategy}")
            for _ in range(100):  # Run for a certain number of steps
                p99_latency = simulate_p99_latency(tuner.bar, workload_type, workload_variability)
                tuner.step(p99_latency)
                if tuner.converged:
                    break

            distance_from_optimal = abs(tuner.bar - optimal_bar)
            results.append({
                'workload_type': workload_type,
                'initial_strategy': initial_strategy,
                'tuning_strategy': tuning_strategy,
                'time_to_converge': tuner.time_to_converge,
                'time_to_meet_limit': tuner.time_to_meet_limit,
                'final_bar': tuner.bar,
                'distance_from_optimal': distance_from_optimal,
                'kept_under_limit': all(lat <= user_latency_limit for lat in
                                        tuner.history[tuner.time_to_meet_limit:]) if tuner.time_to_meet_limit else False
            })
            print(
                f"    Time to converge: {tuner.time_to_converge}, Time to meet limit: {tuner.time_to_meet_limit}, Distance from optimal: {distance_from_optimal:.2f}, Kept under limit: {results[-1]['kept_under_limit']}")

# Display results
import pandas as pd

results_df = pd.DataFrame(results)
print(results_df)
