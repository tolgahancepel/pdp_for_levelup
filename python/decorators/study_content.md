# Decorators: Comprehensive Study Material for Data Engineers

## Table of Contents
1. [Foundation Concepts](#foundation-concepts)
2. [Decorator Mechanics](#decorator-mechanics)
3. [Advanced Patterns](#advanced-patterns)
4. [Chaining Decorators](#chaining-decorators)
5. [Real-World Data Engineering Use Cases](#real-world-data-engineering-use-cases)
6. [Anti-Patterns & When to Avoid](#anti-patterns--when-to-avoid)
7. [Performance Considerations](#performance-considerations)

---

## Foundation Concepts

### What Are Decorators?

A decorator is a **higher-order function** that takes a function as input, modifies its behavior, and returns a new function. In Python, decorators are syntactic sugar for function wrapping.

```python
# Without decorator syntax
def my_function():
    pass

my_function = decorator(my_function)

# With decorator syntax (equivalent)
@decorator
def my_function():
    pass
```

### Why Decorators Matter in Data Engineering

Data engineering pipelines require:
- **Cross-cutting concerns**: Logging, monitoring, error handling across multiple functions
- **Reusable patterns**: Retry logic, caching, authentication
- **Clean separation**: Business logic vs. infrastructure concerns
- **Maintainability**: Single source of truth for common behaviors

---

## Decorator Mechanics

### Simple Decorator Pattern

```python
import functools
from typing import Callable, Any

def simple_decorator(func: Callable) -> Callable:
    """Basic decorator structure"""
    @functools.wraps(func)  # Preserves original function metadata
    def wrapper(*args, **kwargs) -> Any:
        print(f"Before calling {func.__name__}")
        result = func(*args, **kwargs)
        print(f"After calling {func.__name__}")
        return result
    return wrapper

@simple_decorator
def extract_data(source: str) -> dict:
    return {"source": source, "rows": 1000}

# Output:
# Before calling extract_data
# After calling extract_data
```

**Critical Point**: Always use `@functools.wraps()` to preserve:
- `__name__`: Function name
- `__doc__`: Docstring
- `__module__`: Module information
- `__annotations__`: Type hints

### Decorators with Arguments

```python
def decorator_with_args(prefix: str) -> Callable:
    """Decorator factory that accepts arguments"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            print(f"{prefix}: Executing {func.__name__}")
            return func(*args, **kwargs)
        return wrapper
    return decorator

@decorator_with_args(prefix="[ETL]")
def load_data(table: str) -> None:
    print(f"Loading {table}")

load_data("users")
# Output:
# [ETL]: Executing load_data
# Loading users
```

---

## Advanced Patterns

### Class-Based Decorators

```python
class TimingDecorator:
    """Decorator as a class for stateful behavior"""
    
    def __init__(self, func: Callable):
        self.func = func
        self.call_count = 0
        functools.update_wrapper(self, func)
    
    def __call__(self, *args, **kwargs) -> Any:
        import time
        self.call_count += 1
        start = time.time()
        result = self.func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"Call #{self.call_count}: {elapsed:.4f}s")
        return result

@TimingDecorator
def query_database(query: str) -> list:
    import time
    time.sleep(0.1)
    return [1, 2, 3]

query_database("SELECT * FROM events")
query_database("SELECT * FROM events")
# Output:
# Call #1: 0.1005s
# Call #2: 0.1003s
```

**Advantage**: Maintains state across multiple calls (call count, statistics, etc.)

### Decorators with Optional Arguments

```python
from typing import Optional

def flexible_decorator(func: Optional[Callable] = None, *, 
                       enabled: bool = True) -> Callable:
    """Decorator that works with or without arguments"""
    
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Any:
            if enabled:
                print(f"[ENABLED] Running {f.__name__}")
            return f(*args, **kwargs)
        return wrapper
    
    # Called as @flexible_decorator or @flexible_decorator(enabled=False)
    if func is None:
        return decorator
    else:
        return decorator(func)

@flexible_decorator
def process_batch_1():
    pass

@flexible_decorator(enabled=False)
def process_batch_2():
    pass
```

---

## Chaining Decorators

### Understanding Decorator Order

```python
def decorator_a(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print("A: Before")
        result = func(*args, **kwargs)
        print("A: After")
        return result
    return wrapper

def decorator_b(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print("B: Before")
        result = func(*args, **kwargs)
        print("B: After")
        return result
    return wrapper

# Stacking decorators
@decorator_a
@decorator_b
def process():
    print("Processing")

process()
# Output:
# A: Before
# B: Before
# Processing
# B: After
# A: After
```

**Key Insight**: Decorators execute **bottom-to-top** during definition, but **outside-to-inside** during execution.

```
@decorator_a          # Applied second (outermost)
@decorator_b          # Applied first (innermost)
def process():
    pass

# Equivalent to:
process = decorator_a(decorator_b(process))

# Execution flow:
# decorator_a wrapper → decorator_b wrapper → original function
```

### Practical Chaining: Data Pipeline Example

```python
import functools
import time
from typing import Callable, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_execution(func: Callable) -> Callable:
    """Log function execution"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        logger.info(f"Starting: {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Completed: {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Failed: {func.__name__} - {str(e)}")
            raise
    return wrapper

def retry(max_attempts: int = 3, backoff: float = 1.0) -> Callable:
    """Retry with exponential backoff"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(f"Max retries exceeded for {func.__name__}")
                        raise
                    wait_time = backoff ** attempt
                    logger.warning(f"Retry {attempt}/{max_attempts} after {wait_time}s")
                    time.sleep(wait_time)
        return wrapper
    return decorator

def validate_input(func: Callable) -> Callable:
    """Validate input parameters"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        if not kwargs.get('source'):
            raise ValueError("'source' parameter is required")
        logger.info(f"Input validated for {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

# Chaining order matters!
@log_execution           # Outermost: catches all logs
@retry(max_attempts=3)   # Middle: handles retries
@validate_input          # Innermost: validates first
def extract_from_api(source: str, endpoint: str) -> dict:
    """Extract data from external API"""
    logger.info(f"Fetching from {source}/{endpoint}")
    return {"data": "sample", "rows": 100}

# Usage
result = extract_from_api(source="api.example.com", endpoint="/users")
```

**Execution Order Analysis**:
1. `log_execution` wrapper starts
2. `retry` wrapper starts
3. `validate_input` wrapper starts
4. Original function executes
5. `validate_input` wrapper ends
6. `retry` wrapper ends (handles retries if needed)
7. `log_execution` wrapper ends (logs completion)

### Chaining Considerations

```python
# ❌ WRONG ORDER - Validation happens after retry
@retry(max_attempts=3)
@validate_input
def bad_order(source: str):
    pass

# ✅ CORRECT ORDER - Validation happens before retry
@retry(max_attempts=3)
@validate_input
def good_order(source: str):
    pass

# ❌ WRONG ORDER - Logging doesn't capture retry attempts
@validate_input
@retry(max_attempts=3)
@log_execution
def poor_logging(source: str):
    pass

# ✅ CORRECT ORDER - Logging captures everything
@log_execution
@retry(max_attempts=3)
@validate_input
def good_logging(source: str):
    pass
```

---

## Real-World Data Engineering Use Cases

### Use Case 1: Data Quality Validation Pipeline

```python
from typing import Callable, Any, Dict
import pandas as pd
from datetime import datetime

def validate_schema(expected_columns: list) -> Callable:
    """Ensure DataFrame has required columns"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> pd.DataFrame:
            df = func(*args, **kwargs)
            missing = set(expected_columns) - set(df.columns)
            if missing:
                raise ValueError(f"Missing columns: {missing}")
            return df
        return wrapper
    return decorator

def check_null_percentage(threshold: float = 0.1) -> Callable:
    """Fail if null percentage exceeds threshold"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> pd.DataFrame:
            df = func(*args, **kwargs)
            null_pct = df.isnull().sum() / len(df)
            violations = null_pct[null_pct > threshold]
            if not violations.empty:
                raise ValueError(f"Null threshold exceeded: {violations.to_dict()}")
            return df
        return wrapper
    return decorator

def log_data_profile(func: Callable) -> Callable:
    """Log data shape and types"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> pd.DataFrame:
        df = func(*args, **kwargs)
        logger.info(f"Shape: {df.shape}, Dtypes: {df.dtypes.to_dict()}")
        return df
    return wrapper

@log_data_profile
@check_null_percentage(threshold=0.05)
@validate_schema(expected_columns=['user_id', 'event_type', 'timestamp'])
def load_events() -> pd.DataFrame:
    """Load events from database"""
    return pd.DataFrame({
        'user_id': [1, 2, 3],
        'event_type': ['click', 'view', 'purchase'],
        'timestamp': [datetime.now()] * 3
    })

# Usage
events_df = load_events()
```

### Use Case 2: Caching for Expensive Operations

```python
from functools import lru_cache
from typing import Callable, Any
import hashlib
import json

class CacheDecorator:
    """Custom cache with TTL and size limits"""
    
    def __init__(self, ttl_seconds: int = 3600, max_size: int = 128):
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self.cache = {}
        self.timestamps = {}
    
    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Create cache key from arguments
            key = self._make_key(func.__name__, args, kwargs)
            
            # Check if cached and not expired
            if key in self.cache:
                age = time.time() - self.timestamps[key]
                if age < self.ttl_seconds:
                    logger.info(f"Cache hit for {func.__name__}")
                    return self.cache[key]
                else:
                    del self.cache[key]
                    del self.timestamps[key]
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            
            # Enforce size limit
            if len(self.cache) >= self.max_size:
                oldest_key = min(self.timestamps, key=self.timestamps.get)
                del self.cache[oldest_key]
                del self.timestamps[oldest_key]
            
            self.cache[key] = result
            self.timestamps[key] = time.time()
            return result
        
        return wrapper
    
    @staticmethod
    def _make_key(func_name: str, args: tuple, kwargs: dict) -> str:
        """Create hashable cache key"""
        key_data = f"{func_name}:{args}:{sorted(kwargs.items())}"
        return hashlib.md5(key_data.encode()).hexdigest()

# Usage
@CacheDecorator(ttl_seconds=300, max_size=50)
def query_expensive_metric(metric_id: int, date: str) -> float:
    """Simulate expensive database query"""
    logger.info(f"Computing metric {metric_id} for {date}")
    time.sleep(2)  # Simulate computation
    return 42.5

# First call: executes function
result1 = query_expensive_metric(1, "2024-01-01")  # 2 seconds

# Second call: returns cached result
result2 = query_expensive_metric(1, "2024-01-01")  # Instant
```

### Use Case 3: Monitoring and Alerting

```python
from dataclasses import dataclass
from enum import Enum

class AlertLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

@dataclass
class MetricThresholds:
    execution_time_ms: float = 5000
    error_rate: float = 0.05
    row_count_min: int = 0

def monitor_metrics(thresholds: MetricThresholds) -> Callable:
    """Monitor function performance and data quality"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            errors = 0
            
            try:
                result = func(*args, **kwargs)
                
                # Check execution time
                elapsed_ms = (time.time() - start_time) * 1000
                if elapsed_ms > thresholds.execution_time_ms:
                    logger.warning(
                        f"Slow execution: {func.__name__} took {elapsed_ms:.0f}ms"
                    )
                
                # Check result if it's a DataFrame
                if isinstance(result, pd.DataFrame):
                    if len(result) < thresholds.row_count_min:
                        logger.warning(
                            f"Low row count: {func.__name__} returned {len(result)} rows"
                        )
                
                return result
                
            except Exception as e:
                logger.error(f"Function failed: {func.__name__} - {str(e)}")
                raise
        
        return wrapper
    return decorator

@monitor_metrics(MetricThresholds(
    execution_time_ms=1000,
    row_count_min=100
))
def fetch_user_events() -> pd.DataFrame:
    """Fetch user events from warehouse"""
    return pd.DataFrame({'user_id': range(500), 'event': ['click'] * 500})

# Usage
events = fetch_user_events()
```

### Use Case 4: Database Connection Management

```python
from contextlib import contextmanager
from typing import Generator

def with_db_connection(db_config: dict) -> Callable:
    """Manage database connections automatically"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            connection = None
            try:
                # Simulate connection creation
                connection = f"Connection to {db_config['host']}"
                logger.info(f"Connected to {db_config['host']}")
                
                # Pass connection as first argument
                result = func(connection, *args, **kwargs)
                return result
                
            except Exception as e:
                logger.error(f"Database error: {str(e)}")
                raise
            finally:
                if connection:
                    logger.info("Connection closed")
        
        return wrapper
    return decorator

@with_db_connection(db_config={'host': 'warehouse.internal'})
def extract_incremental(connection: str, table: str, last_id: int) -> list:
    """Extract new records since last_id"""
    logger.info(f"Querying {table} from {connection} where id > {last_id}")
    return [{'id': i, 'data': f'row_{i}'} for i in range(last_id + 1, last_id + 100)]

# Usage
new_records = extract_incremental(table='events', last_id=1000)
```

### Use Case 5: Distributed Task Tracking

```python
import uuid
from typing import Dict, List

class TaskTracker:
    """Track distributed task execution"""
    
    def __init__(self):
        self.tasks: Dict[str, dict] = {}
    
    def track_task(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            task_id = str(uuid.uuid4())
            self.tasks[task_id] = {
                'function': func.__name__,
                'status': 'running',
                'start_time': time.time(),
                'args': str(args)[:100],  # Truncate for logging
            }
            
            try:
                result = func(*args, **kwargs)
                self.tasks[task_id]['status'] = 'completed'
                self.tasks[task_id]['end_time'] = time.time()
                logger.info(f"Task {task_id} completed")
                return result
            except Exception as e:
                self.tasks[task_id]['status'] = 'failed'
                self.tasks[task_id]['error'] = str(e)
                logger.error(f"Task {task_id} failed: {str(e)}")
                raise
        
        return wrapper
    
    def get_status(self) -> Dict:
        """Get all task statuses"""
        return self.tasks

# Usage
tracker = TaskTracker()

@tracker.track_task
def process_partition(partition_id: int) -> int:
    """Process a data partition"""
    time.sleep(1)
    return partition_id * 1000

# Execute tasks
for i in range(3):
    process_partition(i)

# Check status
print(tracker.get_status())
```

---

## Anti-Patterns & When to Avoid

### Anti-Pattern 1: Over-Decoration

```python
# ❌ TOO MANY DECORATORS - Difficult to debug and maintain
@decorator_a
@decorator_b
@decorator_c
@decorator_d
@decorator_e
@decorator_f
def simple_function(x):
    return x * 2

# ✅ BETTER - Combine related concerns
def combined_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Logging + validation + monitoring in one place
        logger.info(f"Starting {func.__name__}")
        validate_args(*args, **kwargs)
        result = func(*args, **kwargs)
        monitor_result(result)
        return result
    return wrapper

@combined_decorator
def simple_function(x):
    return x * 2
```

**When to Avoid**: More than 3-4 decorators on a single function becomes a code smell.

### Anti-Pattern 2: Decorators Hiding Critical Logic

```python
# ❌ BAD - Critical error handling hidden in decorator
def silent_fail_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            return None  # Silent failure!
    return wrapper

@silent_fail_decorator
def load_critical_data():
    """This should NEVER silently fail"""
    pass

# ✅ GOOD - Explicit error handling
def load_critical_data():
    """Explicit error handling in function"""
    try:
        # load data
        pass
    except SpecificError as e:
        logger.error(f"Failed to load: {e}")
        raise  # Re-raise for caller to handle
```

**When to Avoid**: Decorators that hide exceptions or critical failures. Errors should be explicit and visible.

### Anti-Pattern 3: Decorators with Side Effects

```python
# ❌ BAD - Decorator modifies global state
global_cache = {}

def bad_caching_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        key = str(args)
        if key in global_cache:
            return global_cache[key]
        result = func(*args, **kwargs)
        global_cache[key] = result  # Global mutation!
        return result
    return wrapper

# ✅ GOOD - Encapsulated state
class BetterCache:
    def __init__(self):
        self.cache = {}
    
    def decorator(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args)
            if key in self.cache:
                return self.cache[key]
            result = func(*args, **kwargs)
            self.cache[key] = result
            return result
        return wrapper

cache = BetterCache()

@cache.decorator
def expensive_function(x):
    return x ** 2
```

**When to Avoid**: Decorators that modify global state or have unpredictable side effects.

### Anti-Pattern 4: Decorators Breaking Type Hints

```python
# ❌ BAD - Type hints lost
def bad_decorator(func):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@bad_decorator
def typed_function(x: int, y: str) -> float:
    return float(x)

# IDE can't infer types anymore!

# ✅ GOOD - Preserve type hints
from typing import TypeVar, Callable

F = TypeVar('F', bound=Callable)

def good_decorator(func: F) -> F:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@good_decorator
def typed_function(x: int, y: str) -> float:
    return float(x)

# Type hints preserved!
```

**When to Avoid**: Decorators that don't use `@functools.wraps()` or proper type hints.

### Anti-Pattern 5: Decorators on Async Functions (Without Proper Handling)

```python
import asyncio

# ❌ BAD - Decorator doesn't handle async
def sync_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print("Before")
        result = func(*args, **kwargs)
        print("After")
        return result
    return wrapper

@sync_decorator
async def async_function():
    await asyncio.sleep(1)
    return "done"

# This returns a coroutine object, not the result!

# ✅ GOOD - Async-aware decorator
def async_decorator(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        print("Before")
        result = await func(*args, **kwargs)
        print("After")
        return result
    return wrapper

@async_decorator
async def async_function():
    await asyncio.sleep(1)
    return "done"

# Properly awaits the coroutine
```

**When to Avoid**: Using sync decorators on async functions without proper async/await handling.

### Anti-Pattern 6: Performance-Critical Paths

```python
# ❌ BAD - Decorator overhead in tight loop
def logging_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Calling {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

@logging_decorator
def process_row(row):
    """Called millions of times"""
    return row * 2

# This adds logging overhead to every single call!

# ✅ GOOD - Conditional logging or batch operations
def conditional_logging_decorator(log_every_n: int = 1000):
    def decorator(func):
        call_count = [0]  # Use list to allow modification in nested function
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] % log_every_n == 0:
                logger.info(f"Processed {call_count[0]} calls to {func.__name__}")
            return func(*args, **kwargs)
        return wrapper
    return decorator

@conditional_logging_decorator(log_every_n=10000)
def process_row(row):
    """Called millions of times"""
    return row * 2
```

**When to Avoid**: Decorators in performance-critical code paths without considering overhead.

### Decision Matrix: When to Use Decorators

| Scenario | Use Decorator? | Alternative |
|----------|---|---|
| Cross-cutting concern (logging, monitoring) | ✅ Yes | Middleware, aspect-oriented programming |
| Reusable validation logic | ✅ Yes | Validator classes, context managers |
| Caching expensive operations | ✅ Yes | Explicit cache management |
| Authentication/authorization | ✅ Yes | Middleware, guards |
| Simple one-off logic | ❌ No | Direct implementation |
| Performance-critical tight loops | ❌ No | Inline optimization |
| Complex conditional logic | ❌ No | Explicit functions |
| Hiding errors | ❌ No | Explicit error handling |
| Modifying function signature | ⚠️ Maybe | Wrapper class, composition |

---

## Performance Considerations

### Measuring Decorator Overhead

```python
import timeit
from typing import Callable

def measure_decorator_overhead(func: Callable, iterations: int = 100000):
    """Measure performance impact of decorators"""
    
    # Baseline: no decorator
    baseline_time = timeit.timeit(
        lambda: func(5),
        number=iterations
    )
    
    return baseline_time

# Test function
def add(x):
    return x + 1

# With decorator
def timing_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@timing_decorator
def add_decorated(x):
    return x + 1

# Benchmark
baseline = measure_decorator_overhead(add, 100000)
decorated = measure_decorator_overhead(add_decorated, 100000)

overhead_pct = ((decorated - baseline) / baseline) * 100
print(f"Decorator overhead: {overhead_pct:.2f}%")
# Typical result: 15-25% overhead per decorator
```

### Optimization Strategies

```python
# Strategy 1: Use __slots__ for class-based decorators
class OptimizedDecorator:
    __slots__ = ['func', 'cache']
    
    def __init__(self, func):
        self.func = func
        self.cache = {}
    
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

# Strategy 2: Lazy decorator application
def lazy_decorator(func):
    """Only apply decorator if needed"""
    if should_apply_decorator():
        return actual_decorator(func)
    return func

# Strategy 3: Batch operations to reduce decorator calls
def batch_process(items, batch_size=1000):
    """Process in batches to reduce decorator overhead"""
    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        process_batch(batch)

# Strategy 4: Use functools.lru_cache for pure functions
@functools.lru_cache(maxsize=128)
def expensive_pure_function(x: int) -> int:
    """Built-in caching for pure functions"""
    return x ** 2
```

---

## Summary & Best Practices

### When to Use Decorators ✅

1. **Cross-cutting concerns**: Logging, monitoring, metrics
2. **Reusable patterns**: Retry logic, caching, validation
3. **Infrastructure concerns**: Authentication, rate limiting, connection management
4. **Consistent behavior**: Ensuring all functions follow same pattern
5. **Separation of concerns**: Keeping business logic clean

### When to Avoid Decorators ❌

1. **Performance-critical paths**: Tight loops processing millions of items
2. **Complex conditional logic**: Better as explicit functions
3. **Error hiding**: Exceptions should be explicit
4. **Single-use logic**: Not worth the abstraction
5. **Async/sync mixing**: Without proper async handling

### Best Practices

```python
# 1. Always use @functools.wraps
@functools.wraps(func)

# 2. Preserve type hints
from typing import TypeVar, Callable
F = TypeVar('F', bound=Callable)

# 3. Document decorator behavior
def my_decorator(func: Callable) -> Callable:
    """
    Decorator that does X.
    
    Args:
        func: Function to decorate
    
    Returns:
        Wrapped function with X behavior
    
    Raises:
        ValueError: If condition Y
    """
    pass

# 4. Keep decorators focused
# One decorator = one responsibility

# 5. Order matters in chains
# @outer_concern
# @middle_concern
# @inner_concern
# def function(): pass

# 6. Test decorators independently
def test_my_decorator():
    @my_decorator
    def dummy():
        return 42
    
    assert dummy() == 42

# 7. Consider performance impact
# Measure overhead in critical paths
```

---

## Conclusion

Decorators are powerful tools for data engineers to implement cross-cutting concerns cleanly and maintainably. However, they should be used judiciously:

- **Master chaining** to understand execution order and control flow
- **Know your use cases**: Logging, caching, validation, monitoring
- **Avoid anti-patterns**: Over-decoration, hidden errors, global state
- **Consider performance**: Measure overhead in critical paths
- **Keep it simple**: When in doubt, use explicit code

The key is finding the right balance between abstraction and clarity.