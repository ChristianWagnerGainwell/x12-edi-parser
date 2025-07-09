import time
from typing import Callable, Any


def time_execution(func: Callable, *args, **kwargs) -> float:
    """
    A simple function that measures the execution time of another function.
    
    Args:
        func: The function to be timed
        *args: Positional arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The execution time in seconds as a float
        
    Example:
        def slow_function():
            time.sleep(2)
            return "Done"
            
        execution_time = time_execution(slow_function)
        print(f"Function took {execution_time:.4f} seconds to execute")
    """
    start_time = time.time()
    func(*args, **kwargs)
    end_time = time.time()
    return end_time - start_time 