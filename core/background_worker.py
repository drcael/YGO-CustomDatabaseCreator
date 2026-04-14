import time
import threading

class WorkerThread(threading.Thread):
    """
    A generic worker thread to handle background tasks without freezing the UI.
    """
    def __init__(self, task_func, on_progress=None, on_complete=None, on_error=None):
        super().__init__()
        self.task_func = task_func
        self.on_progress = on_progress
        self.on_complete = on_complete
        self.on_error = on_error
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set() # Set implies 'not paused'

    def run(self):
        try:
            # We pass self to the task function so it can check for pause/stop events
            result = self.task_func(self)
            if self.on_complete:
                self.on_complete(result)
        except Exception as e:
            if self.on_error:
                self.on_error(e)

    def stop(self):
        self._stop_event.set()
        # If paused, resume to allow it to stop
        self.resume()

    def pause(self):
        self._pause_event.clear()

    def resume(self):
        self._pause_event.set()
        
    def is_stopped(self):
        return self._stop_event.is_set()
        
    def check_pause(self):
        # Block until the event is set
        self._pause_event.wait()
