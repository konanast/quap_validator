# progress_bar.py
import sys
import time


class ProgressBar:
    def __init__(self, total: int, prefix: str = "", length: int = 40):
        self.start = time.time()
        self.total = total
        self.prefix = prefix
        self.length = length
        self.current = 0

    def update(self, step: int = 1, message: str = ""):
        """Advance the progress bar by `step`."""
        self.current += step
        progress = self.current / self.total
        filled = int(self.length * progress)
        bar = "█" * filled + "-" * (self.length - filled)
        elapsed = time.time() - self.start
        sys.stdout.write(
            f"\r{self.prefix} |{bar}| {self.current}/{self.total} "
            f"{message:<20} ({elapsed:5.1f}s)"
        )
        sys.stdout.flush()
        if self.current >= self.total:
            self.finish()

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()


def spinner(msg: str, duration: float = 1.5, delay: float = 0.1):
    """Optional simple spinner for quick feedback."""
    for c in "|/-\\":
        sys.stdout.write(f"\r{msg} {c}")
        sys.stdout.flush()
        time.sleep(delay)
    sys.stdout.write("\r" + " " * (len(msg) + 2) + "\r")
    sys.stdout.flush()
