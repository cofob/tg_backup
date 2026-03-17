from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Sequence
from pathlib import Path


class ProgressTracker[T]:
    def __init__(
        self,
        sequence: Sequence[T],
        *,
        tracking_file: Path,
        item_name: str = "item",
        logger: logging.Logger | None = None,
    ) -> None:
        self.sequence = sequence
        self.tracking_file = tracking_file
        self.item_name = item_name
        self.logger = logger

    def load_current_porgress(self) -> int:
        tracking_file_directory = self.tracking_file.absolute().parent
        if not tracking_file_directory.exists():
            raise ValueError(f"Tracking file directory does not exists: {tracking_file_directory}")

        try:
            with self.tracking_file.open("r") as f:
                current_index = json.load(f)
                if not isinstance(current_index, int):
                    raise TypeError(
                        f"Tracking file content invalid: expected int, got {type(current_index).__name__}."
                    )
        except FileNotFoundError:
            current_index = 0

        return current_index

    def __iter__(self) -> Iterator[T]:
        current_index = self.load_current_porgress()

        if current_index > 0 and self.logger:
            self.logger.info("Continue from %s %s of %s", self.item_name, current_index, len(self.sequence))

        for index, element in enumerate(self.sequence, start=1):
            if index <= current_index:
                continue

            if self.logger:
                self.logger.info("Processing %s %s of %s", self.item_name, index, len(self.sequence))

            yield element

            with self.tracking_file.open("w") as f:
                json.dump(index, f)
