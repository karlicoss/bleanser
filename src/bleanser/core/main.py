from __future__ import annotations

from . import cli
from .processor import (
    BaseNormaliser,
)


def main(*, Normaliser: type[BaseNormaliser]) -> None:
    cli.main(obj={'normaliser': Normaliser})
