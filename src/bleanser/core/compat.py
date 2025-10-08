from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    # NOTE: keeping for now for backwards compatibility (some user modules might have used these)
    from typing import Never, Self, assert_never, assert_type  # noqa: F401
