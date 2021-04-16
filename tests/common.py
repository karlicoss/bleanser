import os

import pytest

V = 'TEST_AS_KARLICOSS'

skip_if_not_karlicoss = pytest.mark.skipif(
    V not in os.environ, reason=f'test only works on @karlicoss data for now. Set env variable {V}=true to override.',
)
