# not functional yet, just to collect tests I want to move from the dev notebooks

import pytest
from feedstock.bigquery_interface import IIDEntry

class TestInputChecks:
    def test_iid_validation(self):
        with pytest.raises(ValueError, match='IID does not conform to CMIP6'):
            IIDEntry(iid='too_short', store='test')

class TestEndToEnd:
    # can I create a bq table as part of a fixture and then delete it?
    # - Test that I can create a table
    # - Test writing to the table
    # - Test reading from the table
    # - Test iid exist
