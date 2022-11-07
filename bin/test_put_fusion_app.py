import pytest
import put_fusion_app as p
from put_fusion_app import OBJ_TYPES


class TestGetFusionApp:
    def test_get_suffix(self):
        # readable example
        assert p.get_suffix("fusionApps") == "_APP.json"

        # test all the keys in OBJ_TYPES
        for key in OBJ_TYPES:
            suf = OBJ_TYPES[key]["ext"]
            assert p.get_suffix(key) == f"_{suf}.json"
