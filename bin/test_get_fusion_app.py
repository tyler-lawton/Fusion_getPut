import pytest
import get_fusion_app as g
from get_fusion_app import OBJ_TYPES


class TestGetFusionApp:
    def test_get_suffix(self):
        # readable example
        assert g.get_suffix("fusionApps") == "_APP.json"

        # test all the keys in OBJ_TYPES
        for key in OBJ_TYPES:
            suf = OBJ_TYPES[key]["ext"]
            assert g.get_suffix(key) == f"_{suf}.json"

    def test_apply_suffix(self):
        # readable example
        assert g.apply_suffix(f="some_file", suffix_type="fusionApps") == "some_file_APP.json"

        # test all the keys in OBJ_TYPES
        for key in OBJ_TYPES:
            suf = OBJ_TYPES[key]["ext"]
            # ie "some_file_APP.json for "fusionApps"
            assert g.apply_suffix(f="some_file", suffix_type=key) == f"some_file_{suf}.json"
