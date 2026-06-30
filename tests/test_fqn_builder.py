import tempfile
import unittest
from pathlib import Path

from generator.fqn_builder import FQNBuilder


class FQNBuilderTest(unittest.TestCase):
    def test_save_mapping_preserves_strip_d_suffix_setting(self):
        with tempfile.TemporaryDirectory() as tmp:
            mapping_path = Path(tmp) / "server_mapping.yaml"
            mapping_path.write_text(
                """
server_mapping:
  click-do-lake-r: do-lake-r
default_behavior: passthrough
strip_d_suffix: false
""",
                encoding="utf-8",
            )

            builder = FQNBuilder(str(mapping_path))
            builder.add_mapping("click-do-ch13_sterhov", "do-ch13")
            builder.save_mapping(str(mapping_path))

            reloaded = FQNBuilder(str(mapping_path))

        self.assertFalse(reloaded.strip_d_suffix)
        self.assertEqual(
            reloaded.build_fqn("click-do-lake-r", "datamart", "v3_by_srid_d"),
            "do-lake-r.datamart.v3_by_srid_d",
        )


if __name__ == "__main__":
    unittest.main()
