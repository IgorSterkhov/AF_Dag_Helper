import unittest

from analyzer.sql_analyzer import SQLAnalyzer


def refs(result):
    return {
        "inlets": {item.full_name for item in result.inlets},
        "outlets": {item.full_name for item in result.outlets},
        "dicts": {item.full_name for item in result.dictionaries},
    }


class SQLAnalyzerRegressionTest(unittest.TestCase):
    def test_mixed_insert_and_partition_alter_keeps_both_lineage_steps(self):
        sql = """
        INSERT INTO buffer.target
        SELECT *
          FROM datamart.source;

        ALTER TABLE datamart.final
        ATTACH PARTITION '202406'
        FROM buffer.target
        """

        combined = refs(SQLAnalyzer().analyze(sql))

        self.assertEqual(combined["inlets"], {"datamart.source", "buffer.target"})
        self.assertEqual(combined["outlets"], {"buffer.target", "datamart.final"})

    def test_partition_alter_on_cluster_extracts_source_and_target(self):
        sql = """
        ALTER TABLE datamart.final ON CLUSTER cluster_a
        ATTACH PARTITION '202406'
        FROM buffer.target
        """

        combined = refs(SQLAnalyzer().analyze(sql))

        self.assertEqual(combined["inlets"], {"buffer.target"})
        self.assertEqual(combined["outlets"], {"datamart.final"})


if __name__ == "__main__":
    unittest.main()
