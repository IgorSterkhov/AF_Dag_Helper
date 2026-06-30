import unittest

from analyzer.models import DataFlowGroup, EntityType, OMEntityItem
from generator.fqn_builder import FQNBuilder
from generator.omentity_generator import OMEntityGenerator


def table(fqn: str) -> OMEntityItem:
    return OMEntityItem(EntityType.TABLE, fqn)


class OMEntityFlowMergingTest(unittest.TestCase):
    def test_sequential_sql_merge_does_not_use_later_writes_as_earlier_sources(self):
        generator = OMEntityGenerator(FQNBuilder())
        flows = [
            DataFlowGroup(
                flow_type="sql",
                inlets=[table("server.schema.b")],
                outlets=[table("server.schema.c")],
            ),
            DataFlowGroup(
                flow_type="sql",
                inlets=[table("server.schema.a")],
                outlets=[table("server.schema.b")],
            ),
        ]

        merged = generator._merge_sequential_sql_flows(flows)

        self.assertEqual(
            [[item.fqn for item in flow.outlets] for flow in merged],
            [["server.schema.c"], ["server.schema.b"]],
        )

    def test_unrelated_source_only_flow_is_not_merged_into_cross_server_flow(self):
        generator = OMEntityGenerator(FQNBuilder())
        source_flows = [
            DataFlowGroup(
                flow_type="sql",
                inlets=[table("do-lake-r.logs.run_status")],
                outlets=[],
            )
        ]
        cross_server_flows = [
            DataFlowGroup(
                flow_type="cross_server",
                inlets=[table("do-lake-r.datamart.v3_by_srid_d")],
                outlets=[table("do-ch13.buffer.v3_by_subject_fo")],
            )
        ]

        remaining = generator._merge_source_only_flows_into_cross_server(
            source_flows,
            cross_server_flows,
        )

        self.assertEqual(remaining, source_flows)
        self.assertEqual(
            [item.fqn for item in cross_server_flows[0].inlets],
            ["do-lake-r.datamart.v3_by_srid_d"],
        )


if __name__ == "__main__":
    unittest.main()
