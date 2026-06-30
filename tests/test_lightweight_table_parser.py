import unittest

from analyzer.lightweight_table_parser import extract_sql_references


class LightweightTableParserTest(unittest.TestCase):
    def test_extracts_from_and_join_tables(self):
        refs = extract_sql_references(
            "SELECT * FROM db.table1 JOIN db.table2 ON table1.id = table2.id"
        )

        self.assertEqual(refs.tables, ["db.table1", "db.table2"])
        self.assertEqual(refs.dicts, [])

    def test_extracts_insert_target_and_select_source(self):
        refs = extract_sql_references(
            "INSERT INTO schema1.target_table SELECT * FROM schema2.source"
        )

        self.assertEqual(refs.tables, ["schema1.target_table", "schema2.source"])
        self.assertEqual(refs.dicts, [])

    def test_extracts_dictget_dictionary_separately_from_table(self):
        refs = extract_sql_references(
            "SELECT dictGet('db.my_dict', 'col', id) FROM db.some_table"
        )

        self.assertEqual(refs.tables, ["db.some_table"])
        self.assertEqual(refs.dicts, ["db.my_dict"])

    def test_extracts_typed_dictget_family_once(self):
        refs = extract_sql_references(
            """
            SELECT dictGetInt32OrDefault('dict.product_cards_nm_short', 'seller_id', nm_id, 0),
                   dictGetUInt16OrDefault('dict.product_cards_nm_short', 'subject_id', nm_id, 0)
              FROM datamart.v3_by_srid_d AS src FINAL
            """
        )

        self.assertEqual(refs.tables, ["datamart.v3_by_srid_d"])
        self.assertEqual(refs.dicts, ["dict.product_cards_nm_short"])

    def test_extracts_truncate_table(self):
        refs = extract_sql_references("TRUNCATE TABLE db.old_table")

        self.assertEqual(refs.tables, ["db.old_table"])
        self.assertEqual(refs.dicts, [])

    def test_ignores_python_import_lines(self):
        refs = extract_sql_references(
            '''
            from module import something
            import another_module
            sql = "SELECT * FROM db.real_table"
            '''
        )

        self.assertEqual(refs.tables, ["db.real_table"])
        self.assertEqual(refs.dicts, [])

    def test_extracts_alter_attach_partition_source_and_target(self):
        refs = extract_sql_references(
            """
            ALTER TABLE datamart.v3_by_subject_fo
            ATTACH PARTITION '202406'
            FROM buffer.v3_by_subject_fo
            """
        )

        self.assertEqual(
            refs.tables,
            ["buffer.v3_by_subject_fo", "datamart.v3_by_subject_fo"],
        )
        self.assertEqual(refs.dicts, [])


if __name__ == "__main__":
    unittest.main()
