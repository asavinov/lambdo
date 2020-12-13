import unittest

from lambdo.Workflow import *

class LinkTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_one_key(self):

        #
        # One key to another table
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Table 1",
                    "columns": [
                        {
                            "id": "My Link",
                            "operation": "link",

                            "keys": ["A"],

                            "linked_table": "Table 2",
                            "linked_keys": ["B"]
                        }
                    ]
                },
                {
                    "id": "Table 2",
                    "operation": "noop",
                    "columns": [
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        # Main table
        df = pd.DataFrame({'A': ['a', 'a', 'b', 'b']})
        main_tb = wf.tables[0]
        main_tb.data = df

        # Secondary table (more data than used in the main table)
        df = pd.DataFrame({'B': ['a', 'b', 'c'], 'C': [1, 2, 3]})
        sec_tb = wf.tables[1]
        sec_tb.data = df

        wf.execute()

        merged_tb = wf.tables[0]
        self.assertEqual(len(merged_tb.data), 4)  # Same number of rows
        self.assertEqual(len(merged_tb.data.columns), 2)

        link_column = main_tb.data['My Link']
        self.assertEqual(link_column[0], 0)
        self.assertEqual(link_column[1], 0)
        self.assertEqual(link_column[2], 1)
        self.assertEqual(link_column[3], 1)

    def test_two_keys(self):

        #
        # One key to another table
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Table 1",
                    "columns": [
                        {
                            "id": "My Link",
                            "operation": "link",

                            "keys": ["A", "B"],

                            "linked_table": "Table 2",
                            "linked_keys": ["A", "B"]
                        }
                    ]
                },
                {
                    "id": "Table 2",
                    "operation": "noop",
                    "columns": [
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        # Main table
        df = pd.DataFrame({'A': ['a', 'b', 'b', 'a'], 'B': ['b', 'c', 'c', 'a']})
        main_tb = wf.tables[0]
        main_tb.data = df

        # Secondary table (more data than used in the main table)
        df = pd.DataFrame({'A': ['a', 'b', 'a'], 'B': ['b', 'c', 'c'], 'C': [1, 2, 3]})
        sec_tb = wf.tables[1]
        sec_tb.data = df

        wf.execute()

        merged_tb = wf.tables[0]
        self.assertEqual(len(merged_tb.data), 4)  # Same number of rows
        self.assertEqual(len(merged_tb.data.columns), 3)

        link_column = main_tb.data['My Link']
        self.assertEqual(link_column[0], 0)
        self.assertEqual(link_column[1], 1)
        self.assertEqual(link_column[2], 1)
        self.assertTrue(pd.isna(link_column[3]))


if __name__ == '__main__':
    unittest.main()
