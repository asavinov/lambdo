import unittest

from lambdo.Workflow import *

class ComposeTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_compose_simple(self):
        """Materialize a simple (two segments) column path as a new column of the table using an explicit definition"""
        
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Table 1",
                    "attributes": ["A"],
                    "columns": [
                        {
                            "id": "Compose",
                            "operation": "compose",
                            "inputs": ["Link", "B"]
                        },
                        {
                            "id": "Link",
                            "operation": "link",

                            "keys": ["A"],

                            "linked_table": "Table 2",
                            "linked_keys": ["A"]
                        }
                    ]
                },
                {
                    "id": "Table 2",
                    "operation": "noop",
                    "attributes": ["A", "B"],
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
        df = pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [1, 2, 3]})
        sec_tb = wf.tables[1]
        sec_tb.data = df

        tp = Topology(wf)
        tp.translate()

        layers = tp.layers
        # Layers:
        # 0 "Table 1" "Table 2"
        # 1 "Link"
        # 2 "Compose"
        self.assertEqual(len(layers), 3)
        self.assertEqual(len(layers[0]), 2)
        self.assertEqual(len(layers[1]), 1)
        self.assertEqual(len(layers[2]), 1)

        self.assertEqual(layers[1][0].id, 'Link')
        self.assertEqual(layers[2][0].id, 'Compose')

        wf.execute()

        # Complex column values:  [1, 1, 2, 2]
        compose_column = main_tb.data['Compose']
        self.assertEqual(compose_column[0], 1)
        self.assertEqual(compose_column[1], 1)
        self.assertEqual(compose_column[2], 2)
        self.assertEqual(compose_column[3], 2)

    def test_compose_complex(self):
        """Materialize a long (three segments) column path as a new column of the table using an explicit definition. A nested compose column has to be automatically created."""

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Table 1",
                    "attributes": ["A"],
                    "columns": [
                        {
                            "id": "Compose",
                            "operation": "compose",
                            "inputs": ["Link", "Link", "C"]
                        },
                        {
                            "id": "Link",
                            "operation": "link",

                            "keys": ["A"],

                            "linked_table": "Table 2",
                            "linked_keys": ["A"]
                        }
                    ]
                },
                {
                    "id": "Table 2",
                    "operation": "noop",
                    "attributes": ["A", "B"],
                    "columns": [
                        {
                            "id": "Link",
                            "operation": "link",

                            "keys": ["B"],

                            "linked_table": "Table 3",
                            "linked_keys": ["B"]
                        }
                    ]
                },
                {
                    "id": "Table 3",
                    "operation": "noop",
                    "attributes": ["B", "C"],
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
        df = pd.DataFrame({'A': ['a', 'b', 'c'], 'B': ['e', 'f', 'g']})
        sec_tb = wf.tables[1]
        sec_tb.data = df

        # Third table
        df = pd.DataFrame({'B': ['e', 'f'], 'C': [1, 2]})
        thd_tb = wf.tables[2]
        thd_tb.data = df

        # Complex column values: [1, 1, 2, 2]

        # Check if a nested compose column was created correctly in the second table: [1, 2, None]


if __name__ == '__main__':
    unittest.main()
