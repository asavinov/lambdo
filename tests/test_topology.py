import unittest

from lambdo.Workflow import *
from lambdo.Topology import *

class TopologyTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_calculate(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {
                            "id": "C",
                            "operation": "calculate",
                            "inputs": ["A", "B"]
                        }
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        tp = Topology(wf)

        layers = tp.layers

        self.assertEqual(len(layers), 2)
        self.assertEqual(len(layers[0]), 1)
        self.assertEqual(len(layers[1]), 1)

    def test_link(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Facts",
                    "columns": [
                        {
                            "id": "A",
                            "operation": "calculate",
                            "inputs": ["B", "C"]
                        },
                        {
                            "id": "Link",
                            "operation": "link",

                            "keys": ["A"],

                            "linked_table": "Groups",
                            "linked_keys": ["A"]
                        }
                    ]
                },
                {
                    "id": "Groups",
                    "columns": [
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        tp = Topology(wf)

        layers = tp.layers

        self.assertEqual(len(layers), 3)
        self.assertEqual(len(layers[0]), 2)
        self.assertEqual(len(layers[1]), 1)
        self.assertEqual(len(layers[2]), 1)

        self.assertEqual(layers[1][0].id, 'A')
        self.assertEqual(layers[2][0].id, 'Link')

    def test_aggregate(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Facts",
                    "columns": [
                        {
                            "id": "A",
                            "operation": "calculate",
                            "inputs": ["B", "C"]
                        },
                        {
                            "id": "Link",
                            "operation": "link",

                            "keys": ["A"],

                            "linked_table": "Groups",
                            "linked_keys": ["A"]
                        }
                    ]
                },
                {
                    "id": "Groups",
                    "columns": [
                        {
                            "id": "Aggregate",
                            "operation": "aggregate",

                            "fact_table": "Facts",
                            "group_column": "Link",

                            "function": "my_module:my_function",
                            "inputs": ["A"]
                        }
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        tp = Topology(wf)

        layers = tp.layers

        self.assertEqual(len(layers), 4)
        self.assertEqual(len(layers[0]), 2)
        self.assertEqual(len(layers[1]), 1)
        self.assertEqual(len(layers[2]), 1)
        self.assertEqual(len(layers[3]), 1)

        self.assertEqual(layers[1][0].id, 'A')
        self.assertEqual(layers[2][0].id, 'Link')
        self.assertEqual(layers[3][0].id, 'Aggregate')


if __name__ == '__main__':
    unittest.main()
