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
        # Layers:
        # 0 "My table"
        # 1 "C"

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
                    "operation": "noop",
                    "columns": [
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        tp = Topology(wf)

        layers = tp.layers
        # Layers:
        # 0 "Facts" "Groups"
        # 1 "A"
        # 2 "Link"

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
                    "operation": "noop",
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
        # Layers:
        # 0 "Facts" "Groups"
        # 1 "A"
        # 2 "Link"
        # 3 "Aggregate"

        self.assertEqual(len(layers), 4)
        self.assertEqual(len(layers[0]), 2)
        self.assertEqual(len(layers[1]), 1)
        self.assertEqual(len(layers[2]), 1)
        self.assertEqual(len(layers[3]), 1)

        self.assertEqual(layers[1][0].id, 'A')
        self.assertEqual(layers[2][0].id, 'Link')
        self.assertEqual(layers[3][0].id, 'Aggregate')

    def test_join(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Source 1",
                    # Noop population because of no definition and no parent to extend
                    "columns": [
                        {
                            "id": "A",
                            "operation": "calculate",
                            "inputs": ["B"]
                        }
                    ]
                },
                {
                    "id": "Source 2",
                    "operation": "noop",  # Do not populate (normally if data is provided from outside programmatically)
                    "columns": [
                        {
                            "id": "C",
                            "operation": "calculate",
                            "inputs": ["D"]
                        }
                    ]
                },
                {
                    "id": "Join",
                    "function": "lambdo.std:join",
                    "inputs": ["Source 1", "Source 2"],
                    "model": {"keys": ["A", "C"]},

                    "columns": [
                        {
                            "id": "E",
                            "operation": "calculate",
                            "inputs": ["F"]
                        }
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        tp = Topology(wf)

        layers = tp.layers
        # Layers:
        # "Source 1", "Source 2"
        # "A", "C"
        # "Join"
        # "E"

        self.assertEqual(len(layers), 4)

        self.assertEqual(layers[0][0].id, 'Source 1')
        self.assertEqual(layers[0][1].id, 'Source 2')

        self.assertEqual(layers[1][0].id, 'A')
        self.assertEqual(layers[1][1].id, 'C')

        self.assertEqual(layers[2][0].id, 'Join')

        self.assertEqual(layers[3][0].id, 'E')

    def test_extend(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Base Table",  # noop - no function, no parent
                    "columns": [
                    ]
                },
                {
                    "id": "Extended Table",  # extend - no function, there is parent
                    "columns": [
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        tp = Topology(wf)

        layers = tp.layers
        # Layers:
        # 0 "Table 1"
        # 1 "Table 2"
        self.assertEqual(len(layers), 2)

        self.assertEqual(layers[0][0].id, 'Base Table')
        self.assertEqual(layers[1][0].id, 'Extended Table')


if __name__ == '__main__':
    unittest.main()
