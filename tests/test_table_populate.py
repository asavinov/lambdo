import unittest

from lambdo.Workflow import *

class TablePopulateTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_read_csv(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "function": "pandas:read_csv",
                    "inputs": [],
                    "model": {
                        "filepath_or_buffer": "./tests/test1.csv",
                        "nrows": 4
                    }
                }
            ]
        }

        wf = Workflow(wf_json)

        wf.execute()

        tb = wf.tables[0].data

        self.assertEqual(len(tb.columns), 3)
        self.assertEqual(len(tb), 4)

    def test_extend(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Base Table",
                    "operation": "noop",

                    "columns": [
                        {
                            "id": "B",
                            "operation": "calculate",
                            "function": "lambda x: x + 1",
                            "inputs": ["A"]
                        }
                    ]
                },
                {
                    "id": "Extended Table",
                    # "operation": "extend" - by default

                    "columns": [
                        {
                            "id": "C",
                            "operation": "calculate",
                            "function": "lambda x: x + 1",
                            "inputs": ["B"]
                        }
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        tp = Topology(wf)

        layers = tp.layers
        # Layers:
        # 0 "Base Table"
        # 1 "B"
        # 2 "Extended Table"
        # 3 "C"

        self.assertEqual(len(layers), 4)

        # Base tables
        df = pd.DataFrame({'A': [1.0, 2.0, 3.0]})
        base_tb = wf.tables[0]
        base_tb.data = df

        wf.execute()

        ext_tb = wf.tables[1]
        self.assertEqual(len(ext_tb.data), 3)
        self.assertEqual(len(ext_tb.data.columns), 3)

        ext_column = ext_tb.data["C"]
        self.assertEqual(ext_column[0], 3.0)
        self.assertEqual(ext_column[1], 4.0)
        self.assertEqual(ext_column[2], 5.0)


if __name__ == '__main__':
    unittest.main()
