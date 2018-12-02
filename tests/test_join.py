import unittest

from lambdo.Workflow import *

class TablesTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_join_by_key(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Main Table"
                },
                {
                    "id": "Second Table"
                },
                {
                    "id": "Merged Table",
                    "function": "lambdo.std:join",
                    "inputs": ["Main Table", "Second Table"],
                    "model": {"suffixes": ["", "_JOINED"]}
                }
            ]
        }

        wf = Workflow(wf_json)

        # Main table
        data = {'A': [0, 1, 2]}
        df = pd.DataFrame(data)
        main_tb = wf.tables[0]
        main_tb.data = df

        # Secondary table (more rows than in the main table)
        data = {'A': [3, 4, 5, 6, 7]}
        df = pd.DataFrame(data)
        sec_tb = wf.tables[1]
        sec_tb.data = df

        wf.execute()

        merged_tb = wf.tables[2]

        self.assertEqual(len(merged_tb.data.columns), 2)
        self.assertEqual(len(merged_tb.data), 3)
        self.assertEqual(merged_tb.data.columns[1], 'A_JOINED')

        # Secondary table (fewer rows than in the main table)
        data = {'B': [3, 4]}
        df = pd.DataFrame(data)
        sec_tb = wf.tables[1]
        sec_tb.data = df

        wf.execute()

        merged_tb = wf.tables[2]

        self.assertEqual(len(merged_tb.data.columns), 2)
        self.assertEqual(len(merged_tb.data), 3)

    def test_join_by_columns(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Main Table"
                },
                {
                    "id": "Second Table"
                },
                {
                    "id": "Merged Table",
                    "function": "lambdo.std:join",
                    "inputs": ["Main Table", "Second Table"],
                    "model": {"keys": ["A", "B"]}
                }
            ]
        }

        wf = Workflow(wf_json)

        # Main table
        data = {'A': ['a', 'a', 'b', 'b']}
        df = pd.DataFrame(data)
        main_tb = wf.tables[0]
        main_tb.data = df

        # Secondary table (more data than required by the main table)
        data = {'B': ['a', 'b', 'c'], 'C': [1, 2, 3]}
        df = pd.DataFrame(data)
        sec_tb = wf.tables[1]
        sec_tb.data = df

        wf.execute()

        merged_tb = wf.tables[2]

        self.assertEqual(len(merged_tb.data.columns), 2)
        self.assertEqual(len(merged_tb.data), 4)


if __name__ == '__main__':
    unittest.main()
