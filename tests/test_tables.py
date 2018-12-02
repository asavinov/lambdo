import unittest

from lambdo.Workflow import *

class TablesTestCase(unittest.TestCase):

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

    def test_dropna(self):

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "row_filter": {"dropna": True}
                }
            ]
        }

        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [np.nan, 2, 3], 'B': [np.nan, 5, np.nan]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 2)
        self.assertEqual(len(tb.data), 1)

    def test_predicate(self):
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "row_filter": {"predicate": ["B", "C"]}
                }
            ]
        }

        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3], 'B': [True, True, False], 'C': [True, False, False]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 1)  # Predicate columns will be removed by default
        self.assertEqual(len(tb.data), 1)

    def test_sample(self):
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "row_filter": {"sample": {"frac": 0.6}}
                }
            ]
        }

        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 1)  # Predicate columns will be removed by default
        self.assertEqual(len(tb.data), 2)

    def test_slice(self):
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "row_filter": {"slice": {"start": 1, "end": 4, "step": 2}}
                }
            ]
        }

        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3, 4, 5, 6]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 1)  # Predicate columns will be removed by default
        self.assertEqual(len(tb.data), 2)

        self.assertEqual(tb.data["A"][0], 2)
        self.assertEqual(tb.data["A"][1], 4)

    def test_exclude(self):
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {"id": "A"},
                        {"id": "B", "exclude": True},
                        {"id": "C", "exclude": True}
                    ]
                }
            ]
        }

        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3], 'B': [True, True, False], 'C': [True, False, False]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 1)
        self.assertEqual(len(tb.data), 3)

    def test_column_filter(self):
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "column_filter": {"exclude": ["B", "C"]}
                }
            ]
        }
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 1)
        self.assertEqual(len(tb.data), 3)

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "column_filter": ["A", "B"]
                }
            ]
        }
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 2)
        self.assertEqual(len(tb.data), 3)


if __name__ == '__main__':
    unittest.main()
