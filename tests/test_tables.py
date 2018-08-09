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
                        "filepath_or_buffer": "../tests/test1.csv",
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
        data = {'A': [1, 2, np.nan], 'B': [4, np.nan, np.nan]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        self.assertEqual(len(tb.data.columns), 2)
        self.assertEqual(len(tb.data), 1)


if __name__ == '__main__':
    unittest.main()
