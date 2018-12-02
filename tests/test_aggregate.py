import unittest

from lambdo.Workflow import *

class AggregateTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_grouping(self):
        """
        Test only how records are grouped without aggregation.
        """

        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Fact Table"
                },
                {
                    "id": "Group Table",
                    "function": "lambdo.std:aggregate",
                    "inputs": ["Fact Table"],
                    "model": {
                        "keys": ["A"],
                        "aggregate": []
                    }
                }
            ]
        }

        wf = Workflow(wf_json)

        # Fact table
        data = {'A': [0, 1, 0, 1], 'B': [1.0, 2.0, 3.0, 4.0]}
        df = pd.DataFrame(data)
        main_tb = wf.tables[0]
        main_tb.data = df

        wf.execute()

        df2 = wf.tables[1]

        self.assertEqual(len(df2.data.columns), 0)
        self.assertEqual(len(df2.data), 2)


if __name__ == '__main__':
    unittest.main()
