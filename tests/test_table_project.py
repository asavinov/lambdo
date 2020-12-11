import unittest

from lambdo.Workflow import *

class TableProjectTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_project(self):

        #
        # Project one column
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Source",
                    "attributes": ["A"],
                    "columns": [
                    ]
                },
                {
                    "id": "Destination",
                    "operation": "project",

                    "source_table": "Source",
                    "inputs": ["A"],  # Source columns to be projected. If not specified then all columns will be used.
                    "outputs": ["B"],  # New names in the target table. If not specified then the same names will be used.
                    "attributes": [],  # Alternatively, we could declare attributes as names of the target columns

                    "columns": [
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        # Source tables
        df = pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0]})
        facts_tb = wf.tables[0]
        facts_tb.data = df

        wf.execute()

        proj_tb = wf.tables[1]
        self.assertEqual(len(proj_tb.data), 2)  # Number of unique records in the source tables
        self.assertEqual(len(proj_tb.data.columns), 1)  # Number of input columns

        out_column = proj_tb.data["B"]
        self.assertEqual(out_column[0], 'a')
        self.assertEqual(out_column[1], 'b')


if __name__ == '__main__':
    unittest.main()
