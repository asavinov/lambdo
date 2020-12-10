import unittest

from lambdo.Workflow import *

class AggregateTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_aggregate(self):

        #
        # One key to another table
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "Facts",
                    "columns": [
                        {
                            "id": "Group Link",
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
                            "group_column": "Group Link",

                            # Computational (functional) definitions
                            "function": "numpy.core.fromnumeric:sum", # One input is expected
                            "inputs": ["M"],  # Select measure columns from the fact table: single or multiple
                            "model": {}, # Passed to the aggregation function as usual
                            #"outputs": ["M"]  # In the case, the function returns several results we need column ids

                            # Post-processing options
                            "fillna_value": 0.0,  # Replace NaN in the result, for instance, of an empty group has no fact, a function will never be called and the value will be NaN

                            # "function": "numpy.core.fromnumeric:size", # No need in inputs - how it works then? The function is a applied to a subset but this means there are parameters?
                        }
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        # Facts
        df = pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0]})
        facts_tb = wf.tables[0]
        facts_tb.data = df

        # Secondary table (more data than used in the main table)
        df = pd.DataFrame({'A': ['a', 'b', 'c']})
        groups_tb = wf.tables[1]
        groups_tb.data = df

        wf.execute()

        groups_tb = wf.tables[1]
        self.assertEqual(len(groups_tb.data), 3)  # Same number of rows
        self.assertEqual(len(groups_tb.data.columns), 3)  # One aggregate column was added (and one technical "id" column was added which might be removed in future)

        agg_column = groups_tb.data['Aggregate']
        self.assertEqual(agg_column[0], 3.0)
        self.assertEqual(agg_column[1], 7.0)
        self.assertEqual(agg_column[2], 0.0)


if __name__ == '__main__':
    unittest.main()
