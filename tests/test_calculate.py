import unittest

from lambdo.Workflow import *

class CalculateTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_calculate(self):

        #
        # Row-based apply
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {
                            "id": "My column",
                            "function": "builtins:float",
                            "window": "one",
                            "inputs": ["A"],
                            "outputs": ["float(A)"]
                        }
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        wf.execute()

        v0 = tb.data['float(A)'][0]
        v1 = tb.data['float(A)'][1]
        v2 = tb.data['float(A)'][2]

        self.assertAlmostEqual(v0, 1.0)
        self.assertAlmostEqual(v1, 2.0)
        self.assertAlmostEqual(v2, 3.0)

        self.assertIsInstance(v0, float)
        self.assertIsInstance(v1, float)
        self.assertIsInstance(v2, float)

    def test_all(self):
        #
        # Shift one column: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.shift.html
        #
        wf_json = \
            {
                "id": "My workflow",
                "tables": [
                    {
                        "id": "My table",
                        "columns": [
                            {
                                "id": "My Column",
                                "function": "pandas.core.series:Series.shift",
                                "window": "all",
                                "inputs": ["A"],
                                "outputs": ["next(A)"],
                                "model": {"periods": -1}
                            }
                        ]
                    }
                ]
            }
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        wf.execute()

        self.assertAlmostEqual(tb.data['next(A)'][0], 2.0)
        self.assertAlmostEqual(tb.data['next(A)'][1], 3.0)
        self.assertTrue(pd.isna(tb.data['next(A)'][2]))


if __name__ == '__main__':
    unittest.main()
