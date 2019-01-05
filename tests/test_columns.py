import unittest

from lambdo.Workflow import *

class ColumnsTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_single_columns(self):

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

        tb.populate()

        v0 = tb.data['float(A)'][0]
        v1 = tb.data['float(A)'][1]
        v2 = tb.data['float(A)'][2]

        self.assertAlmostEqual(v0, 1.0)
        self.assertAlmostEqual(v1, 2.0)
        self.assertAlmostEqual(v2, 3.0)

        self.assertIsInstance(v0, float)
        self.assertIsInstance(v1, float)
        self.assertIsInstance(v2, float)

        #
        # Rolling sum
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {
                            "id": "sum(A)",
                            "function": "numpy.core.fromnumeric:sum",
                            "window": "2",
                            "inputs": ["A"],
                            "model": {}
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

        tb.populate()

        v0 = tb.data['sum(A)'][0]
        v1 = tb.data['sum(A)'][1]
        v2 = tb.data['sum(A)'][2]

        self.assertTrue(pd.isna(v0))
        self.assertAlmostEqual(v1, 3.0)
        self.assertAlmostEqual(v2, 5.0)

    def test_family_columns(self):
        #
        # Same function and inputs but different windows
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {
                            "id": "sum(A)",
                            "function": "numpy.core.fromnumeric:sum",
                            "inputs": ["A"],
                            "extensions": [
                                {"window": "2"},
                                {"window": "3", "outputs": ["sum(A)_win3"]}
                            ]
                        }
                    ]
                }
            ]
        }

        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3, 4]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.populate()

        col0 = tb.data['sum(A)_0']
        col1 = tb.data['sum(A)_win3']

        self.assertAlmostEqual(col0[2], 5.0)
        self.assertAlmostEqual(col1[2], 6.0)

        self.assertAlmostEqual(col0[3], 7.0)
        self.assertAlmostEqual(col1[3], 9.0)

        #
        # Same input, different functions
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {
                            "id": "A",
                            "inputs": ["A"],
                            "window": "2",
                            "extensions": [
                                {"function": "numpy.core.fromnumeric:sum", "outputs": "A_sum"},
                                {"function": "numpy.core.fromnumeric:mean", "outputs": "A_mean"}
                            ]
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

        tb.populate()

        col0 = tb.data['A_sum']
        col1 = tb.data['A_mean']

        self.assertAlmostEqual(col0[1], 3.0)
        self.assertAlmostEqual(col0[2], 5.0)

        self.assertAlmostEqual(col1[1], 1.5)
        self.assertAlmostEqual(col1[2], 2.5)

    def test_standard_functions(self):

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

        tb.populate()

        self.assertAlmostEqual(tb.data['next(A)'][0], 2.0)
        self.assertAlmostEqual(tb.data['next(A)'][1], 3.0)
        self.assertTrue(pd.isna(tb.data['next(A)'][2]))

    def test_single_columns(self):

        #
        # Weighted rolling mean
        #
        wf_json = {
            "id": "My workflow",
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {
                            "id": "mean_w(A)",
                            "function": "lambdo.std:mean_weighted",
                            "window": "2",
                            "inputs": ["A","W"],
                            "model": {}
                        }
                    ]
                }
            ]
        }
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3], 'W': [3, 2, 1]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.populate()

        v0 = tb.data['mean_w(A)'][0]
        v1 = tb.data['mean_w(A)'][1]
        v2 = tb.data['mean_w(A)'][2]

        self.assertTrue(pd.isna(v0))
        self.assertAlmostEqual(v1, 1.4)
        self.assertAlmostEqual(v2, 2.33333333)

if __name__ == '__main__':
    unittest.main()
