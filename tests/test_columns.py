import unittest

from lambdo.Workflow import *

class ColumnsTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_single_columns(self):

        #
        # Test 1 - row-based apply
        #
        with open('../tests/test2.jsonc', encoding='utf-8') as f:
            wf_json = json.loads(f.read())
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

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
        # Test 2 - rolling sum
        #
        with open('../tests/test3.jsonc', encoding='utf-8') as f:
            wf_json = json.loads(f.read())
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        v0 = tb.data['sum(A)'][0]
        v1 = tb.data['sum(A)'][1]
        v2 = tb.data['sum(A)'][2]

        self.assertTrue(pd.isna(v0))
        self.assertAlmostEqual(v1, 3.0)
        self.assertAlmostEqual(v2, 5.0)

    def test_family_columns(self):
        #
        # Test 3 - same function and inputs but different scopes (windows)
        #
        with open('../tests/test4.jsonc', encoding='utf-8') as f:
            wf_json = json.loads(f.read())
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3, 4]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        col0 = tb.data['sum(A)_0']
        col1 = tb.data['sum(A)_win3']

        self.assertAlmostEqual(col0[2], 5.0)
        self.assertAlmostEqual(col1[2], 6.0)

        self.assertAlmostEqual(col0[3], 7.0)
        self.assertAlmostEqual(col1[3], 9.0)

        #
        # Test 4 - same input, different functions
        #
        with open('../tests/test5.jsonc', encoding='utf-8') as f:
            wf_json = json.loads(f.read())
        wf = Workflow(wf_json)

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        tb.execute()

        col0 = tb.data['A_sum']
        col1 = tb.data['A_mean']

        self.assertAlmostEqual(col0[1], 3.0)
        self.assertAlmostEqual(col0[2], 5.0)

        self.assertAlmostEqual(col1[1], 1.5)
        self.assertAlmostEqual(col1[2], 2.5)

if __name__ == '__main__':
    unittest.main()
