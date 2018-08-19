import unittest

from lambdo.Workflow import *

class TablesTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_imports(self):
        wf_json = {
            "id": "My workflow",
            "imports": ["tests.udf", "os.path"],
            "tables": [
                {
                    "id": "My table",
                    "columns": [
                        {
                            "id": "A",
                            "inputs": ["A"],
                            "scope": "1",
                            "extensions": [
                                {"function": "tests.udf:user_import_fn", "outputs": "Success"}
                            ]
                        }
                    ]
                }
            ]
        }

        wf = Workflow(wf_json)

        self.assertEqual(len(wf.modules), 2)
        self.assertTrue(hasattr(wf.modules[0], 'user_import_fn'))

        # Provide data directly (without table population)
        data = {'A': [1, 2, 3]}
        df = pd.DataFrame(data)
        tb = wf.tables[0]
        tb.data = df

        wf.execute()

        self.assertEqual(wf.tables[0].data['Success'][0], 'Success')
        self.assertEqual(wf.tables[0].data['Success'].nunique(), 1)


if __name__ == '__main__':
    unittest.main()
