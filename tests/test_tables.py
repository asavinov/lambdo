import unittest

from lambdo.Workflow import *

class TablesTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_read_csv(self):
        with open('../tests/test1.jsonc', encoding='utf-8') as f:
            wf_json = json.loads(f.read())
        wf = Workflow(wf_json)

        wf.execute()

        tb = wf.tables[0].data

        self.assertEqual(len(tb.columns), 3)
        self.assertEqual(len(tb), 4)


if __name__ == '__main__':
    unittest.main()
