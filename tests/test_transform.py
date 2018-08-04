import unittest

import sklearn.preprocessing  # To get rid of ImportWarning

from lambdo.resolve import *
from lambdo.transform import *

#
# UDF tests
#
def udf1(value):  # Single value input. No model
    return value + 1.0

def udf2(value, addition):  # Single value input. Assumes flattened model
    return value + addition

def udf3(fields):  # Row input. No model
    return fields[0] + fields[1]  # Works for both Series and ndarray argument
    #return fields['col1'] + fields['col2']  # Works for Series only (row=False, data_type="ndarray")

def udf4(fields, addition):  # Row input. Has model
    return fields[0] + fields[1] + addition  # Works for both Series and ndarray argument
    #return fields['col1'] + fields['col2'] + addition  # Works for Series only (row=False, data_type="ndarray")

class TransformTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def my_custom_method(X, model):
        return 'Custom method'

    def test_scale(self):
        data = {'col1': [1.0, 2.0, 3.0], 'col2': [4.0, 5.0, 6.0]}

        #
        # No model
        #
        X = pd.DataFrame(data)
        y = transform(resolve_full_name('sklearn.preprocessing:scale'), 'all', X['col1'], None, {}, None)
        # It returns ndarray

        self.assertEqual(len(y), 3)

        self.assertAlmostEqual(y.mean(), 0.0)
        self.assertAlmostEqual(y.std(ddof=0), 1.0)

        #
        # Use some model, 2 input columns and one output column (so second input column will be overwritten)
        #
        X = pd.DataFrame(data)
        model = {'with_mean': True, 'with_std': False}
        y = transform(resolve_full_name('sklearn.preprocessing:scale'), 'all', X['col2'], None, model, None)

        self.assertAlmostEqual(y.mean(), 0.0)
        self.assertAlmostEqual(y.std(ddof=0), 0.816496580927726)

    def test_UDF(self):
        data = {'col1': [1.0, 2.0, 3.0], 'col2': [4.0, 5.0, 6.0]}

        #
        # No model. Single input
        #
        X = pd.DataFrame(data)
        out = transform(resolve_full_name('test_transform:udf1'), 'one', X[['col2']], None, {}, None)

        self.assertEqual(len(out), 3)

        self.assertAlmostEqual(out[0], 5.0)
        self.assertAlmostEqual(out[1], 6.0)
        self.assertAlmostEqual(out[2], 7.0)

        #
        # Has model. Parameters flattened.
        #
        model = {'addition': 1.0}
        X = pd.DataFrame(data)
        out = transform(resolve_full_name('test_transform:udf2'), 'one', X['col2'], None, model, None)

        self.assertEqual(len(out), 3)

        self.assertAlmostEqual(out[0], 5.0)
        self.assertAlmostEqual(out[1], 6.0)
        self.assertAlmostEqual(out[2], 7.0)

        #
        # No model. Row input
        #
        X = pd.DataFrame(data)
        out = transform(resolve_full_name('test_transform:udf3'), 'one', X[['col1', 'col2']], None, {}, None)

        self.assertEqual(len(out), 3)

        self.assertAlmostEqual(out[0], 5.0)
        self.assertAlmostEqual(out[1], 7.0)
        self.assertAlmostEqual(out[2], 9.0)

        #
        # Has model. Row input
        #
        model = {'addition': 1.0}
        X = pd.DataFrame(data)
        out = transform(resolve_full_name('test_transform:udf4'), 'one', X[['col1', 'col2']], None, model, None)

        self.assertEqual(len(out), 3)

        self.assertAlmostEqual(out[0], 6.0)
        self.assertAlmostEqual(out[1], 8.0)
        self.assertAlmostEqual(out[2], 10.0)


if __name__ == '__main__':
    unittest.main()
