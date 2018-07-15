import unittest

from sklearn.svm import SVR  # To get rid of ImportWarning

from lambdo.resolve import *

#
# Resolution tests
#
def my_custom_func(X, model):
    return 'Custom function'

class ResolveTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def my_custom_method(X, model):
        return 'Custom method'

    def test_resolve(self):

        ff = resolve_full_name('builtins:float')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'float')
        self.assertEqual(ff.__qualname__, 'float')
        self.assertEqual(ff.__module__, 'builtins')
        self.assertEqual(type(ff).__name__, 'type')
        self.assertTrue(inspect.isclass(ff))

        ff = resolve_full_name('datetime:datetime')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'datetime')
        self.assertEqual(ff.__qualname__, 'datetime')
        self.assertEqual(ff.__module__, 'datetime')
        self.assertEqual(type(ff).__name__, 'type')
        self.assertTrue(inspect.isclass(ff))

        ff = resolve_full_name('numpy.core.fromnumeric:sum')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'sum')
        self.assertEqual(ff.__qualname__, 'sum')
        self.assertEqual(ff.__module__, 'numpy.core.fromnumeric')
        self.assertEqual(type(ff).__name__, 'function')
        self.assertTrue(inspect.isfunction(ff))

        ff = resolve_full_name('sklearn.svm.base:BaseLibSVM.predict')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'predict')
        self.assertEqual(ff.__qualname__, 'BaseLibSVM.predict')
        self.assertEqual(ff.__module__, 'sklearn.svm.base')
        self.assertEqual(type(ff).__name__, 'function')
        self.assertTrue(inspect.isfunction(ff))

        ff = resolve_full_name('sklearn.preprocessing:scale')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'scale')
        self.assertEqual(ff.__qualname__, 'scale')
        self.assertEqual(ff.__module__, 'sklearn.preprocessing.data')
        self.assertEqual(type(ff).__name__, 'function')
        self.assertTrue(inspect.isfunction(ff))

        ff = resolve_full_name('sklearn.preprocessing:StandardScaler')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'StandardScaler')
        self.assertEqual(ff.__qualname__, 'StandardScaler')
        self.assertEqual(ff.__module__, 'sklearn.preprocessing.data')
        self.assertEqual(type(ff).__name__, 'type')
        self.assertTrue(inspect.isclass(ff))

        ff = resolve_full_name('sklearn.preprocessing:StandardScaler.transform')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'transform')
        self.assertEqual(ff.__qualname__, 'StandardScaler.transform')
        self.assertEqual(ff.__module__, 'sklearn.preprocessing.data')
        self.assertEqual(type(ff).__name__, 'function')
        self.assertTrue(inspect.isfunction(ff))

        # Test custom function defined at module level
        ff = resolve_full_name('test_resolve:my_custom_func')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'my_custom_func')
        self.assertEqual(ff.__qualname__, 'my_custom_func')
        self.assertEqual(ff.__module__, 'test_resolve')
        self.assertEqual(type(ff).__name__, 'function')
        self.assertTrue(inspect.isfunction(ff))

        # Test custom method defined at class level
        ff = resolve_full_name('test_resolve:ResolveTestCase.my_custom_method')
        self.assertIsNotNone(ff)
        self.assertEqual(ff.__name__, 'my_custom_method')
        self.assertEqual(ff.__qualname__, 'ResolveTestCase.my_custom_method')
        self.assertEqual(ff.__module__, 'test_resolve')
        self.assertEqual(type(ff).__name__, 'function')
        self.assertTrue(inspect.isfunction(ff))

        # TODO: Finding function within another function does not work even we we use its qualname
        def test_func(value):
            return value.__name__
        ff = resolve_full_name('test_resolve:ResolveTestCase.test_resolve.<locals>.test_func')
        ff = None
        # __name__ 'test_func'
        # __qualname__ = 'ResolveTestCase.test_resolve.<locals>.test_func'
        # __module__ = 'test_resolve'


if __name__ == '__main__':
    unittest.main()
