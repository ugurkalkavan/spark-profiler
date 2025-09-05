import unittest

def add(a, b):
    return a + b


class TestMathOperations(unittest.TestCase):
    def test_add_positive_numbers(self):
        self.assertEqual(add(2, 3), 5)
    
    def test_add_negative_numbers(self):
        self.assertEqual(add(-2, -3), -5)
        
    def test_add_mixed_numbers(self):
        self.assertEqual(add(-2, 3), 1)
        self.assertEqual(add(2, -3), -1)
    
    if __name__ == '__main__':
        unittest.test_add_positive_numbers()