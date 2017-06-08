import unittest
from captcha_decoder import decoder


class TestStringMethods(unittest.TestCase):

    def test_images(self):
        self.assertEqual(decoder('test.jpg'), '1eda2')
        self.assertEqual(decoder('test2.jpg'), '7z4ca')

if __name__ == '__main__':
    unittest.main()
