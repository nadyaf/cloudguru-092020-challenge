import unittest
import pandas as pd
import us_covid_etl

class EtlTest(unittest.TestCase):

    TESTS_PATH = ''
    
    def setUp(self):
        self.TESTS_PATH = './tests/'

    def test_correct_table_join(self):
        df = us_covid_etl.prepare_data(self.TESTS_PATH+'test_nyt_join.csv', self.TESTS_PATH+'test_jh_join.csv')
        self.assertEquals(df.shape, (9,4))

    def test_join_with_extra_date(self):
        df = us_covid_etl.prepare_data(self.TESTS_PATH+'test_nyt_join.csv', self.TESTS_PATH+'test_jh_join_extra_date.csv')
        self.assertEquals(df.shape, (9,4))

    @unittest.expectedFailure
    def test_date_parsing(self):
        with self.assertRaises(Exception):
            df = us_covid_etl.prepare_data(self.TESTS_PATH+'test_wrong_date.csv', self.TESTS_PATH+'test_jh.csv')

    @unittest.expectedFailure
    def test_missing_value(self):
        with self.assertRaises(ValueError):
            df = us_covid_etl.prepare_data(self.TESTS_PATH+'test_missing_value.csv', self.TESTS_PATH+'test_jh.csv')

    @unittest.expectedFailure
    def test_negative_value(self):
        with self.assertRaises(ValueError):
            df = us_covid_etl.prepare_data(self.TESTS_PATH+'test_negative_value.csv', self.TESTS_PATH+'test_jh.csv')

    @unittest.expectedFailure
    def test_not_integer(self):
        with self.assertRaises(ValueError):
            df = us_covid_etl.prepare_data(self.TESTS_PATH+'test_not_integer.csv', self.TESTS_PATH+'test_jh.csv')

    def test_missing_us_country(self):
        df = us_covid_etl.prepare_data(self.TESTS_PATH+'test_nyt_join.csv', self.TESTS_PATH+'test_jh_missing_country.csv')
        self.assertEquals(df.shape, (0,4))

if __name__ == '__main__':
    unittest.main()