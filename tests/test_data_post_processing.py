import unittest
from datetime import datetime

import pandas as pd

from pipeline.manager import DataManager
from tests.fixtures import DATA_MOCK_INPUT_DATA


class TestManagerPipeline(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(DATA_MOCK_INPUT_DATA)

    def test_transform_data_timestamp_filter(self):
        pipeline = DataManager(self.df)
        pipeline.transform_data(timestamp_filter=datetime(2022, 5, 1, 1, 0, 0))
        self.assertEqual(len(pipeline.data), 1)
        self.assertEqual(pipeline.data.iloc[0]['time'], pd.Timestamp('2022-05-01 01:00:00'))

    def test_transform_data_h3_index_filter(self):
        pipeline = DataManager(self.df)
        pipeline.transform_data(h3_index_filter='8a0326233ab7fff')
        self.assertEqual(len(pipeline.data), 3)
        self.assertTrue(all(pipeline.data['h3_index'] == '8a0326233ab7fff'))

    def test_transform_data_with_both_timestamp_and_h3_index_filter(self):
        pipeline = DataManager(self.df)
        pipeline.transform_data(timestamp_filter=datetime(2022, 5, 1, 1, 0, 0), h3_index_filter='8c0326233ab7fff')
        self.assertEqual(len(pipeline.data), 1)

    def test_invalid_transform_data(self):
        pipeline = DataManager(self.df)
        with self.assertRaises(ValueError):
            pipeline.transform_data(timestamp_filter='8c0326233ab7fff')

    def test_invalid_transform_data_by_date(self):
        pipeline = DataManager(self.df)
        with self.assertRaises(ValueError):
            pipeline.transform_data(filter_by_date=True)


if __name__ == '__main__':
    unittest.main()
