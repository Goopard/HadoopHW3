"""
This is a unittest test suite for the reducer function of the job CityStatsJob.
"""
import unittest

from city_stats_job import CityStatsJob


class Test(unittest.TestCase):
    def test_without_partitioner_correct_city_id(self):
        """
        This test checks if the reducer works correctly for some input data with --partitioner=False and correct city_id.
        """
        test_job = CityStatsJob(['--cities=city.en.txt'])
        key = '217'
        values = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        self.assertEqual(next(test_job.reducer(key, values)), ("guangzhou", 55))

    def test_with_partitioner_correct_city_id(self):
        """
        This test checks if the reducer works correctly for some input data with --partitioner=True
        """
        test_job = CityStatsJob(['--cities=city.en.txt', '--partitioner'])
        key = '217;Windows'
        values = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        self.assertEqual(next(test_job.reducer(key, values)), ("guangzhou (Windows)", 55))

    def test_without_partitioner_incorrect_city_id(self):
        """
        This test checks if the reducer works correctly for some input data with --partitioner=False and incorrect
        city_id.
        """
        test_job = CityStatsJob(['--cities=city.en.txt'])
        key = '216'
        values = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        self.assertEqual(next(test_job.reducer(key, values)), ('216', 55))

    def test_with_partitioner_incorrect_city_id(self):
        """
        This test checks if the reducer works correctly for some input data with --partitioner=True and incorrect
        city_id.
        """
        test_job = CityStatsJob(['--cities=city.en.txt', '--partitioner'])
        key = '216;IOS'
        values = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        self.assertEqual(next(test_job.reducer(key, values)), ("216 (IOS)", 55))


if __name__ == '__main__':
    unittest.main()
