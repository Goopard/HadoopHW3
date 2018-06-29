"""
This is a unittest testcase  for the job CityStatsJob.
"""
import unittest
import os

from io import BytesIO

from city_stats_job import CityStatsJob


class JobTest(unittest.TestCase):
    def setUp(self):
        directory = os.path.dirname(__file__)
        path = os.path.abspath(directory)
        with open(os.path.join(path, 'test_inputs', 'input.txt'), 'rb') as file:
            self.stdin = BytesIO(file.read())

    def test_without_partitioner(self):
        """
        This test checks if the job works correctly for some input file with --partitioner=False.
        """
        test_job = CityStatsJob(['--cities=city.en.txt'])
        test_job.sandbox(stdin=self.stdin)
        result = []
        with test_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = test_job.parse_output_line(line)
                result.append((key, value))
        correct_result = [('216', 4),
                          ('guangzhou', 19),
                          ('shenzhen', 22),
                          ('zhuhai', 4),
                          ('shantou', 2),
                          ('foshan', 10),
                          ('jiangmen', 2),
                          ('maoming', 4),
                          ('zhaoqing', 4),
                          ('huizhou', 3),
                          ('meizhou', 4),
                          ('yangjiang', 4),
                          ('dongguan', 10),
                          ('zhongshan', 6),
                          ('jieyang', 1),
                          ('yunfu', 1)]
        self.assertEqual(result, correct_result)

    def test_with_partitioner(self):
        """
        This test checks if the job works correctly for some input file with --partitioner=True.
        """
        test_job = CityStatsJob(['--cities=city.en.txt', '--partitioner'])
        test_job.sandbox(stdin=self.stdin)
        result = []
        with test_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = test_job.parse_output_line(line)
                result.append((key, value))
        correct_result = [('guangzhou (iOS)', 1),
                          ('shenzhen (iOS)', 2),
                          ('zhongshan (iOS)', 1),
                          ('216 (Windows)', 4),
                          ('guangzhou (Windows)', 18),
                          ('shenzhen (Windows)', 20),
                          ('zhuhai (Windows)', 4),
                          ('shantou (Windows)', 2),
                          ('foshan (Windows)', 10),
                          ('jiangmen (Windows)', 2),
                          ('maoming (Windows)', 4),
                          ('zhaoqing (Windows)', 4),
                          ('huizhou (Windows)', 3),
                          ('meizhou (Windows)', 4),
                          ('yangjiang (Windows)', 4),
                          ('dongguan (Windows)', 10),
                          ('zhongshan (Windows)', 5),
                          ('jieyang (Windows)', 1),
                          ('yunfu (Windows)', 1)]
        self.assertEqual(result, correct_result)


if __name__ == '__main__':
    unittest.main()
