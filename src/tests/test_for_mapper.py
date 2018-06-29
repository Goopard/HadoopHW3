"""
This is a unittest test case for the mapper function of the job CityStatsJob.
"""
import unittest

from city_stats_job import CityStatsJob


class Tests(unittest.TestCase):
    def test_correct_line_without_partitioner(self):
        """
        This test checks if the mapper function works correctly for the correct input line with --partitioner=False.
        """
        test_job = CityStatsJob()
        line = 'b98810f66b9e22a10bf3565ab333b1b7	20131019154704523	1	D94GV48_siR	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	163.177.136.*	216	216	3	740200eac37181bf76dc01564d6f37a2	eef5cf7a16cfff90973dba61555f03ea	null	ALLINONE_F_Width1	1000	90	Na	Na	70	7336	294	70	null	2259	14273,10006,13403,16753,10063,11092'
        self.assertEqual(next(test_job.mapper(None, line)), ('216', 1))

    def test_correct_line_with_partitioner(self):
        """
        This test checks if the mapper function works correctly for the correct input line with --partitioner=False.
        """
        test_job = CityStatsJob(['--partitioner'])
        line = 'a390722941955e7ac06551724bdcbde5	20131019162602881	1	D93M1l6axzh	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	117.136.32.*	216	217	2	e4d96a965264884905794bfade88dcad	a72f8164c897093588b7090e75c8c8e2	null	1253131916	300	250	OtherView	Na	5	7323	277	10	null	2259	null'
        test_job.mapper_init()
        self.assertEqual(next(test_job.mapper(None, line)), ('217;Windows', 1))


if __name__ == '__main__':
    unittest.main()
