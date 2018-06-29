from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
import re
from user_agents import parse


class CityStatsJob(MRJob):
    INTERNAL_PROTOCOL = PickleProtocol

    KEY_FIELD_SEPARATOR = ';'

    REGEXP = re.compile('([a-z\d]+)\s+(\d+)\s+(\d+)\s+([\w\d~]+)\s+(?P<user_agent>.*)\s+([\d]+\.[\d]+\.[\d]+\.\*)\s+([\w\d]+)\s+(?P<city_id>[\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+([\w\d]+)\s+(?P<price>[\w\d]+)')

    def partitioner(self):
        return 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    def configure_options(self):
        super().configure_options()
        self.add_file_option("--cities")
        self.add_passthrough_option("--partitioner", action='store_true')
        self.add_passthrough_option("--min_price", type='int', default=250)
        self.add_passthrough_option("--reduces", type='int', default=1)

    def jobconf(self):
        conf = super().jobconf()
        if self.options.partitioner:
            enable_partitioner = {'mapreduce.job.reduces': self.options.reduces,
                                  'mapreduce.map.output.key.field.separator': self.KEY_FIELD_SEPARATOR,
                                  'mapreduce.partition.keypartitioner.options': '-k2'}
            conf.update(enable_partitioner)
        return conf

    def mapper_init(self):
        self.known_user_agents = {}

    def mapper(self, _, line):
        try:
            values_dict = re.match(self.REGEXP, line).groupdict()
            city_id = values_dict['city_id']
            price = values_dict['price']
            try:
                price = int(price)
            except ValueError:
                price = 0
            if price > self.options.min_price:
                if self.options.partitioner:
                    user_agent = values_dict['user_agent']
                    try:
                        os = self.known_user_agents[user_agent]
                    except KeyError:
                        os = parse(user_agent).os.family
                        self.known_user_agents.update({user_agent: os})
                    if os.startswith('Windows'):
                        os = 'Windows'
                    yield self.KEY_FIELD_SEPARATOR.join([city_id, os]), 1
                else:
                    yield city_id, 1
        except AttributeError:
            pass

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        if self.options.partitioner:
            city_id, user_agent = key.split(';')
        else:
            city_id = key
        try:
            city_id = int(city_id)
        except ValueError:
            pass
        with open(self.options.cities, 'r') as cities_file:
            r = '(\d+)\s+(\w+)'
            cities_dict = {int(pair[0]): pair[1] for pair in re.findall(r, cities_file.read())}
            try:
                city_name = cities_dict[city_id]
            except KeyError:
                city_name = str(city_id)
        if self.options.partitioner:
            result_key = "{} ({})".format(city_name, user_agent)
        else:
            result_key = city_name
        yield result_key, sum(values)


if __name__ == '__main__':
    CityStatsJob().run()
