#!/usr/bin/env python

class ibQuery:
    def __init__(self, data_type, isoblue_id):
      self.query_param = self._get_query_param(data_type)
      self.first_row = self._get_first_row(data_type, isoblue_id)

    def _get_query_param(self, data_type):
        '''
        Return query parameters based on the query data type.
        Return None if no matching data type.
        '''
        return {
            'hb': "`ts`, `isoblue_id`, `wifins`, `cellns`, `netled`, `statled`",
            'gps': "`ts`, `isoblue_id`, `lat`, `lon`, `alt`, `speed`",
        }.get(data_type, None)

    def _get_first_row(self, data_type, isoblue_id):
        '''
        Return csv header line based on data type and where isoblue_id presents.
        Return None if no matching data type.
        '''
        if isoblue_id is None:
            return {
                'hb': ['ts', 'isoblue_id', 'wifins', 'cellns', 'netled', \
                    'statled'],
                'gps': ['ts', 'isoblue_id', 'lat', 'lon', 'alt', 'speed'],
            }.get(data_type, None)
        else:
            return {
                'hb': ['ts', 'wifins', 'cellns', 'netled', 'statled'],
                'gps': ['ts', 'lat', 'lon', 'alt', 'speed'],
            }.get(data_type, None)
