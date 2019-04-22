# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from abc import abstractmethod
from advisor.db_log_parser import DataSource
from enum import Enum
import math


NO_ENTITY = 'ENTITY_PLACEHOLDER'


class TimeSeriesData(DataSource):
    class Behavior(Enum):
        bursty = 1
        evaluate_expression = 2

    class AggregationOperator(Enum):
        avg = 1
        max = 2
        min = 3
        latest = 4
        oldest = 5

    def __init__(self):
        super().__init__(DataSource.Type.TIME_SERIES)
        self.keys_ts = None  # Dict[entity, Dict[key, Dict[timestamp, value]]]
        self.stats_freq_sec = None

    @abstractmethod
    def get_keys_from_conditions(self, conditions):
        # This method takes in a list of time-series conditions; for each
        # condition it manipulates the 'keys' in the way that is supported by
        # the subclass implementing this method
        pass

    @abstractmethod
    def fetch_timeseries(self, required_statistics):
        # this method takes in a list of statistics and fetches the timeseries
        # for each of them and populates the 'keys_ts' dictionary
        pass

    def fetch_burst_epochs(
        self, entities, statistic, window_sec, threshold, percent
    ):
        # type: (str, int, float, bool) -> Dict[str, Dict[int, float]]
        # this method calculates the (percent) rate change in the 'statistic'
        # for each entity (over 'window_sec' seconds) and returns the epochs
        # where this rate change is greater than or equal to the 'threshold'
        # value
        if self.stats_freq_sec == 0:
            # not time series data, cannot check for bursty behavior
            return
        if window_sec < self.stats_freq_sec:
            window_sec = self.stats_freq_sec
        # 'window_samples' is the number of windows to go back to
        # compare the current window with, while calculating rate change.
        window_samples = math.ceil(window_sec / self.stats_freq_sec)
        burst_epochs = {}
        # if percent = False:
        # curr_val = value at window for which rate change is being calculated
        # prev_val = value at window that is window_samples behind curr_window
        # Then rate_without_percent =
        # ((curr_val-prev_val)*duration_sec)/(curr_timestamp-prev_timestamp)
        # if percent = True:
        # rate_with_percent = (rate_without_percent * 100) / prev_val
        # These calculations are in line with the rate() transform supported
        # by ODS
        for entity in entities:
            if statistic not in self.keys_ts[entity]:
                continue
            timestamps = sorted(list(self.keys_ts[entity][statistic].keys()))
            for ix in range(window_samples, len(timestamps), 1):
                first_ts = timestamps[ix - window_samples]
                last_ts = timestamps[ix]
                first_val = self.keys_ts[entity][statistic][first_ts]
                last_val = self.keys_ts[entity][statistic][last_ts]
                diff = last_val - first_val
                if percent:
                    diff = diff * 100 / first_val
                rate = (diff * self.duration_sec) / (last_ts - first_ts)
                # if the rate change is greater than the provided threshold,
                # then the condition is triggered for entity at time 'last_ts'
                if rate >= threshold:
                    if entity not in burst_epochs:
                        burst_epochs[entity] = {}
                    burst_epochs[entity][last_ts] = rate
        return burst_epochs

    def fetch_aggregated_values(self, entity, statistics, aggregation_op):
        # type: (str, AggregationOperator) -> Dict[str, float]
        # this method performs the aggregation specified by 'aggregation_op'
        # on the timeseries of 'statistics' for 'entity' and returns:
        # Dict[statistic, aggregated_value]
        result = {}
        for stat in statistics:
            if stat not in self.keys_ts[entity]:
                continue
            agg_val = None
            if aggregation_op is self.AggregationOperator.latest:
                latest_timestamp = max(list(self.keys_ts[entity][stat].keys()))
                agg_val = self.keys_ts[entity][stat][latest_timestamp]
            elif aggregation_op is self.AggregationOperator.oldest:
                oldest_timestamp = min(list(self.keys_ts[entity][stat].keys()))
                agg_val = self.keys_ts[entity][stat][oldest_timestamp]
            elif aggregation_op is self.AggregationOperator.max:
                agg_val = max(list(self.keys_ts[entity][stat].values()))
            elif aggregation_op is self.AggregationOperator.min:
                agg_val = min(list(self.keys_ts[entity][stat].values()))
            elif aggregation_op is self.AggregationOperator.avg:
                values = list(self.keys_ts[entity][stat].values())
                agg_val = sum(values) / len(values)
            result[stat] = agg_val
        return result

    def check_and_trigger_conditions(self, conditions):
        # get the list of statistics that need to be fetched
        reqd_keys = self.get_keys_from_conditions(conditions)
        # fetch the required statistics and populate the map 'keys_ts'
        self.fetch_timeseries(reqd_keys)
        # Trigger the appropriate conditions
        for cond in conditions:
            complete_keys = self.get_keys_from_conditions([cond])
            # Get the entities that have all statistics required by 'cond':
            # an entity is checked for a given condition only if we possess all
            # of the condition's 'keys' for that entity
            entities_with_stats = []
            for entity in self.keys_ts:
                stat_missing = False
                for stat in complete_keys:
                    if stat not in self.keys_ts[entity]:
                        stat_missing = True
                        break
                if not stat_missing:
                    entities_with_stats.append(entity)
            if not entities_with_stats:
                continue
            if cond.behavior is self.Behavior.bursty:
                # for a condition that checks for bursty behavior, only one key
                # should be present in the condition's 'keys' field
                result = self.fetch_burst_epochs(
                    entities_with_stats,
                    complete_keys[0],  # there should be only one key
                    cond.window_sec,
                    cond.rate_threshold,
                    True
                )
                # Trigger in this case is:
                # Dict[entity_name, Dict[timestamp, rate_change]]
                # where the inner dictionary contains rate_change values when
                # the rate_change >= threshold provided, with the
                # corresponding timestamps
                if result:
                    cond.set_trigger(result)
            elif cond.behavior is self.Behavior.evaluate_expression:
                self.handle_evaluate_expression(
                    cond,
                    complete_keys,
                    entities_with_stats
                )

    def handle_evaluate_expression(self, condition, statistics, entities):
        trigger = {}
        # check 'condition' for each of these entities
        for entity in entities:
            if hasattr(condition, 'aggregation_op'):
                # in this case, the aggregation operation is performed on each
                # of the condition's 'keys' and then with aggregated values
                # condition's 'expression' is evaluated; if it evaluates to
                # True, then list of the keys values is added to the
                # condition's trigger: Dict[entity_name, List[stats]]
                result = self.fetch_aggregated_values(
                        entity, statistics, condition.aggregation_op
                )
                keys = [result[key] for key in statistics]
                try:
                    if eval(condition.expression):
                        trigger[entity] = keys
                except Exception as e:
                    print(
                        'WARNING(TimeSeriesData) check_and_trigger: ' + str(e)
                    )
            else:
                # assumption: all stats have same series of timestamps
                # this is similar to the above but 'expression' is evaluated at
                # each timestamp, since there is no aggregation, and all the
                # epochs are added to the trigger when the condition's
                # 'expression' evaluated to true; so trigger is:
                # Dict[entity, Dict[timestamp, List[stats]]]
                for epoch in self.keys_ts[entity][statistics[0]].keys():
                    keys = [
                        self.keys_ts[entity][key][epoch]
                        for key in statistics
                    ]
                    try:
                        if eval(condition.expression):
                            if entity not in trigger:
                                trigger[entity] = {}
                            trigger[entity][epoch] = keys
                    except Exception as e:
                        print(
                            'WARNING(TimeSeriesData) check_and_trigger: ' +
                            str(e)
                        )
        if trigger:
            condition.set_trigger(trigger)
