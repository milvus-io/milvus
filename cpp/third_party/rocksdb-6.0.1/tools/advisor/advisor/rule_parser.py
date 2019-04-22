# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from abc import ABC, abstractmethod
from advisor.db_log_parser import DataSource, NO_COL_FAMILY
from advisor.db_timeseries_parser import TimeSeriesData
from enum import Enum
from advisor.ini_parser import IniParser
import re


class Section(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def set_parameter(self, key, value):
        pass

    @abstractmethod
    def perform_checks(self):
        pass


class Rule(Section):
    def __init__(self, name):
        super().__init__(name)
        self.conditions = None
        self.suggestions = None
        self.overlap_time_seconds = None
        self.trigger_entities = None
        self.trigger_column_families = None

    def set_parameter(self, key, value):
        # If the Rule is associated with a single suggestion/condition, then
        # value will be a string and not a list. Hence, convert it to a single
        # element list before storing it in self.suggestions or
        # self.conditions.
        if key == 'conditions':
            if isinstance(value, str):
                self.conditions = [value]
            else:
                self.conditions = value
        elif key == 'suggestions':
            if isinstance(value, str):
                self.suggestions = [value]
            else:
                self.suggestions = value
        elif key == 'overlap_time_period':
            self.overlap_time_seconds = value

    def get_suggestions(self):
        return self.suggestions

    def perform_checks(self):
        if not self.conditions or len(self.conditions) < 1:
            raise ValueError(
                self.name + ': rule must have at least one condition'
            )
        if not self.suggestions or len(self.suggestions) < 1:
            raise ValueError(
                self.name + ': rule must have at least one suggestion'
            )
        if self.overlap_time_seconds:
            if len(self.conditions) != 2:
                raise ValueError(
                    self.name + ": rule must be associated with 2 conditions\
                    in order to check for a time dependency between them"
                )
            time_format = '^\d+[s|m|h|d]$'
            if (
                not
                re.match(time_format, self.overlap_time_seconds, re.IGNORECASE)
            ):
                raise ValueError(
                    self.name + ": overlap_time_seconds format: \d+[s|m|h|d]"
                )
            else:  # convert to seconds
                in_seconds = int(self.overlap_time_seconds[:-1])
                if self.overlap_time_seconds[-1] == 'm':
                    in_seconds *= 60
                elif self.overlap_time_seconds[-1] == 'h':
                    in_seconds *= (60 * 60)
                elif self.overlap_time_seconds[-1] == 'd':
                    in_seconds *= (24 * 60 * 60)
                self.overlap_time_seconds = in_seconds

    def get_overlap_timestamps(self, key1_trigger_epochs, key2_trigger_epochs):
        # this method takes in 2 timeseries i.e. timestamps at which the
        # rule's 2 TIME_SERIES conditions were triggered and it finds
        # (if present) the first pair of timestamps at which the 2 conditions
        # were triggered within 'overlap_time_seconds' of each other
        key1_lower_bounds = [
            epoch - self.overlap_time_seconds
            for epoch in key1_trigger_epochs
        ]
        key1_lower_bounds.sort()
        key2_trigger_epochs.sort()
        trigger_ix = 0
        overlap_pair = None
        for key1_lb in key1_lower_bounds:
            while (
                key2_trigger_epochs[trigger_ix] < key1_lb and
                trigger_ix < len(key2_trigger_epochs)
            ):
                trigger_ix += 1
            if trigger_ix >= len(key2_trigger_epochs):
                break
            if (
                key2_trigger_epochs[trigger_ix] <=
                key1_lb + (2 * self.overlap_time_seconds)
            ):
                overlap_pair = (
                    key2_trigger_epochs[trigger_ix],
                    key1_lb + self.overlap_time_seconds
                )
                break
        return overlap_pair

    def get_trigger_entities(self):
        return self.trigger_entities

    def get_trigger_column_families(self):
        return self.trigger_column_families

    def is_triggered(self, conditions_dict, column_families):
        if self.overlap_time_seconds:
            condition1 = conditions_dict[self.conditions[0]]
            condition2 = conditions_dict[self.conditions[1]]
            if not (
                condition1.get_data_source() is DataSource.Type.TIME_SERIES and
                condition2.get_data_source() is DataSource.Type.TIME_SERIES
            ):
                raise ValueError(self.name + ': need 2 timeseries conditions')

            map1 = condition1.get_trigger()
            map2 = condition2.get_trigger()
            if not (map1 and map2):
                return False

            self.trigger_entities = {}
            is_triggered = False
            entity_intersection = (
                set(map1.keys()).intersection(set(map2.keys()))
            )
            for entity in entity_intersection:
                overlap_timestamps_pair = (
                    self.get_overlap_timestamps(
                        list(map1[entity].keys()), list(map2[entity].keys())
                    )
                )
                if overlap_timestamps_pair:
                    self.trigger_entities[entity] = overlap_timestamps_pair
                    is_triggered = True
            if is_triggered:
                self.trigger_column_families = set(column_families)
            return is_triggered
        else:
            all_conditions_triggered = True
            self.trigger_column_families = set(column_families)
            for cond_name in self.conditions:
                cond = conditions_dict[cond_name]
                if not cond.get_trigger():
                    all_conditions_triggered = False
                    break
                if (
                    cond.get_data_source() is DataSource.Type.LOG or
                    cond.get_data_source() is DataSource.Type.DB_OPTIONS
                ):
                    cond_col_fam = set(cond.get_trigger().keys())
                    if NO_COL_FAMILY in cond_col_fam:
                        cond_col_fam = set(column_families)
                    self.trigger_column_families = (
                        self.trigger_column_families.intersection(cond_col_fam)
                    )
                elif cond.get_data_source() is DataSource.Type.TIME_SERIES:
                    cond_entities = set(cond.get_trigger().keys())
                    if self.trigger_entities is None:
                        self.trigger_entities = cond_entities
                    else:
                        self.trigger_entities = (
                            self.trigger_entities.intersection(cond_entities)
                        )
                if not (self.trigger_entities or self.trigger_column_families):
                    all_conditions_triggered = False
                    break
            if not all_conditions_triggered:  # clean up if rule not triggered
                self.trigger_column_families = None
                self.trigger_entities = None
            return all_conditions_triggered

    def __repr__(self):
        # Append conditions
        rule_string = "Rule: " + self.name + " has conditions:: "
        is_first = True
        for cond in self.conditions:
            if is_first:
                rule_string += cond
                is_first = False
            else:
                rule_string += (" AND " + cond)
        # Append suggestions
        rule_string += "\nsuggestions:: "
        is_first = True
        for sugg in self.suggestions:
            if is_first:
                rule_string += sugg
                is_first = False
            else:
                rule_string += (", " + sugg)
        if self.trigger_entities:
            rule_string += (', entities:: ' + str(self.trigger_entities))
        if self.trigger_column_families:
            rule_string += (', col_fam:: ' + str(self.trigger_column_families))
        # Return constructed string
        return rule_string


class Suggestion(Section):
    class Action(Enum):
        set = 1
        increase = 2
        decrease = 3

    def __init__(self, name):
        super().__init__(name)
        self.option = None
        self.action = None
        self.suggested_values = None
        self.description = None

    def set_parameter(self, key, value):
        if key == 'option':
            # Note:
            # case 1: 'option' is supported by Rocksdb OPTIONS file; in this
            # case the option belongs to one of the sections in the config
            # file and it's name is prefixed by "<section_type>."
            # case 2: 'option' is not supported by Rocksdb OPTIONS file; the
            # option is not expected to have the character '.' in its name
            self.option = value
        elif key == 'action':
            if self.option and not value:
                raise ValueError(self.name + ': provide action for option')
            self.action = self.Action[value]
        elif key == 'suggested_values':
            if isinstance(value, str):
                self.suggested_values = [value]
            else:
                self.suggested_values = value
        elif key == 'description':
            self.description = value

    def perform_checks(self):
        if not self.description:
            if not self.option:
                raise ValueError(self.name + ': provide option or description')
            if not self.action:
                raise ValueError(self.name + ': provide action for option')
            if self.action is self.Action.set and not self.suggested_values:
                raise ValueError(
                    self.name + ': provide suggested value for option'
                )

    def __repr__(self):
        sugg_string = "Suggestion: " + self.name
        if self.description:
            sugg_string += (' description : ' + self.description)
        else:
            sugg_string += (
                ' option : ' + self.option + ' action : ' + self.action.name
            )
            if self.suggested_values:
                sugg_string += (
                    ' suggested_values : ' + str(self.suggested_values)
                )
        return sugg_string


class Condition(Section):
    def __init__(self, name):
        super().__init__(name)
        self.data_source = None
        self.trigger = None

    def perform_checks(self):
        if not self.data_source:
            raise ValueError(self.name + ': condition not tied to data source')

    def set_data_source(self, data_source):
        self.data_source = data_source

    def get_data_source(self):
        return self.data_source

    def reset_trigger(self):
        self.trigger = None

    def set_trigger(self, condition_trigger):
        self.trigger = condition_trigger

    def get_trigger(self):
        return self.trigger

    def is_triggered(self):
        if self.trigger:
            return True
        return False

    def set_parameter(self, key, value):
        # must be defined by the subclass
        raise NotImplementedError(self.name + ': provide source for condition')


class LogCondition(Condition):
    @classmethod
    def create(cls, base_condition):
        base_condition.set_data_source(DataSource.Type['LOG'])
        base_condition.__class__ = cls
        return base_condition

    def set_parameter(self, key, value):
        if key == 'regex':
            self.regex = value

    def perform_checks(self):
        super().perform_checks()
        if not self.regex:
            raise ValueError(self.name + ': provide regex for log condition')

    def __repr__(self):
        log_cond_str = "LogCondition: " + self.name
        log_cond_str += (" regex: " + self.regex)
        # if self.trigger:
        #     log_cond_str += (" trigger: " + str(self.trigger))
        return log_cond_str


class OptionCondition(Condition):
    @classmethod
    def create(cls, base_condition):
        base_condition.set_data_source(DataSource.Type['DB_OPTIONS'])
        base_condition.__class__ = cls
        return base_condition

    def set_parameter(self, key, value):
        if key == 'options':
            if isinstance(value, str):
                self.options = [value]
            else:
                self.options = value
        elif key == 'evaluate':
            self.eval_expr = value

    def perform_checks(self):
        super().perform_checks()
        if not self.options:
            raise ValueError(self.name + ': options missing in condition')
        if not self.eval_expr:
            raise ValueError(self.name + ': expression missing in condition')

    def __repr__(self):
        opt_cond_str = "OptionCondition: " + self.name
        opt_cond_str += (" options: " + str(self.options))
        opt_cond_str += (" expression: " + self.eval_expr)
        if self.trigger:
            opt_cond_str += (" trigger: " + str(self.trigger))
        return opt_cond_str


class TimeSeriesCondition(Condition):
    @classmethod
    def create(cls, base_condition):
        base_condition.set_data_source(DataSource.Type['TIME_SERIES'])
        base_condition.__class__ = cls
        return base_condition

    def set_parameter(self, key, value):
        if key == 'keys':
            if isinstance(value, str):
                self.keys = [value]
            else:
                self.keys = value
        elif key == 'behavior':
            self.behavior = TimeSeriesData.Behavior[value]
        elif key == 'rate_threshold':
            self.rate_threshold = float(value)
        elif key == 'window_sec':
            self.window_sec = int(value)
        elif key == 'evaluate':
            self.expression = value
        elif key == 'aggregation_op':
            self.aggregation_op = TimeSeriesData.AggregationOperator[value]

    def perform_checks(self):
        if not self.keys:
            raise ValueError(self.name + ': specify timeseries key')
        if not self.behavior:
            raise ValueError(self.name + ': specify triggering behavior')
        if self.behavior is TimeSeriesData.Behavior.bursty:
            if not self.rate_threshold:
                raise ValueError(self.name + ': specify rate burst threshold')
            if not self.window_sec:
                self.window_sec = 300  # default window length is 5 minutes
            if len(self.keys) > 1:
                raise ValueError(self.name + ': specify only one key')
        elif self.behavior is TimeSeriesData.Behavior.evaluate_expression:
            if not (self.expression):
                raise ValueError(self.name + ': specify evaluation expression')
        else:
            raise ValueError(self.name + ': trigger behavior not supported')

    def __repr__(self):
        ts_cond_str = "TimeSeriesCondition: " + self.name
        ts_cond_str += (" statistics: " + str(self.keys))
        ts_cond_str += (" behavior: " + self.behavior.name)
        if self.behavior is TimeSeriesData.Behavior.bursty:
            ts_cond_str += (" rate_threshold: " + str(self.rate_threshold))
            ts_cond_str += (" window_sec: " + str(self.window_sec))
        if self.behavior is TimeSeriesData.Behavior.evaluate_expression:
            ts_cond_str += (" expression: " + self.expression)
            if hasattr(self, 'aggregation_op'):
                ts_cond_str += (" aggregation_op: " + self.aggregation_op.name)
        if self.trigger:
            ts_cond_str += (" trigger: " + str(self.trigger))
        return ts_cond_str


class RulesSpec:
    def __init__(self, rules_path):
        self.file_path = rules_path

    def initialise_fields(self):
        self.rules_dict = {}
        self.conditions_dict = {}
        self.suggestions_dict = {}

    def perform_section_checks(self):
        for rule in self.rules_dict.values():
            rule.perform_checks()
        for cond in self.conditions_dict.values():
            cond.perform_checks()
        for sugg in self.suggestions_dict.values():
            sugg.perform_checks()

    def load_rules_from_spec(self):
        self.initialise_fields()
        with open(self.file_path, 'r') as db_rules:
            curr_section = None
            for line in db_rules:
                line = IniParser.remove_trailing_comment(line)
                if not line:
                    continue
                element = IniParser.get_element(line)
                if element is IniParser.Element.comment:
                    continue
                elif element is not IniParser.Element.key_val:
                    curr_section = element  # it's a new IniParser header
                    section_name = IniParser.get_section_name(line)
                    if element is IniParser.Element.rule:
                        new_rule = Rule(section_name)
                        self.rules_dict[section_name] = new_rule
                    elif element is IniParser.Element.cond:
                        new_cond = Condition(section_name)
                        self.conditions_dict[section_name] = new_cond
                    elif element is IniParser.Element.sugg:
                        new_suggestion = Suggestion(section_name)
                        self.suggestions_dict[section_name] = new_suggestion
                elif element is IniParser.Element.key_val:
                    key, value = IniParser.get_key_value_pair(line)
                    if curr_section is IniParser.Element.rule:
                        new_rule.set_parameter(key, value)
                    elif curr_section is IniParser.Element.cond:
                        if key == 'source':
                            if value == 'LOG':
                                new_cond = LogCondition.create(new_cond)
                            elif value == 'OPTIONS':
                                new_cond = OptionCondition.create(new_cond)
                            elif value == 'TIME_SERIES':
                                new_cond = TimeSeriesCondition.create(new_cond)
                        else:
                            new_cond.set_parameter(key, value)
                    elif curr_section is IniParser.Element.sugg:
                        new_suggestion.set_parameter(key, value)

    def get_rules_dict(self):
        return self.rules_dict

    def get_conditions_dict(self):
        return self.conditions_dict

    def get_suggestions_dict(self):
        return self.suggestions_dict

    def get_triggered_rules(self, data_sources, column_families):
        self.trigger_conditions(data_sources)
        triggered_rules = []
        for rule in self.rules_dict.values():
            if rule.is_triggered(self.conditions_dict, column_families):
                triggered_rules.append(rule)
        return triggered_rules

    def trigger_conditions(self, data_sources):
        for source_type in data_sources:
            cond_subset = [
                cond
                for cond in self.conditions_dict.values()
                if cond.get_data_source() is source_type
            ]
            if not cond_subset:
                continue
            for source in data_sources[source_type]:
                source.check_and_trigger_conditions(cond_subset)

    def print_rules(self, rules):
        for rule in rules:
            print('\nRule: ' + rule.name)
            for cond_name in rule.conditions:
                print(repr(self.conditions_dict[cond_name]))
            for sugg_name in rule.suggestions:
                print(repr(self.suggestions_dict[sugg_name]))
            if rule.trigger_entities:
                print('scope: entities:')
                print(rule.trigger_entities)
            if rule.trigger_column_families:
                print('scope: col_fam:')
                print(rule.trigger_column_families)
