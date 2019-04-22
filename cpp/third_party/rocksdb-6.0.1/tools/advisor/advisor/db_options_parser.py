# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import copy
from advisor.db_log_parser import DataSource, NO_COL_FAMILY
from advisor.ini_parser import IniParser
import os


class OptionsSpecParser(IniParser):
    @staticmethod
    def is_new_option(line):
        return '=' in line

    @staticmethod
    def get_section_type(line):
        '''
        Example section header: [TableOptions/BlockBasedTable "default"]
        Here ConfigurationOptimizer returned would be
        'TableOptions.BlockBasedTable'
        '''
        section_path = line.strip()[1:-1].split()[0]
        section_type = '.'.join(section_path.split('/'))
        return section_type

    @staticmethod
    def get_section_name(line):
        # example: get_section_name('[CFOptions "default"]')
        token_list = line.strip()[1:-1].split('"')
        # token_list = ['CFOptions', 'default', '']
        if len(token_list) < 3:
            return None
        return token_list[1]  # return 'default'

    @staticmethod
    def get_section_str(section_type, section_name):
        # Example:
        # Case 1: get_section_str('DBOptions', NO_COL_FAMILY)
        # Case 2: get_section_str('TableOptions.BlockBasedTable', 'default')
        section_type = '/'.join(section_type.strip().split('.'))
        # Case 1: section_type = 'DBOptions'
        # Case 2: section_type = 'TableOptions/BlockBasedTable'
        section_str = '[' + section_type
        if section_name == NO_COL_FAMILY:
            # Case 1: '[DBOptions]'
            return (section_str + ']')
        else:
            # Case 2: '[TableOptions/BlockBasedTable "default"]'
            return section_str + ' "' + section_name + '"]'

    @staticmethod
    def get_option_str(key, values):
        option_str = key + '='
        # get_option_str('db_log_dir', None), returns 'db_log_dir='
        if values:
            # example:
            # get_option_str('max_bytes_for_level_multiplier_additional',
            # [1,1,1,1,1,1,1]), returned string:
            # 'max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1'
            if isinstance(values, list):
                for value in values:
                    option_str += (str(value) + ':')
                option_str = option_str[:-1]
            else:
                # example: get_option_str('write_buffer_size', 1048576)
                # returned string: 'write_buffer_size=1048576'
                option_str += str(values)
        return option_str


class DatabaseOptions(DataSource):

    @staticmethod
    def is_misc_option(option_name):
        # these are miscellaneous options that are not yet supported by the
        # Rocksdb options file, hence they are not prefixed with any section
        # name
        return '.' not in option_name

    @staticmethod
    def get_options_diff(opt_old, opt_new):
        # type: Dict[option, Dict[col_fam, value]] X 2 ->
        # Dict[option, Dict[col_fam, Tuple(old_value, new_value)]]
        # note: diff should contain a tuple of values only if they are
        # different from each other
        options_union = set(opt_old.keys()).union(set(opt_new.keys()))
        diff = {}
        for opt in options_union:
            diff[opt] = {}
            # if option in options_union, then it must be in one of the configs
            if opt not in opt_old:
                for col_fam in opt_new[opt]:
                    diff[opt][col_fam] = (None, opt_new[opt][col_fam])
            elif opt not in opt_new:
                for col_fam in opt_old[opt]:
                    diff[opt][col_fam] = (opt_old[opt][col_fam], None)
            else:
                for col_fam in opt_old[opt]:
                    if col_fam in opt_new[opt]:
                        if opt_old[opt][col_fam] != opt_new[opt][col_fam]:
                            diff[opt][col_fam] = (
                                opt_old[opt][col_fam],
                                opt_new[opt][col_fam]
                            )
                    else:
                        diff[opt][col_fam] = (opt_old[opt][col_fam], None)
                for col_fam in opt_new[opt]:
                    if col_fam in opt_old[opt]:
                        if opt_old[opt][col_fam] != opt_new[opt][col_fam]:
                            diff[opt][col_fam] = (
                                opt_old[opt][col_fam],
                                opt_new[opt][col_fam]
                            )
                    else:
                        diff[opt][col_fam] = (None, opt_new[opt][col_fam])
            if not diff[opt]:
                diff.pop(opt)
        return diff

    def __init__(self, rocksdb_options, misc_options=None):
        super().__init__(DataSource.Type.DB_OPTIONS)
        # The options are stored in the following data structure:
        # Dict[section_type, Dict[section_name, Dict[option_name, value]]]
        self.options_dict = None
        self.column_families = None
        # Load the options from the given file to a dictionary.
        self.load_from_source(rocksdb_options)
        # Setup the miscellaneous options expected to be List[str], where each
        # element in the List has the format "<option_name>=<option_value>"
        # These options are the ones that are not yet supported by the Rocksdb
        # OPTIONS file, so they are provided separately
        self.setup_misc_options(misc_options)

    def setup_misc_options(self, misc_options):
        self.misc_options = {}
        if misc_options:
            for option_pair_str in misc_options:
                option_name = option_pair_str.split('=')[0].strip()
                option_value = option_pair_str.split('=')[1].strip()
                self.misc_options[option_name] = option_value

    def load_from_source(self, options_path):
        self.options_dict = {}
        with open(options_path, 'r') as db_options:
            for line in db_options:
                line = OptionsSpecParser.remove_trailing_comment(line)
                if not line:
                    continue
                if OptionsSpecParser.is_section_header(line):
                    curr_sec_type = (
                        OptionsSpecParser.get_section_type(line)
                    )
                    curr_sec_name = OptionsSpecParser.get_section_name(line)
                    if curr_sec_type not in self.options_dict:
                        self.options_dict[curr_sec_type] = {}
                    if not curr_sec_name:
                        curr_sec_name = NO_COL_FAMILY
                    self.options_dict[curr_sec_type][curr_sec_name] = {}
                    # example: if the line read from the Rocksdb OPTIONS file
                    # is [CFOptions "default"], then the section type is
                    # CFOptions and 'default' is the name of a column family
                    # that for this database, so it's added to the list of
                    # column families stored in this object
                    if curr_sec_type == 'CFOptions':
                        if not self.column_families:
                            self.column_families = []
                        self.column_families.append(curr_sec_name)
                elif OptionsSpecParser.is_new_option(line):
                    key, value = OptionsSpecParser.get_key_value_pair(line)
                    self.options_dict[curr_sec_type][curr_sec_name][key] = (
                        value
                    )
                else:
                    error = 'Not able to parse line in Options file.'
                    OptionsSpecParser.exit_with_parse_error(line, error)

    def get_misc_options(self):
        # these are options that are not yet supported by the Rocksdb OPTIONS
        # file, hence they are provided and stored separately
        return self.misc_options

    def get_column_families(self):
        return self.column_families

    def get_all_options(self):
        # This method returns all the options that are stored in this object as
        # a: Dict[<sec_type>.<option_name>: Dict[col_fam, option_value]]
        all_options = []
        # Example: in the section header '[CFOptions "default"]' read from the
        # OPTIONS file, sec_type='CFOptions'
        for sec_type in self.options_dict:
            for col_fam in self.options_dict[sec_type]:
                for opt_name in self.options_dict[sec_type][col_fam]:
                    option = sec_type + '.' + opt_name
                    all_options.append(option)
        all_options.extend(list(self.misc_options.keys()))
        return self.get_options(all_options)

    def get_options(self, reqd_options):
        # type: List[str] -> Dict[str, Dict[str, Any]]
        # List[option] -> Dict[option, Dict[col_fam, value]]
        reqd_options_dict = {}
        for option in reqd_options:
            if DatabaseOptions.is_misc_option(option):
                # the option is not prefixed by '<section_type>.' because it is
                # not yet supported by the Rocksdb OPTIONS file; so it has to
                # be fetched from the misc_options dictionary
                if option not in self.misc_options:
                    continue
                if option not in reqd_options_dict:
                    reqd_options_dict[option] = {}
                reqd_options_dict[option][NO_COL_FAMILY] = (
                    self.misc_options[option]
                )
            else:
                # Example: option = 'TableOptions.BlockBasedTable.block_align'
                # then, sec_type = 'TableOptions.BlockBasedTable'
                sec_type = '.'.join(option.split('.')[:-1])
                # opt_name = 'block_align'
                opt_name = option.split('.')[-1]
                if sec_type not in self.options_dict:
                    continue
                for col_fam in self.options_dict[sec_type]:
                    if opt_name in self.options_dict[sec_type][col_fam]:
                        if option not in reqd_options_dict:
                            reqd_options_dict[option] = {}
                        reqd_options_dict[option][col_fam] = (
                            self.options_dict[sec_type][col_fam][opt_name]
                        )
        return reqd_options_dict

    def update_options(self, options):
        # An example 'options' object looks like:
        # {'DBOptions.max_background_jobs': {NO_COL_FAMILY: 2},
        # 'CFOptions.write_buffer_size': {'default': 1048576, 'cf_A': 128000},
        # 'bloom_bits': {NO_COL_FAMILY: 4}}
        for option in options:
            if DatabaseOptions.is_misc_option(option):
                # this is a misc_option i.e. an option that is not yet
                # supported by the Rocksdb OPTIONS file, so it is not prefixed
                # by '<section_type>.' and must be stored in the separate
                # misc_options dictionary
                if NO_COL_FAMILY not in options[option]:
                    print(
                        'WARNING(DatabaseOptions.update_options): not ' +
                        'updating option ' + option + ' because it is in ' +
                        'misc_option format but its scope is not ' +
                        NO_COL_FAMILY + '. Check format of option.'
                    )
                    continue
                self.misc_options[option] = options[option][NO_COL_FAMILY]
            else:
                sec_name = '.'.join(option.split('.')[:-1])
                opt_name = option.split('.')[-1]
                if sec_name not in self.options_dict:
                    self.options_dict[sec_name] = {}
                for col_fam in options[option]:
                    # if the option is not already present in the dictionary,
                    # it will be inserted, else it will be updated to the new
                    # value
                    if col_fam not in self.options_dict[sec_name]:
                        self.options_dict[sec_name][col_fam] = {}
                    self.options_dict[sec_name][col_fam][opt_name] = (
                        copy.deepcopy(options[option][col_fam])
                    )

    def generate_options_config(self, nonce):
        # this method generates a Rocksdb OPTIONS file in the INI format from
        # the options stored in self.options_dict
        this_path = os.path.abspath(os.path.dirname(__file__))
        file_name = '../temp/OPTIONS_' + str(nonce) + '.tmp'
        file_path = os.path.join(this_path, file_name)
        with open(file_path, 'w') as fp:
            for section in self.options_dict:
                for col_fam in self.options_dict[section]:
                    fp.write(
                        OptionsSpecParser.get_section_str(section, col_fam) +
                        '\n'
                    )
                    for option in self.options_dict[section][col_fam]:
                        values = self.options_dict[section][col_fam][option]
                        fp.write(
                            OptionsSpecParser.get_option_str(option, values) +
                            '\n'
                        )
                fp.write('\n')
        return file_path

    def check_and_trigger_conditions(self, conditions):
        for cond in conditions:
            reqd_options_dict = self.get_options(cond.options)
            # This contains the indices of options that are specific to some
            # column family and are not database-wide options.
            incomplete_option_ix = []
            options = []
            missing_reqd_option = False
            for ix, option in enumerate(cond.options):
                if option not in reqd_options_dict:
                    print(
                        'WARNING(DatabaseOptions.check_and_trigger): ' +
                        'skipping condition ' + cond.name + ' because it '
                        'requires option ' + option + ' but this option is' +
                        ' not available'
                    )
                    missing_reqd_option = True
                    break  # required option is absent
                if NO_COL_FAMILY in reqd_options_dict[option]:
                    options.append(reqd_options_dict[option][NO_COL_FAMILY])
                else:
                    options.append(None)
                    incomplete_option_ix.append(ix)

            if missing_reqd_option:
                continue

            # if all the options are database-wide options
            if not incomplete_option_ix:
                try:
                    if eval(cond.eval_expr):
                        cond.set_trigger({NO_COL_FAMILY: options})
                except Exception as e:
                    print(
                        'WARNING(DatabaseOptions) check_and_trigger:' + str(e)
                    )
                continue

            # for all the options that are not database-wide, we look for their
            # values specific to column families
            col_fam_options_dict = {}
            for col_fam in self.column_families:
                present = True
                for ix in incomplete_option_ix:
                    option = cond.options[ix]
                    if col_fam not in reqd_options_dict[option]:
                        present = False
                        break
                    options[ix] = reqd_options_dict[option][col_fam]
                if present:
                    try:
                        if eval(cond.eval_expr):
                            col_fam_options_dict[col_fam] = (
                                copy.deepcopy(options)
                            )
                    except Exception as e:
                        print(
                            'WARNING(DatabaseOptions) check_and_trigger: ' +
                            str(e)
                        )
            # Trigger for an OptionCondition object is of the form:
            # Dict[col_fam_name: List[option_value]]
            # where col_fam_name is the name of a column family for which
            # 'eval_expr' evaluated to True and List[option_value] is the list
            # of values of the options specified in the condition's 'options'
            # field
            if col_fam_options_dict:
                cond.set_trigger(col_fam_options_dict)
