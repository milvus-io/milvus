// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <sstream>
#include <tuple>

#include "db/metax/MetaQuery.h"
#include "db/metax/MetaQueryTraits.h"
#include "db/metax/MetaResField.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::metax {

template <typename T>
inline std::string
v2sqlstring(const T& t) {
    if constexpr(std::is_arithmetic_v<T>) {
        return std::to_string(t);
    } else if constexpr (decay_equal_v<T, std::string>) {
        return "\'" + t + "\'";
    } else if constexpr (decay_equal_v<T, json>) {
        // TODO(yhz): Json to string
        return "";
    } else {
        throw std::runtime_error("Unknown value type");
    }
}

///////////////

template <typename R, typename F>
inline std::string
SerializeColumn(Column<R,F> c, bool as = false) {
    std::string tfs = std::string(c.ResName) + "." + c.FieldName;
    std::stringstream ss;
    ss << tfs;
    if (as) {
        ss << " AS " << v2sqlstring<std::string>(tfs);
    }
    return ss.str();
}

template<int index, typename... Ts>
struct ColumnsTraverser {
    void operator() (std::tuple<Ts...>& t, std::vector<std::string>& selected, bool as = false) {
        selected.emplace_back(SerializeColumn(std::get<index>(t), as));
        ColumnsTraverser<index - 1, Ts...>{}(t, selected, as);
    }
};

template<typename... Ts>
struct ColumnsTraverser<0, Ts...> {
    void operator() (std::tuple<Ts...>& t, std::vector<std::string>& selected, bool as = false) {
        selected.emplace_back(SerializeColumn(std::get<0>(t), as));
    }
};

template<typename... Ts>
void TraverseColumns(Columns<Ts...>& t, std::vector<std::string>& selected, bool as = false) {
    size_t constexpr size = std::tuple_size<std::tuple<Ts...>>::value;
    decltype(auto) columns = t.cols_;
    ColumnsTraverser<size - 1, Ts...>{}(columns, selected, as);
}

template<int index, typename T>
struct TableTraverser {
    void operator() (MetaResFieldTuple& t, Table<T> tab, std::vector<std::string>& selected, bool as = false) {
        using element_type = typename std::tuple_element_t<index, MetaResFieldTuple>::FType;
        if constexpr(is_decay_base_of_v<element_type, T>) {
            selected.emplace_back(SerializeColumn(Col_<T, element_type>(), as));
            TableTraverser<index - 1, T>{}(t, tab, selected, as);
            return;
        } else {
            TableTraverser<index - 1, T>{}(t, tab, selected, as);
            return;
        }
//        selected.emplace_back(SerializeColumn(std::get<index>(t), as));
//        ColumnsTraverser<index - 1, Ts...>{}(t, selected, as);
    }
};

template<typename T>
struct TableTraverser<0, T> {
    void operator() (MetaResFieldTuple& t, Table<T> tab, std::vector<std::string>& selected, bool as = false) {
        using element_type = std::tuple_element_t<0, MetaResFieldTuple>;
        if constexpr(is_decay_base_of_v<element_type, T>) {
            selected.emplace_back(SerializeColumn(Col_<T, element_type>(), as));
            return;
        } else {
            return;
        }
    }
};

template<typename T>
void TraverseColumns(Table<T>& t, std::vector<std::string>& selected, bool as = false) {
    MetaResFieldTuple fields;
    size_t constexpr size = std::tuple_size<decltype(fields)>::value;
    TableTraverser<size - 1, T>{}(fields, t, selected, as);
}
/////////////////////////////////////////////////////////////
template <typename R>
inline std::string
SerializeTable(Table<R>& c) {
    return std::string(c.name);
}

template<int index, typename... Ts>
struct TablesTraverser {
    void operator() (std::tuple<Ts...>& t, std::vector<std::string>& selected) {
        selected.emplace_back(SerializeTable(std::get<index>(t)));
        TablesTraverser<index - 1, Ts...>{}(t);
    }
};

template<typename... Ts>
struct TablesTraverser<0, Ts...> {
    void operator() (std::tuple<Ts...>& t, std::vector<std::string>& selected) {
        selected.emplace_back(SerializeTable(std::get<0>(t)));
    }
};

template<typename... Ts>
void TraverseTables(Tables<Ts...>& t, std::vector<std::string>& selected) {
    size_t constexpr size = std::tuple_size<std::tuple<Ts...>>::value;
    decltype(auto) tables = t.tabs_;
    TablesTraverser<size - 1, Ts...>{}(tables, selected);
}

template<typename... Ts>
void TraverseTables(From<Ts...>& t, std::string& selected) {
    size_t constexpr size = std::tuple_size<std::tuple<Ts...>>::value;
    decltype(auto) tables = t.args_;

    std::vector<std::string> selecteds;
    TablesTraverser<size - 1, Ts...>{}(tables, selecteds);

    std::string table_out;
    StringHelpFunctions::MergeStringWithDelimeter(selecteds, ",", table_out);
    selected = " FROM " + table_out;
}

template<typename... Ts>
void TraverseTables(FullJoin<Ts...>& t, std::vector<std::string>& selected) {
    size_t constexpr size = std::tuple_size<std::tuple<Ts...>>::value;
    decltype(auto) tables = t.tabs_;
    TablesTraverser<size - 1, Ts...>{}(tables, selected);
}

//////////////////////
//template <typename R, typename F>
//inline std::string
//SerializeRangeItem(Column<R, F> col) {
//    return SerializeColumn<R, F>(col);
//}

template <typename T>
inline std::string
SerializeRangeItem(T t) {
    if constexpr(is_column_v<T>) {
        return SerializeColumn<typename T::RType, typename T::FType>(t);
    } else if constexpr(std::is_arithmetic_v<T>) {
        return std::to_string(t);
    } else if constexpr(decay_equal_v<T, std::string>) {
        return v2sqlstring<std::string>(t);
    } else {
        static_assert(is_column_v<T>, "Unknown Range item type");
        return "";
    }
//    return std::to_string(num);
}

//template <typename T>
//inline std::string
//SerializeRangeItem(std::enable_if_t<decay_equal_v<T, std::string>, T>& str) {
//    return v2sqlstring<std::string>(str);
//}

// TODO(yhz): To be completed
template <typename L, typename R>
inline std::string
SerializeConditionCase(Range<L, R>& range) {
    switch (range.range_type_) {
        case eq_: {
            return SerializeRangeItem(range.l_cond_) + " = " + SerializeRangeItem(range.r_cond_);
        }
        case neq_: {
            return SerializeRangeItem(range.l_cond_) + " != " + SerializeRangeItem(range.r_cond_);
        }
        case gt_: {
            return SerializeRangeItem(range.l_cond_) + " > " + SerializeRangeItem(range.r_cond_);
        }
        case gte_: {
            return SerializeRangeItem(range.l_cond_) + " >= " + SerializeRangeItem(range.r_cond_);
        }
        case lt_: {
            return SerializeRangeItem(range.l_cond_) + " < " + SerializeRangeItem(range.r_cond_);
        }
        case lte_: {
            return SerializeRangeItem(range.l_cond_) + " <= " + SerializeRangeItem(range.r_cond_);
        }
        default: {
            throw std::runtime_error("Unknown condition query expression");
        }
    }
}

template <template<typename...> typename H, typename V, typename C>
inline std::string
SerializeConditionCase(In<H, V, C>& in) {
    static_assert(is_column_v<typename In<H, V, C>::CType>);
    std::string col_str = SerializeColumn(in.col_);
    std::stringstream ss;
    ss << col_str << " IN " << "(";
    std::vector<std::string> vss;
    for (auto& v : in.value_) {
        vss.emplace_back(v2sqlstring(v));
    }
    std::string vsout;
    StringHelpFunctions::MergeStringWithDelimeter(vss, ",", vsout);

    ss << vsout << ")";
    return ss.str();
}

////////////// Traverse case item
template<int index, typename... Ts>
struct ConditionRelationTraverser {
    void operator() (std::tuple<Ts...>& t, std::vector<std::string>& selected) {
        selected.emplace_back(SerializeConditionCase(std::get<index>(t)));
        ConditionRelationTraverser<index - 1, Ts...>{}(t, selected);
    }
};

template<typename... Ts>
struct ConditionRelationTraverser<0, Ts...> {
    void operator() (std::tuple<Ts...>& t, std::vector<std::string>& selected) {
        selected.emplace_back(SerializeConditionCase(std::get<0>(t)));
    }
};

template<typename... Ts>
void TraverseConditionRelation(And<Ts...>& t, std::vector<std::string>& selected) {
    size_t constexpr size = std::tuple_size_v<typename And<Ts...>::cond_tuple>;
    decltype(auto) conds = t.conds_;
    ConditionRelationTraverser<size - 1, Ts...>{}(conds, selected);
}

template<typename... Ts>
void TraverseConditionRelation(Or<Ts...>& t, std::vector<std::string>& selected) {
    size_t constexpr size = std::tuple_size_v<typename And<Ts...>::cond_tuple>;
    decltype(auto) conds = t.conds_;
    ConditionRelationTraverser<size - 1, Ts...>{}(conds, selected);
}

///// One case

template <typename Case>
inline std::string
SerializeConditionRelation(One<Case>& c) {
    return SerializeConditionCase(c.cond_);
}

// TODO(yhz): And case
template <typename... Args>
inline std::string
SerializeConditionRelation(And<Args...> c) {
    std::vector<std::string> selected;
    TraverseConditionRelation(c, selected);
    std::string out;
    StringHelpFunctions::MergeStringWithDelimeter(selected, " AND ", out);
    return out;
}

// TODO(yhz): Or case
template <typename...Args>
inline std::string
SerializeConditionRelation(Or<Args...>& c) {
    std::vector<std::string> selected;
    TraverseConditionRelation(c, selected);
    std::string out;
    StringHelpFunctions::MergeStringWithDelimeter(selected, " OR ", out);
    return out;
}

///////////////////

template<int index, typename... Ts>
struct ConditionsTraverser {
    void operator() (std::tuple<Ts...>& t, std::string& selected) {
        selected += SerializeConditionRelation(std::get<index>(t));
        ConditionsTraverser<index - 1, Ts...>{}(t);
    }
};

template<typename... Ts>
struct ConditionsTraverser<0, Ts...> {
    void operator() (std::tuple<Ts...>& t, std::string& selected) {
        selected += SerializeConditionRelation(std::get<0>(t));
    }
};

template<typename... Ts>
void
TraverseConditions(Where<Ts...>& t, std::string& selected) {
    size_t constexpr size = std::tuple_size<std::tuple<Ts...>>::value;
    decltype(auto) conds = t.condition_;
    ConditionsTraverser<size - 1, Ts...>{}(conds, selected);
    selected = " WHERE " + selected;
}

template<typename... Ts>
void
TraverseConditions(On<Ts...>& t, std::string& selected) {
    size_t constexpr size = std::tuple_size<std::tuple<Ts...>>::value;
    decltype(auto) conds = t.condition_;
    ConditionsTraverser<size - 1, Ts...>{}(conds, selected);
}

}  // namespace milvus::engine::metax
