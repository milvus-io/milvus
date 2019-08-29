// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSONPOINTER_JSONPATCH_HPP
#define JSONCONS_JSONPOINTER_JSONPATCH_HPP

#include <string>
#include <sstream>
#include <vector> 
#include <memory>
#include <algorithm> // std::min
#include <utility> // std::move
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch_error.hpp>

namespace jsoncons { namespace jsonpatch {

namespace detail {

    JSONCONS_STRING_LITERAL(test,'t','e','s','t')
    JSONCONS_STRING_LITERAL(add,'a','d','d')
    JSONCONS_STRING_LITERAL(remove,'r','e','m','o','v','e')
    JSONCONS_STRING_LITERAL(replace,'r','e','p','l','a','c','e')
    JSONCONS_STRING_LITERAL(move,'m','o','v','e')
    JSONCONS_STRING_LITERAL(copy,'c','o','p','y')
    JSONCONS_STRING_LITERAL(op,'o','p')
    JSONCONS_STRING_LITERAL(path,'p','a','t','h')
    JSONCONS_STRING_LITERAL(from,'f','r','o','m')
    JSONCONS_STRING_LITERAL(value,'v','a','l','u','e')

    enum class op_type {add,remove,replace};
    enum class state_type {begin,abort,commit};

    template <class Json>
    struct operation_unwinder
    {
        typedef typename Json::string_type string_type;
        typedef typename Json::string_view_type string_view_type;

        struct entry
        {
            op_type op;
            string_type path;
            Json value;
        };

        Json& target;
        state_type state;
        std::vector<entry> stack;

        operation_unwinder(Json& j)
            : target(j), state(state_type::begin)
        {
        }

        ~operation_unwinder()
        {
            std::error_code ec;
            //std::cout << "state: " << std::boolalpha << (state == state_type::commit) << ", stack size: " << stack.size() << std::endl;
            if (state != state_type::commit)
            {
                for (auto it = stack.rbegin(); it != stack.rend(); ++it)
                {
                    if (it->op == op_type::add)
                    {
                        jsonpointer::insert_or_assign(target,it->path,it->value,ec);
                        if (ec)
                        {
                            //std::cout << "add: " << it->path << std::endl;
                            break;
                        }
                    }
                    else if (it->op == op_type::remove)
                    {
                        jsonpointer::remove(target,it->path,ec);
                        if (ec)
                        {
                            //std::cout << "remove: " << it->path << std::endl;
                            break;
                        }
                    }
                    else if (it->op == op_type::replace)
                    {
                        jsonpointer::replace(target,it->path,it->value,ec);
                        if (ec)
                        {
                            //std::cout << "replace: " << it->path << std::endl;
                            break;
                        }
                    }
                }
            }
        }
    };

    template <class Json>
    Json from_diff(const Json& source, const Json& target, const typename Json::string_type& path)
    {
        typedef typename Json::char_type char_type;

        Json result = typename Json::array();

        if (source == target)
        {
            return result;
        }

        if (source.is_array() && target.is_array())
        {
            size_t common = (std::min)(source.size(),target.size());
            for (size_t i = 0; i < common; ++i)
            {
                std::basic_ostringstream<char_type> ss; 
                ss << path << '/' << i;
                auto temp_diff = from_diff(source[i],target[i],ss.str());
                result.insert(result.array_range().end(),temp_diff.array_range().begin(),temp_diff.array_range().end());
            }
            // Element in source, not in target - remove
            for (size_t i = target.size(); i < source.size(); ++i)
            {
                std::basic_ostringstream<char_type> ss; 
                ss << path << '/' << i;
                Json val = typename Json::object();
                val.insert_or_assign(op_literal<char_type>(), remove_literal<char_type>());
                val.insert_or_assign(path_literal<char_type>(), ss.str());
                result.push_back(std::move(val));
            }
            // Element in target, not in source - add, 
            // Fix contributed by Alexander rog13
            for (size_t i = source.size(); i < target.size(); ++i)
            {
                const auto& a = target[i];
                std::basic_ostringstream<char_type> ss; 
                ss << path << '/' << i;
                Json val = typename Json::object();
                val.insert_or_assign(op_literal<char_type>(), add_literal<char_type>());
                val.insert_or_assign(path_literal<char_type>(), ss.str());
                val.insert_or_assign(value_literal<char_type>(), a);
                result.push_back(std::move(val));
            }
        }
        else if (source.is_object() && target.is_object())
        {
            for (const auto& a : source.object_range())
            {
                std::basic_ostringstream<char_type> ss; 
                ss << path << '/';
                jsonpointer::escape(a.key(),ss);
                auto it = target.find(a.key());
                if (it != target.object_range().end())
                {
                    auto temp_diff = from_diff(a.value(),it->value(),ss.str());
                    result.insert(result.array_range().end(),temp_diff.array_range().begin(),temp_diff.array_range().end());
                }
                else
                {
                    Json val = typename Json::object();
                    val.insert_or_assign(op_literal<char_type>(), remove_literal<char_type>());
                    val.insert_or_assign(path_literal<char_type>(), ss.str());
                    result.push_back(std::move(val));
                }
            }
            for (const auto& a : target.object_range())
            {
                auto it = source.find(a.key());
                if (it == source.object_range().end())
                {
                    std::basic_ostringstream<char_type> ss; 
                    ss << path << '/';
                    jsonpointer::escape(a.key(),ss);
                    Json val = typename Json::object();
                    val.insert_or_assign(op_literal<char_type>(), add_literal<char_type>());
                    val.insert_or_assign(path_literal<char_type>(), ss.str());
                    val.insert_or_assign(value_literal<char_type>(), a.value());
                    result.push_back(std::move(val));
                }
            }
        }
        else
        {
            Json val = typename Json::object();
            val.insert_or_assign(op_literal<char_type>(), replace_literal<char_type>());
            val.insert_or_assign(path_literal<char_type>(), path);
            val.insert_or_assign(value_literal<char_type>(), target);
            result.push_back(std::move(val));
        }

        return result;
    }
}

template <class Json>
void apply_patch(Json& target, const Json& patch, std::error_code& patch_ec)
{
    typedef typename Json::char_type char_type;
    typedef typename Json::string_type string_type;
    typedef typename Json::string_view_type string_view_type;

   jsoncons::jsonpatch::detail::operation_unwinder<Json> unwinder(target);

    // Validate
    
    string_type bad_path;
    for (const auto& operation : patch.array_range())
    {
        unwinder.state =jsoncons::jsonpatch::detail::state_type::begin;

        if (operation.count(detail::op_literal<char_type>()) != 1 || operation.count(detail::path_literal<char_type>()) != 1)
        {
            patch_ec = jsonpatch_errc::invalid_patch;
            unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
        }
        else
        {
            string_view_type op = operation.at(detail::op_literal<char_type>()).as_string_view();
            string_view_type path = operation.at(detail::path_literal<char_type>()).as_string_view();

            if (op ==jsoncons::jsonpatch::detail::test_literal<char_type>())
            {
                std::error_code ec;
                Json val = jsonpointer::get(target,path,ec);
                if (ec)
                {
                    patch_ec = jsonpatch_errc::test_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else if (operation.count(detail::value_literal<char_type>()) != 1)
                {
                    patch_ec = jsonpatch_errc::invalid_patch;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else if (val != operation.at(detail::value_literal<char_type>()))
                {
                    patch_ec = jsonpatch_errc::test_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
            }
            else if (op ==jsoncons::jsonpatch::detail::add_literal<char_type>())
            {
                if (operation.count(detail::value_literal<char_type>()) != 1)
                {
                    patch_ec = jsonpatch_errc::invalid_patch;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else
                {
                    std::error_code insert_ec;
                    Json val = operation.at(detail::value_literal<char_type>());
                    auto npath = jsonpointer::normalized_path(target,path);
                    jsonpointer::insert(target,npath,val,insert_ec); // try insert without replace
                    if (insert_ec) // try a replace
                    {
                        std::error_code select_ec;
                        Json orig_val = jsonpointer::get(target,npath,select_ec);
                        if (select_ec) // shouldn't happen
                        {
                            patch_ec = jsonpatch_errc::add_failed;
                            unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                        }
                        else
                        {
                            std::error_code replace_ec;
                            jsonpointer::replace(target,npath,val,replace_ec);
                            if (replace_ec)
                            {
                                patch_ec = jsonpatch_errc::add_failed;
                                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                            }
                            else
                            {
                                unwinder.stack.push_back({detail::op_type::replace,npath,orig_val});
                            }
                        }
                    }
                    else // insert without replace succeeded
                    {
                        unwinder.stack.push_back({detail::op_type::remove,npath,Json::null()});
                    }
                }
            }
            else if (op ==jsoncons::jsonpatch::detail::remove_literal<char_type>())
            {
                std::error_code ec;
                Json val = jsonpointer::get(target,path,ec);
                if (ec)
                {
                    patch_ec = jsonpatch_errc::remove_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else
                {
                    jsonpointer::remove(target,path,ec);
                    if (ec)
                    {
                        patch_ec = jsonpatch_errc::remove_failed;
                        unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    }
                    else
                    {
                        unwinder.stack.push_back({detail::op_type::add,string_type(path),val});
                    }
                }
            }
            else if (op ==jsoncons::jsonpatch::detail::replace_literal<char_type>())
            {
                std::error_code ec;
                Json val = jsonpointer::get(target,path,ec);
                if (ec)
                {
                    patch_ec = jsonpatch_errc::replace_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else if (operation.count(detail::value_literal<char_type>()) != 1)
                {
                    patch_ec = jsonpatch_errc::invalid_patch;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else
                {
                    jsonpointer::replace(target,path,operation.at(detail::value_literal<char_type>()),ec);
                    if (ec)
                    {
                        patch_ec = jsonpatch_errc::replace_failed;
                        unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    }
                    else
                    {
                        unwinder.stack.push_back({detail::op_type::replace,string_type(path),val});
                    }
                }
            }
            else if (op ==jsoncons::jsonpatch::detail::move_literal<char_type>())
            {
                if (operation.count(detail::from_literal<char_type>()) != 1)
                {
                    patch_ec = jsonpatch_errc::invalid_patch;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else
                {
                    string_view_type from = operation.at(detail::from_literal<char_type>()).as_string_view();
                    std::error_code ec;
                    Json val = jsonpointer::get(target,from,ec);
                    if (ec)
                    {
                        patch_ec = jsonpatch_errc::move_failed;
                        unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    }
                    else 
                    {
                        jsonpointer::remove(target,from,ec);
                        if (ec)
                        {
                            patch_ec = jsonpatch_errc::move_failed;
                            unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                        }
                        else
                        {
                            unwinder.stack.push_back({detail::op_type::add,string_type(from),val});
                            // add
                            std::error_code insert_ec;
                            auto npath = jsonpointer::normalized_path(target,path);
                            jsonpointer::insert(target,npath,val,insert_ec); // try insert without replace
                            if (insert_ec) // try a replace
                            {
                                std::error_code select_ec;
                                Json orig_val = jsonpointer::get(target,npath,select_ec);
                                if (select_ec) // shouldn't happen
                                {
                                    patch_ec = jsonpatch_errc::copy_failed;
                                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                                }
                                else
                                {
                                    std::error_code replace_ec;
                                    jsonpointer::replace(target, npath, val, replace_ec);
                                    if (replace_ec)
                                    {
                                        patch_ec = jsonpatch_errc::copy_failed;
                                        unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;

                                    }
                                    else
                                    {
                                        unwinder.stack.push_back({jsoncons::jsonpatch::detail::op_type::replace,npath,orig_val });
                                    }
                                    
                                }
                            }
                            else
                            {
                                unwinder.stack.push_back({detail::op_type::remove,npath,Json::null()});
                            }
                        }           
                    }
                }
            }
            else if (op ==jsoncons::jsonpatch::detail::copy_literal<char_type>())
            {
                if (operation.count(detail::from_literal<char_type>()) != 1)
                {
                    patch_ec = jsonpatch_errc::invalid_patch;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                }
                else
                {
                    std::error_code ec;
                    string_view_type from = operation.at(detail::from_literal<char_type>()).as_string_view();
                    Json val = jsonpointer::get(target,from,ec);
                    if (ec)
                    {
                        patch_ec = jsonpatch_errc::copy_failed;
                        unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    }
                    else
                    {
                        // add
                        auto npath = jsonpointer::normalized_path(target,path);
                        std::error_code insert_ec;
                        jsonpointer::insert(target,npath,val,insert_ec); // try insert without replace
                        if (insert_ec) // Failed, try a replace
                        {
                            std::error_code select_ec;
                            Json orig_val = jsonpointer::get(target,npath, select_ec);
                            if (select_ec) // shouldn't happen
                            {
                                patch_ec = jsonpatch_errc::copy_failed;
                                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                            }
                            else 
                            {
                                std::error_code replace_ec;
                                jsonpointer::replace(target, npath, val,replace_ec);
                                if (replace_ec)
                                {
                                    patch_ec = jsonpatch_errc::copy_failed;
                                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                                }
                                else
                                {
                                    unwinder.stack.push_back({jsoncons::jsonpatch::detail::op_type::replace,npath,orig_val });
                                }
                            }
                        }
                        else
                        {
                            unwinder.stack.push_back({detail::op_type::remove,npath,Json::null()});
                        }
                    }
                }
            }
            if (unwinder.state !=jsoncons::jsonpatch::detail::state_type::begin)
            {
                bad_path = string_type(path);
            }
        }
        if (unwinder.state !=jsoncons::jsonpatch::detail::state_type::begin)
        {
            break;
        }
    }
    if (unwinder.state ==jsoncons::jsonpatch::detail::state_type::begin)
    {
        unwinder.state =jsoncons::jsonpatch::detail::state_type::commit;
    }
}

template <class Json>
Json from_diff(const Json& source, const Json& target)
{
    typename Json::string_type path;
    return jsoncons::jsonpatch::detail::from_diff(source, target, path);
}

template <class Json>
void apply_patch(Json& target, const Json& patch)
{
    std::error_code ec;
    apply_patch(target, patch, ec);
    if (ec)
    {
        JSONCONS_THROW(jsonpatch_error(ec));
    }
}

}}

#endif
