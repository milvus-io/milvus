// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_ARGUMENTSPARSER_H_
#define _SPTAG_HELPER_ARGUMENTSPARSER_H_

#include "inc/Helper/StringConvert.h"

#include <cstdint>
#include <cstddef> 
#include <memory>
#include <vector>
#include <string>

namespace SPTAG
{
namespace Helper
{

class ArgumentsParser
{
public:
    ArgumentsParser();

    virtual ~ArgumentsParser();

    virtual bool Parse(int p_argc, char** p_args);

    virtual void PrintHelp();

protected:
    class IArgument
    {
    public:
        IArgument();

        virtual ~IArgument();

        virtual bool ParseValue(int& p_restArgc, char** (&p_args)) = 0;

        virtual void PrintDescription(FILE* p_output) = 0;

        virtual bool IsRequiredButNotSet() const = 0;
    };


    template<typename DataType>
    class ArgumentT : public IArgument
    {
    public:
        ArgumentT(DataType& p_target,
                  const std::string& p_representStringShort,
                  const std::string& p_representString,
                  const std::string& p_description,
                  bool p_followedValue,
                  const DataType& p_switchAsValue,
                  bool p_isRequired)
            : m_value(p_target),
              m_representStringShort(p_representStringShort),
              m_representString(p_representString),
              m_description(p_description),
              m_followedValue(p_followedValue),
              c_switchAsValue(p_switchAsValue),
              m_isRequired(p_isRequired),
              m_isSet(false)
        {
        }

        virtual ~ArgumentT()
        {
        }


        virtual bool ParseValue(int& p_restArgc, char** (&p_args))
        {
            if (0 == p_restArgc)
            {
                return true;
            }

            if (0 != strcmp(*p_args, m_representString.c_str())
                && 0 != strcmp(*p_args, m_representStringShort.c_str()))
            {
                return true;
            }

            if (!m_followedValue)
            {
                m_value = c_switchAsValue;
                --p_restArgc;
                ++p_args;
                m_isSet = true;
                return true;
            }

            if (p_restArgc < 2)
            {
                return false;
            }

            DataType tmp;
            if (!Helper::Convert::ConvertStringTo(p_args[1], tmp))
            {
                return false;
            }

            m_value = std::move(tmp);

            p_restArgc -= 2;
            p_args += 2;
            m_isSet = true;
            return true;
        }


        virtual void PrintDescription(FILE* p_output)
        {
            std::size_t padding = 30;
            if (!m_representStringShort.empty())
            {
                fprintf(p_output, "%s", m_representStringShort.c_str());
                padding -= m_representStringShort.size();
            }

            if (!m_representString.empty())
            {
                if (!m_representStringShort.empty())
                {
                    fprintf(p_output, ", ");
                    padding -= 2;
                }

                fprintf(p_output, "%s", m_representString.c_str());
                padding -= m_representString.size();
            }

            if (m_followedValue)
            {
                fprintf(p_output, " <value>");
                padding -= 8;
            }

            while (padding-- > 0)
            {
                fputc(' ', p_output);
            }

            fprintf(p_output, "%s", m_description.c_str());
        }


        virtual bool IsRequiredButNotSet() const
        {
            return m_isRequired && !m_isSet;
        }

    private:
        DataType & m_value;

        std::string m_representStringShort;

        std::string m_representString;

        std::string m_description;

        bool m_followedValue;

        const DataType c_switchAsValue;

        bool m_isRequired;

        bool m_isSet;
    };


    template<typename DataType>
    void AddRequiredOption(DataType& p_target,
                           const std::string& p_representStringShort,
                           const std::string& p_representString,
                           const std::string& p_description)
    {
        m_arguments.emplace_back(std::shared_ptr<IArgument>(
            new ArgumentT<DataType>(p_target,
                                    p_representStringShort,
                                    p_representString,
                                    p_description,
                                    true,
                                    DataType(),
                                    true)));
    }


    template<typename DataType>
    void AddOptionalOption(DataType& p_target,
                           const std::string& p_representStringShort,
                           const std::string& p_representString,
                           const std::string& p_description)
    {
        m_arguments.emplace_back(std::shared_ptr<IArgument>(
            new ArgumentT<DataType>(p_target,
                                    p_representStringShort,
                                    p_representString,
                                    p_description,
                                    true,
                                    DataType(),
                                    false)));
    }


    template<typename DataType>
    void AddRequiredSwitch(DataType& p_target,
                           const std::string& p_representStringShort,
                           const std::string& p_representString,
                           const std::string& p_description,
                           const DataType& p_switchAsValue)
    {
        m_arguments.emplace_back(std::shared_ptr<IArgument>(
            new ArgumentT<DataType>(p_target,
                                    p_representStringShort,
                                    p_representString,
                                    p_description,
                                    false,
                                    p_switchAsValue,
                                    true)));
    }


    template<typename DataType>
    void AddOptionalSwitch(DataType& p_target,
                           const std::string& p_representStringShort,
                           const std::string& p_representString,
                           const std::string& p_description,
                           const DataType& p_switchAsValue)
    {
        m_arguments.emplace_back(std::shared_ptr<IArgument>(
            new ArgumentT<DataType>(p_target,
                                    p_representStringShort,
                                    p_representString,
                                    p_description,
                                    false,
                                    p_switchAsValue,
                                    false)));
    }

private:
    std::vector<std::shared_ptr<IArgument>> m_arguments;
};


} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_ARGUMENTSPARSER_H_
