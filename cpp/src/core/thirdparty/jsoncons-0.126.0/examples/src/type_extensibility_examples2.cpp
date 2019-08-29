// Copyright 2017 Daniel Parker
// Distributed under Boost license

#include <string>
#include <unordered_map>
#include <memory>
#include <jsoncons/json.hpp>

using namespace jsoncons;
using namespace jsoncons::literals;

class Employee
{
    std::string name_;
public:
    Employee(const std::string& name)
        : name_(name)
    {
    }
    virtual ~Employee() = default;
    const std::string& name() const
    {
        return name_;
    }
    virtual double calculatePay() const = 0;
};

class HourlyEmployee : public Employee
{
public:
    HourlyEmployee(const std::string& name)
        : Employee(name)
    {
    }
    double calculatePay() const override
    {
        return 10000;
    }
};

class CommissionedEmployee : public Employee
{
public:
    CommissionedEmployee(const std::string& name)
        : Employee(name)
    {
    }
    double calculatePay() const override
    {
        return 20000;
    }
};

class EmployeeRegistry
{
    typedef std::unordered_map<std::string,std::shared_ptr<Employee>> employee_map;
    employee_map employees_;
public:
    EmployeeRegistry()
    {
        employees_.try_emplace("John Smith",std::make_shared<HourlyEmployee>("John Smith"));
        employees_.try_emplace("Jane Doe",std::make_shared<CommissionedEmployee>("Jane Doe"));
    }

    bool contains(const std::string& name) const
    {
        return employees_.count(name) > 0; 
    }

    std::shared_ptr<Employee> get(const std::string& name) const
    {
        auto it = employees_.find(name);
        if (it == employees_.end())
        {
            throw std::runtime_error("Employee not found");
        }
        return it->second; 
    }
};

namespace jsoncons
{
    template<class Json>
    struct json_type_traits<Json, std::shared_ptr<Employee>>
    {
        static bool is(const Json& j, const EmployeeRegistry& registry) noexcept
        {
            return j.is_string() && registry.contains(j.as<std::string>());
        }
        static std::shared_ptr<Employee> as(const Json& j, 
                                            const EmployeeRegistry& registry)
        {
            return registry.get(j.as<std::string>());
        }
        static Json to_json(std::shared_ptr<Employee> val)
        {
            Json j(val->name());
            return j;
        }
    };
};

void type_extensibility_examples2()
{
    std::cout << "\nType extensibility examples 2\n\n";

    json j = R"(
    {
        "EmployeeName" : "John Smith"
    }
    )"_json;

    EmployeeRegistry registry;

    std::shared_ptr<Employee> employee = j["EmployeeName"].as<std::shared_ptr<Employee>>(registry);

    std::cout << "(1) " << employee->name() << " => " 
              << employee->calculatePay() << std::endl;

    // j does not have a key "SalesRep", so get_with_default returns "Jane Doe"
    // The template parameter is explicitly specified as json, to return a json string
    // json::as is then applied to the returned json string  
    std::shared_ptr<Employee> salesRep = j.get_with_default<json>("SalesRep","Jane Doe")
                                          .as<std::shared_ptr<Employee>>(registry);

    std::cout << "(2) " << salesRep->name() << " => " 
              << salesRep->calculatePay() << std::endl;

    json j2;
    j2["EmployeeName"] = employee;
    j2["SalesRep"] = salesRep;

    std::cout << "(3)\n" << pretty_print(j2) << std::endl;
}
