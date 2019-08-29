// Copyright 2016 Daniel Parker
// Distributed under Boost license

#include "example_types.hpp"
#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <iomanip>
#include <jsoncons/json.hpp>

using namespace jsoncons;

namespace ns {

class Employee
{
    std::string firstName_;
    std::string lastName_;
public:
    Employee(const std::string& firstName, const std::string& lastName)
        : firstName_(firstName), lastName_(lastName)
    {
    }
    virtual ~Employee() = default;

    virtual double calculatePay() const = 0;

    const std::string& firstName() const {return firstName_;}
    const std::string& lastName() const {return lastName_;}
};

class HourlyEmployee : public Employee
{
    double wage_;
    unsigned hours_;
public:
    HourlyEmployee(const std::string& firstName, const std::string& lastName, 
                   double wage, unsigned hours)
        : Employee(firstName, lastName), 
          wage_(wage), hours_(hours)
    {
    }
    HourlyEmployee(const HourlyEmployee&) = default;
    HourlyEmployee(HourlyEmployee&&) = default;
    HourlyEmployee& operator=(const HourlyEmployee&) = default;
    HourlyEmployee& operator=(HourlyEmployee&&) = default;

    double wage() const {return wage_;}

    unsigned hours() const {return hours_;}

    double calculatePay() const override
    {
        return wage_*hours_;
    }
};

class CommissionedEmployee : public Employee
{
    double baseSalary_;
    double commission_;
    unsigned sales_;
public:
    CommissionedEmployee(const std::string& firstName, const std::string& lastName, 
                         double baseSalary, double commission, unsigned sales)
        : Employee(firstName, lastName), 
          baseSalary_(baseSalary), commission_(commission), sales_(sales)
    {
    }
    CommissionedEmployee(const CommissionedEmployee&) = default;
    CommissionedEmployee(CommissionedEmployee&&) = default;
    CommissionedEmployee& operator=(const CommissionedEmployee&) = default;
    CommissionedEmployee& operator=(CommissionedEmployee&&) = default;

    double baseSalary() const
    {
        return baseSalary_;
    }

    double commission() const
    {
        return commission_;
    }

    unsigned sales() const
    {
        return sales_;
    }

    double calculatePay() const override
    {
        return baseSalary_ + commission_*sales_;
    }
};
} // ns

JSONCONS_GETTER_CTOR_TRAITS_DECL(ns::HourlyEmployee, firstName, lastName, wage, hours)
JSONCONS_GETTER_CTOR_TRAITS_DECL(ns::CommissionedEmployee, firstName, lastName, baseSalary, commission, sales)

namespace jsoncons {

template<class Json>
struct json_type_traits<Json, std::shared_ptr<ns::Employee>> 
{
    static bool is(const Json& j) noexcept
    { 
        return j.is<ns::HourlyEmployee>() || j.is<ns::CommissionedEmployee>();
    }
    static std::shared_ptr<ns::Employee> as(const Json& j)
    {   
        if (j.at("type").as<std::string>() == "Hourly")
        {
            return std::make_shared<ns::HourlyEmployee>(j.as<ns::HourlyEmployee>());
        }
        else if (j.at("type").as<std::string>() == "Commissioned")
        {
            return std::make_shared<ns::CommissionedEmployee>(j.as<ns::CommissionedEmployee>());
        }
        else
        {
            throw std::runtime_error("Not an employee");
        }
    }
    static Json to_json(const std::shared_ptr<ns::Employee>& ptr)
    {
        if (ns::HourlyEmployee* p = dynamic_cast<ns::HourlyEmployee*>(ptr.get()))
        {
            Json j(*p);
            j.try_emplace("type","Hourly");
            return j;
        }
        else if (ns::CommissionedEmployee* p = dynamic_cast<ns::CommissionedEmployee*>(ptr.get()))
        {
            Json j(*p);
            j.try_emplace("type","Commissioned");
            return j;
        }
        else
        {
            throw std::runtime_error("Not an employee");
        }
    }
};

} // jsoncons

void employee_polymorphic_example()
{
    std::vector<std::shared_ptr<ns::Employee>> v;

    v.push_back(std::make_shared<ns::HourlyEmployee>("John", "Smith", 40.0, 1000));
    v.push_back(std::make_shared<ns::CommissionedEmployee>("Jane", "Doe", 30000, 0.25, 1000));

    json j(v);
    std::cout << pretty_print(j) << "\n\n";

    assert(j[0].is<ns::HourlyEmployee>());
    assert(!j[0].is<ns::CommissionedEmployee>());
    assert(!j[1].is<ns::HourlyEmployee>());
    assert(j[1].is<ns::CommissionedEmployee>());


    for (size_t i = 0; i < j.size(); ++i)
    {
        auto p = j[i].as<std::shared_ptr<ns::Employee>>();
        assert(p->firstName() == v[i]->firstName());
        assert(p->lastName() == v[i]->lastName());
        assert(p->calculatePay() == v[i]->calculatePay());
    }
}

void polymorphic_examples()
{
    std::cout << "\nType extensibility examples\n\n";

    employee_polymorphic_example();

    std::cout << std::endl;
}

