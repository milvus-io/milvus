#ifndef REGISTRY_TEST_H_
#define REGISTRY_TEST_H_

#include "test.h"

class Person {
public:
    Person(const std::string& name, unsigned int num) : m_name(name), m_num(num) {}
    const std::string& name(void) const { return m_name; }
    unsigned int num(void) const { return m_num; }
private:
    std::string m_name;
    unsigned int m_num;
};

class PersonPred {
public:
    PersonPred(const std::string& name, unsigned int num) : name(name), n(num) {

    }
    bool operator()(const Person* p) {
        return p != nullptr && p->name() == name && p->num() == n;
    }
private:
    std::string name;
    unsigned int n;
};

class People : public Registry<Person> {
public:
    void regNew(const char* name, Person* person) {
        Registry<Person>::registerNew(name, person);
    }
    void clear() {
        Registry<Person>::unregisterAll();
    }
    Person* getPerson(const char* name) {
        return Registry<Person>::get(name);
    }
};

class PeopleWithPred : public RegistryWithPred<Person, PersonPred> {
public:
    void regNew(Person* person) {
        RegistryWithPred<Person, PersonPred>::registerNew(person);
    }
    void clear() {
        RegistryWithPred<Person, PersonPred>::unregisterAll();
    }
    Person* get(const std::string& name, unsigned int numb) {
        return RegistryWithPred<Person, PersonPred>::get(name, numb);
    }
};

/// Tests for usage of registry (Thread unsafe but its OK with gtest)
TEST(RegistryTest, RegisterAndUnregister) {
    People people;
    Person* john = new Person("John", 433212345);
    people.regNew("John", john);

    Person* john2 = new Person("John", 123456);
    people.regNew("John", john2);

    EXPECT_EQ(1, people.size());
    unsigned int n = people.getPerson("John")->num();
    EXPECT_EQ(n, 123456);

    People people2;
    people2 = people;
    EXPECT_EQ(1, people2.size());
    EXPECT_EQ(1, people.size());

    people.clear();
    EXPECT_TRUE(people.empty());
    EXPECT_EQ(1, people2.size());
    people2.clear();
    EXPECT_TRUE(people2.empty());

    PeopleWithPred peopleWithPred;
    peopleWithPred.regNew(new Person("McDonald", 123));
    peopleWithPred.regNew(new Person("McDonald", 157));
    EXPECT_EQ(peopleWithPred.size(), 2);

    Person *p = peopleWithPred.get("McDonald", 157);
    EXPECT_EQ(p->name(), "McDonald");
    EXPECT_EQ(p->num(), 157);

    PeopleWithPred peopleWithPred2;
    peopleWithPred2 = peopleWithPred;
    EXPECT_EQ(peopleWithPred.size(), 2);
    EXPECT_EQ(peopleWithPred2.size(), 2);

    peopleWithPred.clear();
    EXPECT_TRUE(peopleWithPred.empty());
    peopleWithPred2.clear();
    EXPECT_TRUE(peopleWithPred2.empty());
}

#endif // REGISTRY_TEST_H_
