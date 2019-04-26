/**
 *  Implemented this example https://www.w3resource.com/sqlite/sqlite-natural-join.php
 */

#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <iostream>

using std::cout;
using std::endl;

struct Doctor {
    int doctor_id;
    std::string doctor_name;
    std::string degree;
};

struct Speciality {
    int spl_id;
    std::string spl_descrip;
    int doctor_id;
};

struct Visit {
    int doctor_id;
    std::string patient_name;
    std::string vdate;
};

int main(){
    using namespace sqlite_orm;
    
    auto storage = make_storage("natural_join.sqlite",
                                make_table("doctors",
                                           make_column("doctor_id", &Doctor::doctor_id, primary_key()),
                                           make_column("doctor_name", &Doctor::doctor_name),
                                           make_column("degree", &Doctor::degree)),
                                make_table("speciality",
                                           make_column("spl_id", &Speciality::spl_id, primary_key()),
                                           make_column("spl_descrip", &Speciality::spl_descrip),
                                           make_column("doctor_id", &Speciality::doctor_id)),
                                make_table("visits",
                                           make_column("doctor_id", &Visit::doctor_id),
                                           make_column("patient_name", &Visit::patient_name),
                                           make_column("vdate", &Visit::vdate)));
    storage.sync_schema();
    storage.remove_all<Doctor>();
    storage.remove_all<Speciality>();
    storage.remove_all<Visit>();
    
    storage.replace(Doctor{210, "Dr. John Linga", "MD"});
    storage.replace(Doctor{211, "Dr. Peter Hall", "MBBS"});
    storage.replace(Doctor{212, "Dr. Ke Gee", "MD"});
    storage.replace(Doctor{213, "Dr. Pat Fay", "MD"});
    
    storage.replace(Speciality{1, "CARDIO", 211});
    storage.replace(Speciality{2, "NEURO", 213});
    storage.replace(Speciality{3, "ARTHO", 212});
    storage.replace(Speciality{4, "GYNO", 210});
    
    storage.replace(Visit{210, "Julia Nayer", "2013-10-15"});
    storage.replace(Visit{214, "TJ Olson", "2013-10-14"});
    storage.replace(Visit{215, "John Seo", "2013-10-15"});
    storage.replace(Visit{212, "James Marlow", "2013-10-16"});
    storage.replace(Visit{212, "Jason Mallin", "2013-10-12"});
    
    {
        //  SELECT doctor_id,doctor_name,degree,patient_name,vdate
        //  FROM doctors
        //  NATURAL JOIN visits
        //  WHERE doctors.degree="MD";
        auto rows = storage.select(columns(&Doctor::doctor_id,
                                           &Doctor::doctor_name,
                                           &Doctor::degree,
                                           &Visit::patient_name,
                                           &Visit::vdate),
                                   natural_join<Visit>(),
                                   where(c(&Doctor::degree) == "MD"));
        cout << "rows count = " << rows.size() << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t'
            << std::get<1>(row) << '\t'
            << std::get<2>(row) << '\t'
            << std::get<3>(row) << '\t'
            << std::get<4>(row) << endl;
        }
    }
    cout << endl;
    {
        //  SELECT doctor_id,doctor_name,degree,spl_descrip,patient_name,vdate
        //  FROM doctors
        //  NATURAL JOIN speciality
        //  NATURAL JOIN visits
        //  WHERE doctors.degree='MD';
        auto rows = storage.select(columns(&Doctor::doctor_id,
                                           &Doctor::doctor_name,
                                           &Doctor::degree,
                                           &Speciality::spl_descrip,
                                           &Visit::patient_name,
                                           &Visit::vdate),
                                   natural_join<Speciality>(),
                                   natural_join<Visit>(),
                                   where(c(&Doctor::degree) == "MD"));
        cout << "rows count = " << rows.size() << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t'
            << std::get<1>(row) << '\t'
            << std::get<2>(row) << '\t'
            << std::get<3>(row) << '\t'
            << std::get<4>(row) << '\t'
            << std::get<5>(row) << endl;
        }
    }
    
    return 0;
}
