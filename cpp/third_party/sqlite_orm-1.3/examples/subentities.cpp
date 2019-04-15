/**
 *  Our goal is to manage a Student class. Student has basic id, name and roll_number and one more thing:
 *  vector of marks. Mark is a subentity with one to many relation. This example shows how to manage this kind of case.
 *  First of all we got to understand how to keep data in the db. We need two tables: `students` and `marks`. Students
 *  table has column equal to all Student class members exept marks. Marks table has two columns: student_id and value itself.
 *  We create two functions here: inserting/updating student (with his/her marks) and getting student (also with his/her marks).
 *  Schema is:
 *  `CREATE TABLE students (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, roll_no INTEGER NOT NULL)`
 *  `CREATE TABLE marks (mark INTEGER NOT NULL, student_id INTEGER NOT NULL)`
 *  One of the main ideas of `sqlite_orm` is to give a developer ability to name tables/columns as he/she wants.
 *  Many other ORM libraries manage subentities automatically and it is not correct cause developer must understand
 *  how everything works inside sqlite otherwise his/her app might not work properly. Also developer must know schema in case he or she needs
 *  a strict access with sqlite client.
 */

#include <sqlite_orm/sqlite_orm.h>
#include <iostream>

using std::cout;
using std::endl;

class Mark {
public:
    int value;
    int student_id;
};

class Student{
public:
    int id;
    std::string name;
    int roll_number;
    std::vector<decltype(Mark::value)> marks;
};

using namespace sqlite_orm;
auto storage = make_storage("subentities.sqlite",
                            make_table("students",
                                       make_column("id",
                                                   &Student::id,
                                                   primary_key()),
                                       make_column("name",
                                                   &Student::name),
                                       make_column("roll_no",
                                                   &Student::roll_number)),
                            make_table("marks",
                                       make_column("mark",
                                                   &Mark::value),
                                       make_column("student_id",
                                                   &Mark::student_id)));

//  inserts or updates student and does the same with marks
int addStudent(const Student &student) {
    auto studentId = student.id;
    if(storage.count<Student>(where(c(&Student::id) == student.id))){
        storage.update(student);
    }else{
        studentId = storage.insert(student);
    }
    //  insert all marks within a transaction
    storage.transaction([&]{
        storage.remove_all<Mark>(where(c(&Mark::student_id) == studentId));
        for(auto &mark : student.marks) {
            storage.insert(Mark{ mark, studentId });
        }
        return true;
    });
    return studentId;
}

/**
 *  To get student from db we have to execute two queries:
 *  `SELECT * FROM students WHERE id = ?`
 *  `SELECT mark FROM marks WHERE student_id = ?`
 */
Student getStudent(int studentId) {
    auto res = storage.get<Student>(studentId);
    res.marks = storage.select(&Mark::value, where(c(&Mark::student_id) == studentId));
    return res; //  must be moved automatically by compiler
}

int main(int argc, char **argv) {
    decltype(Student::id) mikeId;
    decltype(Student::id) annaId;
    
    {
        storage.sync_schema();    // create tables if they don't exist
        
        Student mike{ -1, "Mike", 123 };    //  create student named `Mike` without marks and without id
        mike.marks = { 3, 4, 5 };
        mike.id = addStudent(mike);
        mikeId = mike.id;
        
        //  also let's create another students with marks..
        Student anna{ -1, "Anna", 555 };
        anna.marks.push_back(6);
        anna.marks.push_back(7);
        anna.id = addStudent(anna);
        annaId = anna.id;
    }
    // now let's assume we forgot about object `mike`, let's try to get him with his marks
    //  assume we know `mikeId` variable only
    
    {
        auto mike = getStudent(mikeId);
        cout << "mike = " << storage.dump(mike) << endl;
        cout << "mike.marks = ";
        for(auto &m : mike.marks) {
            cout << m << " ";
        }
        cout << endl;
        
        auto anna = getStudent(annaId);
        cout << "anna = " << storage.dump(anna) << endl;
        cout << "anna.marks = ";
        for(auto &m : anna.marks) {
            cout << m << " ";
        }
        cout << endl;
    }
    
    return 0;
}
