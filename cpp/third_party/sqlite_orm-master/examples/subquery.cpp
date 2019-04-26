/**
 *  Implemented from here https://www.w3resource.com/sqlite/sqlite-subqueries.php
 */
#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <memory>
#include <iostream>

using std::cout;
using std::endl;

struct Employee {
    int id;
    std::string firstName;
    std::string lastName;
    std::string email;
    std::string phoneNumber;
    std::string hireDate;
    std::string jobId;
    long salary;
    std::unique_ptr<double> commission_pct;
    std::unique_ptr<decltype(id)> managerId;
    int departmentId;
};

struct Department {
    int id;
    std::string name;
    int managerId;
    int locationId;
};

struct JobHistory {
    decltype(Employee::id) employeeId;
    std::string startDate;
    std::string endDate;
    decltype(Employee::jobId) jobId;
    decltype(Employee::departmentId) departmentId;
};

int main(int argc, char **argv) {
    using namespace sqlite_orm;
    auto storage = make_storage("subquery.sqlite",
                                make_table("employees",
                                           make_column("EMPLOYEE_ID", &Employee::id, primary_key()),
                                           make_column("FIRST_NAME", &Employee::firstName),
                                           make_column("LAST_NAME", &Employee::lastName),
                                           make_column("EMAIL", &Employee::email),
                                           make_column("PHONE_NUMBER", &Employee::phoneNumber),
                                           make_column("HIRE_DATE", &Employee::hireDate),
                                           make_column("JOB_ID", &Employee::jobId),
                                           make_column("SALARY", &Employee::salary),
                                           make_column("COMMISSION_PCT", &Employee::commission_pct),
                                           make_column("MANAGER_ID", &Employee::managerId),
                                           make_column("DEPARTMENT_ID", &Employee::departmentId)),
                                make_table("departments",
                                           make_column("DEPARTMENT_ID", &Department::id, primary_key()),
                                           make_column("DEPARTMENT_NAME", &Department::name),
                                           make_column("MANAGER_ID", &Department::managerId),
                                           make_column("LOCATION_ID", &Department::locationId)),
                                make_table("job_history",
                                           make_column("employee_id", &JobHistory::employeeId),
                                           make_column("start_date", &JobHistory::startDate),
                                           make_column("end_date", &JobHistory::endDate),
                                           make_column("job_id", &JobHistory::jobId),
                                           make_column("department_id", &JobHistory::departmentId)));
    storage.sync_schema();
    storage.remove_all<Employee>();
    
    storage.replace(Employee{100, "Steven", "King", "SKING", "515.123.4567", "17-Jun-87", "AD_PRES", 24000, {}, {}, 90});
    storage.replace(Employee{101, "Neena", "Kochhar", "NKOCHHAR", "515.123.4568", "21-Sep-89", "AD_VP", 17000, {}, std::make_unique<int>(100), 90});
    storage.replace(Employee{102, "Lex", "De Haan", "LDEHAAN", "515.123.4569", "13-Jan-93", "AD_VP", 17000, {}, std::make_unique<int>(100), 90});
    storage.replace(Employee{103, "Alexander", "Hunold", "AHUNOLD", "590.423.4567", "3-Jan-90", "IT_PROG", 9000, {}, std::make_unique<int>(102), 60});
    storage.replace(Employee{104, "Bruce", "Ernst", "BERNST", "590.423.4568", "21-May-91", "IT_PROG", 6000, {}, std::make_unique<int>(103), 60});
    storage.replace(Employee{105, "David", "Austin", "DAUSTIN", "590.423.4569", "25-Jun-97", "IT_PROG", 4800, {}, std::make_unique<int>(103), 60});
    storage.replace(Employee{106, "Valli", "Pataballa", "VPATABAL", "590.423.4560", "5-Feb-98", "IT_PROG", 4800, {}, std::make_unique<int>(103), 60});
    storage.replace(Employee{107, "Diana", "Lorentz", "DLORENTZ", "590.423.5567", "7-Feb-99", "IT_PROG", 4200, {}, std::make_unique<int>(103), 60});
    storage.replace(Employee{108, "Nancy", "Greenberg", "NGREENBE", "515.124.4569", "17-Aug-94", "FI_MGR", 12000, {}, std::make_unique<int>(101), 100});
    storage.replace(Employee{109, "Daniel", "Faviet", "DFAVIET", "515.124.4169", "16-Aug-94", "FI_ACCOUNT", 9000, {}, std::make_unique<int>(108), 100});
    storage.replace(Employee{110, "John", "Chen", "JCHEN", "515.124.4269", "28-Sep-97", "FI_ACCOUNT", 8200, {}, std::make_unique<int>(108), 100});
    storage.replace(Employee{111, "Ismael", "Sciarra", "ISCIARRA", "515.124.4369", "30-Sep-97", "FI_ACCOUNT", 7700, {}, std::make_unique<int>(108), 100});
    storage.replace(Employee{112, "Jose Manuel", "Urman", "JMURMAN", "515.124.4469", "7-Mar-98", "FI_ACCOUNT", 7800, {}, std::make_unique<int>(108), 100});
    storage.replace(Employee{113, "Luis", "Popp", "LPOPP", "515.124.4567", "7-Dec-99", "FI_ACCOUNT", 6900, {}, std::make_unique<int>(108), 100});
    storage.replace(Employee{114, "Den", "Raphaely", "DRAPHEAL", "515.127.4561", "7-Dec-94", "PU_MAN", 11000, {}, std::make_unique<int>(100), 30});
    storage.replace(Employee{115, "Alexander", "Khoo", "AKHOO", "515.127.4562", "18-May-95", "PU_CLERK", 3100, {}, std::make_unique<int>(114), 30});
    storage.replace(Employee{116, "Shelli", "Baida", "SBAIDA", "515.127.4563", "24-Dec-97", "PU_CLERK", 2900, {}, std::make_unique<int>(114), 30});
    storage.replace(Employee{117, "Sigal", "Tobias", "STOBIAS", "515.127.4564", "24-Jul-97", "PU_CLERK", 2800, {}, std::make_unique<int>(114), 30});
    storage.replace(Employee{118, "Guy", "Himuro", "GHIMURO", "515.127.4565", "15-Nov-98", "PU_CLERK", 2600, {}, std::make_unique<int>(114), 30});
    storage.replace(Employee{119, "Karen", "Colmenares", "KCOLMENA", "515.127.4566", "10-Aug-99", "PU_CLERK", 2500, {}, std::make_unique<int>(114), 30});
    storage.replace(Employee{120, "Matthew", "Weiss", "MWEISS", "650.123.1234", "18-Jul-96", "ST_MAN", 8000, {}, std::make_unique<int>(100), 50});
    storage.replace(Employee{121, "Adam", "Fripp", "AFRIPP", "650.123.2234", "10-Apr-97", "ST_MAN", 8200, {}, std::make_unique<int>(100), 50});
    storage.replace(Employee{122, "Payam", "Kaufling", "PKAUFLIN", "650.123.3234", "1-May-95", "ST_MAN", 7900, {}, std::make_unique<int>(100), 50});
    storage.replace(Employee{123, "Shanta", "Vollman", "SVOLLMAN", "650.123.4234", "10-Oct-97", "ST_MAN", 6500, {}, std::make_unique<int>(100), 50});
    storage.replace(Employee{124, "Kevin", "Mourgos", "KMOURGOS", "650.123.5234", "16-Nov-99", "ST_MAN", 5800, {}, std::make_unique<int>(100), 50});
    storage.replace(Employee{125, "Julia", "Nayer", "JNAYER", "650.124.1214", "16-Jul-97", "ST_CLERK", 3200, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{126, "Irene", "Mikkilineni", "IMIKKILI", "650.124.1224", "28-Sep-98", "ST_CLERK", 2700, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{127, "James", "Landry", "JLANDRY", "650.124.1334", "14-Jan-99", "ST_CLERK", 2400, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{128, "Steven", "Markle", "SMARKLE", "650.124.1434", "8-Mar-00", "ST_CLERK", 2200, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{129, "Laura", "Bissot", "LBISSOT", "650.124.5234", "20-Aug-97", "ST_CLERK", 3300, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{130, "Mozhe", "Atkinson", "MATKINSO", "650.124.6234", "30-Oct-97", "ST_CLERK", 2800, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{131, "James", "Marlow", "JAMRLOW", "650.124.7234", "16-Feb-97", "ST_CLERK", 2500, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{132, "TJ", "Olson", "TJOLSON", "650.124.8234", "10-Apr-99", "ST_CLERK", 2100, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{133, "Jason", "Mallin", "JMALLIN", "650.127.1934", "14-Jun-96", "ST_CLERK", 3300, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{134, "Michael", "Rogers", "MROGERS", "650.127.1834", "26-Aug-98", "ST_CLERK", 2900, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{135, "Ki", "Gee", "KGEE", "650.127.1734", "12-Dec-99", "ST_CLERK", 2400, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{136, "Hazel", "Philtanker", "HPHILTAN", "650.127.1634", "6-Feb-00", "ST_CLERK", 2200, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{137, "Renske", "Ladwig", "RLADWIG", "650.121.1234", "14-Jul-95", "ST_CLERK", 3600, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{138, "Stephen", "Stiles", "SSTILES", "650.121.2034", "26-Oct-97", "ST_CLERK", 3200, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{139, "John", "Seo", "JSEO", "650.121.2019", "12-Feb-98", "ST_CLERK", 2700, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{140, "Joshua", "Patel", "JPATEL", "650.121.1834", "6-Apr-98", "ST_CLERK", 2500, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{141, "Trenna", "Rajs", "TRAJS", "650.121.8009", "17-Oct-95", "ST_CLERK", 3500, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{142, "Curtis", "Davies", "CDAVIES", "650.121.2994", "29-Jan-97", "ST_CLERK", 3100, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{143, "Randall", "Matos", "RMATOS", "650.121.2874", "15-Mar-98", "ST_CLERK", 2600, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{144, "Peter", "Vargas", "PVARGAS", "650.121.2004", "9-Jul-98", "ST_CLERK", 2500, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{145, "John", "Russell", "JRUSSEL", "011.44.1344.429268", "1-Oct-96", "SA_MAN", 14000, std::make_unique<double>(0.4), std::make_unique<int>(100), 80});
    storage.replace(Employee{146, "Karen", "Partners", "KPARTNER", "011.44.1344.467268", "5-Jan-97", "SA_MAN", 13500, std::make_unique<double>(0.3), std::make_unique<int>(100), 80});
    storage.replace(Employee{147, "Alberto", "Errazuriz", "AERRAZUR", "011.44.1344.429278", "10-Mar-97", "SA_MAN", 12000, std::make_unique<double>(0.3), std::make_unique<int>(100), 80});
    storage.replace(Employee{148, "Gerald", "Cambrault", "GCAMBRAU", "011.44.1344.619268", "15-Oct-99", "SA_MAN", 11000, std::make_unique<double>(0.3), std::make_unique<int>(100), 80});
    storage.replace(Employee{149, "Eleni", "Zlotkey", "EZLOTKEY", "011.44.1344.429018", "29-Jan-00", "SA_MAN", 10500, std::make_unique<double>(0.2), std::make_unique<int>(100), 80});
    storage.replace(Employee{150, "Peter", "Tucker", "PTUCKER", "011.44.1344.129268", "30-Jan-97", "SA_REP", 10000, std::make_unique<double>(0.3), std::make_unique<int>(145), 80});
    storage.replace(Employee{151, "David", "Bernstein", "DBERNSTE", "011.44.1344.345268", "24-Mar-97", "SA_REP", 9500, std::make_unique<double>(0.25), std::make_unique<int>(145), 80});
    storage.replace(Employee{152, "Peter", "Hall", "PHALL", "011.44.1344.478968", "20-Aug-97", "SA_REP", 9000, std::make_unique<double>(0.25), std::make_unique<int>(145), 80});
    storage.replace(Employee{153, "Christopher", "Olsen", "COLSEN", "011.44.1344.498718", "30-Mar-98", "SA_REP", 8000, std::make_unique<double>(0.2), std::make_unique<int>(145), 80});
    storage.replace(Employee{154, "Nanette", "Cambrault", "NCAMBRAU", "011.44.1344.987668", "9-Dec-98", "SA_REP", 7500, std::make_unique<double>(0.2), std::make_unique<int>(145), 80});
    storage.replace(Employee{155, "Oliver", "Tuvault", "OTUVAULT", "011.44.1344.486508", "23-Nov-99", "SA_REP", 7000, std::make_unique<double>(0.15), std::make_unique<int>(145), 80});
    storage.replace(Employee{156, "Janette", "King", "JKING", "011.44.1345.429268", "30-Jan-96", "SA_REP", 10000, std::make_unique<double>(0.35), std::make_unique<int>(146), 80});
    storage.replace(Employee{157, "Patrick", "Sully", "PSULLY", "011.44.1345.929268", "4-Mar-96", "SA_REP", 9500, std::make_unique<double>(0.35), std::make_unique<int>(146), 80});
    storage.replace(Employee{158, "Allan", "McEwen", "AMCEWEN", "011.44.1345.829268", "1-Aug-96", "SA_REP", 9000, std::make_unique<double>(0.35), std::make_unique<int>(146), 80});
    storage.replace(Employee{159, "Lindsey", "Smith", "LSMITH", "011.44.1345.729268", "10-Mar-97", "SA_REP", 8000, std::make_unique<double>(0.3), std::make_unique<int>(146), 80});
    storage.replace(Employee{160, "Louise", "Doran", "LDORAN", "011.44.1345.629268", "15-Dec-97", "SA_REP", 7500, std::make_unique<double>(0.3), std::make_unique<int>(146), 80});
    storage.replace(Employee{161, "Sarath", "Sewall", "SSEWALL", "011.44.1345.529268", "3-Nov-98", "SA_REP", 7000, std::make_unique<double>(0.25), std::make_unique<int>(146), 80});
    storage.replace(Employee{162, "Clara", "Vishney", "CVISHNEY", "011.44.1346.129268", "11-Nov-97", "SA_REP", 10500, std::make_unique<double>(0.25), std::make_unique<int>(147), 80});
    storage.replace(Employee{163, "Danielle", "Greene", "DGREENE", "011.44.1346.229268", "19-Mar-99", "SA_REP", 9500, std::make_unique<double>(0.15), std::make_unique<int>(147), 80});
    storage.replace(Employee{164, "Mattea", "Marvins", "MMARVINS", "011.44.1346.329268", "24-Jan-00", "SA_REP", 7200, std::make_unique<double>(0.1), std::make_unique<int>(147), 80});
    storage.replace(Employee{165, "David", "Lee", "DLEE", "011.44.1346.529268", "23-Feb-00", "SA_REP", 6800, std::make_unique<double>(0.1), std::make_unique<int>(147), 80});
    storage.replace(Employee{166, "Sundar", "Ande", "SANDE", "011.44.1346.629268", "24-Mar-00", "SA_REP", 6400, std::make_unique<double>(0.1), std::make_unique<int>(147), 80});
    storage.replace(Employee{167, "Amit", "Banda", "ABANDA", "011.44.1346.729268", "21-Apr-00", "SA_REP", 6200, std::make_unique<double>(0.1), std::make_unique<int>(147), 80});
    storage.replace(Employee{168, "Lisa", "Ozer", "LOZER", "011.44.1343.929268", "11-Mar-97", "SA_REP", 11500, std::make_unique<double>(0.25), std::make_unique<int>(148), 80});
    storage.replace(Employee{169, "Harrison", "Bloom", "HBLOOM", "011.44.1343.829268", "23-Mar-98", "SA_REP", 10000, std::make_unique<double>(0.2), std::make_unique<int>(148), 80});
    storage.replace(Employee{170, "Tayler", "Fox", "TFOX", "011.44.1343.729268", "24-Jan-98", "SA_REP", 9600, std::make_unique<double>(0.2), std::make_unique<int>(148), 80});
    storage.replace(Employee{171, "William", "Smith", "WSMITH", "011.44.1343.629268", "23-Feb-99", "SA_REP", 7400, std::make_unique<double>(0.15), std::make_unique<int>(148), 80});
    storage.replace(Employee{172, "Elizabeth", "Bates", "EBATES", "011.44.1343.529268", "24-Mar-99", "SA_REP", 7300, std::make_unique<double>(0.15), std::make_unique<int>(148), 80});
    storage.replace(Employee{173, "Sundita", "Kumar", "SKUMAR", "011.44.1343.329268", "21-Apr-00", "SA_REP", 6100, std::make_unique<double>(0.1), std::make_unique<int>(148), 80});
    storage.replace(Employee{174, "Ellen", "Abel", "EABEL", "011.44.1644.429267", "11-May-96", "SA_REP", 11000, std::make_unique<double>(0.3), std::make_unique<int>(149), 80});
    storage.replace(Employee{175, "Alyssa", "Hutton", "AHUTTON", "011.44.1644.429266", "19-Mar-97", "SA_REP", 8800, std::make_unique<double>(0.25), std::make_unique<int>(149), 80});
    storage.replace(Employee{176, "Jonathon", "Taylor", "JTAYLOR", "011.44.1644.429265", "24-Mar-98", "SA_REP", 8600, std::make_unique<double>(0.2), std::make_unique<int>(149), 80});
    storage.replace(Employee{177, "Jack", "Livingston", "JLIVINGS", "011.44.1644.429264", "23-Apr-98", "SA_REP", 8400, std::make_unique<double>(0.2), std::make_unique<int>(149), 80});
    storage.replace(Employee{178, "Kimberely", "Grant", "KGRANT", "011.44.1644.429263", "24-May-99", "SA_REP", 7000, std::make_unique<double>(0.15), std::make_unique<int>(149), 80});
    storage.replace(Employee{179, "Charles", "Johnson", "CJOHNSON", "011.44.1644.429262", "4-Jan-00", "SA_REP", 6200, std::make_unique<double>(0.1), std::make_unique<int>(149), 80});
    storage.replace(Employee{180, "Winston", "Taylor", "WTAYLOR", "650.507.9876", "24-Jan-98", "SH_CLERK", 3200, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{181, "Jean", "Fleaur", "JFLEAUR", "650.507.9877", "23-Feb-98", "SH_CLERK", 3100, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{182, "Martha", "Sullivan", "MSULLIVA", "650.507.9878", "21-Jun-99", "SH_CLERK", 2500, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{183, "Girard", "Geoni", "GGEONI", "650.507.9879", "3-Feb-00", "SH_CLERK", 2800, {}, std::make_unique<int>(120), 50});
    storage.replace(Employee{184, "Nandita", "Sarchand", "NSARCHAN", "650.509.1876", "27-Jan-96", "SH_CLERK", 4200, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{185, "Alexis", "Bull", "ABULL", "650.509.2876", "20-Feb-97", "SH_CLERK", 4100, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{186, "Julia", "Dellinger", "JDELLING", "650.509.3876", "24-Jun-98", "SH_CLERK", 3400, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{187, "Anthony", "Cabrio", "ACABRIO", "650.509.4876", "7-Feb-99", "SH_CLERK", 3000, {}, std::make_unique<int>(121), 50});
    storage.replace(Employee{188, "Kelly", "Chung", "KCHUNG", "650.505.1876", "14-Jun-97", "SH_CLERK", 3800, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{189, "Jennifer", "Dilly", "JDILLY", "650.505.2876", "13-Aug-97", "SH_CLERK", 3600, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{190, "Timothy", "Gates", "TGATES", "650.505.3876", "11-Jul-98", "SH_CLERK", 2900, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{191, "Randall", "Perkins", "RPERKINS", "650.505.4876", "19-Dec-99", "SH_CLERK", 2500, {}, std::make_unique<int>(122), 50});
    storage.replace(Employee{192, "Sarah", "Bell", "SBELL", "650.501.1876", "4-Feb-96", "SH_CLERK", 4000, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{193, "Britney", "Everett", "BEVERETT", "650.501.2876", "3-Mar-97", "SH_CLERK", 3900, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{194, "Samuel", "McCain", "SMCCAIN", "650.501.3876", "1-Jul-98", "SH_CLERK", 3200, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{195, "Vance", "Jones", "VJONES", "650.501.4876", "17-Mar-99", "SH_CLERK", 2800, {}, std::make_unique<int>(123), 50});
    storage.replace(Employee{196, "Alana", "Walsh", "AWALSH", "650.507.9811", "24-Apr-98", "SH_CLERK", 3100, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{197, "Kevin", "Feeney", "KFEENEY", "650.507.9822", "23-May-98", "SH_CLERK", 3000, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{198, "Donald", "OConnell", "DOCONNEL", "650.507.9833", "21-Jun-99", "SH_CLERK", 2600, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{199, "Douglas", "Grant", "DGRANT", "650.507.9844", "13-Jan-00", "SH_CLERK", 2600, {}, std::make_unique<int>(124), 50});
    storage.replace(Employee{200, "Jennifer", "Whalen", "JWHALEN", "515.123.4444", "17-Sep-87", "AD_ASST", 4400, {}, std::make_unique<int>(101), 10});
    storage.replace(Employee{201, "Michael", "Hartstein", "MHARTSTE", "515.123.5555", "17-Feb-96", "MK_MAN", 13000, {}, std::make_unique<int>(100), 20});
    storage.replace(Employee{202, "Pat", "Fay", "PFAY", "603.123.6666", "17-Aug-97", "MK_REP", 6000, {}, std::make_unique<int>(201), 20});
    storage.replace(Employee{203, "Susan", "Mavris", "SMAVRIS", "515.123.7777", "7-Jun-94", "HR_REP", 6500, {}, std::make_unique<int>(101), 40});
    storage.replace(Employee{204, "Hermann", "Baer", "HBAER", "515.123.8888", "7-Jun-94", "PR_REP", 10000, {}, std::make_unique<int>(101), 70});
    storage.replace(Employee{205, "Shelley", "Higgins", "SHIGGINS", "515.123.8080", "7-Jun-94", "AC_MGR", 12000, {}, std::make_unique<int>(101), 110});
    storage.replace(Employee{206, "William", "Gietz", "WGIETZ", "515.123.8181", "7-Jun-94", "AC_ACCOUNT", 8300, {}, std::make_unique<int>(205), 110});
    
    storage.replace(Department{10, "Administration", 200, 1700});
    storage.replace(Department{20, "Marketing", 201, 1800});
    storage.replace(Department{30, "Purchasing", 114, 1700});
    storage.replace(Department{40, "Human Resources", 203, 2400});
    storage.replace(Department{50, "Shipping", 121, 1500});
    storage.replace(Department{60, "IT", 103, 1400});
    storage.replace(Department{70, "Public Relations", 204, 2700});
    storage.replace(Department{80, "Sales", 145, 2500});
    storage.replace(Department{90, "Executive", 100, 1700});
    storage.replace(Department{100, "Finance", 108, 1700});
    storage.replace(Department{110, "Accounting", 205, 1700});
    storage.replace(Department{120, "Treasury", 0, 1700});
    storage.replace(Department{130, "Corporate Tax", 0, 1700});
    storage.replace(Department{140, "Control And Credit", 0, 1700});
    storage.replace(Department{150, "Shareholder Services", 0, 1700});
    storage.replace(Department{160, "Benefits", 0, 1700});
    storage.replace(Department{170, "Manufacturing", 0, 1700});
    storage.replace(Department{180, "Construction", 0, 1700});
    storage.replace(Department{190, "Contracting", 0, 1700});
    storage.replace(Department{200, "Operations", 0, 1700});
    storage.replace(Department{210, "IT Support", 0, 1700});
    storage.replace(Department{220, "NOC", 0, 1700});
    storage.replace(Department{230, "IT Helpdesk", 0, 1700});
    storage.replace(Department{240, "Government Sales", 0, 1700});
    storage.replace(Department{250, "Retail Sales", 0, 1700});
    storage.replace(Department{260, "Recruiting", 0, 1700});
    storage.replace(Department{270, "Payroll", 0, 1700});
    
    storage.replace(JobHistory{102, "1993-01-13", "1998-07-24", "IT_PROG", 60});
    storage.replace(JobHistory{101, "1989-09-21", "1993-10-27", "AC_ACCOUNT", 110});
    storage.replace(JobHistory{101, "1993-10-28", "1997-03-15", "AC_MGR", 110});
    storage.replace(JobHistory{201, "1996-02-17", "1999-12-19", "MK_REP", 20});
    storage.replace(JobHistory{114, "1998-03-24", "1999-12-31", "ST_CLERK", 50});
    storage.replace(JobHistory{122, "1999-01-01", "1999-12-31", "ST_CLERK", 50});
    storage.replace(JobHistory{200, "1987-09-17", "1993-06-17", "AD_ASST", 90});
    storage.replace(JobHistory{176, "1998-03-24", "1998-12-31", "SA_REP", 80});
    storage.replace(JobHistory{176, "1999-01-01", "1999-12-31", "SA_MAN", 80});
    storage.replace(JobHistory{200, "1994-07-01", "1998-12-31", "AC_ACCOUNT", 90});
    
    {
        //  SELECT first_name, last_name, salary
        //  FROM employees
        //  WHERE salary >(
        //          SELECT salary
        //          FROM employees
        //          WHERE first_name='Alexander');
        auto rows = storage.select(columns(&Employee::firstName, &Employee::lastName, &Employee::salary),
                                   where(greater_than(&Employee::salary,
                                                      select(&Employee::salary,
                                                             where(is_equal(&Employee::firstName, "Alexander"))))));
        cout << "first_name  last_name   salary" << endl;
        cout << "----------  ----------  ----------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << endl;
        }
    }
    {
        //  SELECT employee_id,first_name,last_name,salary
        //  FROM employees
        //  WHERE salary > (SELECT AVG(SALARY) FROM employees);
        auto rows = storage.select(columns(&Employee::id, &Employee::firstName, &Employee::lastName, &Employee::salary),
                                   where(greater_than(&Employee::salary,
                                                      select(avg(&Employee::salary)))));
        cout << "employee_id  first_name  last_name   salary" << endl;
        cout << "-----------  ----------  ----------  ----------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
    }
    {
        //  SELECT first_name, last_name, department_id
        //  FROM employees
        //  WHERE department_id IN
        //      (SELECT DEPARTMENT_ID FROM departments
        //      WHERE location_id=1700);
        auto rows = storage.select(columns(&Employee::firstName, &Employee::lastName, &Employee::departmentId),
                                   where(in(&Employee::departmentId,
                                            select(&Department::id,
                                                   where(c(&Department::locationId) == 1700)))));
        cout << "first_name  last_name   department_id" << endl;
        cout << "----------  ----------  -------------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << endl;
        }
    }
    {
        
        //  SELECT first_name, last_name, department_id
        //  FROM employees
        //  WHERE department_id NOT IN
        //  (SELECT DEPARTMENT_ID FROM departments
        //      WHERE manager_id
        //      BETWEEN 100 AND 200);
        auto rows = storage.select(columns(&Employee::firstName, &Employee::lastName, &Employee::departmentId),
                                   where(not_in(&Employee::departmentId,
                                                select(&Department::id,
                                                       where(between(&Department::managerId, 100, 200))))));
        cout << "first_name  last_name   department_id" << endl;
        cout << "----------  ----------  -------------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << endl;
        }
        
    }
    {
        //  SELECT last_name, salary, department_id
        //  FROM employees e
        //  WHERE salary >(SELECT AVG(salary)
        //      FROM employees
        //      WHERE department_id = e.department_id);
        using als = alias_e<Employee>;
        auto rows = storage.select(columns(alias_column<als>(&Employee::lastName), alias_column<als>(&Employee::salary), alias_column<als>(&Employee::departmentId)),
                                   where(greater_than(alias_column<als>(&Employee::salary),
                                                      select(avg(&Employee::salary),
                                                             where(is_equal(&Employee::departmentId, alias_column<als>(&Employee::departmentId)))))));
        cout << "last_name   salary      department_id" << endl;
        cout << "----------  ----------  -------------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << endl;
        }
    }
    {
        //  SELECT first_name, last_name, employee_id, job_id
        //  FROM employees
        //  WHERE 1 <=
        //      (SELECT COUNT(*) FROM Job_history
        //      WHERE employee_id = employees.employee_id);
        auto rows = storage.select(columns(&Employee::firstName,
                                           &Employee::lastName,
                                           &Employee::id,
                                           &Employee::jobId),
                                   where(lesser_or_equal(1, select(count<JobHistory>(),
                                                                   where(is_equal(&Employee::id, &JobHistory::employeeId))))));
        cout << "first_name  last_name   employee_id  job_id" << endl;
        cout << "----------  ----------  -----------  ----------" << endl;
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
    }
    
    return 0;
}
