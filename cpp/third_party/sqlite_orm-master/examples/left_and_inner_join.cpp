/**
 *  Example is imlemented from here http://www.sqlitetutorial.net/sqlite-left-join/
 *  In this example you got to download db file 'chinook.db' first from here http://www.sqlitetutorial.net/sqlite-sample-database/
 *  an place the file near executable.
 */

#include <sqlite_orm/sqlite_orm.h>
#include <string>
#include <iostream>
#include <memory>

using std::cout;
using std::endl;

struct Artist {
    std::unique_ptr<int> artistId;
    std::unique_ptr<std::string> name;
};

struct Album {
    std::unique_ptr<int> albumId;
    std::unique_ptr<std::string> title;
    std::unique_ptr<int> artistId;
};

struct Track {
    int trackId;
    std::string name;
    std::unique_ptr<int> albumId;
    int mediaTypeId;
    std::unique_ptr<int> genreId;
    std::unique_ptr<std::string> composer;
    long milliseconds;
    std::unique_ptr<long> bytes;
    double unitPrice;
};

inline auto initStorage(const std::string &path){
    using namespace sqlite_orm;
    return make_storage(path,
                        make_table("artists",
                                   make_column("ArtistId", &Artist::artistId, primary_key()),
                                   make_column("Name", &Artist::name)),
                        make_table("albums",
                                   make_column("AlbumId", &Album::albumId, primary_key()),
                                   make_column("Title", &Album::title),
                                   make_column("ArtistId", &Album::artistId)),
                        make_table("tracks",
                                   make_column("TrackId", &Track::trackId, primary_key()),
                                   make_column("Name", &Track::name),
                                   make_column("AlbumId", &Track::albumId),
                                   make_column("MediaTypeId", &Track::mediaTypeId),
                                   make_column("GenreId", &Track::genreId),
                                   make_column("Composer", &Track::composer),
                                   make_column("Milliseconds", &Track::milliseconds),
                                   make_column("Bytes", &Track::bytes),
                                   make_column("UnitPrice", &Track::unitPrice)));
}

int main(int argc, char **argv) {
    
    auto storage = initStorage("chinook.db");

    using namespace sqlite_orm;
    
    //  SELECT
    //      artists.ArtistId,
    //      albumId
    //  FROM
    //      artists
    //  LEFT JOIN albums ON albums.artistid = artists.artistid
    //  ORDER BY
    //      albumid;
    auto rows = storage.select(columns(&Artist::artistId, &Album::albumId),
                               left_join<Album>(on(c(&Album::artistId) == &Artist::artistId)),
                               order_by(&Album::albumId));
    cout << "rows count = " << rows.size() << endl;
    for(auto &row : rows) {
        auto &artistId = std::get<0>(row);
        if(artistId){
            cout << *artistId;
        }else{
            cout << "null";
        }
        cout << '\t';
        auto &albumId = std::get<1>(row);
        if(albumId){
            cout << *albumId;
        }else{
            cout << "null";
        }
        cout << endl;
    }
    
    cout << endl;
    
    
    //  SELECT
    //      artists.ArtistId,
    //      albumId
    //  FROM
    //      artists
    //  LEFT JOIN albums ON albums.artistid = artists.artistid
    //  WHERE
    //      albumid IS NULL;
    rows = storage.select(columns(&Artist::artistId, &Album::albumId),
                          left_join<Album>(on(c(&Album::artistId) == &Artist::artistId)),
                          where(is_null(&Album::albumId)));
    cout << "rows count = " << rows.size() << endl;
    for(auto &row : rows) {
        auto &artistId = std::get<0>(row);
        if(artistId){
            cout << *artistId;
        }else{
            cout << "null";
        }
        cout << '\t';
        auto &albumId = std::get<1>(row);
        if(albumId){
            cout << *albumId;
        }else{
            cout << "null";
        }
        cout << endl;
    }
    
    cout << endl;
    
    //  SELECT
    //      trackid,
    //      name,
    //      title
    //  FROM
    //      tracks
    //  INNER JOIN albums ON albums.albumid = tracks.albumid;
    auto innerJoinRows0 = storage.select(columns(&Track::trackId, &Track::name, &Album::title),
                                         inner_join<Album>(on(c(&Track::albumId) == &Album::albumId)));
    cout << "innerJoinRows0 count = " << innerJoinRows0.size() << endl;
    for(auto &row : innerJoinRows0) {
        cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t';
        if(std::get<2>(row)){
            cout << *std::get<2>(row);
        }else{
            cout << "null";
        }
        cout << endl;
    }
    cout << endl;
    
    //  SELECT
    //      trackid,
    //      name,
    //      tracks.AlbumId,
    //      albums.AlbumId,
    //      title
    //  FROM
    //      tracks
    //  INNER JOIN albums ON albums.albumid = tracks.albumid;
    auto innerJoinRows1 = storage.select(columns(&Track::trackId, &Track::name, &Track::albumId, &Album::albumId, &Album::title),
                                         inner_join<Album>(on(c(&Album::albumId) == &Track::trackId)));
    cout << "innerJoinRows1 count = " << innerJoinRows1.size() << endl;
    for(auto &row : innerJoinRows1) {
        cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t';
        if(std::get<2>(row)){
            cout << *std::get<2>(row);
        }else{
            cout << "null";
        }
        cout << '\t';
        if(std::get<3>(row)){
            cout << *std::get<3>(row);
        }else{
            cout << "null";
        }
        cout << '\t';
        if(std::get<4>(row)){
            cout << *std::get<4>(row);
        }else{
            cout << "null";
        }
        cout << '\t';
        cout << endl;
    }
    cout << endl;
    
    //  SELECT
    //      trackid,
    //      tracks.name AS Track,
    //      albums.title AS Album,
    //      artists.name AS Artist
    //  FROM
    //      tracks
    //  INNER JOIN albums ON albums.albumid = tracks.albumid
    //  INNER JOIN artists ON artists.artistid = albums.artistid;
    auto innerJoinRows2 = storage.select(columns(&Track::trackId, &Track::name, &Album::title, &Artist::name),
                                         inner_join<Album>(on(c(&Album::albumId) == &Track::albumId)),
                                         inner_join<Artist>(on(c(&Artist::artistId) == &Album::artistId)));
    cout << "innerJoinRows2 count = " << innerJoinRows2.size() << endl;
    for(auto &row : innerJoinRows2) {
        cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t';
        if(std::get<2>(row)){
            cout << *std::get<2>(row);
        }else{
            cout << "null";
        }
        cout << '\t';
        if(std::get<3>(row)){
            cout << *std::get<3>(row);
        }else{
            cout << "null";
        }
        cout << '\t';
    }
    cout << endl;
    
    {
        /**
         *  JOIN has many usages so this is another example from here http://www.w3resource.com/sqlite/sqlite-left-join.php
         *  and here http://www.w3resource.com/sqlite/sqlite-natural-join.php
         */
        
        struct Doctor {
            int id;
            std::string name;
            std::string degree;
        };
        
        struct Visit {
            int doctorId;
            std::string patientName;
            std::string vdate;
        };
        
        struct A {
            int id;
            std::string des1;
            std::string des2;
        };
        
        struct B {
            int id;
            std::string des3;
            std::string des4;
        };
        
        auto storage2 = make_storage("doctors.sqlite",
                                     make_table("doctors",
                                                make_column("doctor_id", &Doctor::id, primary_key()),
                                                make_column("doctor_name", &Doctor::name),
                                                make_column("degree", &Doctor::degree)),
                                     make_table("visits",
                                                make_column("doctor_id", &Visit::doctorId),
                                                make_column("patient_name", &Visit::patientName),
                                                make_column("vdate", &Visit::vdate)),
                                     make_table("table_a",
                                                make_column("id", &A::id, primary_key()),
                                                make_column("des1", &A::des1),
                                                make_column("des2", &A::des2)),
                                     make_table("table_b",
                                                make_column("id", &B::id, primary_key()),
                                                make_column("des3", &B::des3),
                                                make_column("des4", &B::des4)));
        storage2.sync_schema();
        
        storage2.replace(Doctor{ 210, "Dr. John Linga", "MD", });
        storage2.replace(Doctor{ 211, "Dr. Peter Hall", "MBBS" });
        storage2.replace(Doctor{ 212, "Dr. Ke Gee", "MD" });
        storage2.replace(Doctor{ 213, "Dr. Pat Fay", "MD" });
        
        storage2.replace(Visit{ 210, "Julia Nayer", "2013-10-15" });
        storage2.replace(Visit{ 214, "TJ Olson", "2013-10-14" });
        storage2.replace(Visit{ 215, "John Seo", "2013-10-15" });
        storage2.replace(Visit{ 212, "James Marlow", "2013-10-16" });
        storage2.replace(Visit{ 212, "Jason Mallin", "2013-10-12" });
        
        storage2.replace(A{ 100, "desc11", "desc12" });
        storage2.replace(A{ 101, "desc21", "desc22" });
        storage2.replace(A{ 102, "desc31", "desc32" });
        
        storage2.replace(B{ 101, "desc41", "desc42" });
        storage2.replace(B{ 103, "desc51", "desc52" });
        storage2.replace(B{ 105, "desc61", "desc62" });
        
        
        //  SELECT a.doctor_id,a.doctor_name,
        //      c.patient_name,c.vdate
        //  FROM doctors a
        //  LEFT JOIN visits c
        //  ON a.doctor_id=c.doctor_id;
        auto rows = storage2.select(columns(&Doctor::id, &Doctor::name, &Visit::patientName, &Visit::vdate),
                                    left_join<Visit>(on(c(&Doctor::id) == &Visit::doctorId)));
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
        cout << endl;
        
        //  SELECT a.doctor_id,a.doctor_name,
        //      c.patient_name,c.vdate
        //  FROM doctors a
        //  JOIN visits c
        //  ON a.doctor_id=c.doctor_id;
        rows = storage2.select(columns(&Doctor::id, &Doctor::name, &Visit::patientName, &Visit::vdate),
                               join<Visit>(on(c(&Doctor::id) == &Visit::doctorId)));
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
        cout << endl;
        
        //  SELECT doctor_id,doctor_name,
        //      patient_name,vdate
        //  FROM doctors
        //  LEFT JOIN visits
        //  USING(doctor_id);
        rows = storage2.select(columns(&Doctor::id, &Doctor::name, &Visit::patientName, &Visit::vdate),
                               left_join<Visit>(using_(&Visit::doctorId))); //  or using_(&Doctor::id)
        for(auto &row : rows) {
            cout << std::get<0>(row) << '\t' << std::get<1>(row) << '\t' << std::get<2>(row) << '\t' << std::get<3>(row) << endl;
        }
        cout << endl;
        
        
    }
    
    return 0;
}
