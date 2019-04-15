//
//  The example is implemented from here https://sqlite.org/foreignkeys.html
//

#include <sqlite_orm/sqlite_orm.h>

#include <string>
#include <iostream>
#include <cassert>
#include <memory>

using std::cout;
using std::endl;

struct Artist {
    int artistId;
    std::string artistName;
};

struct Track {
    int trackId;
    std::string trackName;
    std::shared_ptr<int> trackArtist;    //  must map to &Artist::artistId
};

int main(int argc, char **argv) {
    cout << "path = " << argv[0] << endl;
    
    using namespace sqlite_orm;
    auto storage = make_storage("foreign_key.sqlite",
                                make_table("artist",
                                           make_column("artistid",
                                                       &Artist::artistId,
                                                       primary_key()),
                                           make_column("artistname",
                                                       &Artist::artistName)),
                                make_table("track",
                                           make_column("trackid",
                                                       &Track::trackId,
                                                       primary_key()),
                                           make_column("trackname",
                                                       &Track::trackName),
                                           make_column("trackartist",
                                                       &Track::trackArtist),
                                           foreign_key(&Track::trackArtist).references(&Artist::artistId)));
    /*auto syncSchemaRes =*/ storage.sync_schema();
    /*for(auto &p : syncSchemaRes) {
        cout << p.first << " " << p.second << endl;
    }*/
    
    storage.remove_all<Track>();
    storage.remove_all<Artist>();
    
    storage.replace(Artist{ 1, "Dean Martin" });
    storage.replace(Artist{ 2, "Frank Sinatra" });
    
    storage.replace(Track{ 11, "That's Amore", std::make_shared<int>(1) });
    storage.replace(Track{ 12, "Christmas Blues", std::make_shared<int>(1) });
    storage.replace(Track{ 13, "My Way", std::make_shared<int>(2) });
    
    try{
        //  This fails because value inserted into the trackartist column (3)
        //  does not correspond to row in the artist table.
        storage.replace(Track{ 14, "Mr. Bojangles", std::make_shared<int>(3) });
        assert(0);
    }catch(std::system_error e) {
        cout << e.what() << endl;
    }
    
    //  This succeeds because a NULL is inserted into trackartist. A
    //  corresponding row in the artist table is not required in this case.
    storage.replace(Track{ 14, "Mr. Bojangles", nullptr });
    
    //  Trying to modify the trackartist field of the record after it has
    //  been inserted does not work either, since the new value of trackartist (3)
    //  still does not correspond to any row in the artist table.
    try{
        storage.update_all(set(assign(&Track::trackArtist, 3)), where(is_equal(&Track::trackName, "Mr. Bojangles")));
        assert(0);
    }catch(std::system_error e) {
        cout << e.what() << endl;
    }
    
    //  Insert the required row into the artist table. It is then possible to
    //  update the inserted row to set trackartist to 3 (since a corresponding
    //  row in the artist table now exists).
    storage.replace(Artist{ 3, "Sammy Davis Jr." });
    storage.update_all(set(assign(&Track::trackArtist, 3)), where(is_equal(&Track::trackName, "Mr. Bojangles")));
    
    //  Now that "Sammy Davis Jr." (artistid = 3) has been added to the database,
    //  it is possible to INSERT new tracks using this artist without violating
    //  the foreign key constraint:
    storage.replace(Track{ 15, "Boogie Woogie", std::make_shared<int>(3) });
    
    try{
        //  Attempting to delete the artist record for "Frank Sinatra" fails, since
        //  the track table contains a row that refer to it.
        storage.remove_all<Artist>(where(is_equal(&Artist::artistName, "Frank Sinatra")));
        assert(0);
    }catch(std::system_error e) {
        cout << e.what() << endl;
    }
    
    //  Delete all the records from the track table that refer to the artist
    //  "Frank Sinatra". Only then is it possible to delete the artist.
    storage.remove_all<Track>(where(is_equal(&Track::trackName, "My Way")));
    storage.remove_all<Artist>(where(is_equal(&Artist::artistName, "Frank Sinatra")));
    
    try{
        //  Try to update the artistid of a row in the artist table while there
        //  exists records in the track table that refer to it.
        storage.update_all(set(assign(&Artist::artistId, 4)), where(is_equal(&Artist::artistName, "Dean Martin")));
        assert(0);
    }catch(std::system_error e) {
        cout << e.what() << endl;
    }
    
    //  Once all the records that refer to a row in the artist table have
    //  been deleted, it is possible to modify the artistid of the row.
    storage.remove_all<Track>(where(in(&Track::trackName, {"That''s Amore", "Christmas Blues"})));
    storage.update_all(set(c(&Artist::artistId) = 4),
                       where(c(&Artist::artistName) == "Dean Martin"));
    
    return 0;
}
