#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <cstdlib> //std::system
#include <jsoncons/json.hpp>

using namespace jsoncons;

typedef boost::interprocess::allocator<int,
        boost::interprocess::managed_shared_memory::segment_manager> shmem_allocator;

struct boost_sorted_policy : public sorted_policy
{
    template <class T, class Allocator>
    using sequence_container_type = boost::interprocess::vector<T,Allocator>;

    template <class CharT, class CharTraits, class Allocator>
    using key_storage = boost::interprocess::basic_string<CharT, CharTraits, Allocator>;

    template <class CharT, class CharTraits, class Allocator>
    using string_storage = boost::interprocess::basic_string<CharT, CharTraits, Allocator>;
};

typedef basic_json<char,boost_sorted_policy,shmem_allocator> shm_json;

int main(int argc, char *argv[])
{
   typedef std::pair<double, int> MyType;

   if(argc == 1){  //Parent process
      //Remove shared memory on construction and destruction
      struct shm_remove
      {
         shm_remove() { boost::interprocess::shared_memory_object::remove("MySharedMemory"); }
         ~shm_remove(){ boost::interprocess::shared_memory_object::remove("MySharedMemory"); }
      } remover;

      //Construct managed shared memory
      boost::interprocess::managed_shared_memory segment(boost::interprocess::create_only, 
                                                         "MySharedMemory", 65536);

      //Initialize shared memory STL-compatible allocator
      const shmem_allocator allocator(segment.get_segment_manager());

      // Create json value with all dynamic allocations in shared memory

      shm_json* j = segment.construct<shm_json>("my json")(shm_json::array(allocator));
      j->push_back(10);

      shm_json o(allocator);
      //o.try_emplace("category", "reference",allocator);
      //o.try_emplace("author", "Nigel Rees",allocator);
      //o.try_emplace("title", "Sayings of the Century",allocator);
      //o.try_emplace("price", 8.95, allocator);
      o.insert_or_assign("category", "reference");
      o.insert_or_assign("author", "Nigel Rees");
      o.insert_or_assign("title", "Sayings of the Century");
      o.insert_or_assign("price", 8.95);

      j->push_back(o);

      shm_json a = shm_json::array(2,shm_json::object(allocator),allocator);
      a[0]["first"] = 1;

      j->push_back(a);

      std::pair<shm_json*, boost::interprocess::managed_shared_memory::size_type> res;
      res = segment.find<shm_json>("my json");

      std::cout << "Parent:" << std::endl;
      std::cout << pretty_print(*(res.first)) << std::endl;

      //Launch child process
      std::string s(argv[0]); s += " child ";
      if(0 != std::system(s.c_str()))
         return 1;


      //Check child has destroyed all objects
      if(segment.find<MyType>("my json").first)
         return 1;
   }
   else{
      //Open managed shared memory
      boost::interprocess::managed_shared_memory segment(boost::interprocess::open_only, 
                                                         "MySharedMemory");

      std::pair<shm_json*, boost::interprocess::managed_shared_memory::size_type> res;
      res = segment.find<shm_json>("my json");

      if (res.first != nullptr)
      {
          std::cout << "Child:" << std::endl;
          std::cout << pretty_print(*(res.first)) << std::endl;
      }
      else
      {
          std::cout << "Result is null" << std::endl;
      }

      //We're done, delete all the objects
      segment.destroy<shm_json>("my json");
   }
   return 0;
}

