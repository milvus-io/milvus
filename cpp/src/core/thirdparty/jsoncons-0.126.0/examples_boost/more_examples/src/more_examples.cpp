// Copyright 2018 Daniel Parker
// Distributed under Boost license

#include <iostream>

void extensibility_examples();
void pool_allocator_examples();

int main()
{
    try
    {
        extensibility_examples();
        pool_allocator_examples();
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }

    return 0;
}
