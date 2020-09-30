#ifndef UTILIS_H
#define UTILIS_H

#include <iostream>

#include <set>
#include <numeric>
#include <algorithm>
#include<vector>
size_t GetFileSize(const char *filename);
void SplitFile(const char *filename, size_t file_size, char *buffer0, char *buffer1, char *buffer2, char *buffer3);
void SplitMemory(char *buffer, size_t size, char *buffer0, char *buffer1, char *buffer2, char *buffer3);

template<typename T>
void ShowVecValues(std::vector<T> &vec, int col)
{
    auto iter = vec.begin(); 
    int index = 0;
    while(iter != vec.end()){     
        std::cout << *iter << " ";
        iter++;
        index++;

        if(index == col){
            std::cout << "\n";
            index = 0;
        }
    }
    std::cout << std::endl;
}

double Elapsed();
#endif
