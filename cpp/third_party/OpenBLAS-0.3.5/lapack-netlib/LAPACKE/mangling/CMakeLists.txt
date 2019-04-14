cmake_minimum_required(VERSION 2.8.7)
project(MANGLING C Fortran)

add_executable(xintface Fintface.f Cintface.c)
