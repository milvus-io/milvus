### jsoncons::json::make_array

```c++
template <class T>
static json make_array(size_ n, const T& val)

template <class T>
static json make_array(size_ n, const T& val, 
                       const allocator_type& alloc = allocator_type())

template <size_t N>
static json make_array(size_t size1 ... size_t sizeN)

template <size_t N,typename T>
static json make_array(size_t size1 ... size_t sizeN, const T& val)

template <size_t N,typename T>
static json make_array(size_t size1 ... size_t sizeN, const T& val, 
                       const allocator_type& alloc)
```
Makes a multidimensional array with the number of dimensions specified as a template parameter. The size of each dimension is passed as a parameter, and optionally an inital value. If no initial value, the default is an empty json object. The elements may be accessed using familiar C++ native array syntax.

### Examples

#### Make an array of size 10 initialized with zeros
```c++
json a = json::make_array<1>(10,0);
a[1] = 1;
a[2] = 2;
std::cout << pretty_print(a) << std::endl;
```
Output:
```json
[0,1,2,0,0,0,0,0,0,0]
```
#### Make a two dimensional array of size 3x4 initialized with zeros
```c++
json a = json::make_array<2>(3,4,0);
a[0][0] = "Tenor";
a[0][1] = "ATM vol";
a[0][2] = "25-d-MS";
a[0][3] = "25-d-RR";
a[1][0] = "1Y";
a[1][1] = 0.20;
a[1][2] = 0.009;
a[1][3] = -0.006;
a[2][0] = "2Y";
a[2][1] = 0.18;
a[2][2] = 0.009;
a[2][3] = -0.005;

std::cout << pretty_print(a) << std::endl;
```
Output:
```json
[
    ["Tenor","ATM vol","25-d-MS","25-d-RR"],
    ["1Y",0.2,0.009,-0.006],
    ["2Y",0.18,0.009,-0.005]
]
```
#### Make a three dimensional array of size 4x3x2 initialized with zeros
```c++
json a = json::make_array<3>(4,3,2,0);
a[0][2][0] = 2;
a[0][2][1] = 3;
std::cout << pretty_print(a) << std::endl;
```
Output:
```json
[
    [
        [0,0],
        [0,0],
        [2,3]
    ],
    [
        [0,0],
        [0,0],
        [0,0]
    ],
    [
        [0,0],
        [0,0],
        [0,0]
    ],
    [
        [0,0],
        [0,0],
        [0,0]
    ]
]
```

