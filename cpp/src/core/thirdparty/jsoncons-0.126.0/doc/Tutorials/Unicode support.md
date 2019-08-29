### Narrow character support for UTF8 encoding

In the Linux and web worlds, `UTF-8` is the dominant character encoding.

Note that (at least in MSVS) you cannot open a Windows file with a Unicode name using the standard 
```c++
std::fstream fs(const char* filename)
```
Instead you need to use the non standard Microsoft extension
```c++
std::fstream fs(const wchar_t* filename)
```

#### Unicode escaping
```c++
string inputStr("[\"\\u0040\\u0040\\u0000\\u0011\"]");
std::cout << "Input:    " << inputStr << std::endl;

json arr = json::parse(inputStr);
std::string str = arr[0].as<std::string>();
std::cout << "Hex dump: [";
for (size_t i = 0; i < str.size(); ++i)
{
    unsigned int val = static_cast<unsigned int>(str[i]);
    if (i != 0)
    {
        std::cout << " ";
    }
    std::cout << "0x" << std::setfill('0') << std::setw(2) << std::hex << val;
}
std::cout << "]" << std::endl;

std::ostringstream os;
os << arr;
std::cout << "Output:   " << os.str() << std::endl;
```

Output:

```
Input:    ["\u0040\u0040\u0000\u0011"]
Hex dump: [0x40 0x40 0x00 0x11]
Output:   ["@@\u0000\u0011"]
```
Note that just the two control characters are escaped on output.

#### Reading escaped unicode into utf8 encodings and writing back escaped unicode
```c++
string inputStr("[\"\\u007F\\u07FF\\u0800\"]");
std::cout << "Input:    " << inputStr << std::endl;

json arr = json::parse(inputStr);
std::string s = arr[0].as<string>();
std::cout << "Hex dump: [";
for (size_t i = 0; i < s.size(); ++i)
{
    if (i != 0)
        std::cout << " ";
    unsigned int u(s[i] >= 0 ? s[i] : 256 + s[i] );
    std::cout << "0x"  << std::hex<< std::setfill('0') << std::setw(2) << u;
}
std::cout << "]" << std::endl;

std::ostringstream os;
json_options options;
format.escape_all_non_ascii(true);
os << print(arr,options);
std::string outputStr = os.str();
std::cout << "Output:   " << os.str() << std::endl;

json arr2 = json::parse(outputStr);
std::string s2 = arr2[0].as<string>();
std::cout << "Hex dump: [";
for (size_t i = 0; i < s2.size(); ++i)
{
    if (i != 0)
        std::cout << " ";
    unsigned int u(s2[i] >= 0 ? s2[i] : 256 + s2[i] );
    std::cout << "0x"  << std::hex<< std::setfill('0') << std::setw(2) << u;
}
std::cout << "]" << std::endl;
```

Output:

```
Input:    ["\u007F\u07FF\u0800"]
Hex dump: [0x7f 0xdf 0xbf 0xe0 0xa0 0x80]
Output:   ["\u007F\u07FF\u0800"]
Hex dump: [0x7f 0xdf 0xbf 0xe0 0xa0 0x80]
```
Since the escaped unicode consists of a control character (0x7f) and non-ascii, we get back the same text as what we started with.

#### Reading escaped unicode into utf8 encodings and writing back escaped unicode (with continuations)
```c++
string input = "[\"\\u8A73\\u7D30\\u95B2\\u89A7\\uD800\\uDC01\\u4E00\"]";
json value = json::parse(input);
json_options options;
format.escape_all_non_ascii(true);
string output;
value.dump(output,options);

std::cout << "Input:" << std::endl;
std::cout << input << std::endl;
std::cout << std::endl;
std::cout << "Output:" << std::endl;
std::cout << output << std::endl;
```
Since all of the escaped unicode is non-ascii, we get back the same text as what we started with.
```
Input:
["\u8A73\u7D30\u95B2\u89A7\uD800\uDC01\u4E00"]

Output:
["\u8A73\u7D30\u95B2\u89A7\uD800\uDC01\u4E00"]
```
### Wide character support for UTF16 and UTF32 encodings

jsoncons supports wide character strings and streams with `wjson` and `wjson_reader`. It assumes `UTF16` encoding if `wchar_t` has size 2 (Windows) and `UTF32` encoding if `wchar_t` has size 4.

It is necessary to deal with UTF-16 character encoding in the Windows world because of lack of UTF-8 support in the Windows system API. 

Even if you choose to use wide character streams and strings to interact with the Windows API, you can still read and write to files in the more widely supported, endiness independent, UTF-8 format. To handle that you need to imbue your streams with the facet `std::codecvt_utf8_utf16`, which encapsulates the conversion between `UTF-8` and `UTF-16`.

Note that (at least in MSVS) you cannot open a Windows file with a Unicode name using the standard 

    std::wfstream fs(const char* filename)

Instead you need to use the non standard Microsoft extension

    std::wfstream fs(const wchar_t* filename)

#### Constructing a wjson value
```c++
using jsoncons::wjson;

wjson root;
root[L"field1"] = L"test";
root[L"field2"] = 3.9;
root[L"field3"] = true;
std::wcout << root << L"\n";
```
Output:
```
{"field1":"test","field2":3.9,"field3":true}
```
#### Escaped unicode
```c++
wstring input = L"[\"\\u007F\\u07FF\\u0800\"]";
std::wistringstream is(input);

wjson val = wjson::parse(is);

wstring s = val[0].as<wstring>();
std::cout << "length=" << s.length() << std::endl;
std::cout << "Hex dump: [";
for (size_t i = 0; i < s.size(); ++i)
{
    if (i != 0)
        std::cout << " ";
    uint32_t u(s[i] >= 0 ? s[i] : 256 + s[i] );
    std::cout << "0x"  << std::hex<< std::setfill('0') << std::setw(2) << u;
}
std::cout << "]" << std::endl;

std::wofstream os("output/xxx.txt");
os.imbue(std::locale(os.getloc(), new std::codecvt_utf8_utf16<wchar_t>));

wjson_options options;
format.escape_all_non_ascii(true);

os << pretty_print(val,options) << L"\n";
```
Output:
```
length=3
Hex dump: [0x7f 0x7ff 0x800]
```
and the file `xxx.txt` contains
```    
["\u007F\u07FF\u0800"]    
```
