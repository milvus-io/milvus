#include <assert.h>
#include <fstream>

#include "imageloader.h"
#include "../easylogging++.h"

using namespace std;

Image::Image(char* ps, int w, int h) : pixels(ps), width(w), height(h) {
    VLOG(2) << "Constructing Image object [" << w << " x " << h << "]";
}

Image::~Image() {
    VLOG(2) << "Destroying image object...";
    delete[] pixels;
}

namespace {
    //Converts a four-character array to an integer, using little-endian form
    int toInt(const char* bytes) {
        VLOG(2) << "Converting bytes to int";
        return (int)(((unsigned char)bytes[3] << 24) |
                     ((unsigned char)bytes[2] << 16) |
                     ((unsigned char)bytes[1] << 8) |
                     (unsigned char)bytes[0]);
    }
    
    //Converts a two-character array to a short, using little-endian form
    short toShort(const char* bytes) {
        VLOG(2) << "Converting bytes to short";
        return (short)(((unsigned char)bytes[1] << 8) |
                       (unsigned char)bytes[0]);
    }
    
    //Reads the next four bytes as an integer, using little-endian form
    int readInt(ifstream &input) {
        VLOG(2) << "Reading input as int...";
        char buffer[4];
        input.read(buffer, 4);
        return toInt(buffer);
    }
    
    //Reads the next two bytes as a short, using little-endian form
    short readShort(ifstream &input) {
        VLOG(2) << "Reading input as short...";
        char buffer[2];
        input.read(buffer, 2);
        return toShort(buffer);
    }
    
    //Just like auto_ptr, but for arrays
    template<class T>
    class auto_array {
        private:
            T* array;
            mutable bool isReleased;
        public:
            explicit auto_array(T* array_ = NULL) :
                array(array_), isReleased(false) {
                VLOG(2) << "Creating auto_array";
            }
            
            auto_array(const auto_array<T> &aarray) {
                VLOG(2) << "Copying auto_array";
                array = aarray.array;
                isReleased = aarray.isReleased;
                aarray.isReleased = true;
            }
            
            ~auto_array() {
                VLOG(2) << "Destroying auto_array";
                if (!isReleased && array != NULL) {
                    delete[] array;
                }
            }
            
            T* get() const {
                return array;
            }
            
            T &operator*() const {
                return *array;
            }
            
            void operator=(const auto_array<T> &aarray) {
                if (!isReleased && array != NULL) {
                    delete[] array;
                }
                array = aarray.array;
                isReleased = aarray.isReleased;
                aarray.isReleased = true;
            }
            
            T* operator->() const {
                return array;
            }
            
            T* release() {
                isReleased = true;
                return array;
            }
            
            void reset(T* array_ = NULL) {
                if (!isReleased && array != NULL) {
                    delete[] array;
                }
                array = array_;
            }
            
            T* operator+(int i) {
                return array + i;
            }
            
            T &operator[](int i) {
                return array[i];
            }
    };
}

Image* loadBMP(const char* filename) {
    VLOG(1) << "Loading bitmap [" << filename << "]";
    ifstream input;
    input.open(filename, ifstream::binary);
    CHECK(!input.fail()) << "Could not find file";
    char buffer[2];
    input.read(buffer, 2);
    CHECK(buffer[0] == 'B' && buffer[1] == 'M') << "Not a bitmap file";
    input.ignore(8);
    int dataOffset = readInt(input);
    
    //Read the header
    int headerSize = readInt(input);
    int width;
    int height;
    switch(headerSize) {
        case 40:
            //V3
            width = readInt(input);
            height = readInt(input);
            input.ignore(2);
            CHECK_EQ(readShort(input), 24) << "Image is not 24 bits per pixel";
            CHECK_EQ(readShort(input), 0) << "Image is compressed";
            break;
        case 12:
            //OS/2 V1
            width = readShort(input);
            height = readShort(input);
            input.ignore(2);
            CHECK_EQ(readShort(input), 24) << "Image is not 24 bits per pixel";
            break;
        case 64:
            //OS/2 V2
            LOG(FATAL) << "Can't load OS/2 V2 bitmaps";
            break;
        case 108:
            //Windows V4
            LOG(FATAL) << "Can't load Windows V4 bitmaps";
            break;
        case 124:
            //Windows V5
            LOG(FATAL) << "Can't load Windows V5 bitmaps";
            break;
        default:
            LOG(FATAL) << "Unknown bitmap format";
    }
    //Read the data
    VLOG(1) << "Reading bitmap data";
    int bytesPerRow = ((width * 3 + 3) / 4) * 4 - (width * 3 % 4);
    int size = bytesPerRow * height;
    VLOG(1) << "Size of bitmap is [" << bytesPerRow << " x " << height << " = " << size;
    auto_array<char> pixels(new char[size]);
    input.seekg(dataOffset, ios_base::beg);
    input.read(pixels.get(), size);
    
    //Get the data into the right format
    auto_array<char> pixels2(new char[width * height * 3]);
    for(int y = 0; y < height; y++) {
        for(int x = 0; x < width; x++) {
            for(int c = 0; c < 3; c++) {
                pixels2[3 * (width * y + x) + c] =
                    pixels[bytesPerRow * y + 3 * x + (2 - c)];
            }
        }
    }
    
    input.close();
    return new Image(pixels2.release(), width, height);
}
