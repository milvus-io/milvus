#include <GL/gl.h>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main(void) {

    GLfloat f = 0.1f;
    LOG(INFO) << f;

    return 0;
}
