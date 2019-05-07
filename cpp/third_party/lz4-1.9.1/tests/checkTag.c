/*
    checkTag.c - Version validation tool for LZ4
    Copyright (C) Yann Collet 2018 - present

    GPL v2 License

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

    You can contact the author at :
    - LZ4 homepage : http://www.lz4.org
    - LZ4 source repo : https://github.com/lz4/lz4
*/

/* checkTag command :
 * $ ./checkTag tag
 * checkTag validates tags of following format : v[0-9].[0-9].[0-9]{any}
 * The tag is then compared to LZ4 version number.
 * They are compatible if first 3 digits are identical.
 * Anything beyond that is free, and doesn't impact validation.
 * Example : tag v1.8.1.2 is compatible with version 1.8.1
 * When tag and version are not compatible, program exits with error code 1.
 * When they are compatible, it exists with a code 0.
 * checkTag is intended to be used in automated testing environment.
 */

#include <stdio.h>   /* printf */
#include <string.h>  /* strlen, strncmp */
#include "lz4.h"     /* LZ4_VERSION_STRING */


/*  validate() :
 * @return 1 if tag is compatible, 0 if not.
 */
static int validate(const char* const tag)
{
    size_t const tagLength = strlen(tag);
    size_t const verLength = strlen(LZ4_VERSION_STRING);

    if (tagLength < 2) return 0;
    if (tag[0] != 'v') return 0;
    if (tagLength <= verLength) return 0;

    if (strncmp(LZ4_VERSION_STRING, tag+1, verLength)) return 0;

    return 1;
}

int main(int argc, const char** argv)
{
    const char* const exeName = argv[0];
    const char* const tag = argv[1];
    if (argc!=2) {
        printf("incorrect usage : %s tag \n", exeName);
        return 2;
    }

    printf("Version : %s \n", LZ4_VERSION_STRING);
    printf("Tag     : %s \n", tag);

    if (validate(tag)) {
        printf("OK : tag is compatible with lz4 version \n");
        return 0;
    }

    printf("!! error : tag and versions are not compatible !! \n");
    return 1;
}
