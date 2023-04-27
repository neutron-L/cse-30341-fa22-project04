/* utils.h: Utility Macros */

#ifndef UTILS_H
#define UTILS_H

/* Macros */

#define min(a, b)   \
    (((a) < (b)) ? (a) : (b))

#define max(a, b)   \
    (((a) > (b)) ? (a) : (b))

#define UPPER_ROUND(x, size) (((x) + (size - 1)) / (size))

#endif

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
