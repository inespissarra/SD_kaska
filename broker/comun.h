/*
 * Incluya en este fichero todas las definiciones que pueden
 * necesitar compartir el broker y la biblioteca, si es que las hubiera.
 */

#ifndef _COMUN_H
#define _COMUN_H        1

#include <sys/uio.h>
#include <string.h>
#include <arpa/inet.h>

#define CREATE_TOPIC 0
#define NTOPICS 1
#define SEND_MSG 2
#define MSG_LENGTH 3
#define END_OFFSET 4
#define POLL 5
#define COMMIT 6
#define COMMITED 7

#endif // _COMUN_H
