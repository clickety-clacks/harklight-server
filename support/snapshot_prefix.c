#include <stdint.h>
#include <stdio.h>
#include <sqlite3.h>

enum { prefix_bytes = 256 };

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    sqlite3_blob *blob = NULL;
    unsigned char output[4 + prefix_bytes];
    int result = 1;

    if (argc != 2)
        return result;

    if (sqlite3_open_v2(argv[1], &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK)
        goto done;

    if (sqlite3_blob_open(db, "main", "relay_state", "payload", 1, 0, &blob) != SQLITE_OK)
        goto done;

    uint32_t size = (uint32_t) sqlite3_blob_bytes(blob);
    int read_size = size > prefix_bytes ? prefix_bytes : (int) size;

    if (read_size == 0 || sqlite3_blob_read(blob, output + 4, read_size, 0) != SQLITE_OK)
        goto done;

    output[0] = (unsigned char) (size >> 24);
    output[1] = (unsigned char) (size >> 16);
    output[2] = (unsigned char) (size >> 8);
    output[3] = (unsigned char) size;

    size_t length = 4 + (size_t) read_size;
    if (fwrite(output, 1, length, stdout) == length && fflush(stdout) == 0)
        result = 0;

done:
    if (blob != NULL)
        sqlite3_blob_close(blob);
    if (db != NULL)
        sqlite3_close(db);
    return result;
}
