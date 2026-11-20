#ifndef CINDER_AOF_H
#define CINDER_AOF_H

/* Append-only log. Every mutating command is recorded as a RESP array so
   the dataset can be rebuilt by replaying the file through the same
   command dispatcher used for live clients. */

/* Open (creating if needed) the AOF file. Returns 0 on success. */
int aof_open(const char *path);
/* Append one command, then fdatasync so it survives a crash. */
void aof_append(char **argv, int argc);
void aof_close(void);

#endif
