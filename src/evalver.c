#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <assert.h>
#include "sqlite3.h"

#define LOOP_COUNTER 1000000
#define DB_NAME "eval.db"
#define SQL_CREATE "CREATE TABLE evaltab (no INT, data TEXT, time REAL);"
#define IsSucceed(X, rc) \
  if( (X) != (rc) ){ \
    if(db == NULL){ \
      fprintf(stderr, "Can not open database : %d\n", rc); \
    }else if(zErrMsg==NULL){ \
      fprintf(stderr, "%s( %4d ) : %s\n", __FILE__, __LINE__, sqlite3_errmsg(db)); \
    }else{ \
      fprintf(stderr, "%s( %4d ) : %s\n", __FILE__, __LINE__, zErrMsg); \
      sqlite3_free(zErrMsg); \
    } \
    sqlite3_finalize(pStmt); \
    sqlite3_close(db); \
    remove(DB_NAME); \
    return(1); \
  }
#define ExecSQL(CMD, SQL) \
  clock_gettime(CLOCK_REALTIME, &tStart); \
  IsSucceed(sqlite3_exec(db, SQL, NULL, NULL, &zErrMsg), SQLITE_OK); \
  clock_gettime(CLOCK_REALTIME, &tEnd); \
  print_result(CMD, &tStart, &tEnd);

void print_result(char *cmd, struct timespec *ptStart, struct timespec *ptEnd){
  if(ptEnd->tv_nsec < ptStart->tv_nsec){
    printf("%6s : %10ld.%09ld (sec)\n",cmd, ptEnd->tv_sec - ptStart->tv_sec - 1
          ,ptEnd->tv_nsec + 1000000000 - ptStart->tv_nsec);
  }else{
    printf("%6s : %10ld.%09ld (sec)\n",cmd, ptEnd->tv_sec - ptStart->tv_sec
          ,ptEnd->tv_nsec - ptStart->tv_nsec);
  }
}

int main(int argc, char **argv){
  sqlite3 *db = NULL;
  int i;
  char *zErrMsg = 0;
  char *zSQL = "INSERT INTO evaltab VALUES (?, ?, ?);";
  sqlite3_stmt *pStmt = 0;
  struct timespec tStart, tEnd;
  const char *data = "01234567890123456789012345678901234567890123456789"
                     "01234567890123456789012345678901234567890123456789";

  printf("Version Evaluation, SQLite3 version = %s\n", sqlite3_version);

  /* Prepare for performance measurement */
  IsSucceed(sqlite3_open(DB_NAME, &db), SQLITE_OK);
  IsSucceed(sqlite3_exec(db, SQL_CREATE, NULL, NULL, &zErrMsg), SQLITE_OK);
  IsSucceed(sqlite3_prepare_v2(db, zSQL, -1, &pStmt, NULL), SQLITE_OK);

  /* Performance measurement for INSERT statement */
  clock_gettime(CLOCK_REALTIME, &tStart);
  IsSucceed(sqlite3_exec(db, "BEGIN;", NULL, NULL, &zErrMsg), SQLITE_OK);
  for(i=0; i<LOOP_COUNTER; i++){
    IsSucceed(sqlite3_bind_int(pStmt, 1, i), SQLITE_OK);
    IsSucceed(sqlite3_bind_text(pStmt, 2, data, -1, SQLITE_TRANSIENT), SQLITE_OK);
    IsSucceed(sqlite3_bind_double(pStmt, 3, (double)time(NULL)), SQLITE_OK);
    IsSucceed(sqlite3_step(pStmt), SQLITE_DONE);
    IsSucceed(sqlite3_reset(pStmt), SQLITE_OK);
  }
  IsSucceed(sqlite3_exec(db, "COMMIT;", NULL, NULL, &zErrMsg), SQLITE_OK);
  clock_gettime(CLOCK_REALTIME, &tEnd);
  print_result("INSERT", &tStart, &tEnd);
  sqlite3_finalize(pStmt);

  /* Performance measurement for SELECT count() statement */
  ExecSQL("SELECT", "SELECT COUNT() FROM evaltab;");

  /* Performance measurement for UPDATE statement */
  ExecSQL("UPDATE","UPDATE evaltab SET no = no + 1;");

  /* Performance measurement for DELETE statement */
  ExecSQL("DELETE", "DELETE FROM evaltab;");

  /* Performance measurement for VACUUM */
  ExecSQL("VACUUM", "VACUUM;");

  sqlite3_close(db);
  remove(DB_NAME);
  return(0);
}
